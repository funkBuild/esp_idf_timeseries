#include "timeseries_points_iterator.h"
#include "esp_err.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "gorilla/gorilla_stream_decoder.h"
#include "timeseries_compression.h"
#include "timeseries_data.h"
#include "timeseries_internal.h"
#include "alp/alp_decoder.h"
#include "alp/alp_int_codec.h"

#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "PointsIterator";

/* --------------------------------------------------------------------------
 * Gorilla decoder fill callback
 * --------------------------------------------------------------------------*/
static bool decoder_fill_callback(void *context, uint8_t *buffer,
                                  size_t max_len, size_t *filled) {
  DecoderContext *ctx = (DecoderContext *)context;
  CompressedBuffer *cb = ctx->cb;

  if (ctx->offset >= cb->size) {
    *filled = 0;
    ESP_LOGE(TAG, "Decoder fill callback: no more data");
    return false;
  }

  size_t remaining = cb->size - ctx->offset;
  size_t to_copy = (remaining < max_len) ? remaining : max_len;
  memcpy(buffer, cb->data + ctx->offset, to_copy);
  ctx->offset += to_copy;
  *filled = to_copy;

  return true;
}

/* --------------------------------------------------------------------------
 * timeseries_points_iterator_init(...)
 * --------------------------------------------------------------------------*/
bool timeseries_points_iterator_init(
    timeseries_db_t *db, uint32_t record_data_offset, uint32_t record_length,
    uint16_t record_count, timeseries_field_type_e series_type,
    uint8_t data_flags,
    timeseries_points_iterator_t *iter) {
  if (!db || !iter) {
    return false;
  }
  memset(iter, 0, sizeof(*iter));
  iter->db = db;
  iter->valid = true;
  iter->ts_count = record_count;
  iter->series_type = series_type;
  iter->is_column_format = true;
  // Derive compressed from flags: bit1=0 means compressed, bit1=1 means raw
  bool compressed = (data_flags & TSDB_FIELDDATA_FLAG_COMPRESSED) == 0;
  iter->is_compressed = compressed;
  iter->current_idx = 0;

  // Boundary check
  if (record_data_offset + record_length > db->partition->size) {
    ESP_LOGE(TAG, "Record end out of bounds: 0x%08X + %u > 0x%08X",
             (unsigned)record_data_offset, (unsigned)record_length,
             (unsigned)db->partition->size);
    iter->valid = false;
    return false;
  }

  // Profiling for Phase 3 investigation
  uint64_t prof_start, prof_end;
  uint64_t prof_hdr_read = 0;
  uint64_t prof_ts_malloc = 0;
  uint64_t prof_ts_read = 0;
  uint64_t prof_val_malloc = 0;
  uint64_t prof_val_read = 0;
  uint64_t prof_decoder_init = 0;

  // 1) Read col_data_header
  if (record_length < sizeof(timeseries_col_data_header_t)) {
    ESP_LOGE(TAG, "record_length too small for col_header: %u < %zu",
             (unsigned)record_length, sizeof(timeseries_col_data_header_t));
    iter->valid = false;
    return false;
  }

  // 1) Read col_data_header — for compressed path, read entire record in one go
  timeseries_col_data_header_t col_hdr;

  // 2) If compressed => single flash read for header + ts + val blocks
  if (compressed) {
    prof_start = esp_timer_get_time();
    uint8_t *record_buf = (uint8_t *)malloc(record_length);
    prof_end = esp_timer_get_time();
    prof_ts_malloc = prof_end - prof_start;

    if (!record_buf) {
      ESP_LOGE(TAG, "OOM for record buffer of size %u",
               (unsigned)record_length);
      iter->valid = false;
      return false;
    }

    prof_start = esp_timer_get_time();
    esp_err_t err = esp_partition_read(db->partition, record_data_offset,
                                       record_buf, record_length);
    prof_end = esp_timer_get_time();
    prof_hdr_read = prof_end - prof_start;

    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading record at 0x%08X (err=0x%x)",
               (unsigned)record_data_offset, err);
      free(record_buf);
      iter->valid = false;
      return false;
    }

    memcpy(&col_hdr, record_buf, sizeof(col_hdr));

    // Basic boundary check
    uint32_t val_end = (uint32_t)sizeof(col_hdr) + col_hdr.ts_len + col_hdr.val_len;
    if (val_end > record_length ||
        record_data_offset + val_end > db->partition->size) {
      ESP_LOGE(TAG, "Column data extends out of partition");
      free(record_buf);
      iter->valid = false;
      return false;
    }

    // Pointers into the single buffer
    uint8_t *ts_data = record_buf + sizeof(col_hdr);
    uint8_t *val_data = record_buf + sizeof(col_hdr) + col_hdr.ts_len;

    iter->ts_comp_len = col_hdr.ts_len;
    iter->val_comp_len = col_hdr.val_len;

    // Check if this record uses ALP encoding (bit2=0 means ALP)
    bool use_alp = (data_flags & TSDB_FIELDDATA_FLAG_ENCODING_ALP) == 0;

    if (use_alp) {
      /* --- ALP decode path: decode directly from record_buf, then free it --- */
      iter->alp_ts = (int64_t *)malloc(record_count * sizeof(int64_t));
      if (!iter->alp_ts) {
        ESP_LOGE(TAG, "OOM for ALP timestamp array of size %u", record_count);
        free(record_buf);
        iter->valid = false;
        return false;
      }
      if (!alp_decode_int(ts_data, col_hdr.ts_len,
                          iter->alp_ts, record_count)) {
        ESP_LOGE(TAG, "ALP timestamp decode failed");
        free(iter->alp_ts);
        free(record_buf);
        iter->alp_ts = NULL;
        iter->valid = false;
        return false;
      }

      if (series_type == TIMESERIES_FIELD_TYPE_FLOAT) {
        iter->alp_float = (double *)malloc(record_count * sizeof(double));
        if (!iter->alp_float) {
          ESP_LOGE(TAG, "OOM for ALP float array of size %u", record_count);
          free(iter->alp_ts);
          free(record_buf);
          iter->alp_ts = NULL;
          iter->valid = false;
          return false;
        }
        if (!alp_decode(val_data, col_hdr.val_len,
                        iter->alp_float, record_count)) {
          ESP_LOGE(TAG, "ALP float value decode failed");
          free(iter->alp_ts);
          free(iter->alp_float);
          free(record_buf);
          iter->alp_ts = NULL;
          iter->alp_float = NULL;
          iter->valid = false;
          return false;
        }
      } else { /* INT */
        iter->alp_int = (int64_t *)malloc(record_count * sizeof(int64_t));
        if (!iter->alp_int) {
          ESP_LOGE(TAG, "OOM for ALP int array of size %u", record_count);
          free(iter->alp_ts);
          free(record_buf);
          iter->alp_ts = NULL;
          iter->valid = false;
          return false;
        }
        if (!alp_decode_int(val_data, col_hdr.val_len,
                            iter->alp_int, record_count)) {
          ESP_LOGE(TAG, "ALP int value decode failed");
          free(iter->alp_ts);
          free(iter->alp_int);
          free(record_buf);
          iter->alp_ts = NULL;
          iter->alp_int = NULL;
          iter->valid = false;
          return false;
        }
      }

      // Single record buffer consumed; free it now
      free(record_buf);

      ESP_LOGD(TAG, "Iterator init: ALP, record_count=%u", record_count);
      return iter->valid;
    }

    // Gorilla path: keep record_buf alive; ts_comp_buf owns the allocation
    iter->ts_comp_buf = record_buf;   // base allocation (includes col_hdr prefix)
    iter->val_comp_buf = NULL;        // val data is inside record_buf

    // Now initialize Gorilla decoders (timestamps + values) in zero-copy mode
    prof_start = esp_timer_get_time();

    if (!gorilla_decoder_init_direct(&iter->ts_decoder, GORILLA_STREAM_INT,
                                     ts_data, col_hdr.ts_len)) {
      ESP_LOGE(TAG, "Failed to init Gorilla ts_decoder.");
      free(iter->ts_comp_buf);
      iter->ts_comp_buf = NULL;
      iter->valid = false;
      return false;
    }

    gorilla_stream_type_t val_mode;
    switch (series_type) {
    case TIMESERIES_FIELD_TYPE_FLOAT:
      val_mode = GORILLA_STREAM_FLOAT;
      break;
    case TIMESERIES_FIELD_TYPE_BOOL:
      val_mode = GORILLA_STREAM_BOOL;
      break;
    case TIMESERIES_FIELD_TYPE_STRING:
      val_mode = GORILLA_STREAM_STRING;
      break;
    default:
      val_mode = GORILLA_STREAM_INT;
      break;
    }

    // String type still needs fill callback (uses zlib internally)
    bool val_init_ok;
    if (val_mode == GORILLA_STREAM_STRING) {
      iter->val_cb_storage.data = val_data;
      iter->val_cb_storage.size = col_hdr.val_len;
      iter->val_cb_storage.capacity = col_hdr.val_len;
      iter->val_decoder_context.cb = &iter->val_cb_storage;
      iter->val_decoder_context.offset = 0;
      val_init_ok = gorilla_decoder_init(&iter->val_decoder, val_mode,
                                         decoder_fill_callback,
                                         &iter->val_decoder_context);
    } else {
      val_init_ok = gorilla_decoder_init_direct(&iter->val_decoder, val_mode,
                                                val_data, col_hdr.val_len);
    }

    if (!val_init_ok) {
      ESP_LOGE(TAG, "Failed to init Gorilla val_decoder.");
      gorilla_decoder_deinit(&iter->ts_decoder);
      free(iter->ts_comp_buf);  // frees the single record_buf allocation
      iter->ts_comp_buf = NULL;
      iter->valid = false;
      return false;
    }

    // Batch-decode all Gorilla timestamps into an array so we can
    // random-access them (enables time-range pruning via binary search).
    iter->gorilla_batch_ts = (int64_t *)malloc(record_count * sizeof(int64_t));
    if (!iter->gorilla_batch_ts) {
      ESP_LOGE(TAG, "OOM for gorilla_batch_ts of size %u", record_count);
      gorilla_decoder_deinit(&iter->ts_decoder);
      gorilla_decoder_deinit(&iter->val_decoder);
      free(iter->ts_comp_buf);
      iter->ts_comp_buf = NULL;
      iter->valid = false;
      return false;
    }
    for (uint16_t i = 0; i < record_count; i++) {
      uint64_t ts;
      if (!gorilla_decoder_get_timestamp(&iter->ts_decoder, &ts)) {
        ESP_LOGE(TAG, "Gorilla batch ts decode failed at i=%u", i);
        free(iter->gorilla_batch_ts);
        iter->gorilla_batch_ts = NULL;
        gorilla_decoder_deinit(&iter->ts_decoder);
        gorilla_decoder_deinit(&iter->val_decoder);
        free(iter->ts_comp_buf);
        iter->ts_comp_buf = NULL;
        iter->valid = false;
        return false;
      }
      iter->gorilla_batch_ts[i] = (int64_t)ts;
    }
    // ts_decoder is fully consumed — deinit it
    gorilla_decoder_deinit(&iter->ts_decoder);

    prof_end = esp_timer_get_time();
    prof_decoder_init = prof_end - prof_start;

    // Log detailed profiling breakdown
    uint64_t total_time = prof_hdr_read + prof_ts_malloc + prof_ts_read +
                          prof_val_malloc + prof_val_read + prof_decoder_init;
    ESP_LOGD(TAG, "[ITER INIT] Total: %.3f ms (hdr: %.3f, ts_malloc: %.3f, ts_read: %.3f, val_malloc: %.3f, val_read: %.3f, decoder: %.3f) [ts_len=%u, val_len=%u]",
             total_time / 1000.0,
             prof_hdr_read / 1000.0,
             prof_ts_malloc / 1000.0,
             prof_ts_read / 1000.0,
             prof_val_malloc / 1000.0,
             prof_val_read / 1000.0,
             prof_decoder_init / 1000.0,
             (unsigned)col_hdr.ts_len,
             (unsigned)col_hdr.val_len);

    ESP_LOGV(TAG, "Iterator init: compressed, record_count=%u", record_count);
  } else {
    /* ----------------------------------------------------------------------
     * 3) Not compressed => single flash read for header + ts + val
     * --------------------------------------------------------------------*/
    timeseries_col_data_header_t uc_hdr;

    // Read entire record in one flash read into a temp buffer
    uint8_t *record_buf = (uint8_t *)malloc(record_length);
    if (!record_buf) {
      ESP_LOGE(TAG, "OOM for uncompressed record buffer of size %u",
               (unsigned)record_length);
      iter->valid = false;
      return false;
    }
    esp_err_t uc_err = esp_partition_read(db->partition, record_data_offset,
                                          record_buf, record_length);
    if (uc_err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading uncompressed record (err=0x%x)", uc_err);
      free(record_buf);
      iter->valid = false;
      return false;
    }
    memcpy(&uc_hdr, record_buf, sizeof(uc_hdr));

    uint32_t ts_size = record_count * sizeof(uint64_t);

    // A) Allocate and copy timestamps
    iter->ts_array = (uint64_t *)malloc(ts_size);
    if (!iter->ts_array) {
      ESP_LOGE(TAG, "OOM for uncompressed ts array");
      free(record_buf);
      iter->valid = false;
      return false;
    }
    memcpy(iter->ts_array, record_buf + sizeof(uc_hdr), ts_size);

    // B) Allocate and copy values
    uint8_t *val_buf = (uint8_t *)malloc(uc_hdr.val_len);
    if (!val_buf) {
      ESP_LOGE(TAG, "OOM for uncompressed values buffer");
      free(iter->ts_array);
      free(record_buf);
      iter->ts_array = NULL;
      iter->valid = false;
      return false;
    }
    memcpy(val_buf, record_buf + sizeof(uc_hdr) + uc_hdr.ts_len, uc_hdr.val_len);
    free(record_buf);

    // Parse val_buf according to series_type
    // We'll assume each sample has a fixed size for demonstration:
    //   Float => 8 bytes (double)
    //   Int   => 8 bytes (int64_t)
    //   Bool  => 1 byte
    // If your code also has strings or variable lengths, you'd do more complex
    // parsing.

    switch (series_type) {
    case TIMESERIES_FIELD_TYPE_FLOAT: {
      if (uc_hdr.val_len < record_count * sizeof(double)) {
        ESP_LOGE(TAG, "Value buffer too small for %u doubles", record_count);
        free(iter->ts_array);
        free(val_buf);
        iter->ts_array = NULL;
        iter->valid = false;
        return false;
      }
      // val_buf is already in native double[] layout — adopt it directly
      iter->float_array = (double *)val_buf;
      val_buf = NULL; // deinit will free float_array
      break;
    }
    case TIMESERIES_FIELD_TYPE_INT: {
      if (uc_hdr.val_len < record_count * sizeof(int64_t)) {
        ESP_LOGE(TAG, "Value buffer too small for %u int64s", record_count);
        free(iter->ts_array);
        free(val_buf);
        iter->ts_array = NULL;
        iter->valid = false;
        return false;
      }
      // val_buf is already in native int64_t[] layout — adopt it directly
      iter->int_array = (int64_t *)val_buf;
      val_buf = NULL; // deinit will free int_array
      break;
    }
    case TIMESERIES_FIELD_TYPE_BOOL: {
      iter->bool_array = (bool *)malloc(record_count * sizeof(bool));
      if (!iter->bool_array) {
        ESP_LOGE(TAG, "OOM for bool_array of size=%u", record_count);
        free(iter->ts_array);
        free(val_buf);
        iter->ts_array = NULL;
        iter->valid = false;
        return false;
      }

      size_t offset = 0;
      for (size_t i = 0; i < record_count; i++) {
        if (offset + 1 > uc_hdr.val_len) {
          ESP_LOGE(TAG, "Value buffer too small for bool at i=%zu", i);
          iter->valid = false;
          break;
        }
        bool b = (val_buf[offset] != 0);
        offset += 1;
        iter->bool_array[i] = b;
      }
      break;
    }
    case TIMESERIES_FIELD_TYPE_STRING: {
      // Allocate array for string values
      iter->string_array =
          (string_array_t *)malloc(record_count * sizeof(string_array_t));

      if (!iter->string_array) {
        ESP_LOGE(TAG, "OOM for string_array of size=%u", record_count);
        free(iter->ts_array);
        free(val_buf);
        iter->ts_array = NULL;
        iter->valid = false;
        return false;
      }

      size_t offset = 0;
      size_t i; // Move i declaration outside the loop for later use in cleanup
      for (i = 0; i < record_count; i++) {
        // Read string length (4 bytes)
        uint32_t str_len = 0;
        if (offset + 4 > uc_hdr.val_len) {
          ESP_LOGE(TAG, "Value buffer too small for string length at i=%zu", i);
          iter->valid = false;
          break;
        }
        memcpy(&str_len, val_buf + offset, 4);
        offset += 4;

        // Store the length
        iter->string_array[i].length = str_len;

        // Allocate and copy the string data
        if (str_len > 0) {
          if (offset + str_len > uc_hdr.val_len) {
            ESP_LOGE(TAG, "Value buffer too small for string data at i=%zu", i);
            iter->valid = false;
            break;
          }

          iter->string_array[i].str =
              (char *)malloc(str_len + 1); // +1 for null terminator
          if (!iter->string_array[i].str) {
            ESP_LOGE(TAG, "OOM for string data of length=%u",
                     (unsigned)str_len);
            iter->valid = false;
            break;
          }

          memcpy(iter->string_array[i].str, val_buf + offset, str_len);
          iter->string_array[i].str[str_len] = '\0'; // Null-terminate
          offset += str_len;
        } else {
          iter->string_array[i].str = NULL;
        }
      }

      // Handle any error that occurred in the loop
      if (!iter->valid) {
        // Clean up previously allocated strings
        for (size_t j = 0; j < i; j++) {
          if (iter->string_array[j].str) {
            free(iter->string_array[j].str);
          }
        }
        free(iter->string_array);
        free(iter->ts_array);
        free(val_buf);
        iter->string_array = NULL;
        iter->ts_array = NULL;
        return false;
      }
      break;
    }
    default: {
      ESP_LOGE(TAG, "Unsupported uncompressed type=%d", (int)series_type);
      iter->valid = false;
      break;
    }
    }

    free(val_buf);
    if (!iter->valid) {
      // we already freed ts_array => done
      return false;
    }

    ESP_LOGD(TAG, "Iterator init: uncompressed, record_count=%u", record_count);
  }

  return iter->valid;
}

/* --------------------------------------------------------------------------
 * timeseries_points_iterator_set_time_range(...)
 * --------------------------------------------------------------------------*/
void timeseries_points_iterator_set_time_range(
    timeseries_points_iterator_t *iter, uint64_t start_ms, uint64_t end_ms) {
  if (!iter || !iter->valid) return;

  /* We need a random-access timestamp array. ALP path uses alp_ts,
   * Gorilla batch path uses gorilla_batch_ts,
   * uncompressed path uses ts_array. */
  const int64_t *ts_i64 = NULL;   /* ALP or Gorilla batch timestamps (signed) */
  const uint64_t *ts_u64 = NULL;  /* uncompressed timestamps */
  uint16_t count = iter->ts_count;

  if (iter->alp_ts) {
    ts_i64 = iter->alp_ts;
  } else if (iter->gorilla_batch_ts) {
    ts_i64 = iter->gorilla_batch_ts;
  } else if (!iter->is_compressed && iter->ts_array) {
    ts_u64 = iter->ts_array;
  } else {
    return; /* no random-access timestamps available */
  }

  /* Binary search for lower bound (first index >= start_ms) */
  uint16_t old_idx = iter->current_idx;
  if (start_ms > 0) {
    uint16_t lo = 0, hi = count;
    while (lo < hi) {
      uint16_t mid = lo + (hi - lo) / 2;
      uint64_t t = ts_i64 ? (uint64_t)ts_i64[mid] : ts_u64[mid];
      if (t < start_ms) lo = mid + 1; else hi = mid;
    }
    iter->current_idx = lo;
  }

  /* Binary search for upper bound (last index <= end_ms) */
  if (end_ms > 0) {
    uint16_t lo = iter->current_idx, hi = count;
    while (lo < hi) {
      uint16_t mid = lo + (hi - lo) / 2;
      uint64_t t = ts_i64 ? (uint64_t)ts_i64[mid] : ts_u64[mid];
      if (t <= end_ms) lo = mid + 1; else hi = mid;
    }
    iter->ts_count = lo;
  }

  /* Gorilla batch path: val_decoder is still streaming, so we must
   * consume and discard the first (current_idx - old_idx) values to
   * keep the value stream aligned with the timestamp array position. */
  if (iter->gorilla_batch_ts && iter->current_idx > old_idx) {
    uint16_t skip = iter->current_idx - old_idx;
    for (uint16_t i = 0; i < skip; i++) {
      switch (iter->series_type) {
      case TIMESERIES_FIELD_TYPE_FLOAT: {
        double dval;
        gorilla_decoder_get_float(&iter->val_decoder, &dval);
        break;
      }
      case TIMESERIES_FIELD_TYPE_INT: {
        uint64_t i64;
        gorilla_decoder_get_timestamp(&iter->val_decoder, &i64);
        break;
      }
      case TIMESERIES_FIELD_TYPE_BOOL: {
        bool bval;
        gorilla_decoder_get_boolean(&iter->val_decoder, &bval);
        break;
      }
      case TIMESERIES_FIELD_TYPE_STRING: {
        uint8_t *str = NULL;
        size_t slen = 0;
        gorilla_decoder_get_string(&iter->val_decoder, &str, &slen);
        free(str);
        break;
      }
      default:
        break;
      }
    }
  }
}

/* --------------------------------------------------------------------------
 * timeseries_points_iterator_next_timestamp(...)
 * --------------------------------------------------------------------------*/
bool timeseries_points_iterator_next_timestamp(
    timeseries_points_iterator_t *iter, uint64_t *out_timestamp) {
  if (!iter || !iter->valid || iter->current_idx >= iter->ts_count) {
    return false;
  }
  if (!out_timestamp) {
    return false;
  }

  if (iter->alp_ts) {
    // ALP path: read from decoded array; index advances in next_value
    *out_timestamp = (uint64_t)iter->alp_ts[iter->current_idx];
    return true;
  } else if (iter->gorilla_batch_ts) {
    // Gorilla batch-decoded timestamps: random-access from array
    *out_timestamp = (uint64_t)iter->gorilla_batch_ts[iter->current_idx];
  } else if (iter->is_compressed) {
    // Gorilla streaming approach (fallback, shouldn't normally reach here)
    if (!gorilla_decoder_get_timestamp(&iter->ts_decoder, out_timestamp)) {
      ESP_LOGE(TAG, "Failed to decode next timestamp at idx=%u",
               iter->current_idx);
      iter->valid = false;
      return false;
    }
  } else {
    // Uncompressed => read from ts_array
    *out_timestamp = iter->ts_array[iter->current_idx];
  }

  // Bump index (Gorilla and raw paths; ALP index is bumped in next_value)
  iter->current_idx++;
  return true;
}

/* --------------------------------------------------------------------------
 * timeseries_points_iterator_next_value(...)
 * --------------------------------------------------------------------------*/
bool timeseries_points_iterator_next_value(
    timeseries_points_iterator_t *iter, timeseries_field_value_t *out_value) {
  if (!iter || !iter->valid) {
    return false;
  }
  if (!out_value) {
    return false;
  }

  out_value->type = iter->series_type;

  // ALP path: check BEFORE idx computation because next_timestamp does not
  // advance current_idx for ALP (it advances in next_value instead).
  if (iter->alp_ts) {
    if (iter->current_idx >= iter->ts_count) {
      return false;
    }
    size_t i = iter->current_idx++;
    if (iter->series_type == TIMESERIES_FIELD_TYPE_FLOAT) {
      out_value->data.float_val = iter->alp_float[i];
    } else {
      out_value->data.int_val = iter->alp_int[i];
    }
    return true;
  }

  // For non-ALP paths, next_timestamp already advanced current_idx.
  uint16_t idx = (uint16_t)(iter->current_idx - 1);
  if (idx >= iter->ts_count) {
    return false;
  }

  // If uncompressed:
  if (!iter->is_compressed) {
    // Use the data arrays
    switch (iter->series_type) {
    case TIMESERIES_FIELD_TYPE_FLOAT: {
      double d = iter->float_array[idx];
      out_value->data.float_val = (float)d; // or keep double
      return true;
    }
    case TIMESERIES_FIELD_TYPE_INT: {
      int64_t i64 = iter->int_array[idx];
      out_value->data.int_val = i64;
      return true;
    }
    case TIMESERIES_FIELD_TYPE_BOOL: {
      bool b = iter->bool_array[idx];
      out_value->data.bool_val = b;
      return true;
    }
    case TIMESERIES_FIELD_TYPE_STRING: {
      size_t slen = iter->string_array[idx].length;
      const char *src = iter->string_array[idx].str;
      if (src && slen > 0) {
        char *copy = malloc(slen + 1);
        if (!copy) {
          iter->valid = false;
          return false;
        }
        memcpy(copy, src, slen);
        copy[slen] = '\0';
        out_value->data.string_val.str = copy;
      } else {
        out_value->data.string_val.str = NULL;
      }
      out_value->data.string_val.length = slen;
      return true;
    }
    default:
      ESP_LOGE(TAG, "Unsupported uncompressed type in next_value");
      iter->valid = false;
      return false;
    }
  }

  // Else compressed => Gorilla decode next
  switch (iter->series_type) {
  case TIMESERIES_FIELD_TYPE_FLOAT: {
    double dval = 0.0;
    if (!gorilla_decoder_get_float(&iter->val_decoder, &dval)) {
      ESP_LOGE(TAG, "Failed to decode next float at idx=%u", idx);
      iter->valid = false;
      return false;
    }
    out_value->data.float_val = (float)dval;
    break;
  }
  case TIMESERIES_FIELD_TYPE_INT: {
    uint64_t i64 = 0;
    // In your code, you used the same "gorilla_decoder_get_timestamp" for int.
    if (!gorilla_decoder_get_timestamp(&iter->val_decoder, &i64)) {
      ESP_LOGE(TAG, "Failed to decode next int at idx=%u", idx);
      iter->valid = false;
      return false;
    }

    // convert to int64_t
    memcpy(&out_value->data.int_val, &i64, sizeof(i64));

    break;
  }
  case TIMESERIES_FIELD_TYPE_BOOL: {
    bool bval = false;
    if (!gorilla_decoder_get_boolean(&iter->val_decoder, &bval)) {
      ESP_LOGE(TAG, "Failed to decode next bool at idx=%u", idx);
      iter->valid = false;
      return false;
    }
    out_value->data.bool_val = bval;
    break;
  }
  case TIMESERIES_FIELD_TYPE_STRING: {
    char *str = NULL;
    size_t str_len = 0;

    if (gorilla_decoder_get_string(&iter->val_decoder, (uint8_t **)&str,
                                   &str_len)) {

      out_value->data.string_val.str = str;
      out_value->data.string_val.length = str_len;
    } else {
      ESP_LOGE(TAG, "Failed to decode next string at idx=%u", idx);
      iter->valid = false;
      return false;
    }
    break;
  }
  default:
    ESP_LOGE(TAG, "Unsupported field type in next_value");
    iter->valid = false;
    return false;
  }

  return true;
}

/* --------------------------------------------------------------------------
 * timeseries_points_iterator_deinit(...)
 * --------------------------------------------------------------------------*/
void timeseries_points_iterator_deinit(timeseries_points_iterator_t *iter) {
  if (!iter) {
    return;
  }

  if (iter->alp_ts) {
    // ALP path: free decoded arrays (comp bufs were freed in init)
    free(iter->alp_ts);
    iter->alp_ts = NULL;
    if (iter->alp_float) {
      // Union covers alp_int too; only one free needed
      free(iter->alp_float);
      iter->alp_float = NULL;
    }
  } else if (iter->is_compressed) {
    // Gorilla path: deinit decoders + free buffers
    // ts_decoder is already deinited if gorilla_batch_ts was used
    if (!iter->gorilla_batch_ts) {
      gorilla_decoder_deinit(&iter->ts_decoder);
    }
    gorilla_decoder_deinit(&iter->val_decoder);
    if (iter->gorilla_batch_ts) {
      free(iter->gorilla_batch_ts);
      iter->gorilla_batch_ts = NULL;
    }
    if (iter->ts_comp_buf) {
      free(iter->ts_comp_buf);
      iter->ts_comp_buf = NULL;
    }
    if (iter->val_comp_buf) {
      free(iter->val_comp_buf);
      iter->val_comp_buf = NULL;
    }
  } else {
    // Uncompressed: free arrays
    if (iter->ts_array) {
      free(iter->ts_array);
      iter->ts_array = NULL;
    }
    if (iter->float_array) {
      free(iter->float_array);
      iter->float_array = NULL;
    }
    if (iter->int_array) {
      free(iter->int_array);
      iter->int_array = NULL;
    }
    if (iter->bool_array) {
      free(iter->bool_array);
      iter->bool_array = NULL;
    }
    if (iter->string_array) {
      // Free each string allocation
      for (size_t i = 0; i < iter->ts_count; i++) {
        if (iter->string_array[i].str) {
          free(iter->string_array[i].str);
        }
      }
      free(iter->string_array);
      iter->string_array = NULL;
    }
  }

  memset(iter, 0, sizeof(*iter));
}
