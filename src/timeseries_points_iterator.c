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
#include "alp/alp_stream_decoder.h"

#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "PointsIterator";

// Same sanity cap the compressed path applies (see
// gorilla/string_stream_decoder.c): no stored string is ever this large, so
// anything bigger is a corrupt length word.
#define TSDB_STRING_MAX_LEN 65536

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
static bool points_iterator_init_impl(
    timeseries_db_t *db, uint32_t record_data_offset, uint32_t record_length,
    uint16_t record_count, timeseries_field_type_e series_type,
    uint8_t data_flags,
    timeseries_points_iterator_t *iter, uint64_t start_ms, uint64_t end_ms, const uint64_t *cached_revision) {
  if (!db || !db->partition || !iter) {
    return false;
  }
  memset(iter, 0, sizeof(*iter));
  iter->db = db;
  iter->valid = true;
  iter->ts_count = record_count;
  iter->series_type = series_type;
  iter->is_column_format = true;
  iter->has_revision = (data_flags & TSDB_FIELDDATA_FLAG_REVISION) == 0;
  // Derive compressed from flags: bit1=0 means compressed, bit1=1 means raw
  bool compressed = (data_flags & TSDB_FIELDDATA_FLAG_COMPRESSED) == 0;
  iter->is_compressed = compressed;
  iter->current_idx = 0;

  /* Every failure below leaves through `fail`: whatever the iterator already
   * owns is released by timeseries_points_iterator_deinit() (which leaves it
   * zeroed, i.e. invalid), and record_buf is freed until the iterator takes
   * it over. */
  uint8_t *record_buf = NULL;
  uint8_t *val_buf = NULL;

  // Boundary check
  if (record_data_offset > db->partition->size ||
      record_length > db->partition->size - record_data_offset) {
    ESP_LOGE(TAG, "Record end out of bounds: 0x%08X + %u > 0x%08X",
             (unsigned)record_data_offset, (unsigned)record_length,
             (unsigned)db->partition->size);
    goto fail;
  }

  // 1) Read col_data_header
  if (record_length < sizeof(timeseries_col_data_header_t)) {
    ESP_LOGE(TAG, "record_length too small for col_header: %u < %zu",
             (unsigned)record_length, sizeof(timeseries_col_data_header_t));
    goto fail;
  }

  // 1) Read col_data_header — for compressed path, read entire record in one go
  timeseries_col_data_header_t col_hdr;

  // 2) If compressed => single flash read for header + ts + val blocks
  if (compressed) {
    record_buf = (uint8_t *)malloc(record_length);
    if (!record_buf) {
      ESP_LOGE(TAG, "OOM for record buffer of size %u",
               (unsigned)record_length);
      goto fail;
    }

    esp_err_t err = esp_partition_read(db->partition, record_data_offset,
                                       record_buf, record_length);
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading record at 0x%08X (err=0x%x)",
               (unsigned)record_data_offset, err);
      goto fail;
    }

    memcpy(&col_hdr, record_buf, sizeof(col_hdr));

    // Boundary check using subtraction only. ts_len/val_len are uint32 read
    // from flash; computing sizeof(col_hdr) + ts_len + val_len could wrap uint32
    // for a corrupt record and falsely pass a "<= record_length" test, letting
    // val_data point past the record buffer and feed a bogus length to the
    // decoders. The record_buf is exactly record_length bytes, so bound against
    // it; a successful read already guarantees it lies within the partition.
    if (record_length < sizeof(col_hdr) ||
        col_hdr.ts_len > record_length - (uint32_t)sizeof(col_hdr) ||
        col_hdr.val_len > record_length - (uint32_t)sizeof(col_hdr) - col_hdr.ts_len) {
      ESP_LOGE(TAG, "Column data extends out of record buffer");
      goto fail;
    }

    size_t columns_length = sizeof(col_hdr) + col_hdr.ts_len + col_hdr.val_len;
    if (!tsdb_revision_decode(data_flags, record_buf + columns_length, record_length - columns_length,
                               record_count, &iter->revisions)) {
      goto fail;
    }

    // Pointers into the single buffer
    uint8_t *ts_data = record_buf + sizeof(col_hdr);
    uint8_t *val_data = record_buf + sizeof(col_hdr) + col_hdr.ts_len;

    iter->ts_comp_len = col_hdr.ts_len;
    iter->val_comp_len = col_hdr.val_len;

    // Check if this record uses ALP encoding (bit2=0 means ALP)
    bool use_alp = (data_flags & TSDB_FIELDDATA_FLAG_ENCODING_ALP) == 0;

    if (use_alp) {
      /* --- ALP decode path --- */

      /* Always decode all timestamps (needed for time-range binary search) */
      iter->alp_ts = (int64_t *)malloc(record_count * sizeof(int64_t));
      if (!iter->alp_ts) {
        ESP_LOGE(TAG, "OOM for ALP timestamp array of size %u", record_count);
        goto fail;
      }
      if (!alp_decode_int(ts_data, col_hdr.ts_len,
                          iter->alp_ts, record_count)) {
        ESP_LOGE(TAG, "ALP timestamp decode failed");
        goto fail;
      }

      /*
       * For values: use streaming decoder when record has multiple ALP blocks
       * (> 128 values). This avoids allocating the full decoded array,
       * reducing peak memory from O(N) to O(128) per iterator.
       * For small records (<= 128), full decode is simpler and equally fast.
       */
      if (record_count > ALP_STREAM_BLOCK_SIZE) {
        /* Streaming ALP path: keep record_buf, init streaming decoder */
        bool is_float = (series_type == TIMESERIES_FIELD_TYPE_FLOAT);
        iter->alp_val_dec = (alp_stream_decoder_t *)malloc(sizeof(alp_stream_decoder_t));
        if (!iter->alp_val_dec) {
          ESP_LOGE(TAG, "OOM for ALP stream decoder");
          goto fail;
        }
        if (!alp_stream_decoder_init(iter->alp_val_dec, val_data,
                                      col_hdr.val_len, is_float)) {
          ESP_LOGE(TAG, "ALP stream decoder init failed");
          goto fail;
        }
        /* Keep record_buf alive — decoder reads from it */
        iter->alp_comp_buf = record_buf;

        ESP_LOGD(TAG, "Iterator init: ALP streaming, record_count=%u", record_count);
        return iter->valid;
      }

      /* Eager ALP path: decode all values now, free record_buf */
      if (series_type == TIMESERIES_FIELD_TYPE_FLOAT) {
        iter->alp_float = (double *)malloc(record_count * sizeof(double));
        if (!iter->alp_float) {
          ESP_LOGE(TAG, "OOM for ALP float array of size %u", record_count);
          goto fail;
        }
        if (!alp_decode(val_data, col_hdr.val_len,
                        iter->alp_float, record_count)) {
          ESP_LOGE(TAG, "ALP float value decode failed");
          goto fail;
        }
      } else { /* INT */
        iter->alp_int = (int64_t *)malloc(record_count * sizeof(int64_t));
        if (!iter->alp_int) {
          ESP_LOGE(TAG, "OOM for ALP int array of size %u", record_count);
          goto fail;
        }
        if (!alp_decode_int(val_data, col_hdr.val_len,
                            iter->alp_int, record_count)) {
          ESP_LOGE(TAG, "ALP int value decode failed");
          goto fail;
        }
      }

      // Single record buffer consumed; free it now
      free(record_buf);

      ESP_LOGD(TAG, "Iterator init: ALP eager, record_count=%u", record_count);
      return iter->valid;
    }

    // Gorilla path: keep record_buf alive; ts_comp_buf owns the allocation
    iter->ts_comp_buf = record_buf;   // base allocation (includes col_hdr prefix)
    iter->val_comp_buf = NULL;        // val data is inside record_buf
    record_buf = NULL;

    // Now initialize Gorilla decoders (timestamps + values) in zero-copy mode
    if (!gorilla_decoder_init_direct(&iter->ts_decoder, GORILLA_STREAM_INT,
                                     ts_data, col_hdr.ts_len)) {
      ESP_LOGE(TAG, "Failed to init Gorilla ts_decoder.");
      goto fail;
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
      goto fail;
    }

    // Batch-decode all Gorilla timestamps into an array so we can
    // random-access them (enables time-range pruning via binary search).
    int64_t *batch_ts = (int64_t *)malloc(record_count * sizeof(int64_t));
    if (!batch_ts) {
      ESP_LOGE(TAG, "OOM for gorilla_batch_ts of size %u", record_count);
      goto fail;
    }
    for (uint16_t i = 0; i < record_count; i++) {
      uint64_t ts;
      if (!gorilla_decoder_get_timestamp(&iter->ts_decoder, &ts)) {
        ESP_LOGE(TAG, "Gorilla batch ts decode failed at i=%u", i);
        free(batch_ts);
        goto fail;
      }
      batch_ts[i] = (int64_t)ts;
    }
    // ts_decoder is fully consumed — deinit it (deinit skips it once
    // gorilla_batch_ts is set)
    gorilla_decoder_deinit(&iter->ts_decoder);
    iter->gorilla_batch_ts = batch_ts;

    ESP_LOGV(TAG, "Iterator init: compressed, record_count=%u", record_count);
  } else {
    /* ----------------------------------------------------------------------
     * 3) Not compressed => targeted flash reads (no intermediate buffer)
     *
     * Read header, timestamps, and values separately to avoid allocating
     * one large record_buf that holds everything simultaneously.
     * --------------------------------------------------------------------*/
    timeseries_col_data_header_t uc_hdr;

    // Read header (8 bytes)
    esp_err_t uc_err = esp_partition_read(db->partition, record_data_offset,
                                          &uc_hdr, sizeof(uc_hdr));
    if (uc_err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading uncompressed header (err=0x%x)", uc_err);
      goto fail;
    }

    // Same subtraction-based bound as the compressed path: ts_len/val_len are
    // uint32 read from flash, and additive forms can wrap for a corrupt record,
    // letting val_offset point at unrelated flash.
    if (record_length < sizeof(uc_hdr) ||
        uc_hdr.ts_len > record_length - (uint32_t)sizeof(uc_hdr) ||
        uc_hdr.val_len > record_length - (uint32_t)sizeof(uc_hdr) - uc_hdr.ts_len) {
      ESP_LOGE(TAG, "Uncompressed column data extends out of record");
      goto fail;
    }

    uint32_t columns_length = sizeof(uc_hdr) + uc_hdr.ts_len + uc_hdr.val_len;
    if (cached_revision && iter->has_revision && (data_flags & TSDB_FIELDDATA_FLAG_REVISION_VECTOR)) {
      if (record_length - columns_length != sizeof(uint64_t)) goto fail;
      iter->revisions.value = *cached_revision;
    } else if (!tsdb_revision_read(db, record_data_offset, record_length, columns_length,
                                    record_count, data_flags, &iter->revisions)) {
      goto fail;
    }
    uint32_t ts_size = record_count * sizeof(uint64_t);
    if (uc_hdr.ts_len != ts_size ||
        uc_hdr.ts_len > record_length - (uint32_t)sizeof(uc_hdr) ||
        uc_hdr.val_len > record_length - (uint32_t)sizeof(uc_hdr) - uc_hdr.ts_len) {
      ESP_LOGE(TAG, "Uncompressed column lengths exceed record bounds");
      goto fail;
    }
    uint32_t ts_offset = record_data_offset + sizeof(uc_hdr);
    uint32_t val_offset = ts_offset + uc_hdr.ts_len;

    // A) Allocate and read timestamps directly from flash
    iter->ts_array = (uint64_t *)malloc(ts_size);
    if (!iter->ts_array) {
      ESP_LOGE(TAG, "OOM for uncompressed ts array");
      goto fail;
    }
    uc_err = esp_partition_read(db->partition, ts_offset,
                                iter->ts_array, ts_size);
    if (uc_err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading uncompressed timestamps (err=0x%x)", uc_err);
      goto fail;
    }

    // Use decoded timestamps to trim fixed-width columns before reading
    // values. Validate the ORIGINAL column length before narrowing it.
    if ((start_ms || end_ms) && series_type != TIMESERIES_FIELD_TYPE_STRING) {
      uint32_t width;
      switch (series_type) {
        case TIMESERIES_FIELD_TYPE_INT:
        case TIMESERIES_FIELD_TYPE_FLOAT: width = 8; break;
        case TIMESERIES_FIELD_TYPE_BOOL: width = 1; break;
        default: goto fail;
      }
      if (uc_hdr.val_len < (uint32_t)record_count * width) goto fail;
      timeseries_points_iterator_set_time_range(iter, start_ms, end_ms);
      uint16_t first = iter->current_idx;
      iter->revision_first = first;
      record_count = iter->ts_count - first;
      memmove(iter->ts_array, iter->ts_array + first, record_count * sizeof(uint64_t));
      iter->current_idx = 0;
      iter->ts_count = record_count;
      val_offset += (uint32_t)first * width;
      uc_hdr.val_len = (uint32_t)record_count * width;
      if (record_count == 0) return true;
    }

    // B) Allocate and read values directly from flash
    val_buf = (uint8_t *)malloc(uc_hdr.val_len);
    if (!val_buf) {
      ESP_LOGE(TAG, "OOM for uncompressed values buffer of size %u",
               (unsigned)uc_hdr.val_len);
      goto fail;
    }
    uc_err = esp_partition_read(db->partition, val_offset,
                                val_buf, uc_hdr.val_len);
    if (uc_err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading uncompressed values (err=0x%x)", uc_err);
      goto fail;
    }

    // Parse val_buf according to series_type: float and int values are
    // already in native 8-byte layout, bools one byte each, strings are
    // [u32 length][bytes] each.
    switch (series_type) {
    case TIMESERIES_FIELD_TYPE_FLOAT:
    case TIMESERIES_FIELD_TYPE_INT: {
      if (uc_hdr.val_len < record_count * sizeof(int64_t)) {
        ESP_LOGE(TAG, "Value buffer too small for %u 8-byte values", record_count);
        goto fail;
      }
      // Adopt val_buf directly (float_array / int_array share storage);
      // deinit frees it
      iter->float_array = (double *)val_buf;
      val_buf = NULL;
      break;
    }
    case TIMESERIES_FIELD_TYPE_BOOL: {
      iter->bool_array = (bool *)malloc(record_count * sizeof(bool));
      if (!iter->bool_array) {
        ESP_LOGE(TAG, "OOM for bool_array of size=%u", record_count);
        goto fail;
      }
      for (size_t i = 0; i < record_count; i++) {
        if (i + 1 > uc_hdr.val_len) {
          ESP_LOGE(TAG, "Value buffer too small for bool at i=%zu", i);
          goto fail;
        }
        iter->bool_array[i] = (val_buf[i] != 0);
      }
      break;
    }
    case TIMESERIES_FIELD_TYPE_STRING: {
      // Allocate array for string values; string_array_count only counts
      // entries already filled, so deinit frees exactly those.
      iter->string_array =
          (string_array_t *)malloc(record_count * sizeof(string_array_t));
      if (!iter->string_array) {
        ESP_LOGE(TAG, "OOM for string_array of size=%u", record_count);
        goto fail;
      }
      iter->string_array_count = 0;

      size_t offset = 0;
      for (size_t i = 0; i < record_count; i++) {
        // Read string length (4 bytes)
        uint32_t str_len = 0;
        if (offset + 4 > uc_hdr.val_len) {
          ESP_LOGE(TAG, "Value buffer too small for string length at i=%zu", i);
          goto fail;
        }
        memcpy(&str_len, val_buf + offset, 4);
        offset += 4;

        // Store the length
        iter->string_array[i].length = str_len;
        iter->string_array[i].str = NULL;

        // Allocate and copy the string data
        if (str_len > 0) {
          // Subtraction form: offset <= val_len holds here (the length-word
          // check above just passed), so this cannot wrap the way the
          // additive form could for a corrupt str_len near UINT32_MAX.
          if (str_len > uc_hdr.val_len - offset ||
              str_len > TSDB_STRING_MAX_LEN) {
            ESP_LOGE(TAG, "Value buffer too small for string data at i=%zu", i);
            goto fail;
          }

          iter->string_array[i].str =
              (char *)malloc(str_len + 1); // +1 for null terminator
          if (!iter->string_array[i].str) {
            ESP_LOGE(TAG, "OOM for string data of length=%u",
                     (unsigned)str_len);
            goto fail;
          }

          memcpy(iter->string_array[i].str, val_buf + offset, str_len);
          iter->string_array[i].str[str_len] = '\0'; // Null-terminate
          offset += str_len;
        }
        iter->string_array_count = i + 1;
      }
      iter->string_array_count = record_count;
      break;
    }
    default: {
      ESP_LOGE(TAG, "Unsupported uncompressed type=%d", (int)series_type);
      goto fail;
    }
    }

    free(val_buf);
    ESP_LOGD(TAG, "Iterator init: uncompressed, record_count=%u", record_count);
  }

  return iter->valid;

fail:
  free(val_buf);
  free(record_buf);
  timeseries_points_iterator_deinit(iter);
  return false;
}

bool timeseries_points_iterator_init_range_with_revision(
    timeseries_db_t *db, uint32_t offset, uint32_t length, uint16_t count,
    timeseries_field_type_e type, uint8_t flags, timeseries_points_iterator_t *iter,
    uint64_t start_ms, uint64_t end_ms, const uint64_t *cached_revision) {
  if (!points_iterator_init_impl(db, offset, length, count, type, flags, iter, start_ms, end_ms, cached_revision)) {
    if (iter && db && db->partition) { free(iter->revisions.values); iter->revisions.values = NULL; }
    return false;
  }
  if (start_ms || end_ms) timeseries_points_iterator_set_time_range(iter, start_ms, end_ms);
  return true;
}

bool timeseries_points_iterator_init_range(
    timeseries_db_t *db, uint32_t offset, uint32_t length, uint16_t count,
    timeseries_field_type_e type, uint8_t flags, timeseries_points_iterator_t *iter,
    uint64_t start_ms, uint64_t end_ms) {
  return timeseries_points_iterator_init_range_with_revision(db, offset, length, count, type,
                                                             flags, iter, start_ms, end_ms, NULL);
}

bool timeseries_points_iterator_init(
    timeseries_db_t *db, uint32_t offset, uint32_t length, uint16_t count,
    timeseries_field_type_e type, uint8_t flags, timeseries_points_iterator_t *iter) {
  return timeseries_points_iterator_init_range(db, offset, length, count, type, flags, iter, 0, 0);
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

  /* ALP streaming path: skip values to align with the new start index. */
  if (iter->alp_val_dec && iter->current_idx > old_idx) {
    uint16_t skip = iter->current_idx - old_idx;
    alp_stream_decoder_skip(iter->alp_val_dec, skip);
  }

  /* Gorilla batch path: val_decoder is still streaming, so we must
   * advance past the first (current_idx - old_idx) values to keep the
   * value stream aligned with the timestamp array position.
   * gorilla_decoder_skip_value avoids malloc for strings. */
  if (iter->gorilla_batch_ts && iter->current_idx > old_idx) {
    uint16_t skip = iter->current_idx - old_idx;
    for (uint16_t i = 0; i < skip; i++) {
      gorilla_decoder_skip_value(&iter->val_decoder);
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

    if (iter->alp_val_dec) {
      /* Streaming ALP: decode from the block-based stream decoder */
      if (iter->series_type == TIMESERIES_FIELD_TYPE_FLOAT) {
        double v;
        if (!alp_stream_decoder_next_float(iter->alp_val_dec, &v)) {
          iter->valid = false;
          return false;
        }
        out_value->data.float_val = v;
      } else {
        int64_t v;
        if (!alp_stream_decoder_next_int(iter->alp_val_dec, &v)) {
          iter->valid = false;
          return false;
        }
        out_value->data.int_val = v;
      }
    } else {
      /* Eager ALP: read from pre-decoded arrays */
      if (iter->series_type == TIMESERIES_FIELD_TYPE_FLOAT) {
        out_value->data.float_val = iter->alp_float[i];
      } else {
        out_value->data.int_val = iter->alp_int[i];
      }
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
      out_value->data.float_val = d;
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
      // Transfer ownership to caller — each index is read exactly once
      // during forward iteration, so no copy is needed
      out_value->data.string_val.str = iter->string_array[idx].str;
      out_value->data.string_val.length = slen;
      iter->string_array[idx].str = NULL; // prevent double-free in deinit
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
    out_value->data.float_val = dval;
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

  free(iter->revisions.values);
  iter->revisions.values = NULL;
  if (iter->alp_ts) {
    // ALP path: free decoded arrays
    free(iter->alp_ts);
    iter->alp_ts = NULL;
    if (iter->alp_val_dec) {
      // Streaming ALP: release scratch, then free decoder and retained compressed buffer
      alp_stream_decoder_deinit(iter->alp_val_dec);
      free(iter->alp_val_dec);
      iter->alp_val_dec = NULL;
      if (iter->alp_comp_buf) {
        free(iter->alp_comp_buf);
        iter->alp_comp_buf = NULL;
      }
    } else if (iter->alp_float) {
      // Eager ALP: free decoded value array (union covers alp_int too)
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
    // These pointers share a union. Freeing the scalar pointer first would
    // clear string_array before its unconsumed strings could be released.
    if (iter->series_type == TIMESERIES_FIELD_TYPE_STRING && iter->string_array) {
      for (size_t i = 0; i < iter->string_array_count; i++)
        free(iter->string_array[i].str);
    }
    free(iter->float_array);
    iter->float_array = NULL;
  }

  memset(iter, 0, sizeof(*iter));
}
