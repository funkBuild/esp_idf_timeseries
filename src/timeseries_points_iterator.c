#include "timeseries_points_iterator.h"
#include "esp_err.h"
#include "esp_log.h"
#include "gorilla/gorilla_stream_decoder.h"
#include "timeseries_compression.h"
#include "timeseries_data.h"

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
    uint16_t record_count, timeseries_field_type_e series_type, bool compressed,
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

  // 1) Read col_data_header
  if (record_length < sizeof(timeseries_col_data_header_t)) {
    ESP_LOGE(TAG, "record_length too small for col_header: %u < %zu",
             (unsigned)record_length, sizeof(timeseries_col_data_header_t));
    iter->valid = false;
    return false;
  }

  timeseries_col_data_header_t col_hdr;
  esp_err_t err = esp_partition_read(db->partition, record_data_offset,
                                     &col_hdr, sizeof(col_hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed reading col_header at 0x%08X (err=0x%x)",
             (unsigned)record_data_offset, err);
    iter->valid = false;
    return false;
  }

  // Offsets for the two blocks
  uint32_t ts_offset = record_data_offset + sizeof(col_hdr);
  uint32_t val_offset = ts_offset + col_hdr.ts_len;

  // Basic boundary check
  if (val_offset + col_hdr.val_len > db->partition->size) {
    ESP_LOGE(TAG, "Column data extends out of partition");
    iter->valid = false;
    return false;
  }

  // 2) If compressed => load Gorilla blocks into memory
  if (compressed) {
    iter->ts_comp_len = col_hdr.ts_len;
    iter->ts_comp_buf = (uint8_t *)malloc(col_hdr.ts_len);
    if (!iter->ts_comp_buf) {
      ESP_LOGE(TAG, "OOM for timestamp buffer of size %u",
               (unsigned)col_hdr.ts_len);
      iter->valid = false;
      return false;
    }
    err = esp_partition_read(db->partition, ts_offset, iter->ts_comp_buf,
                             col_hdr.ts_len);
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading timestamp block (err=0x%x)", err);
      free(iter->ts_comp_buf);
      iter->ts_comp_buf = NULL;
      iter->valid = false;
      return false;
    }

    iter->val_comp_len = col_hdr.val_len;
    iter->val_comp_buf = (uint8_t *)malloc(col_hdr.val_len);
    if (!iter->val_comp_buf) {
      ESP_LOGE(TAG, "OOM for value buffer of size %u",
               (unsigned)col_hdr.val_len);
      free(iter->ts_comp_buf);
      iter->ts_comp_buf = NULL;
      iter->valid = false;
      return false;
    }
    err = esp_partition_read(db->partition, val_offset, iter->val_comp_buf,
                             col_hdr.val_len);
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading value block (err=0x%x)", err);
      free(iter->ts_comp_buf);
      free(iter->val_comp_buf);
      iter->ts_comp_buf = NULL;
      iter->val_comp_buf = NULL;
      iter->valid = false;
      return false;
    }

    // Now initialize Gorilla decoders (timestamps + values)
    iter->ts_cb_storage.data = iter->ts_comp_buf;
    iter->ts_cb_storage.size = iter->ts_comp_len;
    iter->ts_cb_storage.capacity = iter->ts_comp_len;

    iter->ts_decoder_context.cb = &iter->ts_cb_storage;
    iter->ts_decoder_context.offset = 0;

    if (!gorilla_decoder_init(&iter->ts_decoder, GORILLA_STREAM_INT,
                              decoder_fill_callback,
                              &iter->ts_decoder_context)) {
      ESP_LOGE(TAG, "Failed to init Gorilla ts_decoder.");
      free(iter->ts_comp_buf);
      free(iter->val_comp_buf);
      iter->ts_comp_buf = NULL;
      iter->val_comp_buf = NULL;
      iter->valid = false;
      return false;
    }

    // Values
    iter->val_cb_storage.data = iter->val_comp_buf;
    iter->val_cb_storage.size = iter->val_comp_len;
    iter->val_cb_storage.capacity = iter->val_comp_len;

    iter->val_decoder_context.cb = &iter->val_cb_storage;
    iter->val_decoder_context.offset = 0;

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

    if (!gorilla_decoder_init(&iter->val_decoder, val_mode,
                              decoder_fill_callback,
                              &iter->val_decoder_context)) {
      ESP_LOGE(TAG, "Failed to init Gorilla val_decoder.");
      gorilla_decoder_deinit(&iter->ts_decoder);
      free(iter->ts_comp_buf);
      free(iter->val_comp_buf);
      iter->ts_comp_buf = NULL;
      iter->val_comp_buf = NULL;
      iter->valid = false;
      return false;
    }

    ESP_LOGV(TAG, "Iterator init: compressed, record_count=%u", record_count);
  } else {
    /* ----------------------------------------------------------------------
     * 3) Not compressed => read timestamps and values from partition
     * --------------------------------------------------------------------*/
    // A) Read timestamps into iter->ts_array
    iter->ts_array = (uint64_t *)malloc(record_count * sizeof(uint64_t));
    if (!iter->ts_array) {
      ESP_LOGE(TAG, "OOM for uncompressed ts array");
      iter->valid = false;
      return false;
    }
    err = esp_partition_read(db->partition, ts_offset, iter->ts_array,
                             record_count * sizeof(uint64_t));
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading uncompressed ts array");
      free(iter->ts_array);
      iter->ts_array = NULL;
      iter->valid = false;
      return false;
    }

    // B) Read values: We'll read them into a temporary buffer,
    //    then parse into float_array / int_array / bool_array
    uint8_t *val_buf = (uint8_t *)malloc(col_hdr.val_len);
    if (!val_buf) {
      ESP_LOGE(TAG, "OOM for uncompressed values buffer");
      free(iter->ts_array);
      iter->ts_array = NULL;
      iter->valid = false;
      return false;
    }
    err =
        esp_partition_read(db->partition, val_offset, val_buf, col_hdr.val_len);
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading uncompressed value array");
      free(iter->ts_array);
      free(val_buf);
      iter->ts_array = NULL;
      iter->valid = false;
      return false;
    }

    // Parse val_buf according to series_type
    // We'll assume each sample has a fixed size for demonstration:
    //   Float => 8 bytes (double)
    //   Int   => 8 bytes (int64_t)
    //   Bool  => 1 byte
    // If your code also has strings or variable lengths, you'd do more complex
    // parsing.

    switch (series_type) {
    case TIMESERIES_FIELD_TYPE_FLOAT: {
      iter->float_array = (double *)malloc(record_count * sizeof(double));
      if (!iter->float_array) {
        ESP_LOGE(TAG, "OOM for float_array of size=%u", record_count);
        free(iter->ts_array);
        free(val_buf);
        iter->ts_array = NULL;
        iter->valid = false;
        return false;
      }

      size_t offset = 0;
      for (size_t i = 0; i < record_count; i++) {
        double dval = 0.0;
        // read 8 bytes
        if (offset + 8 > col_hdr.val_len) {
          ESP_LOGE(TAG, "Value buffer too small for float at i=%zu", i);
          iter->valid = false;
          break;
        }
        memcpy(&dval, val_buf + offset, 8);
        offset += 8;
        iter->float_array[i] = dval;
      }
      break;
    }
    case TIMESERIES_FIELD_TYPE_INT: {
      iter->int_array = (int64_t *)malloc(record_count * sizeof(int64_t));
      if (!iter->int_array) {
        ESP_LOGE(TAG, "OOM for int_array of size=%u", record_count);
        free(iter->ts_array);
        free(val_buf);
        iter->ts_array = NULL;
        iter->valid = false;
        return false;
      }

      size_t offset = 0;
      for (size_t i = 0; i < record_count; i++) {
        int64_t ival = 0;
        if (offset + 8 > col_hdr.val_len) {
          ESP_LOGE(TAG, "Value buffer too small for int at i=%zu", i);
          iter->valid = false;
          break;
        }
        memcpy(&ival, val_buf + offset, 8);
        offset += 8;
        iter->int_array[i] = ival;
      }
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
        if (offset + 1 > col_hdr.val_len) {
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
        if (offset + 4 > col_hdr.val_len) {
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
          if (offset + str_len > col_hdr.val_len) {
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

  if (iter->is_compressed) {
    // Gorilla-decoded approach
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

  // Bump index
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

  // The simplest approach: we treat the last read TS index as current_idx - 1
  uint16_t idx = (uint16_t)(iter->current_idx - 1);
  if (idx >= iter->ts_count) {
    return false;
  }
  if (!out_value) {
    return false;
  }

  out_value->type = iter->series_type;

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
      out_value->data.string_val.length = iter->string_array[idx].length;
      out_value->data.string_val.str = iter->string_array[idx].str;
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

  if (iter->is_compressed) {
    // Deinit gorilla decoders + free buffers
    gorilla_decoder_deinit(&iter->ts_decoder);
    gorilla_decoder_deinit(&iter->val_decoder);
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
