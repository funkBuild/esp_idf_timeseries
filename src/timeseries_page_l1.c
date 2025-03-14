// Timeseries_page_l1.c

#include "timeseries_compaction.h"

#include "esp_err.h"
#include "esp_log.h"
#include "esp_partition.h"

#include "timeseries_data.h" // for tsdb_create_level1_field_page()
#include "timeseries_internal.h"
#include "timeseries_iterator.h" // for fielddata_iterator
#include "timeseries_metadata.h" // for tsdb_lookup_series_type_in_metadata
#include "timeseries_page_cache.h"
#include "timeseries_page_l1.h" // example, your L1 logic

#include "gorilla/gorilla_stream_decoder.h"
#include "gorilla/gorilla_stream_encoder.h"

#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "TimeseriesPageL1";

// Round up to next 4K
static uint32_t round_up_4k(uint32_t x) {
  const uint32_t align = 4096;
  return ((x + align - 1) / align) * align;
}

static void init_compressed_buffer(CompressedBuffer *cb) {
  cb->capacity = 1024;
  cb->size = 0;
  cb->data = (uint8_t *)malloc(cb->capacity);
  if (!cb->data) {
    ESP_LOGE(TAG, "Failed to allocate compressed buffer");
    // handle error
  }
}

static void free_compressed_buffer(CompressedBuffer *cb) {
  free(cb->data);
  cb->data = NULL;
  cb->size = 0;
  cb->capacity = 0;
}

/* Flush callback: appends the data to our CompressedBuffer. */
static bool flush_callback(void *context, const uint8_t *data, size_t len) {
  CompressedBuffer *cb = (CompressedBuffer *)context;
  if (cb->size + len > cb->capacity) {
    size_t new_capacity = (cb->size + len) * 2;
    uint8_t *new_data = (uint8_t *)realloc(cb->data, new_capacity);
    if (!new_data) {
      return false;
    }
    cb->data = new_data;
    cb->capacity = new_capacity;
  }
  memcpy(cb->data + cb->size, data, len);
  cb->size += len;
  return true;
}

/* --------------------------------------------------------------------------------
 */

bool timeseries_copy_flash_block(const esp_partition_t *partition,
                                 uint32_t src_offset, uint32_t dst_offset,
                                 uint32_t size) {
  if (!partition) {
    return false;
  }

  // Allocate a buffer
  uint8_t *buf = malloc(1024);
  if (!buf) {
    ESP_LOGE(TAG, "OOM copying flash block");
    return false;
  }

  // Copy in 1K chunks
  while (size > 0) {
    uint32_t to_read = (size > 1024) ? 1024 : size;
    esp_err_t err = esp_partition_read(partition, src_offset, buf, to_read);
    if (err != ESP_OK) {
      free(buf);
      return false;
    }

    err = esp_partition_write(partition, dst_offset, buf, to_read);
    if (err != ESP_OK) {
      free(buf);
      return false;
    }

    src_offset += to_read;
    dst_offset += to_read;
    size -= to_read;
  }

  free(buf);
  return true;
}

/*
 * Grows the region from old_size to new_size by:
 * 1) Finding + erasing a new region of new_size
 * 2) Copying old_size worth of data from old_offset => new_offset
 * 3) Marking old region as free or ignoring it
 */
static bool relocate_region(timeseries_db_t *db, uint32_t old_offset,
                            uint32_t old_size, uint32_t new_size,
                            uint32_t *out_new_offset) {
  // 1) Find + erase
  timeseries_blank_iterator_t iter;
  timeseries_blank_iterator_init(db, &iter, new_size); // find blank region

  uint32_t found_ofs = 0, found_sz = 0;
  if (!timeseries_blank_iterator_next(&iter, &found_ofs, &found_sz)) {
    ESP_LOGE(TAG, "Failed to find blank region for new size=%u",
             (unsigned)new_size);
    return false;
  }

  // 2) Copy old data
  if (!timeseries_copy_flash_block(db->partition, old_offset, found_ofs,
                                   old_size)) {
    ESP_LOGE(TAG, "Failed copying old region => new region!");
    return false;
  }

  ESP_LOGV(TAG, "Relocated region from 0x%08" PRIx32 " => 0x%08" PRIx32,
           old_offset, found_ofs);

  // (Optionally erase old region, etc.)

  *out_new_offset = found_ofs;
  return true;
}

/**
 * Helper function to compress timestamps using the Gorilla streaming interface.
 * On success, allocates `*out_buf` and sets `*out_size` to the compressed size.
 */
static bool compress_timestamps(const uint64_t *timestamps, size_t count,
                                uint8_t **out_buf, size_t *out_size) {
  CompressedBuffer cb;
  init_compressed_buffer(&cb);

  gorilla_stream_t stream;
  if (!gorilla_stream_init(&stream, GORILLA_STREAM_INT,
                           /*initial_xor=*/NULL, /*leading_zeros=*/0,
                           flush_callback, &cb)) {
    ESP_LOGE(TAG, "Failed to init Gorilla stream for timestamps");
    free_compressed_buffer(&cb);
    return false;
  }

  for (size_t i = 0; i < count; i++) {
    if (!gorilla_stream_add_timestamp(&stream, timestamps[i])) {
      ESP_LOGE(TAG, "Failed to add timestamp at index %zu", i);
      gorilla_stream_deinit(&stream);
      free_compressed_buffer(&cb);
      return false;
    }
  }

  if (!gorilla_stream_finish(&stream)) {
    ESP_LOGE(TAG, "Failed to finish Gorilla stream for timestamps");
    gorilla_stream_deinit(&stream);
    free_compressed_buffer(&cb);
    return false;
  }
  gorilla_stream_deinit(&stream);

  // Now copy out the compressed data
  *out_buf = (uint8_t *)malloc(cb.size);
  if (!*out_buf) {
    ESP_LOGE(TAG, "OOM copying compressed timestamps");
    free_compressed_buffer(&cb);
    return false;
  }
  memcpy(*out_buf, cb.data, cb.size);
  *out_size = cb.size;

  free_compressed_buffer(&cb);
  return true;
}

/**
 * Helper to compress float (double) array with Gorilla streaming.
 */
static bool compress_floats(const double *values, size_t count,
                            uint8_t **out_buf, size_t *out_size) {
  CompressedBuffer cb;
  init_compressed_buffer(&cb);

  gorilla_stream_t stream;
  if (!gorilla_stream_init(&stream, GORILLA_STREAM_FLOAT,
                           /*initial_xor=*/NULL, /*leading_zeros=*/0,
                           flush_callback, &cb)) {
    ESP_LOGE(TAG, "Failed to init Gorilla float stream");
    free_compressed_buffer(&cb);
    return false;
  }

  for (size_t i = 0; i < count; i++) {
    ESP_LOGW(TAG, "Adding float at index %zu: %.3f", i, values[i]);
    if (!gorilla_stream_add_float(&stream, values[i])) {
      ESP_LOGE(TAG, "Failed to add float at index %zu", i);
      gorilla_stream_deinit(&stream);
      free_compressed_buffer(&cb);
      return false;
    }
  }

  if (!gorilla_stream_finish(&stream)) {
    ESP_LOGE(TAG, "Failed finishing Gorilla float stream");
    gorilla_stream_deinit(&stream);
    free_compressed_buffer(&cb);
    return false;
  }
  gorilla_stream_deinit(&stream);

  // Copy out data
  *out_buf = (uint8_t *)malloc(cb.size);
  if (!*out_buf) {
    ESP_LOGE(TAG, "OOM copying compressed float data");
    free_compressed_buffer(&cb);
    return false;
  }
  memcpy(*out_buf, cb.data, cb.size);
  *out_size = cb.size;

  free_compressed_buffer(&cb);
  return true;
}

/**
 * Helper to compress 64-bit integer array with Gorilla streaming.
 * (We reuse the INT stream, just like timestamps.)
 */
static bool compress_int64(const int64_t *values, size_t count,
                           uint8_t **out_buf, size_t *out_size) {
  CompressedBuffer cb;
  init_compressed_buffer(&cb);

  gorilla_stream_t stream;
  if (!gorilla_stream_init(&stream, GORILLA_STREAM_INT,
                           /*initial_xor=*/NULL, /*leading_zeros=*/0,
                           flush_callback, &cb)) {
    ESP_LOGE(TAG, "Failed to init Gorilla int stream");
    free_compressed_buffer(&cb);
    return false;
  }

  for (size_t i = 0; i < count; i++) {
    if (!gorilla_stream_add_timestamp(&stream, (uint64_t)values[i])) {
      // Or if your library provides gorilla_stream_add_int64(...), use that
      ESP_LOGE(TAG, "Failed to add int64 at index %zu", i);
      gorilla_stream_deinit(&stream);
      free_compressed_buffer(&cb);
      return false;
    }
  }

  if (!gorilla_stream_finish(&stream)) {
    ESP_LOGE(TAG, "Failed finishing Gorilla int stream");
    gorilla_stream_deinit(&stream);
    free_compressed_buffer(&cb);
    return false;
  }
  gorilla_stream_deinit(&stream);

  // Copy out data
  *out_buf = (uint8_t *)malloc(cb.size);
  if (!*out_buf) {
    ESP_LOGE(TAG, "OOM copying compressed int data");
    free_compressed_buffer(&cb);
    return false;
  }
  memcpy(*out_buf, cb.data, cb.size);
  *out_size = cb.size;

  free_compressed_buffer(&cb);
  return true;
}

/**
 * Helper to compress bool array with Gorilla streaming.
 */
static bool compress_bools(const bool *values, size_t count, uint8_t **out_buf,
                           size_t *out_size) {
  CompressedBuffer cb;
  init_compressed_buffer(&cb);

  gorilla_stream_t stream;
  if (!gorilla_stream_init(&stream, GORILLA_STREAM_BOOL,
                           /*initial_xor=*/NULL, /*leading_zeros=*/0,
                           flush_callback, &cb)) {
    ESP_LOGE(TAG, "Failed to init Gorilla bool stream");
    free_compressed_buffer(&cb);
    return false;
  }

  for (size_t i = 0; i < count; i++) {
    if (!gorilla_stream_add_boolean(&stream, values[i])) {
      ESP_LOGE(TAG, "Failed to add bool at index %zu", i);
      gorilla_stream_deinit(&stream);
      free_compressed_buffer(&cb);
      return false;
    }
  }

  if (!gorilla_stream_finish(&stream)) {
    ESP_LOGE(TAG, "Failed finishing Gorilla bool stream");
    gorilla_stream_deinit(&stream);
    free_compressed_buffer(&cb);
    return false;
  }
  gorilla_stream_deinit(&stream);

  // Copy out data
  *out_buf = (uint8_t *)malloc(cb.size);
  if (!*out_buf) {
    ESP_LOGE(TAG, "OOM copying compressed bool data");
    free_compressed_buffer(&cb);
    return false;
  }
  memcpy(*out_buf, cb.data, cb.size);
  *out_size = cb.size;

  free_compressed_buffer(&cb);
  return true;
}

/* -------------------------------------------------------------------------- */

/**
 * Writes all series to a dynamically grown region, one series at a time:
 * 1) Start with 4KB
 * 2) If we can’t fit the next series, double size + relocate
 * 3) At end, finalize + write page header
 */
bool tsdb_write_page_dynamic(timeseries_db_t *db,
                             const tsdb_series_buffer_t *series_bufs,
                             size_t series_count, unsigned int level) {
  if (!db || !series_bufs) {
    return false;
  }

  // 1) Find / erase an initial 4KB blank region
  uint32_t cur_size = 4 * 1024;
  uint32_t region_ofs = 0;

  timeseries_blank_iterator_t blank_iter;
  timeseries_blank_iterator_init(db, &blank_iter, cur_size);
  if (!timeseries_blank_iterator_next(&blank_iter, &region_ofs, &cur_size)) {
    ESP_LOGE(TAG, "Failed to find blank region for initial size=%u",
             (unsigned int)cur_size);
    return false;
  }

  if (esp_partition_erase_range(db->partition, region_ofs, cur_size) !=
      ESP_OK) {
    ESP_LOGE(TAG, "Failed to erase region for L1 page");
    return false;
  }

  // Leave space for the final page header at the start
  uint32_t write_ptr = region_ofs + sizeof(timeseries_page_header_t);

  // 2) Write each series
  for (size_t i = 0; i < series_count; i++) {
    const tsdb_series_buffer_t *buf = &series_bufs[i];
    if (buf->count == 0) {
      continue; // no data => skip
    }

    // (A) Write field_data_header_t with placeholders
    timeseries_field_data_header_t fd_hdr;
    memset(&fd_hdr, 0xff, sizeof(fd_hdr));

    fd_hdr.flags &= ~TSDB_FIELDDATA_FLAG_COMPRESSED;
    fd_hdr.record_count = (uint16_t)buf->count;
    fd_hdr.record_length = 0xFFFF; // placeholder
    fd_hdr.start_time = buf->start_ts;
    fd_hdr.end_time = buf->end_ts;
    memcpy(fd_hdr.series_id, buf->series_id, 16);

    uint32_t fd_hdr_ofs = write_ptr;
    esp_err_t err =
        esp_partition_write(db->partition, fd_hdr_ofs, &fd_hdr, sizeof(fd_hdr));
    if (err != ESP_OK) {
      ESP_LOGE(TAG,
               "Failed writing fd_hdr placeholder (err=0x%x) for series #%zu",
               err, i);
      return false;
    }
    write_ptr += sizeof(fd_hdr);

    // (B) Column header placeholder
    timeseries_col_data_header_t col_hdr;
    memset(&col_hdr, 0, sizeof(col_hdr));
    col_hdr.ts_len = 0xFFFF;
    col_hdr.val_len = 0xFFFF;

    uint32_t col_hdr_ofs = write_ptr;
    err = esp_partition_write(db->partition, col_hdr_ofs, &col_hdr,
                              sizeof(col_hdr));
    if (err != ESP_OK) {
      ESP_LOGE(TAG,
               "Failed writing col_hdr placeholder (err=0x%x) for series #%zu",
               err, i);
      return false;
    }
    write_ptr += sizeof(col_hdr);

    // (C) Compress + Write Timestamps
    uint64_t *timestamps = (uint64_t *)malloc(buf->count * sizeof(uint64_t));
    if (!timestamps) {
      ESP_LOGE(TAG, "OOM allocating timestamps for series #%zu", i);
      return false;
    }
    for (size_t j = 0; j < buf->count; j++) {
      timestamps[j] = buf->points[j].timestamp;
    }

    uint8_t *ts_comp_data = NULL;
    size_t ts_comp_size = 0;
    bool ts_ok = compress_timestamps(timestamps, buf->count, &ts_comp_data,
                                     &ts_comp_size);
    free(timestamps);

    if (!ts_ok) {
      ESP_LOGW(TAG, "Timestamp compression failed for series #%zu", i);
      free(ts_comp_data);
      return false;
    }

    // Check space, relocate if needed
    size_t needed_ts = ts_comp_size;
    uint32_t end_ptr = write_ptr + needed_ts;
    if (end_ptr > (region_ofs + cur_size)) {
      uint32_t new_size = cur_size * 2;
      uint32_t new_ofs = 0;
      if (!relocate_region(db, region_ofs, cur_size, new_size, &new_ofs)) {
        free(ts_comp_data);
        return false;
      }
      ESP_LOGV(TAG,
               "Region grown from %u => %u bytes, offset=0x%08" PRIx32
               " => 0x%08" PRIx32,
               (unsigned)cur_size, (unsigned)new_size, region_ofs, new_ofs);

      // Update pointers
      uint32_t offset_diff = new_ofs - region_ofs;
      write_ptr += offset_diff;
      fd_hdr_ofs += offset_diff;
      col_hdr_ofs += offset_diff;
      region_ofs = new_ofs;
      cur_size = new_size;

      end_ptr = write_ptr + needed_ts;
      if (end_ptr > (region_ofs + cur_size)) {
        ESP_LOGE(TAG, "Still not enough space for timestamps in series #%zu",
                 i);
        free(ts_comp_data);
        return false;
      }
    }

    // Write timestamps
    err = esp_partition_write(db->partition, write_ptr, ts_comp_data,
                              ts_comp_size);
    free(ts_comp_data);
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed writing timestamps (err=0x%x) for series #%zu", err,
               i);
      return false;
    }

    col_hdr.ts_len = (uint16_t)ts_comp_size;
    err = esp_partition_write(db->partition, col_hdr_ofs, &col_hdr,
                              sizeof(col_hdr));
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed patching col_hdr.ts_len (err=0x%x)", err);
      return false;
    }
    write_ptr += ts_comp_size;

    // (D) Compress + Write Values (based on field type)
    timeseries_field_type_e series_type = buf->points[0].field_val.type;

    size_t val_comp_size = 0;
    uint8_t *val_comp_data = NULL;
    bool val_ok = true;

    switch (series_type) {
    case TIMESERIES_FIELD_TYPE_FLOAT: {
      double *value_array = (double *)malloc(buf->count * sizeof(double));
      if (!value_array) {
        ESP_LOGE(TAG, "OOM for float array in series #%zu", i);
        return false;
      }
      for (size_t j = 0; j < buf->count; j++) {
        value_array[j] = (double)buf->points[j].field_val.data.float_val;
      }
      val_ok = compress_floats(value_array, buf->count, &val_comp_data,
                               &val_comp_size);
      free(value_array);
      break;
    }
    case TIMESERIES_FIELD_TYPE_INT: {
      int64_t *value_array = (int64_t *)malloc(buf->count * sizeof(int64_t));
      if (!value_array) {
        ESP_LOGE(TAG, "OOM for int array in series #%zu", i);
        return false;
      }
      for (size_t j = 0; j < buf->count; j++) {
        value_array[j] = (int64_t)buf->points[j].field_val.data.int_val;
      }
      val_ok = compress_int64(value_array, buf->count, &val_comp_data,
                              &val_comp_size);
      free(value_array);
      break;
    }
    case TIMESERIES_FIELD_TYPE_BOOL: {
      bool *bool_array = (bool *)malloc(buf->count * sizeof(bool));
      if (!bool_array) {
        ESP_LOGE(TAG, "OOM for bool array in series #%zu", i);
        return false;
      }
      for (size_t j = 0; j < buf->count; j++) {
        bool_array[j] = buf->points[j].field_val.data.bool_val;
      }
      val_ok = compress_bools(bool_array, buf->count, &val_comp_data,
                              &val_comp_size);
      free(bool_array);
      break;
    }
    case TIMESERIES_FIELD_TYPE_STRING:
      // Not implemented => skip or store uncompressed, etc.
      ESP_LOGW(TAG, "String compression not supported, skipping series #%zu",
               i);
      val_ok = true;
      val_comp_data = NULL;
      val_comp_size = 0;
      break;
    default:
      ESP_LOGW(TAG, "Unknown field type in series #%zu, skipping", i);
      val_ok = true;
      val_comp_data = NULL;
      val_comp_size = 0;
      break;
    }

    if (!val_ok) {
      ESP_LOGW(TAG, "Value compression failed for series #%zu", i);
      free(val_comp_data);
      return false;
    }

    // Check space, relocate if needed
    size_t needed_vals = val_comp_size;
    end_ptr = write_ptr + needed_vals;
    if (end_ptr > (region_ofs + cur_size)) {
      uint32_t new_size = cur_size * 2;
      uint32_t new_ofs = 0;
      if (!relocate_region(db, region_ofs, cur_size, new_size, &new_ofs)) {
        free(val_comp_data);
        return false;
      }
      ESP_LOGV(TAG,
               "Region grown from %u => %u, offset=0x%08" PRIx32
               " => 0x%08" PRIx32,
               (unsigned)cur_size, (unsigned)new_size, region_ofs, new_ofs);

      // Update pointers
      uint32_t offset_diff = new_ofs - region_ofs;
      write_ptr += offset_diff;
      fd_hdr_ofs += offset_diff;
      col_hdr_ofs += offset_diff;
      region_ofs = new_ofs;
      cur_size = new_size;

      end_ptr = write_ptr + needed_vals;
      if (end_ptr > (region_ofs + cur_size)) {
        ESP_LOGE(TAG, "Still not enough space for values in series #%zu", i);
        free(val_comp_data);
        return false;
      }
    }

    // Write compressed values
    if (val_comp_size > 0) {
      err = esp_partition_write(db->partition, write_ptr, val_comp_data,
                                val_comp_size);
      free(val_comp_data);
      if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed writing values (err=0x%x) for series #%zu", err,
                 i);
        return false;
      }
    } else {
      // If we skipped or have no data
      if (val_comp_data) {
        free(val_comp_data);
      }
    }
    write_ptr += val_comp_size;

    // (E) Patch the col_hdr + fd_hdr
    col_hdr.val_len = (uint16_t)val_comp_size;
    err = esp_partition_write(db->partition, col_hdr_ofs, &col_hdr,
                              sizeof(col_hdr));
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed patching col_hdr.val_len (err=0x%x)", err);
      return false;
    }

    size_t total_comp_size = sizeof(col_hdr) + col_hdr.ts_len + col_hdr.val_len;
    if (total_comp_size > 0xFFFF) {
      ESP_LOGE(TAG, "Compressed data too large (%zu) for 16-bit record_length!",
               total_comp_size);
      return false;
    }

    fd_hdr.record_length = (uint16_t)total_comp_size;
    err =
        esp_partition_write(db->partition, fd_hdr_ofs, &fd_hdr, sizeof(fd_hdr));
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed patching fd_hdr.record_length (err=0x%x)", err);
      return false;
    }

    ESP_LOGV(TAG,
             "Series #%zu => [ts:%u bytes + val:%u bytes], total=%zu, "
             "next=0x%08" PRIx32,
             i, (unsigned int)col_hdr.ts_len, (unsigned int)col_hdr.val_len,
             total_comp_size, write_ptr);
  }

  // 3) Round up final page size
  uint32_t used_bytes = write_ptr - region_ofs;
  uint32_t final_page_sz = round_up_4k(used_bytes);

  ESP_LOGV(TAG, "All series written => used=%u => final_size=%u",
           (unsigned)used_bytes, (unsigned)final_page_sz);

  // 4) Write final page header
  timeseries_page_header_t final_hdr;
  memset(&final_hdr, 0xFF, sizeof(final_hdr));
  final_hdr.magic_number = TIMESERIES_MAGIC_NUM;
  final_hdr.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
  final_hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  final_hdr.sequence_num = ++db->sequence_num;
  final_hdr.field_data_level = level;
  final_hdr.page_size = final_page_sz;

  esp_err_t err = esp_partition_write(db->partition, region_ofs, &final_hdr,
                                      sizeof(final_hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed writing final page header (err=0x%x)", err);
    return false;
  }

  ESP_LOGV(TAG, "Page created at 0x%08" PRIx32 " with size=%u", region_ofs,
           (unsigned)final_page_sz);

  // Add to page cache
  tsdb_pagecache_add_entry(db, region_ofs, &final_hdr);
  return true;
}
