// timeseries_page_stream_writer.c

#include "timeseries_page_stream_writer.h"

#include "esp_log.h"
#include "esp_partition.h"
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include "esp_timer.h"
#include "gorilla/gorilla_stream_encoder.h"  // Gorilla streaming interface
#include "timeseries_data.h"                 // for tsdb_reserve_blank_region, timeseries_blank_iterator_*
#include "timeseries_internal.h"             // definition of timeseries_db_t, etc.
#include "timeseries_iterator.h"             // for timeseries_blank_iterator_t

#include "timeseries_page_cache.h"  // for tsdb_pagecache_add_entry()
#include "timeseries_page_cache_snapshot.h"

static const char* TAG = "PAGE_STREAM_WRITER";

// Round up to next 4K
static uint32_t round_up_4k(uint32_t x) {
  const uint32_t align = 4096;
  return ((x + align - 1) / align) * align;
}

/**
 * Safely rewrite a page header with an updated page_size field.
 *
 * On NOR flash, writes can only clear bits (1->0). If the new page_size has
 * bits set that are clear in the old page_size, a plain write would silently
 * corrupt the value (hardware ANDs old & new). This function detects that case
 * and falls back to erase-and-rewrite of the first 4K sector.
 */
static bool safe_rewrite_page_header(const esp_partition_t *part,
                                     uint32_t page_offset,
                                     timeseries_page_header_t *hdr,
                                     uint32_t old_page_size) {
  uint32_t new_page_size = hdr->page_size;

  // Check NOR flash constraint: can we reach new_page_size by only clearing bits?
  if ((old_page_size & new_page_size) == new_page_size) {
    // Safe: all bits in new value are already set in old value
    return esp_partition_write(part, page_offset, hdr, sizeof(*hdr)) == ESP_OK;
  }

  // Bit-flip violation: must erase first sector and rewrite
  ESP_LOGW(TAG, "page_size 0x%" PRIx32 "->0x%" PRIx32 " requires sector erase @0x%" PRIx32,
           old_page_size, new_page_size, page_offset);

  uint8_t *sector_buf = malloc(4096);
  if (!sector_buf) {
    ESP_LOGE(TAG, "OOM allocating sector buffer for header rewrite");
    return false;
  }

  // Read the entire first sector
  if (esp_partition_read(part, page_offset, sector_buf, 4096) != ESP_OK) {
    ESP_LOGE(TAG, "Failed reading sector for header rewrite");
    free(sector_buf);
    return false;
  }

  // Patch the header in the buffer
  memcpy(sector_buf, hdr, sizeof(*hdr));

  // Erase the first sector
  if (esp_partition_erase_range(part, page_offset, 4096) != ESP_OK) {
    ESP_LOGE(TAG, "Failed erasing sector for header rewrite");
    free(sector_buf);
    return false;
  }

  // Write the sector back -- retry on failure since data is lost if we don't
  esp_err_t err = ESP_FAIL;
  for (int retry = 0; retry < 3; retry++) {
    err = esp_partition_write(part, page_offset, sector_buf, 4096);
    if (err == ESP_OK) {
      break;
    }
    ESP_LOGE(TAG, "Failed rewriting sector after erase (attempt %d/3)", retry + 1);
  }
  free(sector_buf);
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "CRITICAL: sector data lost after erase @0x%" PRIx32, page_offset);
    return false;
  }

  return true;
}

/**
 * Copy a block of flash from src_offset to dst_offset in 1KB chunks.
 */
static bool copy_flash_block(const esp_partition_t* part, uint32_t src_offset, uint32_t dst_offset, uint32_t size) {
  if (!part) {
    return false;
  }
  uint8_t* buf = (uint8_t*)malloc(1024);
  if (!buf) {
    ESP_LOGE(TAG, "OOM copying flash block");
    return false;
  }

  while (size > 0) {
    uint32_t chunk = (size > 1024) ? 1024 : size;
    if (esp_partition_read(part, src_offset, buf, chunk) != ESP_OK) {
      free(buf);
      return false;
    }
    if (esp_partition_write(part, dst_offset, buf, chunk) != ESP_OK) {
      free(buf);
      return false;
    }
    src_offset += chunk;
    dst_offset += chunk;
    size -= chunk;
  }
  free(buf);
  return true;
}

/**
 * Relocate the page region from old_offset to a new blank region of size
 * new_size. Copies old_size bytes of data.
 */
static bool relocate_region(timeseries_page_stream_writer_t* writer_ctx, uint32_t old_offset, uint32_t old_size, uint32_t new_size,
                            uint32_t* out_new_offset) {
  timeseries_db_t* db = writer_ctx->db;
  if (!db || !db->partition) {
    return false;
  }

  // 1) Reserve a new blank region (mutex-protected)
  uint32_t found_ofs = 0, found_sz = 0;
  if (!tsdb_reserve_blank_region(db, new_size, writer_ctx->batch_snapshot, &found_ofs, &found_sz)) {
    ESP_LOGE(TAG, "Failed to find blank region for size=%u", (unsigned int)new_size);
    return false;
  }

  // 2) Copy old data to new
  if (!copy_flash_block(db->partition, old_offset, found_ofs, old_size)) {
    ESP_LOGE(TAG, "Failed copying region 0x%08" PRIx32 " => 0x%08" PRIx32, old_offset, found_ofs);
    return false;
  }

  *out_new_offset = found_ofs;
  ESP_LOGV(TAG, "Relocated region 0x%08" PRIx32 " => 0x%08" PRIx32, old_offset, found_ofs);
  return true;
}

// ----------------------------------------------------------------------
// Gorilla stream flush callback & "ensure_capacity" usage
// ----------------------------------------------------------------------

/**
 * Ensures that `writer->write_ptr + needed_bytes` fits within the allocated
 * region. If needed, relocate & double capacity.
 */
static bool ensure_capacity(timeseries_page_stream_writer_t* writer, size_t needed_bytes) {
  uint32_t end_needed = writer->write_ptr + needed_bytes;
  uint32_t region_end = writer->base_offset + writer->capacity;
  if (end_needed <= region_end) {
    return true;  // enough space already
  }

  // Need to relocate. Double capacity
  uint32_t old_offset = writer->base_offset;
  uint32_t old_size = writer->capacity;
  uint32_t new_size = old_size + 4096;
  uint32_t new_offset = 0;

  ESP_LOGV(TAG, "Relocation needed: old=0x%08" PRIx32 " size %u => %u",
           old_offset, (unsigned int)old_size, (unsigned int)new_size);

  if (!relocate_region(writer, old_offset, old_size, new_size, &new_offset)) {
    return false;
  }

  ESP_LOGV(TAG, "Relocated: 0x%08" PRIx32 " => 0x%08" PRIx32, old_offset, new_offset);

  // Mark old page as OBSOLETE on flash so it won't be found after reboot
  // (ACTIVE=0x01 -> OBSOLETE=0x00 is bit-clearing, safe on NOR flash)
  timeseries_page_header_t old_hdr;
  if (esp_partition_read(writer->db->partition, old_offset, &old_hdr, sizeof(old_hdr)) == ESP_OK) {
    old_hdr.page_state = TIMESERIES_PAGE_STATE_OBSOLETE;
    esp_partition_write(writer->db->partition, old_offset, &old_hdr, sizeof(old_hdr));
  }

  // Update page cache: remove old entry, add new entry
  if (writer->batch_snapshot) {
    tsdb_pagecache_batch_remove(writer->batch_snapshot, old_offset);
  } else {
    tsdb_pagecache_remove_entry(writer->db, old_offset);
  }

  // Adjust writer pointers by the offset difference
  uint32_t diff = new_offset - old_offset;
  writer->base_offset += diff;
  writer->write_ptr += diff;
  writer->fd_hdr_offset += diff;
  writer->col_hdr_offset += diff;
  // Also adjust ts_end_offset if it has been set (after finalize_timestamp)
  if (writer->ts_end_offset != 0) {
    writer->ts_end_offset += diff;
  }
  writer->capacity = new_size;

  // Verify header was preserved at new location
  timeseries_page_header_t verify_hdr;
  if (esp_partition_read(writer->db->partition, writer->base_offset, &verify_hdr, sizeof(verify_hdr)) == ESP_OK) {
    ESP_LOGV(TAG, "After relocation: type=%u state=%u level=%u",
             verify_hdr.page_type, verify_hdr.page_state, verify_hdr.field_data_level);
    // Add to cache with new offset and updated size
    verify_hdr.page_size = new_size;
    if (writer->batch_snapshot) {
      tsdb_pagecache_batch_add(writer->batch_snapshot, new_offset, &verify_hdr);
    } else {
      tsdb_pagecache_add_entry(writer->db, new_offset, &verify_hdr);
    }
  }

  // Check again
  end_needed = writer->write_ptr + needed_bytes;
  region_end = writer->base_offset + writer->capacity;
  if (end_needed > region_end) {
    ESP_LOGE(TAG, "Relocation insufficient for needed=%zu bytes", needed_bytes);
    return false;
  }
  return true;
}

/**
 * Gorilla flush callback that writes compressed bytes directly to flash,
 * extending the region as needed.
 */
static bool gorilla_flush_to_flash(void* context, const uint8_t* data, size_t len) {
  gorilla_flush_ctx_t* ctx = (gorilla_flush_ctx_t*)context;
  timeseries_page_stream_writer_t* writer = ctx->writer;

  // Ensure we have capacity
  if (!ensure_capacity(writer, len)) {
    return false;
  }

  ESP_LOGV(TAG, "Flushing %zu bytes to flash @0x%08" PRIx32, len, writer->write_ptr);

  // Write to flash
  if (esp_partition_write(writer->db->partition, writer->write_ptr, data, len) != ESP_OK) {
    ESP_LOGE(TAG, "Failed writing Gorilla stream flush chunk");
    return false;
  }

  // Advance
  writer->write_ptr += len;
  // Accumulate total
  *(ctx->accum_bytes) += len;

  return true;
}

// ----------------------------------------------------------------------
//  Implementation of the streaming writer interface
// ----------------------------------------------------------------------

bool timeseries_page_stream_writer_init(timeseries_db_t* db, timeseries_page_stream_writer_t* writer, uint8_t to_level,
                                        uint32_t prev_data_size) {
  if (!db || !writer) {
    return false;
  }
  // Preserve batch_snapshot that may have been set before init
  tsdb_page_cache_snapshot_t *saved_batch = writer->batch_snapshot;
  memset(writer, 0, sizeof(*writer));
  writer->batch_snapshot = saved_batch;
  writer->db = db;

  int64_t start_time = esp_timer_get_time();

  // 1) Reserve a blank region (mutex-protected, registers placeholder in both snapshots)
  uint32_t initial_size = round_up_4k(prev_data_size);
  uint32_t region_ofs = 0;
  uint32_t found_page_size = 0;

  if (!tsdb_reserve_blank_region(db, initial_size, writer->batch_snapshot, &region_ofs, &found_page_size)) {
    ESP_LOGE(TAG, "Failed to find blank region (size=%u)", (unsigned int)initial_size);
    return false;
  }

  int64_t end_time = esp_timer_get_time();
  ESP_LOGI(TAG, "Reserved blank region in %.3f ms", (end_time - start_time) / 1000.0);

  // 2) Write proper page header (replaces placeholder written by reserve)
  timeseries_page_header_t hdr;
  memset(&hdr, 0xFF, sizeof(hdr));
  hdr.magic_number = TIMESERIES_MAGIC_NUM;
  hdr.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
  hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  hdr.sequence_num = ++db->sequence_num;
  hdr.field_data_level = to_level;
  hdr.page_size = initial_size;

  ESP_LOGV(TAG, "Init: Writing header @0x%08" PRIx32 " type=%u state=%u level=%u",
           region_ofs, hdr.page_type, hdr.page_state, to_level);

  if (esp_partition_write(db->partition, region_ofs, &hdr, sizeof(hdr)) != ESP_OK) {
    ESP_LOGE(TAG, "Failed writing initial page header");
    return false;
  }

  // Verify the write by reading back
  timeseries_page_header_t verify_hdr;
  if (esp_partition_read(db->partition, region_ofs, &verify_hdr, sizeof(verify_hdr)) == ESP_OK) {
    ESP_LOGV(TAG, "Init verify: type=%u state=%u level=%u",
             verify_hdr.page_type, verify_hdr.page_state, verify_hdr.field_data_level);
  }

  ESP_LOGV(TAG, "Added initial page to cache @0x%08" PRIx32, region_ofs);

  writer->base_offset = region_ofs;
  writer->write_ptr = region_ofs + sizeof(timeseries_page_header_t);
  writer->capacity = initial_size;
  writer->finalized = false;

  return true;
}

bool timeseries_page_stream_writer_begin_series(timeseries_page_stream_writer_t* writer,
                                                const unsigned char series_id[16], timeseries_field_type_e ftype) {
  if (!writer || writer->finalized) {
    return false;
  }

  // Prepare field_data_header but don't write it until the end
  memset(&writer->fd_hdr, 0xff, sizeof(writer->fd_hdr));
  memcpy(writer->fd_hdr.series_id, series_id, 16);
  writer->fd_hdr.flags &= ~TSDB_FIELDDATA_FLAG_COMPRESSED;
  writer->fd_hdr_offset = writer->write_ptr;
  writer->fd_hdr.record_count = 0;
  writer->fd_hdr.start_time = UINT64_MAX;
  writer->fd_hdr.end_time = 0;

  writer->write_ptr += sizeof(writer->fd_hdr);
  writer->col_hdr_offset = writer->write_ptr;
  writer->write_ptr += sizeof(timeseries_col_data_header_t);

  // Prepare Gorilla streams (timestamps & values)
  memset(&writer->ts_stream, 0, sizeof(writer->ts_stream));
  memset(&writer->val_stream, 0, sizeof(writer->val_stream));
  writer->ts_bytes = 0;
  writer->val_bytes = 0;
  writer->ts_end_offset = 0;  // <--- NEW field to track TS end offset
  writer->series_field_type = ftype;

  // Set up flush contexts
  writer->ts_flush_ctx.writer = writer;
  writer->ts_flush_ctx.accum_bytes = &writer->ts_bytes;

  writer->val_flush_ctx.writer = writer;
  writer->val_flush_ctx.accum_bytes = &writer->val_bytes;

  // Initialize Gorilla streams
  // For timestamps we use GORILLA_STREAM_INT (64-bit integer deltas)
  if (!gorilla_stream_init(&writer->ts_stream, GORILLA_STREAM_INT,
                           /*initial_xor=*/NULL, 0 /*leading_zeros*/, gorilla_flush_to_flash, &writer->ts_flush_ctx)) {
    ESP_LOGE(TAG, "Failed to init Gorilla ts_stream");
    return false;
  }

  // For values, pick an appropriate Gorilla mode
  gorilla_stream_type_t val_mode = GORILLA_STREAM_INT;  // default
  switch (ftype) {
    case TIMESERIES_FIELD_TYPE_FLOAT:
      val_mode = GORILLA_STREAM_FLOAT;
      break;
    case TIMESERIES_FIELD_TYPE_BOOL:
      val_mode = GORILLA_STREAM_BOOL;
      break;
    case TIMESERIES_FIELD_TYPE_STRING:
      ESP_LOGI(TAG, "Setting val_mode to GORILLA_STREAM_STRING");
      val_mode = GORILLA_STREAM_STRING;
      break;
    default:
      val_mode = GORILLA_STREAM_INT;
      break;
  }

  if (!gorilla_stream_init(&writer->val_stream, val_mode,
                           /*initial_xor=*/NULL, 0 /*leading_zeros*/, gorilla_flush_to_flash, &writer->val_flush_ctx)) {
    ESP_LOGE(TAG, "Failed to init Gorilla val_stream");
    // best-effort cleanup
    gorilla_stream_deinit(&writer->ts_stream);
    return false;
  }

  return true;
}

/*
 * Write only the timestamp portion for a new data point.
 * Updates min/max timestamps and increments record_count.
 */
bool timeseries_page_stream_writer_write_timestamp(timeseries_page_stream_writer_t* writer, uint64_t ts) {
  if (!writer || writer->finalized) {
    return false;
  }

  // Update min/max timestamps
  if ((writer->fd_hdr.record_count == 0) || (ts < writer->fd_hdr.start_time)) {
    writer->fd_hdr.start_time = ts;
  }

  if (ts > writer->fd_hdr.end_time) {
    writer->fd_hdr.end_time = ts;
  }

  // Add timestamp to the Gorilla TS stream
  if (!gorilla_stream_add_timestamp(&writer->ts_stream, ts)) {
    ESP_LOGE(TAG, "Failed adding timestamp to gorilla ts_stream");
    return false;
  }

  // Increment record count AFTER successful write
  writer->fd_hdr.record_count++;

  return true;
}

/*
 * ----------------------------------------------------------------------------
 * IMPLEMENTATION of timeseries_page_stream_writer_finalize_timestamp()
 * ----------------------------------------------------------------------------
 * Finishes the Gorilla TS stream and records the offset at which timestamps
 * end. DOES NOT finalize or write the column header yet; that’s done in
 * end_series along with the value data.
 */
bool timeseries_page_stream_writer_finalize_timestamp(timeseries_page_stream_writer_t* writer) {
  if (!writer) {
    ESP_LOGE(TAG, "Invalid writer");
    return false;
  }

  // Finalize the Gorilla TS stream (flush final bits to flash)
  if (!gorilla_stream_finish(&writer->ts_stream)) {
    ESP_LOGE(TAG, "Failed finishing gorilla ts_stream");
    return false;
  }
  // Capture the offset where TS data ends
  writer->ts_end_offset = writer->write_ptr;
  gorilla_stream_deinit(&writer->ts_stream);

  // We can log or check for overflow here if desired.
  // For example, the user might want to confirm that `ts_bytes <= 65535`.
  if (writer->ts_bytes > 0xFFFF) {
    ESP_LOGE(TAG, "TS bytes overflowed 16-bit limit in finalize_timestamp()");
    return false;
  }

  ESP_LOGV(TAG,
           "Series %02X%02X%02X%02X: finalize_timestamp => ts=%u bytes "
           "(offset=0x%08" PRIx32 ")",
           writer->fd_hdr.series_id[0], writer->fd_hdr.series_id[1], writer->fd_hdr.series_id[2],
           writer->fd_hdr.series_id[3], (unsigned int)writer->ts_bytes, writer->ts_end_offset);

  // Make sure the compressed buffer now

  return true;
}

/**
 * Write only the value portion for the previously added timestamp.
 * (Assumes a 1:1 mapping between timestamps and values.)
 */
bool timeseries_page_stream_writer_write_value(timeseries_page_stream_writer_t* writer,
                                               const timeseries_field_value_t* fv) {
  if (!writer || !fv || writer->finalized) {
    return false;
  }

  // Add value to the Gorilla VAL stream
  switch (writer->series_field_type) {
    case TIMESERIES_FIELD_TYPE_INT: {
      int64_t i64 = fv->data.int_val;
      // Reusing add_timestamp() for storing the integer,
      // or you might have a dedicated gorilla_stream_add_int64() call.
      if (!gorilla_stream_add_timestamp(&writer->val_stream, (uint64_t)i64)) {
        ESP_LOGE(TAG, "Failed adding INT value to val_stream");
        return false;
      }
      break;
    }
    case TIMESERIES_FIELD_TYPE_FLOAT: {
      double d = fv->data.float_val;
      if (!gorilla_stream_add_float(&writer->val_stream, d)) {
        ESP_LOGE(TAG, "Failed adding FLOAT value to val_stream");
        return false;
      }
      break;
    }
    case TIMESERIES_FIELD_TYPE_BOOL: {
      bool b = fv->data.bool_val;
      if (!gorilla_stream_add_boolean(&writer->val_stream, b)) {
        ESP_LOGE(TAG, "Failed adding BOOL value to val_stream");
        return false;
      }
      break;
    }
    case TIMESERIES_FIELD_TYPE_STRING: {
      if (!gorilla_stream_add_string(&writer->val_stream, fv->data.string_val.str, fv->data.string_val.length)) {
        ESP_LOGE(TAG, "Failed adding STRING value to val_stream");
        return false;
      }

      break;
    }
    default:
      // Not supported in this compression path
      ESP_LOGV(TAG, "Unsupported field type %d for streaming compression", (int)writer->series_field_type);
      return false;
  }

  return true;
}

/*
 * ----------------------------------------------------------------------------
 * timeseries_page_stream_writer_end_series()
 * NOW does NOT finalize timestamps. That is done by finalize_timestamp().
 * We *only* finalize the VALUE stream here.
 * ----------------------------------------------------------------------------
 */
bool timeseries_page_stream_writer_end_series(timeseries_page_stream_writer_t* writer) {
  if (!writer) {
    ESP_LOGE(TAG, "Invalid writer");
    return false;
  }

  /*
   * 1) We have removed the timestamp finalization code.
   *    finalize_timestamp() should have been called earlier.
   */

  // 2) Finalize the Gorilla VAL stream (flush final bits to flash)
  if (!gorilla_stream_finish(&writer->val_stream)) {
    ESP_LOGE(TAG, "Failed finishing gorilla val_stream");
    return false;
  }
  // Capture the end offset for values
  uint32_t val_end = writer->write_ptr;
  gorilla_stream_deinit(&writer->val_stream);

  uint32_t ts_len = writer->ts_bytes;       // as recorded
  uint32_t ts_end = writer->ts_end_offset;  // offset where TS ended
  uint32_t val_len = val_end - ts_end;      // compute difference

  // 4) Patch the col_data_header with the correct lengths
  timeseries_col_data_header_t col_hdr;
  col_hdr.ts_len = ts_len;
  col_hdr.val_len = val_len;

  ESP_LOGV(TAG, "Series %02X%02X%02X%02X: end_series => ts=%u, val=%u bytes", writer->fd_hdr.series_id[0],
           writer->fd_hdr.series_id[1], writer->fd_hdr.series_id[2], writer->fd_hdr.series_id[3],
           (unsigned int)col_hdr.ts_len, (unsigned int)col_hdr.val_len);

  if (esp_partition_write(writer->db->partition, writer->col_hdr_offset, &col_hdr, sizeof(col_hdr)) != ESP_OK) {
    ESP_LOGE(TAG, "Failed patching col_data_header");
    return false;
  }

  // 5) If no points were added, fix the header's timestamps
  if (writer->fd_hdr.record_count == 0) {
    ESP_LOGV(TAG, "Series %02X%02X%02X%02X... has no points", writer->fd_hdr.series_id[0], writer->fd_hdr.series_id[1],
             writer->fd_hdr.series_id[2], writer->fd_hdr.series_id[3]);
    writer->fd_hdr.start_time = 0;
    writer->fd_hdr.end_time = 0;
  }

  // 6) The "record_length" is total space for col_hdr + TS + VAL
  uint32_t total_col_bytes = sizeof(col_hdr) + ts_len + val_len;
  if (total_col_bytes > 0xFFFF) {
    ESP_LOGE(TAG, "Series chunk = %u bytes (ts=%u, val=%u), exceeds 16-bit limit!",
             (unsigned int)total_col_bytes, (unsigned int)ts_len, (unsigned int)val_len);
    return false;
  }
  writer->fd_hdr.record_length = (uint16_t)total_col_bytes;

  ESP_LOGI(TAG, "Series %02X%02X%02X%02X: record_count=%u, record_length=%u bytes (ts=%u, val=%u), times=[%" PRIu64 "-%" PRIu64 "]",
           writer->fd_hdr.series_id[0], writer->fd_hdr.series_id[1],
           writer->fd_hdr.series_id[2], writer->fd_hdr.series_id[3],
           (unsigned int)writer->fd_hdr.record_count, (unsigned int)writer->fd_hdr.record_length,
           (unsigned int)ts_len, (unsigned int)val_len,
           writer->fd_hdr.start_time, writer->fd_hdr.end_time);

  ESP_LOGV(TAG, "Header offset=0x%08" PRIx32 ", col_hdr=0x%08" PRIx32, writer->fd_hdr_offset, writer->col_hdr_offset);

  // 7) Patch the field_data_header with the updated metadata
  if (esp_partition_write(writer->db->partition, writer->fd_hdr_offset, &writer->fd_hdr, sizeof(writer->fd_hdr)) !=
      ESP_OK) {
    ESP_LOGE(TAG, "Failed patching field_data_header");
    return false;
  }

  return true;
}

bool timeseries_page_stream_writer_finalize(timeseries_page_stream_writer_t* writer) {
  if (!writer || writer->finalized) {
    return false;
  }
  writer->finalized = true;

  // Figure out how many bytes we used
  uint32_t used_bytes = writer->write_ptr - writer->base_offset;
  uint32_t final_size = round_up_4k(used_bytes);

  // Read existing page header
  timeseries_page_header_t hdr;
  if (esp_partition_read(writer->db->partition, writer->base_offset, &hdr, sizeof(hdr)) != ESP_OK) {
    ESP_LOGE(TAG, "Failed reading existing page header");
    return false;
  }

  // Patch page_size using NOR-flash-safe rewrite
  uint32_t old_page_size = hdr.page_size;
  hdr.page_size = final_size;

  if (!safe_rewrite_page_header(writer->db->partition, writer->base_offset, &hdr, old_page_size)) {
    ESP_LOGE(TAG, "Failed patching final page header");
    return false;
  }

  ESP_LOGI(TAG, "Finalize: Page @0x%08" PRIx32 " size=%" PRIu32 " type=%u state=%u level=%u",
           writer->base_offset, final_size, hdr.page_type, hdr.page_state, hdr.field_data_level);

  float page_utilization = (float)used_bytes / final_size;
  ESP_LOGI(TAG, "Page utilization: %.2f%%. page_size=%u level=%u", page_utilization * 100.0, (unsigned int)final_size,
           (unsigned int)hdr.field_data_level);

  // Update page cache entry with final header (batch_add updates in-place if offset exists)
  if (writer->batch_snapshot) {
    tsdb_pagecache_batch_add(writer->batch_snapshot, writer->base_offset, &hdr);
    tsdb_pagecache_batch_sort(writer->batch_snapshot);
    ESP_LOGI(TAG, "Updated batch page cache entry @0x%08" PRIx32,
             writer->base_offset);
  } else {
    tsdb_pagecache_add_entry(writer->db, writer->base_offset, &hdr);
    ESP_LOGI(TAG, "Updated page cache entry @0x%08" PRIx32,
             writer->base_offset);
  }

  return true;
}
