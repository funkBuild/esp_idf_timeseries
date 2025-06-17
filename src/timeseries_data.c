#include "timeseries_data.h"
#include "esp_log.h"
#include "esp_partition.h"
#include "mbedtls/md5.h"
#include "timeseries_compaction.h" // for timeseries_compact_level0_pages()
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "TimeseriesData";

// Forward declarations
static bool find_level0_page_with_space(timeseries_db_t *db,
                                        size_t bytes_needed,
                                        bool *out_any_l0_exists,
                                        uint32_t *out_page_offset,
                                        uint32_t *out_used_offset);
static bool create_level0_field_page(timeseries_db_t *db, uint32_t *out_offset);

// Writes a multi-point record: [field_data_header_t + data].
static bool write_points_to_page(timeseries_db_t *db, uint32_t page_offset,
                                 uint32_t used_offset,
                                 const unsigned char series_id[16],
                                 const uint64_t *timestamps,
                                 const timeseries_field_value_t *vals,
                                 size_t npoints);

/**
 * @brief Returns the uncompressed size for a single field value (in a column).
 */

static size_t
field_value_uncompressed_size(const timeseries_field_value_t *val) {
  switch (val->type) {
  case TIMESERIES_FIELD_TYPE_FLOAT:
  case TIMESERIES_FIELD_TYPE_INT:
    return 8; // store as 8-byte double or int64
  case TIMESERIES_FIELD_TYPE_BOOL:
    return 1; // store as 0/1
  case TIMESERIES_FIELD_TYPE_STRING:
    // store a 4-byte length + the string data
    return 4 + val->data.string_val.length;
  }
  return 0;
}

/**
 * @brief Write the uncompressed value into `out_buf` (which must be big
 * enough). Returns how many bytes were written.
 */
static size_t
field_value_write_uncompressed(const timeseries_field_value_t *val,
                               uint8_t *out_buf) {
  size_t written = 0;
  switch (val->type) {
  case TIMESERIES_FIELD_TYPE_FLOAT: {
    double d = (double)val->data.float_val;
    memcpy(out_buf, &d, 8);
    written = 8;
    break;
  }
  case TIMESERIES_FIELD_TYPE_INT: {
    int64_t i64 = (int64_t)val->data.int_val;
    memcpy(out_buf, &i64, 8);
    written = 8;
    break;
  }
  case TIMESERIES_FIELD_TYPE_BOOL: {
    out_buf[0] = val->data.bool_val ? 1 : 0;
    written = 1;
    break;
  }
  case TIMESERIES_FIELD_TYPE_STRING: {
    uint32_t len = (uint32_t)val->data.string_val.length;
    memcpy(out_buf, &len, 4);
    written = 4;
    if (len > 0) {
      memcpy(out_buf + 4, val->data.string_val.str, len);
      written += len;
    }
    break;
  }
  }
  return written;
}

/**
 * @brief Calculate bytes needed for a 'columnar' uncompressed record of
 * `npoints`.
 *
 * The final layout is:
 *   timeseries_field_data_header_t +
 *   timeseries_col_data_header_t +
 *   (npoints * 8 for timestamps) +
 *   sum of uncompressed sizes of all values.
 */
static size_t
calc_bytes_needed_for_npoints_columnar(size_t npoints,
                                       const timeseries_field_value_t *vals) {
  size_t overhead = sizeof(timeseries_field_data_header_t) +
                    sizeof(timeseries_col_data_header_t);

  // Timestamps always 8 bytes each
  size_t timestamps_sz = npoints * 8;

  // Sum of value sizes
  size_t values_sz = 0;
  for (size_t i = 0; i < npoints; i++) {
    values_sz += field_value_uncompressed_size(&vals[i]);
  }

  return overhead + timestamps_sz + values_sz;
}

/**
 * @brief Append multiple data points (timestamps+values) to level-0 pages,
 *        splitting if needed. If none exist, create a new L0 page.
 *        If all are full, run compaction, then create a new page.
 */
bool tsdb_append_multiple_points(timeseries_db_t *db,
                                 const unsigned char series_id[16],
                                 const uint64_t *timestamps,
                                 const timeseries_field_value_t *values,
                                 size_t count) {
  if (!db || !series_id || !timestamps || !values || (count == 0)) {
    ESP_LOGE(TAG, "Invalid arguments to tsdb_append_multiple_points.");
    return false;
  }

  ESP_LOGV(TAG,
           "tsdb_append_multiple_points: series=%.2X%.2X%.2X%.2X..., count=%zu",
           series_id[0], series_id[1], series_id[2], series_id[3], count);

  size_t remaining = count;
  size_t idx = 0;

  while (remaining > 0) {
    size_t npoints_possible = remaining;

    while (npoints_possible > 0) {
      size_t bytes_needed = calc_bytes_needed_for_npoints_columnar(
          npoints_possible, values + idx);

      bool any_l0_exists = false;
      uint32_t page_offset = 0, used_offset = 0;

      if (find_level0_page_with_space(db, bytes_needed, &any_l0_exists,
                                      &page_offset, &used_offset)) {
        // We have space => write
        if (!write_points_to_page(db, page_offset, used_offset, series_id,
                                  timestamps + idx, values + idx,
                                  npoints_possible)) {
          ESP_LOGE(TAG, "Failed writing points to L0=0x%08" PRIx32,
                   page_offset);
          return false;
        }
        idx += npoints_possible;
        remaining -= npoints_possible;
        break;
      } else {
        // No page can hold npoints_possible
        if (!any_l0_exists) {
          // no L0 pages => create one
          ESP_LOGV(TAG, "No existing L0 pages => creating one...");
          uint32_t new_page_offset = 0;
          if (!create_level0_field_page(db, &new_page_offset)) {
            ESP_LOGE(TAG, "Failed to create L0 page; none existed.");
            return false;
          }
          // now try again
          continue;
        } else {
          // L0 pages exist but all are full
          if (npoints_possible == 1) {
            // can't store even 1 => compaction + new L0
            ESP_LOGV(TAG, "All L0 full => compaction + new page.");
            if (!timeseries_compact_all_levels(db)) {
              ESP_LOGE(TAG, "Compaction failed, can't proceed.");
              return false;
            }
            uint32_t new_page_offset = 0;
            if (!create_level0_field_page(db, &new_page_offset)) {
              ESP_LOGE(TAG, "Failed to create new L0 after compaction.");
              return false;
            }
            continue;
          } else {
            // reduce npoints_possible
            npoints_possible /= 2;
          }
        }
      }
    } // end while (npoints_possible > 0)

    if (npoints_possible == 0) {
      ESP_LOGE(TAG, "Unable to store any points. Out of space?");
      return false;
    }
  } // end while (remaining > 0)

  ESP_LOGV(TAG, "Successfully appended all %zu points to L0 pages", count);
  return true;
}

/**
 * @brief Finds or creates an L0 page that has enough space for `bytes_needed`.
 */
static bool find_level0_page_with_space(timeseries_db_t *db,
                                        size_t bytes_needed,
                                        bool *out_any_l0_exists,
                                        uint32_t *out_page_offset,
                                        uint32_t *out_used_offset) {
  if (!db || !out_any_l0_exists || !out_page_offset || !out_used_offset) {
    return false;
  }

  *out_any_l0_exists = false;

  // ---------------------------------------
  // 1) Quick check against cached L0 page
  // ---------------------------------------
  if (db->last_l0_cache_valid) {
    timeseries_page_header_t hdr;
    esp_err_t err = esp_partition_read(db->partition, db->last_l0_page_offset,
                                       &hdr, sizeof(hdr));
    if (err == ESP_OK) {
      // Confirm the cached page is still a valid, active L0 field-data page
      if (hdr.magic_number == TIMESERIES_MAGIC_NUM &&
          hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
          hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE &&
          hdr.field_data_level == 0) {
        *out_any_l0_exists = true;          // At least one L0 page exists
        uint32_t page_size = hdr.page_size; // from the header
        uint32_t free_space = page_size - db->last_l0_used_offset;

        if (free_space >= bytes_needed) {
          // We can use the cached page immediately
          *out_page_offset = db->last_l0_page_offset;
          *out_used_offset = db->last_l0_used_offset;
          return true;
        }
      }
    }
    // If we got here, the cached page is invalid or no longer suitable
    // => fallback to the full scan below
    db->last_l0_cache_valid = false;
  }

  // ---------------------------------------
  // 2) Fallback: scan all pages in the cache
  // ---------------------------------------
  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    return false;
  }

  bool found_space = false;
  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM &&
        hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE &&
        hdr.field_data_level == 0) {
      *out_any_l0_exists = true; // We have at least one L0 page

      // Determine how many bytes have been used in this L0 page
      // by iterating its field-data records (the existing method):
      timeseries_fielddata_iterator_t f_iter;
      if (!timeseries_fielddata_iterator_init(db, page_offset, page_size,
                                              &f_iter)) {
        continue; // skip on error
      }

      uint32_t last_offset = sizeof(timeseries_page_header_t);
      timeseries_field_data_header_t f_hdr;

      while (timeseries_fielddata_iterator_next(&f_iter, &f_hdr)) {
        last_offset = f_iter.offset;
      }

      uint32_t free_space = page_size - last_offset;
      if (free_space >= bytes_needed) {
        // Found a suitable page
        *out_page_offset = page_offset;
        *out_used_offset = last_offset;
        found_space = true;

        // Update the DB cache for next time
        db->last_l0_cache_valid = true;
        db->last_l0_page_offset = page_offset;
        db->last_l0_used_offset = last_offset;
        break;
      }
    }
  }

  return found_space;
}

/**
 * @brief Creates a new L0 page using the blank iterator to find space.
 */

static bool create_level0_field_page(timeseries_db_t *db,
                                     uint32_t *out_offset) {
  if (!db || !db->partition || !out_offset) {
    return false;
  }

  uint32_t l0_size = TIMESERIES_FIELD_DATA_PAGE_SIZE;

  // 1) Find a blank region
  timeseries_blank_iterator_t blank_iter;
  if (!timeseries_blank_iterator_init(db, &blank_iter, l0_size)) {
    ESP_LOGE(TAG, "Failed to init blank iterator for L0 page creation.");
    return false;
  }

  uint32_t blank_offset = 0;
  uint32_t blank_length = 0;
  if (!timeseries_blank_iterator_next(&blank_iter, &blank_offset,
                                      &blank_length)) {
    ESP_LOGW(TAG, "No blank region found to create L0 page (size=%u).",
             (unsigned int)l0_size);
    return false;
  }

  if (blank_offset + l0_size > db->partition->size) {
    ESP_LOGW(TAG, "Blank region extends beyond partition boundary!");
    return false;
  }

  // 2) Erase it
  esp_err_t err =
      esp_partition_erase_range(db->partition, blank_offset, l0_size);
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to erase region for L0 page (err=0x%x)", err);
    return false;
  }

  // 3) Write new page header
  timeseries_page_header_t new_hdr;
  memset(&new_hdr, 0xFF, sizeof(new_hdr));
  new_hdr.magic_number = TIMESERIES_MAGIC_NUM;
  new_hdr.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
  new_hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  new_hdr.sequence_num = ++db->sequence_num;
  new_hdr.field_data_level = 0;
  new_hdr.page_size = l0_size;

  err = esp_partition_write(db->partition, blank_offset, &new_hdr,
                            sizeof(new_hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to write L0 header (err=0x%x)", err);
    return false;
  }

  *out_offset = blank_offset;
  ESP_LOGV(TAG, "Created new level-0 page @0x%08" PRIx32 " (size=%u bytes).",
           blank_offset, (unsigned int)l0_size);

  // 4) Update the page cache system
  tsdb_pagecache_add_entry(db, blank_offset, &new_hdr);

  // 5) Update the "last L0" cache
  db->last_l0_cache_valid = true;
  db->last_l0_page_offset = blank_offset;
  db->last_l0_used_offset = sizeof(timeseries_page_header_t);

  return true;
}

/**
 * @brief Write one multi-point field data entry using
 * timeseries_field_data_header_t, storing how many bytes follow in
 * 'record_length'.
 */
static bool write_points_to_page(timeseries_db_t *db, uint32_t page_offset,
                                 uint32_t used_offset,
                                 const unsigned char series_id[16],
                                 const uint64_t *timestamps,
                                 const timeseries_field_value_t *vals,
                                 size_t npoints) {
  // 1) Prepare the field_data_header
  timeseries_field_data_header_t fhdr;
  memset(&fhdr, 0xff, sizeof(fhdr));
  // Note flags is deliberately left as 0xFF (all bits set)

  fhdr.record_count = (uint16_t)npoints;
  fhdr.start_time = timestamps[0];
  fhdr.end_time = timestamps[npoints - 1];
  memcpy(fhdr.series_id, series_id, 16);

  // 2) Compute size of timestamps + values
  size_t ts_size = npoints * 8;
  size_t val_size = 0;
  for (size_t i = 0; i < npoints; i++) {
    val_size += field_value_uncompressed_size(&vals[i]);
  }

  // Column header
  timeseries_col_data_header_t col_hdr;
  memset(&col_hdr, 0, sizeof(col_hdr));
  col_hdr.ts_len = (uint16_t)ts_size;
  col_hdr.val_len = (uint16_t)val_size;

  size_t total_col_data = sizeof(col_hdr) + ts_size + val_size;
  if (total_col_data > 0xFFFF) {
    ESP_LOGE(TAG,
             "Record too big for 16-bit field_data_header.record_length: %zu",
             total_col_data);
    return false;
  }
  fhdr.record_length = (uint16_t)total_col_data;

  // 3) Allocate a buffer for [col_hdr + timestamps + values]
  uint8_t *col_buf = (uint8_t *)malloc(total_col_data);
  if (!col_buf) {
    return false;
  }

  // 4) Fill col_buf
  size_t buf_offset = 0;
  memcpy(col_buf + buf_offset, &col_hdr, sizeof(col_hdr));
  buf_offset += sizeof(col_hdr);

  memcpy(col_buf + buf_offset, timestamps, ts_size);
  buf_offset += ts_size;

  for (size_t i = 0; i < npoints; i++) {
    buf_offset +=
        field_value_write_uncompressed(&vals[i], col_buf + buf_offset);
  }

  // 5) Write fhdr + col_buf to flash
  uint32_t write_addr = page_offset + used_offset;
  esp_err_t err =
      esp_partition_write(db->partition, write_addr, &fhdr, sizeof(fhdr));
  if (err != ESP_OK) {
    free(col_buf);
    return false;
  }
  write_addr += sizeof(fhdr);

  err = esp_partition_write(db->partition, write_addr, col_buf, total_col_data);
  free(col_buf);

  if (err != ESP_OK) {
    return false;
  }

  // 6) Update the "last used offset" if this is indeed the last-l0 page
  //    (It should be, but double-check offset matches the db cache.)
  if (db->last_l0_cache_valid && (db->last_l0_page_offset == page_offset)) {
    db->last_l0_used_offset = used_offset + sizeof(fhdr) + fhdr.record_length;
  }

  ESP_LOGV(TAG, "Wrote %zu pts @0x%08" PRIx32 " -> next=0x%08" PRIx32, npoints,
           (page_offset + used_offset),
           (page_offset + used_offset + sizeof(fhdr) + fhdr.record_length));

  return true;
}