#include "timeseries_data.h"
#include "timeseries_revision.h"
#include "esp_log.h"
#include "esp_partition.h"
#include "mbedtls/md.h"
#include "timeseries_compaction.h" // for timeseries_compact_level0_pages()
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

#include <inttypes.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "TimeseriesData";

// ---------------------------------------------------------------------------
// Centralized blank-region allocation (serialized by region_alloc_mutex)
// ---------------------------------------------------------------------------
bool tsdb_reserve_blank_region(timeseries_db_t *db, uint32_t min_size,
                                tsdb_page_cache_snapshot_t *batch_snapshot,
                                uint32_t *out_offset, uint32_t *out_size) {
    if (!db || !db->partition || !db->region_alloc_mutex || !db->snapshot_mutex || !out_offset || !out_size ||
        !min_size || min_size > UINT32_MAX - 4095) return false;
    uint32_t reserve_size = ((min_size + 4095) / 4096) * 4096;
    if (reserve_size > db->partition->size) return false;
    xSemaphoreTake(db->region_alloc_mutex, portMAX_DELAY);

    // All reservations are published in the live snapshot while this mutex is
    // held. A compaction batch may predate an insert, so it cannot be used as
    // the allocator's source of truth.
    timeseries_blank_iterator_t blank_iter;
    if (batch_snapshot && !tsdb_pagecache_batch_reserve(batch_snapshot, 1)) {
        xSemaphoreGive(db->region_alloc_mutex);
        return false;
    }
    if (!timeseries_blank_iterator_init(db, &blank_iter, reserve_size)) {
        xSemaphoreGive(db->region_alloc_mutex);
        return false;
    }

    uint32_t region_ofs = 0, region_size = 0;
    if (!timeseries_blank_iterator_next(&blank_iter, &region_ofs, &region_size)) {
        timeseries_blank_iterator_deinit(&blank_iter);
        xSemaphoreGive(db->region_alloc_mutex);
        return false;
    }
    timeseries_blank_iterator_deinit(&blank_iter);

    // Erase only the reserved portion
    if (esp_partition_erase_range(db->partition, region_ofs, reserve_size) != ESP_OK) {
        xSemaphoreGive(db->region_alloc_mutex);
        return false;
    }

    // Register in both snapshots so neither path can double-allocate
    // Create a placeholder header for registration
    timeseries_page_header_t placeholder_hdr;
    memset(&placeholder_hdr, 0xFF, sizeof(placeholder_hdr));
    placeholder_hdr.magic_number = TIMESERIES_MAGIC_NUM;
    placeholder_hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
    placeholder_hdr.page_size = reserve_size;
    // Sequence 0 (real pages start at 1): a placeholder is always superseded by
    // the real header written at finalize. The commit-batch merge resolves a
    // shared offset by newest sequence_num, so the placeholder must compare as
    // OLDEST — leaving it at the memset 0xFFFFFFFF would make it wrongly win and
    // clobber the real header, hiding the finalized page from readers.
    placeholder_hdr.sequence_num = 0;

    // Publish live first. On failure the caller's batch still has its original
    // entries/removal history. Preallocated private storage cannot fail below.
    if (!tsdb_pagecache_add_entry(db, region_ofs, &placeholder_hdr)) {
        xSemaphoreGive(db->region_alloc_mutex);
        return false;
    }
    if (batch_snapshot) {
        tsdb_pagecache_batch_add(batch_snapshot, region_ofs, &placeholder_hdr);
    }

    *out_offset = region_ofs;
    *out_size = reserve_size;
    xSemaphoreGive(db->region_alloc_mutex);
    return true;
}

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
                                 size_t npoints, const size_t *indices, size_t bytes_needed);

/**
 * @brief Returns the uncompressed size for a single field value (in a column).
 */

/* Effective payload length of a string field.
 *
 * A NULL str with a non-zero length would make the writer memcpy from NULL.
 * Both the sizing and writing paths below route through this so they cannot
 * disagree about how many bytes the record holds — a mismatch there would
 * either truncate the record or overrun the buffer the size call reserved. */
static size_t field_value_string_len(const timeseries_field_value_t *val) {
  if (val->data.string_val.str == NULL) {
    return 0;
  }
  return val->data.string_val.length;
}

bool tsdb_validate_point_values(const timeseries_field_value_t *values, size_t count) {
  const size_t max_string = TIMESERIES_FIELD_DATA_PAGE_SIZE -
      sizeof(timeseries_page_header_t) - sizeof(timeseries_field_data_header_t) -
      sizeof(timeseries_col_data_header_t) - sizeof(uint64_t) - sizeof(uint32_t) - sizeof(uint64_t);
  if (!values || count > SIZE_MAX / sizeof(*values)) return false;
  for (size_t i = 0; i < count; i++) {
    if (values[i].type < TIMESERIES_FIELD_TYPE_FLOAT ||
        values[i].type > TIMESERIES_FIELD_TYPE_STRING) return false;
    if (values[i].type == TIMESERIES_FIELD_TYPE_STRING &&
        field_value_string_len(&values[i]) > max_string) return false;
  }
  return true;
}

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
    return 4 + field_value_string_len(val);
  default:
    ESP_LOGE("TimeseriesData", "Unknown field type %d in uncompressed_size", val->type);
    return 0;
  }
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
    /* Must match field_value_uncompressed_size exactly: the caller sized
     * out_buf from that function, so any divergence here overruns it. */
    uint32_t len = (uint32_t)field_value_string_len(val);
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
                                       const timeseries_field_value_t *vals,
                                       const size_t *indices) {
  size_t overhead = sizeof(timeseries_field_data_header_t) +
                    sizeof(timeseries_col_data_header_t);

  // Timestamps always 8 bytes each
  size_t timestamps_sz = npoints * 8;

  // Sum of value sizes
  size_t values_sz = 0;
  for (size_t i = 0; i < npoints; i++) {
    values_sz += field_value_uncompressed_size(&vals[indices ? indices[i] : i]);
  }

  return overhead + timestamps_sz + values_sz + sizeof(uint64_t);
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
  if (db->chunk_size == 0 || count > SIZE_MAX / sizeof(*values)) {
    return false;
  }

  tsdb_point_order_t order;
  if (!tsdb_point_order_init(timestamps, count, &order)) return false;
  bool ok = tsdb_append_ordered_points(db, series_id, timestamps, values, &order);
  tsdb_point_order_deinit(&order);
  return ok;
}

bool tsdb_append_ordered_points(timeseries_db_t *db, const unsigned char series_id[16],
                                const uint64_t *timestamps, const timeseries_field_value_t *values,
                                const tsdb_point_order_t *order) {
  if (!db || !series_id || !timestamps || !values || !order || !order->count || !db->chunk_size ||
      order->count > order->source_count ||
      (!order->indices && order->count != order->source_count) ||
      !tsdb_validate_point_values(values, order->source_count)) return false;
  if (order->indices) {
    for (size_t i = 0; i < order->count; i++)
      if (order->indices[i] >= order->source_count) return false;
  }
  size_t count = order->count;
  ESP_LOGV(TAG,
           "tsdb_append_multiple_points: series=%.2X%.2X%.2X%.2X..., count=%zu",
           series_id[0], series_id[1], series_id[2], series_id[3], count);

  // Use configurable chunk size from DB context
  const size_t chunk_size = db->chunk_size;

  size_t remaining = count;
  size_t idx = 0;

  while (remaining > 0) {
    size_t npoints_possible = (remaining > chunk_size) ? chunk_size : remaining;

    // Log progress for large inserts
    if (count > chunk_size && remaining % 500 == 0) {
      ESP_LOGI(TAG, "Insert progress: %zu/%zu points completed",
               count - remaining, count);
    }

    // Size caller-owned input before taking the flash mutex. Reuse this
    // result through page allocation and serialization instead of rescanning.
    size_t bytes_needed = calc_bytes_needed_for_npoints_columnar(
        npoints_possible, order->indices ? values : values + idx,
        order->indices ? order->indices + idx : NULL);

    // Take flash write mutex to protect L0 page finding/creation/writing
    if (db->flash_write_mutex) {
      xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY);
    }

    while (npoints_possible > 0) {
      bool any_l0_exists = false;
      uint32_t page_offset = 0, used_offset = 0;

      if (find_level0_page_with_space(db, bytes_needed, &any_l0_exists,
                                      &page_offset, &used_offset)) {
        // We have space => write
        ESP_LOGV(TAG, "Writing %zu points to page @0x%08" PRIX32 " (bytes needed: %zu)",
                 npoints_possible, page_offset, bytes_needed);
        if (!write_points_to_page(db, page_offset, used_offset, series_id,
                                  order->indices ? timestamps : timestamps + idx,
                                  order->indices ? values : values + idx,
                                  npoints_possible, order->indices ? order->indices + idx : NULL, bytes_needed)) {
          ESP_LOGE(TAG, "Failed writing points to L0=0x%08" PRIx32,
                   page_offset);
          if (db->flash_write_mutex) {
            xSemaphoreGive(db->flash_write_mutex);
          }
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
            // No space — trigger background compaction and retry once
            ESP_LOGW(TAG, "No space for L0 page, triggering background compaction...");
            if (db->flash_write_mutex) {
              xSemaphoreGive(db->flash_write_mutex);
            }
            // Launch compaction on background task (dedicated stack) and wait
            tsdb_launch_compaction(db);
            // Wait for compaction to finish (already running or just launched)
            int wait = 10000;
            while (atomic_load(&db->compaction_in_progress) && wait > 0) {
              vTaskDelay(pdMS_TO_TICKS(10));
              wait -= 10;
            }
            if (db->flash_write_mutex) {
              xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY);
            }
            if (!create_level0_field_page(db, &new_page_offset)) {
              ESP_LOGE(TAG, "Failed to create L0 page even after compaction.");
              if (db->flash_write_mutex) {
                xSemaphoreGive(db->flash_write_mutex);
              }
              return false;
            }
          }
          // now try again
          continue;
        } else {
          // L0 pages exist but all are full
          if (npoints_possible == 1) {
            // can't store even 1 => create new L0 page without compaction
            ESP_LOGV(TAG, "All L0 pages full => creating new L0 page (progress: %zu/%zu points)",
                     count - remaining, count);
            uint32_t new_page_offset = 0;
            if (!create_level0_field_page(db, &new_page_offset)) {
              // No space — trigger background compaction and retry once
              ESP_LOGW(TAG, "No space for new L0 page, triggering background compaction...");
              if (db->flash_write_mutex) {
                xSemaphoreGive(db->flash_write_mutex);
              }
              // Launch compaction on background task (dedicated stack) and wait
              tsdb_launch_compaction(db);
              // Wait for compaction to finish (already running or just launched)
              int wait = 10000;
              while (atomic_load(&db->compaction_in_progress) && wait > 0) {
                vTaskDelay(pdMS_TO_TICKS(10));
                wait -= 10;
              }
              if (db->flash_write_mutex) {
                xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY);
              }
              if (!create_level0_field_page(db, &new_page_offset)) {
                ESP_LOGE(TAG, "Failed to create new L0 page even after compaction.");
                if (db->flash_write_mutex) {
                  xSemaphoreGive(db->flash_write_mutex);
                }
                return false;
              }
            }
            ESP_LOGV(TAG, "Created new L0 page @0x%08" PRIX32, new_page_offset);
            continue;
          } else {
            // reduce npoints_possible
            ESP_LOGV(TAG, "Can't fit %zu points (need %zu bytes), trying %zu points",
                     npoints_possible, bytes_needed, npoints_possible / 2);
            npoints_possible /= 2;
            bytes_needed = calc_bytes_needed_for_npoints_columnar(
                npoints_possible, order->indices ? values : values + idx,
                order->indices ? order->indices + idx : NULL);
          }
        }
      }
    } // end while (npoints_possible > 0)

    if (db->flash_write_mutex) {
      xSemaphoreGive(db->flash_write_mutex);
    }

    if (npoints_possible == 0) {
      ESP_LOGE(TAG, "Unable to store any points. Out of space?");
      return false;
    }
  } // end while (remaining > 0)

  ESP_LOGV(TAG, "Successfully appended all %zu points to L0 pages", count);
  return true;
}

static size_t l0_usage_slot(uint32_t offset) {
  return (offset / TIMESERIES_FIELD_DATA_PAGE_SIZE) % TSDB_L0_USAGE_CACHE_SIZE;
}

static bool is_page_claimed_by_compaction(timeseries_db_t *db, uint32_t offset) {
    for (size_t i = 0; i < db->compaction_claimed_count; i++) {
        if (db->compaction_claimed_pages[i] == offset) return true;
    }
    return false;
}

size_t tsdb_count_l0_pages(timeseries_db_t *db) {
    size_t count = 0;
    timeseries_page_cache_iterator_t iter;
    if (!timeseries_page_cache_iterator_init(db, &iter)) return 0;
    timeseries_page_header_t hdr;
    uint32_t offset, size;
    while (timeseries_page_cache_iterator_next(&iter, &hdr, &offset, &size)) {
        if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
            hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE &&
            hdr.field_data_level == 0) {
            count++;
        }
    }
    timeseries_page_cache_iterator_deinit(&iter);
    return count;
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

  // Page to skip in the fallback scan: only set when step 1 found the cached
  // page genuinely too full. It must stay UINT32_MAX otherwise — using the
  // (possibly stale/zero) last_l0_page_offset would wrongly skip a valid L0
  // page that happens to live at that offset (e.g. offset 0 on a cold cache).
  uint32_t skip_page = UINT32_MAX;

  // ---------------------------------------
  // 1) Quick check against cached L0 page (no flash read needed)
  // ---------------------------------------
  if (db->last_l0_cache_valid) {
    // Check if this page is claimed by compaction
    if (is_page_claimed_by_compaction(db, db->last_l0_page_offset)) {
      db->last_l0_cache_valid = false;
      // fall through to full scan
    } else {
      *out_any_l0_exists = true;
      uint32_t free_space = db->last_l0_page_size - db->last_l0_used_offset;

      if (free_space >= bytes_needed) {
        // We can use the cached page immediately
        *out_page_offset = db->last_l0_page_offset;
        *out_used_offset = db->last_l0_used_offset;
        return true;
      }
      // Page too full — remember it so the scan skips its expensive
      // field-data iteration, then invalidate and scan for another.
      skip_page = db->last_l0_page_offset;
      db->last_l0_cache_valid = false;
    }
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

      // Skip pages claimed by compaction early (before expensive scan)
      if (is_page_claimed_by_compaction(db, page_offset)) {
        continue;
      }

      // Skip the page we already know is full from the cache check
      if (page_offset == skip_page) {
        continue;
      }

      size_t slot = l0_usage_slot(page_offset);
      uint32_t last_offset = sizeof(timeseries_page_header_t);
      if (hdr.sequence_num != 0 && db->l0_usage_cache[slot].offset == page_offset &&
          db->l0_usage_cache[slot].sequence == hdr.sequence_num) {
        last_offset = db->l0_usage_cache[slot].used;
      } else {
        // Determine how many bytes have been used in this L0 page.
        // Reuse db->write_buffer to avoid per-scan malloc (safe: caller holds flash_write_mutex).
        timeseries_fielddata_iterator_t f_iter;
        uint32_t data_size = page_size - sizeof(timeseries_page_header_t);
        if (db->write_buffer && db->write_buffer_capacity >= data_size) {
          if (!timeseries_fielddata_iterator_init_with_buffer(
                  db, page_offset, page_size,
                  db->write_buffer, (uint32_t)db->write_buffer_capacity,
                  &f_iter)) {
            continue;
          }
        } else {
          if (!timeseries_fielddata_iterator_init(db, page_offset, page_size,
                                                  &f_iter)) {
            continue;
          }
        }

        timeseries_field_data_header_t f_hdr;
        uint64_t min_time = UINT64_MAX, max_time = 0;

        while (timeseries_fielddata_iterator_next(&f_iter, &f_hdr)) {
          last_offset = f_iter.offset;
          if (f_hdr.start_time < min_time) min_time = f_hdr.start_time;
          if (f_hdr.end_time > max_time) max_time = f_hdr.end_time;
        }
        bool read_error = f_iter.read_error;
        timeseries_fielddata_iterator_deinit(&f_iter);
        if (read_error) continue;
        db->l0_usage_cache[slot].offset = page_offset;
        db->l0_usage_cache[slot].sequence = hdr.sequence_num;
        db->l0_usage_cache[slot].used = last_offset;
        db->l0_usage_cache[slot].min_time = min_time;
        db->l0_usage_cache[slot].max_time = max_time;
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
        db->last_l0_page_size = page_size;
        break;
      }
    }
  }

  timeseries_page_cache_iterator_deinit(&page_iter);
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

  // 1) Reserve a blank region (mutex-protected, registers placeholder)
  uint32_t blank_offset = 0;
  uint32_t blank_length = 0;
  if (!tsdb_reserve_blank_region(db, l0_size, NULL, &blank_offset, &blank_length)) {
    ESP_LOGW(TAG, "No blank region found to create L0 page (size=%u).",
             (unsigned int)l0_size);
    return false;
  }

  if (blank_offset + l0_size > db->partition->size) {
    ESP_LOGW(TAG, "Blank region extends beyond partition boundary!");
    return false;
  }

  // 2) Write proper page header (replaces placeholder written by reserve)
  timeseries_page_header_t new_hdr;
  memset(&new_hdr, 0xFF, sizeof(new_hdr));
  new_hdr.magic_number = TIMESERIES_MAGIC_NUM;
  new_hdr.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
  new_hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  // Reserve a unique physical page sequence across concurrent allocations.
  new_hdr.sequence_num = atomic_fetch_add(&db->sequence_num, 1) + 1;
  new_hdr.field_data_level = 0;
  new_hdr.page_size = l0_size;

  esp_err_t err = esp_partition_write(db->partition, blank_offset, &new_hdr,
                            sizeof(new_hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to write L0 header (err=0x%x)", err);
    return false;
  }

  // 3) Update the live snapshot cache entry with the real header
  // (replaces the placeholder registered by tsdb_reserve_blank_region)
  if (!tsdb_pagecache_add_entry(db, blank_offset, &new_hdr)) {
    ESP_LOGE(TAG, "OOM updating page cache with L0 header @0x%08" PRIx32, blank_offset);
    return false;
  }

  // The compactor owns its batch exclusively. Commit merges this live
  // header, avoiding a second snapshot mutation on every L0 allocation.

  *out_offset = blank_offset;
  ESP_LOGV(TAG, "Created new level-0 page @0x%08" PRIx32 " (size=%u bytes).",
           blank_offset, (unsigned int)l0_size);

  // 4) Update the "last L0" cache
  db->last_l0_cache_valid = true;
  db->last_l0_page_offset = blank_offset;
  db->last_l0_used_offset = sizeof(timeseries_page_header_t);
  db->last_l0_page_size = l0_size;

  size_t slot = l0_usage_slot(blank_offset);
  db->l0_usage_cache[slot].offset = blank_offset;
  db->l0_usage_cache[slot].sequence = new_hdr.sequence_num;
  db->l0_usage_cache[slot].used = sizeof(timeseries_page_header_t);
  db->l0_usage_cache[slot].min_time = UINT64_MAX;
  db->l0_usage_cache[slot].max_time = 0;

  // 5) Increment tracked L0 page count
  atomic_fetch_add(&db->l0_page_count, 1);

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
                                 size_t npoints, const size_t *indices, size_t bytes_needed) {
  // 1) Prepare the field_data_header
  timeseries_field_data_header_t fhdr;
  memset(&fhdr, 0xff, sizeof(fhdr));
  uint64_t revision;
  if (!tsdb_revision_next_locked(db, &revision)) return false;
  fhdr.flags &= ~TSDB_FIELDDATA_FLAG_REVISION;

  if (npoints > UINT16_MAX) {
    ESP_LOGE("TimeseriesData", "npoints %zu exceeds uint16_t max", npoints);
    return false;
  }
  fhdr.record_count = (uint16_t)npoints;
  fhdr.start_time = timestamps[indices ? indices[0] : 0];
  fhdr.end_time = timestamps[indices ? indices[npoints - 1] : npoints - 1];
  memcpy(fhdr.series_id, series_id, 16);

  // 2) Reuse the exact size used to select the page.
  size_t ts_size = npoints * sizeof(uint64_t);
  size_t val_size = bytes_needed - sizeof(fhdr) - sizeof(timeseries_col_data_header_t) - ts_size - sizeof(revision);

  // Column header
  timeseries_col_data_header_t col_hdr;
  memset(&col_hdr, 0, sizeof(col_hdr));
  col_hdr.ts_len = (uint32_t)ts_size;
  col_hdr.val_len = (uint32_t)val_size;

  size_t total_col_data = sizeof(col_hdr) + ts_size + val_size + sizeof(revision);
  if (total_col_data > 0xFFFF) {
    ESP_LOGE(TAG,
             "Record too big for 16-bit field_data_header.record_length: %zu",
             total_col_data);
    return false;
  }
  fhdr.record_length = (uint16_t)total_col_data;

  // 3) Use pre-allocated buffer if available, otherwise allocate.
  //    SAFETY: write_buffer is shared state, safe here because caller holds flash_write_mutex.
  //    Buffer includes fhdr + col_hdr + timestamps + values (single flash write).
  size_t total_write = sizeof(fhdr) + total_col_data;
  uint8_t *write_buf = NULL;
  bool using_preallocated = false;

  if (db->write_buffer && db->write_buffer_capacity >= total_write) {
    write_buf = db->write_buffer;
    using_preallocated = true;
  }

  if (!write_buf) {
    write_buf = (uint8_t *)malloc(total_write);
    if (!write_buf) {
      return false;
    }
  }

  // 4) Fill write_buf: fhdr + col_hdr + timestamps + values
  size_t buf_offset = 0;
  memcpy(write_buf + buf_offset, &fhdr, sizeof(fhdr));
  buf_offset += sizeof(fhdr);

  memcpy(write_buf + buf_offset, &col_hdr, sizeof(col_hdr));
  buf_offset += sizeof(col_hdr);

  if (indices) {
    for (size_t i = 0; i < npoints; i++)
      memcpy(write_buf + buf_offset + i * sizeof(uint64_t), &timestamps[indices[i]], sizeof(uint64_t));
  } else {
    memcpy(write_buf + buf_offset, timestamps, ts_size);
  }
  buf_offset += ts_size;

  for (size_t i = 0; i < npoints; i++) {
    buf_offset +=
        field_value_write_uncompressed(&vals[indices ? indices[i] : i], write_buf + buf_offset);
  }

  memcpy(write_buf + buf_offset, &revision, sizeof(revision));
  buf_offset += sizeof(revision);

  // 5) Single flash write for fhdr + column data
  uint32_t write_addr = page_offset + used_offset;
  esp_err_t err =
      esp_partition_write(db->partition, write_addr, write_buf, total_write);

  if (!using_preallocated) {
    free(write_buf);
  }

  if (err != ESP_OK) {
    return false;
  }

  size_t slot = l0_usage_slot(page_offset);
  if (db->l0_usage_cache[slot].offset == page_offset) {
    db->l0_usage_cache[slot].used = used_offset + sizeof(fhdr) + fhdr.record_length;
    if (fhdr.start_time < db->l0_usage_cache[slot].min_time)
      db->l0_usage_cache[slot].min_time = fhdr.start_time;
    if (fhdr.end_time > db->l0_usage_cache[slot].max_time)
      db->l0_usage_cache[slot].max_time = fhdr.end_time;
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
