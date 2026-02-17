#include "timeseries_compaction.h"

#include "esp_err.h"
#include "esp_log.h"
#include "esp_partition.h"

#include "timeseries_compression.h"
#include "timeseries_data.h"  // for tsdb_write_levelX_page_dynamic()
#include "timeseries_internal.h"
#include "timeseries_iterator.h"  // for fielddata_iterator, points_iterator
#include "timeseries_metadata.h"
#include "timeseries_multi_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include "timeseries_page_rewriter.h"
#include "timeseries_page_stream_writer.h"
#include "timeseries_points_iterator.h"

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include "esp_system.h"
#include "esp_timer.h"

static const char* TAG = "TimeseriesCompaction";

/**
 * @brief Maximum level we support in ascending compaction. Adjust as needed.
 */
#define TSDB_MAX_LEVEL 4

// -----------------------------------------------------------------------------
// Forward Declarations
// -----------------------------------------------------------------------------

static bool tsdb_find_level_pages(timeseries_db_t* db, uint8_t source_level, tsdb_level_page_t** out_pages,
                                  size_t* out_count);

static bool tsdb_mark_old_level_pages_obsolete(timeseries_db_t* db, uint8_t source_level,
                                               const tsdb_level_page_t* pages, size_t page_count,
                                               tsdb_page_cache_snapshot_t* batch);

static bool tsdb_compact_multi_iterator(timeseries_db_t* db, const tsdb_level_page_t* pages, size_t page_count,
                                        uint8_t to_level, tsdb_page_cache_snapshot_t* batch);

typedef struct {
  size_t count;
  size_t capacity;
  uint8_t (*list)[16];  // each element is a 16-byte series ID
} tsdb_series_id_list_t;

static bool append_if_not_found_in_list(uint8_t (**list)[16], size_t* count, size_t* capacity,
                                        const uint8_t series_id[16]) {
  // 1) Check if this series_id is already in the array
  for (size_t i = 0; i < *count; i++) {
    if (memcmp((*list)[i], series_id, 16) == 0) {
      // Already present => nothing to do
      return true;
    }
  }

  // 2) If not found, ensure we have space to append one more
  if (*count == *capacity) {
    size_t new_capacity = (*capacity == 0) ? 8 : (*capacity * 2);
    uint8_t(*new_list)[16] = realloc(*list, new_capacity * sizeof((*list)[0]));
    if (!new_list) {
      // Failed to allocate
      return false;
    }
    *list = new_list;
    *capacity = new_capacity;
  }

  // 3) Append new ID at the end
  memcpy((*list)[*count], series_id, 16);
  (*count)++;

  return true;
}

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

bool timeseries_compact_all_levels(timeseries_db_t* db) {
  if (!db) {
    return false;
  }

  /**
   * For each level from 0 up to TSDB_MAX_LEVEL-1, attempt compaction
   * from that level to the next (level+1).
   *
   * We always do them in ascending order so that if compaction of L0→L1
   * creates multiple L1 pages, the L1→L2 step can then run, and so on.
   */
  for (uint8_t from_level = 0; from_level < TSDB_MAX_LEVEL; from_level++) {
    uint8_t to_level = (uint8_t)(from_level + 1);

    // 1) Figure out how many pages are at `from_level`
    tsdb_level_page_t* pages = NULL;
    size_t page_count = 0;
    if (!tsdb_find_level_pages(db, from_level, &pages, &page_count)) {
      // If we fail to even list pages, treat as an error
      free(pages);
      ESP_LOGE(TAG, "Failed listing pages at level %u", from_level);
      return false;
    }

    // If no pages or fewer than MIN_PAGES_FOR_COMPACTION, skip compaction
    if (page_count < MIN_PAGES_FOR_COMPACTION) {
      free(pages);
      ESP_LOGV(TAG, "Level-%u has %zu page(s), below threshold=%d => skip", from_level, page_count,
               MIN_PAGES_FOR_COMPACTION);
      continue;
    }
    free(pages);

    int64_t start_time = esp_timer_get_time();

    // 2) We have enough pages => do the actual compaction
    if (!timeseries_compact_level_pages(db, from_level, to_level)) {
      ESP_LOGE(TAG, "Compaction from level %u to %u failed.", from_level, to_level);
      // Decide if you want to continue or break on failure
      return false;
    }

    int64_t end_time = esp_timer_get_time();
    ESP_LOGI(TAG, "Compaction from level %u to %u took %.3f ms", from_level, to_level,
             (end_time - start_time) / 1000.0);
  }

  // Count the highest level for debug
  tsdb_level_page_t* pages = NULL;
  size_t page_count = 0;

  if (!tsdb_find_level_pages(db, 4, &pages, &page_count)) {
    // If we fail to even list pages, treat as an error
    free(pages);
    ESP_LOGE(TAG, "Failed listing pages at level %u", 4);
    return false;
  }

  ESP_LOGV(TAG, "Highest level has %zu page(s)", page_count);

  uint32_t partition_size = db->partition->size;
  uint32_t used_space = tsdb_pagecache_get_total_active_size(db);
  ESP_LOGI(TAG, "Total used space: %u, %f", (unsigned int)(used_space / 1024), 100.0f * used_space / partition_size);

  free(pages);

  return true;
}

static bool gather_unique_series_ids(timeseries_db_t* db, const tsdb_level_page_t* pages, size_t page_count,
                                     tsdb_series_id_list_t* out_ids) {
  memset(out_ids, 0, sizeof(*out_ids));

  for (size_t p = 0; p < page_count; p++) {
    const tsdb_level_page_t* pg = &pages[p];
    timeseries_fielddata_iterator_t f_iter;
    if (!timeseries_fielddata_iterator_init(db, pg->offset, pg->size, &f_iter)) {
      ESP_LOGW(TAG, "Failed init iterator @0x%08" PRIx32, pg->offset);
      continue;
    }

    timeseries_field_data_header_t fd_hdr;
    while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
      // Skip if record is "deleted" flag == 0 => truly deleted
      if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
        continue;
      }
      // Add the series_id to out_ids if not present
      ESP_LOGD(TAG, "Found series_id %02X%02X%02X%02X... flags=0x%02X records=%u in page @0x%08" PRIx32,
               fd_hdr.series_id[0], fd_hdr.series_id[1], fd_hdr.series_id[2], fd_hdr.series_id[3],
               fd_hdr.flags, fd_hdr.record_count, pg->offset);
      append_if_not_found_in_list(&out_ids->list, &out_ids->count, &out_ids->capacity, fd_hdr.series_id);
    }
  }
  return true;
}

static bool collect_points_for_series(timeseries_db_t* db, const tsdb_level_page_t* pages, size_t page_count,
                                      const uint8_t series_id[16], tsdb_compact_data_point_t** out_pts,
                                      size_t* out_used, size_t* out_cap, timeseries_field_type_e* out_type,
                                      uint64_t* out_min_ts, uint64_t* out_max_ts) {
  for (size_t p = 0; p < page_count; p++) {
    const tsdb_level_page_t* pg = &pages[p];
    timeseries_fielddata_iterator_t f_iter;
    if (!timeseries_fielddata_iterator_init(db, pg->offset, pg->size, &f_iter)) {
      ESP_LOGW(TAG, "Failed init iterator @0x%08" PRIx32, pg->offset);
      continue;
    }

    timeseries_field_data_header_t fd_hdr;
    while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
      // Skip if not matching series or flagged as truly deleted
      if (memcmp(fd_hdr.series_id, series_id, 16) != 0) {
        continue;
      }
      if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
        continue;
      }

      // If we haven't yet looked up the field type, do so now
      if (*out_type == TIMESERIES_FIELD_TYPE_FLOAT) {
        if (!tsdb_lookup_series_type_in_metadata(db, fd_hdr.series_id, out_type)) {
          ESP_LOGW(TAG, "No known field type for series => skipping");
          continue;
        }
      }

      // Points-iterator
      uint32_t abs_offset = pg->offset + f_iter.current_record_offset + sizeof(timeseries_field_data_header_t);
      timeseries_points_iterator_t pts_iter;
      if (!timeseries_points_iterator_init(db, abs_offset, fd_hdr.record_length, fd_hdr.record_count, *out_type,
                                           /*is_compressed=*/false, &pts_iter)) {
        ESP_LOGW(TAG, "Failed init points iterator => skipping record");
        continue;
      }

      // Read points
      for (size_t i = 0; i < fd_hdr.record_count; i++) {
        uint64_t ts;
        timeseries_field_value_t fv;
        if (!timeseries_points_iterator_next_timestamp(&pts_iter, &ts) ||
            !timeseries_points_iterator_next_value(&pts_iter, &fv)) {
          break;
        }

        // Grow out_pts if needed
        if (*out_used == *out_cap) {
          size_t new_cap = (*out_cap == 0) ? 64 : (*out_cap * 2);
          tsdb_compact_data_point_t* tmp = realloc(*out_pts, new_cap * sizeof(tsdb_compact_data_point_t));
          if (!tmp) {
            ESP_LOGE(TAG, "OOM collecting points for series");
            // Free string values already accumulated
            if (*out_type == TIMESERIES_FIELD_TYPE_STRING) {
              for (size_t k = 0; k < *out_used; k++) {
                free((*out_pts)[k].field_val.data.string_val.str);
              }
            }
            timeseries_points_iterator_deinit(&pts_iter);
            return false;
          }
          *out_pts = tmp;
          *out_cap = new_cap;
        }

        (*out_pts)[*out_used].timestamp = ts;
        (*out_pts)[*out_used].field_val = fv;
        (*out_used)++;

        if (ts < *out_min_ts) *out_min_ts = ts;
        if (ts > *out_max_ts) *out_max_ts = ts;
      }
      timeseries_points_iterator_deinit(&pts_iter);
    }
  }
  return true;
}

static int compare_points_by_timestamp(const void* a, const void* b) {
  const tsdb_compact_data_point_t* A = (const tsdb_compact_data_point_t*)a;
  const tsdb_compact_data_point_t* B = (const tsdb_compact_data_point_t*)b;
  if (A->timestamp < B->timestamp) return -1;
  if (A->timestamp > B->timestamp) return +1;
  return 0;
}

static size_t remove_duplicates_in_place(tsdb_compact_data_point_t* pts, size_t count,
                                          timeseries_field_type_e ftype) {
  if (count < 2) return count;
  size_t write_idx = 1;
  for (size_t i = 1; i < count; i++) {
    if (pts[i].timestamp != pts[write_idx - 1].timestamp) {
      pts[write_idx++] = pts[i];
    } else {
      // Free the old value's string before overwriting with the newer one
      if (ftype == TIMESERIES_FIELD_TYPE_STRING) {
        free(pts[write_idx - 1].field_val.data.string_val.str);
      }
      pts[write_idx - 1] = pts[i];
    }
  }
  return write_idx;
}

bool timeseries_compact_level_pages(timeseries_db_t* db, uint8_t from_level, uint8_t to_level) {
  if (!db) {
    return false;
  }
  ESP_LOGV(TAG, "Starting compaction from level=%u to level=%u ...", from_level, to_level);

  // 1) Find all active pages at `from_level`.
  tsdb_level_page_t* pages = NULL;
  size_t page_count = 0;
  if (!tsdb_find_level_pages(db, from_level, &pages, &page_count)) {
    return false;
  }
  if (page_count < MIN_PAGES_FOR_COMPACTION) {
    // skip
    ESP_LOGV(TAG, "Level-%u has only %zu pages => skip", from_level, page_count);
    free(pages);
    return true;
  }

  // Begin a batch snapshot for all cache mutations during compaction
  tsdb_page_cache_snapshot_t* batch = tsdb_pagecache_begin_batch(db);
  if (!batch) {
    ESP_LOGE(TAG, "Failed to begin batch for compaction");
    free(pages);
    return false;
  }
  // Set active_batch under region_alloc_mutex to synchronize with insert path
  if (db->region_alloc_mutex) {
    xSemaphoreTake(db->region_alloc_mutex, portMAX_DELAY);
  }
  db->active_batch = batch;
  if (db->region_alloc_mutex) {
    xSemaphoreGive(db->region_alloc_mutex);
  }

  // FIX 2C: Claim pages so inserts won't write to them
  if (db->flash_write_mutex) {
    xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY);
  }
  size_t claim_count = (page_count > TSDB_MAX_COMPACTION_CLAIMED_PAGES)
                        ? TSDB_MAX_COMPACTION_CLAIMED_PAGES : page_count;
  for (size_t i = 0; i < claim_count; i++) {
    db->compaction_claimed_pages[i] = pages[i].offset;
  }
  db->compaction_claimed_count = claim_count;
  // If the cached L0 page is being claimed, invalidate cache
  if (db->last_l0_cache_valid) {
    for (size_t i = 0; i < claim_count; i++) {
      if (db->last_l0_page_offset == pages[i].offset) {
        db->last_l0_cache_valid = false;
        break;
      }
    }
  }
  if (db->flash_write_mutex) {
    xSemaphoreGive(db->flash_write_mutex);
  }

  bool success = false;

  if (from_level == 0) {
    // --------------------------------------------------------------------
    // LEVEL-0 => LEVEL-1
    // Use single-series-at-a-time with timeseries_page_stream_writer
    // --------------------------------------------------------------------
    ESP_LOGI(TAG, "Compacting from level-0 => level-1 (stream-writer per series).");

    int64_t start_time, end_time;

    start_time = esp_timer_get_time();

    // (A) Gather all unique series IDs from L0
    tsdb_series_id_list_t uniq_ids;
    memset(&uniq_ids, 0, sizeof(uniq_ids));
    if (!gather_unique_series_ids(db, pages, page_count, &uniq_ids)) {
      ESP_LOGE(TAG, "Failed gathering unique series IDs");
      if (db->flash_write_mutex) { xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY); }
      db->compaction_claimed_count = 0;
      if (db->flash_write_mutex) { xSemaphoreGive(db->flash_write_mutex); }
      if (db->region_alloc_mutex) { xSemaphoreTake(db->region_alloc_mutex, portMAX_DELAY); }
      db->active_batch = NULL;
      if (db->region_alloc_mutex) { xSemaphoreGive(db->region_alloc_mutex); }
      tsdb_snapshot_release(batch);
      free(pages);
      return false;
    }

    if (uniq_ids.count == 0) {
      // No data => just mark pages obsolete
      ESP_LOGV(TAG, "No valid series in L0 => marking pages obsolete.");
      tsdb_mark_old_level_pages_obsolete(db, from_level, pages, page_count, batch);
      if (db->flash_write_mutex) { xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY); }
      db->compaction_claimed_count = 0;
      db->last_l0_cache_valid = false;
      if (db->flash_write_mutex) { xSemaphoreGive(db->flash_write_mutex); }
      db->active_batch = NULL;
      tsdb_pagecache_batch_sort(batch);
      tsdb_pagecache_commit_batch(db, batch);
      free(pages);
      return true;
    }

    end_time = esp_timer_get_time();
    ESP_LOGI(TAG, "Gathered %zu unique series IDs in %.3f ms", uniq_ids.count, (end_time - start_time) / 1000.0);

    // Estimate output size: sum source page sizes, halve for compression
    uint32_t total_source_size = 0;
    for (size_t i = 0; i < page_count; i++) {
      total_source_size += pages[i].size;
    }
    uint32_t estimated_size = total_source_size / 2;
    if (estimated_size < 4096) {
      estimated_size = 4096;
    }

    timeseries_page_stream_writer_t sw;
    sw.batch_snapshot = batch;  // Set before init so it uses batch
    if (!timeseries_page_stream_writer_init(db, &sw, to_level, estimated_size)) {
      ESP_LOGE(TAG, "Failed to init page_stream_writer for new L1 page.");
      if (db->flash_write_mutex) { xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY); }
      db->compaction_claimed_count = 0;
      if (db->flash_write_mutex) { xSemaphoreGive(db->flash_write_mutex); }
      if (db->region_alloc_mutex) { xSemaphoreTake(db->region_alloc_mutex, portMAX_DELAY); }
      db->active_batch = NULL;
      if (db->region_alloc_mutex) { xSemaphoreGive(db->region_alloc_mutex); }
      tsdb_snapshot_release(batch);
      free(uniq_ids.list);
      free(pages);
      return false;
    }

    // (C) For each series ID, read points, sort, deduplicate, then stream
    bool series_loop_failed = false;
    for (size_t i = 0; i < uniq_ids.count; i++) {
      tsdb_compact_data_point_t* pts = NULL;
      size_t pts_used = 0, pts_cap = 0;
      timeseries_field_type_e ftype = TIMESERIES_FIELD_TYPE_FLOAT;
      uint64_t min_ts = UINT64_MAX, max_ts = 0;

      start_time = esp_timer_get_time();

      // Collect this series’s points from all L0 pages
      if (!collect_points_for_series(db, pages, page_count, uniq_ids.list[i], &pts, &pts_used, &pts_cap, &ftype,
                                     &min_ts, &max_ts)) {
        // If we fail, bail out (string values already freed by collect_points_for_series on OOM)
        free(pts);
        series_loop_failed = true;
        break;
      }
      if (pts_used == 0) {
        free(pts);
        continue;
      }

      end_time = esp_timer_get_time();
      ESP_LOGI(TAG, "Collected %zu points in %.3f ms", pts_used, (end_time - start_time) / 1000.0);

      start_time = esp_timer_get_time();
      // Sort by timestamp
      qsort(pts, pts_used, sizeof(tsdb_compact_data_point_t), compare_points_by_timestamp);

      end_time = esp_timer_get_time();
      ESP_LOGI(TAG, "Sorted %zu points in %.3f ms", pts_used, (end_time - start_time) / 1000.0);

      // Remove duplicates (by timestamp)
      start_time = esp_timer_get_time();
      pts_used = remove_duplicates_in_place(pts, pts_used, ftype);
      end_time = esp_timer_get_time();
      ESP_LOGI(TAG, "Removed duplicates, now %zu points in %.3f ms", pts_used, (end_time - start_time) / 1000.0);

      start_time = esp_timer_get_time();

      // Begin streaming this series
      if (!timeseries_page_stream_writer_begin_series(&sw, uniq_ids.list[i], ftype)) {
        ESP_LOGE(TAG, "Failed to begin_series for i=%zu", i);
        if (ftype == TIMESERIES_FIELD_TYPE_STRING) {
          for (size_t j = 0; j < pts_used; j++) {
            free(pts[j].field_val.data.string_val.str);
          }
        }
        free(pts);
        series_loop_failed = true;
        break;
      }

      // PASS #1: Write timestamps
      bool write_failed = false;
      for (size_t j = 0; j < pts_used && !write_failed; j++) {
        uint64_t ts = pts[j].timestamp;
        if (!timeseries_page_stream_writer_write_timestamp(&sw, ts)) {
          ESP_LOGE(TAG, "Failed writing timestamp for j=%zu", j);
          write_failed = true;
        }
      }

      if (write_failed) {
        ESP_LOGE(TAG, "Timestamp write failed for series %zu, aborting compaction", i);
        // Free strings if needed
        if (ftype == TIMESERIES_FIELD_TYPE_STRING) {
          for (size_t j = 0; j < pts_used; j++) {
            free(pts[j].field_val.data.string_val.str);
          }
        }
        free(pts);
        series_loop_failed = true;
        break;  // Page is in inconsistent state, abort
      }

      // finalize timestamps
      timeseries_page_stream_writer_finalize_timestamp(&sw);

      // PASS #2: Write values
      size_t values_written = 0;
      for (size_t j = 0; j < pts_used && !write_failed; j++) {
        if (!timeseries_page_stream_writer_write_value(&sw, &pts[j].field_val)) {
          ESP_LOGE(TAG, "Failed writing value for j=%zu", j);
          write_failed = true;
        }

        // If we have a string, free the allocated memory
        if (ftype == TIMESERIES_FIELD_TYPE_STRING) {
          free(pts[j].field_val.data.string_val.str);
          pts[j].field_val.data.string_val.str = NULL;
        }
        values_written = j + 1;
      }

      // If value write failed, free remaining string values and abort
      if (write_failed) {
        ESP_LOGE(TAG, "Value write failed for series %zu, aborting compaction", i);
        if (ftype == TIMESERIES_FIELD_TYPE_STRING) {
          for (size_t k = values_written; k < pts_used; k++) {
            free(pts[k].field_val.data.string_val.str);
          }
        }
        free(pts);
        series_loop_failed = true;
        break;  // Page is in inconsistent state, abort
      }

      // end this series
      if (!timeseries_page_stream_writer_end_series(&sw)) {
        ESP_LOGE(TAG, "Failed to end_series for i=%zu with %zu points", i, pts_used);
        free(pts);
        series_loop_failed = true;
        break;
      }

      free(pts);

      end_time = esp_timer_get_time();
      ESP_LOGI(TAG, "Wrote series %zu with %zu points in %.3f ms", i, pts_used, (end_time - start_time) / 1000.0);
    }

    free(uniq_ids.list);

    // (D) Finalize the new page
    if (!timeseries_page_stream_writer_finalize(&sw)) {
      ESP_LOGE(TAG, "Failed finalizing new L1 page via stream-writer.");
      if (db->flash_write_mutex) { xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY); }
      db->compaction_claimed_count = 0;
      if (db->flash_write_mutex) { xSemaphoreGive(db->flash_write_mutex); }
      if (db->region_alloc_mutex) { xSemaphoreTake(db->region_alloc_mutex, portMAX_DELAY); }
      db->active_batch = NULL;
      if (db->region_alloc_mutex) { xSemaphoreGive(db->region_alloc_mutex); }
      tsdb_snapshot_release(batch);
      free(pages);
      return false;
    }

    // Debug: Log the cache state after finalize
    ESP_LOGI(TAG, "After finalize - batch snapshot state (%zu entries):", batch->count);
    for (size_t ci = 0; ci < batch->count; ci++) {
      timeseries_cached_page_t* ce = &batch->entries[ci];
      ESP_LOGI(TAG, "  [%zu] offset=0x%08" PRIx32 " type=%u state=%u level=%u size=%" PRIu32,
               ci, ce->offset, ce->header.page_type, ce->header.page_state,
               ce->header.field_data_level, ce->header.page_size);
    }

    success = !series_loop_failed;
  } else {
    // --------------------------------------------------------------------
    // LEVEL-1 (or higher) => LEVEL-2
    // Use your existing multi-iterator approach
    // --------------------------------------------------------------------
    ESP_LOGI(TAG, "Compacting from level-%u => level-%u using multi-iterator...", from_level, to_level);
    success = tsdb_compact_multi_iterator(db, pages, page_count, to_level, batch);
  }

  // If we succeeded, mark old pages as obsolete
  if (success) {
    if (!tsdb_mark_old_level_pages_obsolete(db, from_level, pages, page_count, batch)) {
      ESP_LOGW(TAG, "Failed marking old level-%u pages obsolete!", from_level);
    }
  }

  // Clear claimed set and active_batch before commit
  if (db->flash_write_mutex) {
    xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY);
  }
  db->compaction_claimed_count = 0;
  db->last_l0_cache_valid = false;
  if (db->flash_write_mutex) {
    xSemaphoreGive(db->flash_write_mutex);
  }

  // Clear active_batch under region_alloc_mutex before commit
  if (db->region_alloc_mutex) {
    xSemaphoreTake(db->region_alloc_mutex, portMAX_DELAY);
  }
  db->active_batch = NULL;
  if (db->region_alloc_mutex) {
    xSemaphoreGive(db->region_alloc_mutex);
  }

  if (success) {
    // Commit the batch to merge with any concurrent inserts
    tsdb_pagecache_batch_sort(batch);
    tsdb_pagecache_commit_batch(db, batch);
  } else {
    // Don't commit a failed compaction — discard the batch to avoid
    // making partial/duplicate data visible to queries.
    tsdb_snapshot_release(batch);
  }

  free(pages);
  return success;
}

bool timeseries_compact_level0_pages(timeseries_db_t* db) {
  // Just call the generic function with from_level=0, to_level=1
  return timeseries_compact_level_pages(db, 0, 1);
}

// -----------------------------------------------------------------------------
// Step 1) Find pages at the source level
// -----------------------------------------------------------------------------

static bool tsdb_find_level_pages(timeseries_db_t* db, uint8_t source_level, tsdb_level_page_t** out_pages,
                                  size_t* out_count) {
  if (!db || !out_pages || !out_count) {
    return false;
  }
  *out_pages = NULL;
  *out_count = 0;

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t offset = 0, size = 0;

// For demo, limit to 32 pages
#define MAX_PAGES_PER_LEVEL 32
  tsdb_level_page_t temp[MAX_PAGES_PER_LEVEL];
  size_t found = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &offset, &size)) {
    // Is it an active field-data page at the desired level?
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM && hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE && hdr.field_data_level == source_level) {
      if (found < MAX_PAGES_PER_LEVEL) {
        temp[found].offset = offset;
        temp[found].size = size;
        found++;
      } else {
        ESP_LOGW(TAG, "Too many pages found at level=%u (> %d).", source_level, MAX_PAGES_PER_LEVEL);
        break;
      }
    }
  }
  timeseries_page_cache_iterator_deinit(&page_iter);

  if (found == 0) {
    return true;  // no pages => nothing to do
  }

  tsdb_level_page_t* arr = (tsdb_level_page_t*)malloc(found * sizeof(tsdb_level_page_t));
  if (!arr) {
    return false;
  }
  memcpy(arr, temp, found * sizeof(tsdb_level_page_t));
  *out_pages = arr;
  *out_count = found;
  return true;
}

// -----------------------------------------------------------------------------
// Step 4) Mark old pages as obsolete
// -----------------------------------------------------------------------------

static bool tsdb_mark_old_level_pages_obsolete(timeseries_db_t* db, uint8_t source_level,
                                               const tsdb_level_page_t* pages, size_t page_count,
                                               tsdb_page_cache_snapshot_t* batch) {
  if (!db || !pages) {
    return false;
  }

  ESP_LOGI(TAG, "Marking %zu old level-%u pages as obsolete",
           page_count, source_level);

  for (size_t i = 0; i < page_count; i++) {
    uint32_t pofs = pages[i].offset;
    ESP_LOGI(TAG, "Marking level-%u page @0x%08" PRIx32 " as obsolete.", source_level, pofs);

    // 1) Read the existing page header
    timeseries_page_header_t hdr;
    esp_err_t err = esp_partition_read(db->partition, pofs, &hdr, sizeof(hdr));
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed to read page header @0x%08" PRIx32 " (err=0x%x)", pofs, err);
      continue;
    }

    // 2) Verify it's the correct level
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM && hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE && hdr.field_data_level == source_level) {
      // 3) Mark obsolete
      hdr.page_state = TIMESERIES_PAGE_STATE_OBSOLETE;

      // 4) Write updated header (protected by flash_write_mutex)
      if (db->flash_write_mutex) {
        xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY);
      }
      err = esp_partition_write(db->partition, pofs, &hdr, sizeof(hdr));
      if (db->flash_write_mutex) {
        xSemaphoreGive(db->flash_write_mutex);
      }
      if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed marking page @0x%08" PRIx32 " obsolete (err=0x%x)", pofs, err);
        continue;
      }

      // Remove from cache (batch or live)
      if (batch) {
        tsdb_pagecache_batch_remove(batch, pofs);
      } else {
        tsdb_pagecache_remove_entry(db, pofs);
      }

      ESP_LOGI(TAG, "Removed page @0x%08" PRIx32 " from cache", pofs);
    } else {
      ESP_LOGW(TAG, "Skipping page @0x%08" PRIx32 " (not an active level-%u field-data page).", pofs, source_level);
    }
  }

  return true;
}

size_t timeseries_field_value_serialize_size(const timeseries_field_value_t* val) {
  switch (val->type) {
    case TIMESERIES_FIELD_TYPE_STRING:
      // 4 + actual bytes
      return 4 + val->data.string_val.length;
    case TIMESERIES_FIELD_TYPE_FLOAT:
    case TIMESERIES_FIELD_TYPE_INT:
      return 8;
    case TIMESERIES_FIELD_TYPE_BOOL:
      return 1;
  }
  return 0;
}

size_t timeseries_field_value_serialize(const timeseries_field_value_t* val, unsigned char* out_buf) {
  size_t written = 0;
  switch (val->type) {
    case TIMESERIES_FIELD_TYPE_STRING: {
      uint32_t len = (uint32_t)val->data.string_val.length;
      memcpy(out_buf, &len, 4);
      written += 4;
      if (len > 0) {
        memcpy(out_buf + written, val->data.string_val.str, len);
        written += len;
      }
      break;
    }
    case TIMESERIES_FIELD_TYPE_FLOAT: {
      double d = (double)val->data.float_val;
      memcpy(out_buf, &d, 8);
      written += 8;
      break;
    }
    case TIMESERIES_FIELD_TYPE_INT: {
      int64_t i64 = (int64_t)val->data.int_val;
      memcpy(out_buf, &i64, 8);
      written += 8;
      break;
    }
    case TIMESERIES_FIELD_TYPE_BOOL: {
      out_buf[0] = val->data.bool_val ? 1 : 0;
      written++;
      break;
    }
  }
  return written;
}

// Example qsort compare function:
static int sort_by_seq_and_offset(const void* a, const void* b) {
  const tsdb_record_descriptor_t* A = (const tsdb_record_descriptor_t*)a;
  const tsdb_record_descriptor_t* B = (const tsdb_record_descriptor_t*)b;
  if (A->page_seq < B->page_seq)
    return -1;
  else if (A->page_seq > B->page_seq)
    return +1;

  // tie-break by data_offset:
  if (A->data_offset < B->data_offset)
    return -1;
  else if (A->data_offset > B->data_offset)
    return +1;
  return 0;
}

static bool tsdb_compact_multi_iterator(timeseries_db_t* db, const tsdb_level_page_t* pages, size_t page_count,
                                        uint8_t to_level, tsdb_page_cache_snapshot_t* batch) {
  if (!db || !pages || page_count == 0) {
    return false;
  }

  // printf("START free heap size:%ld\n", esp_get_free_heap_size());

  // ----------------------------------------------------------------
  // 1) Build a map of series => record descriptors (unchanged)
  // ----------------------------------------------------------------
  tsdb_series_descriptors_t* series_map = NULL;
  size_t series_map_used = 0;
  size_t series_map_cap = 0;
  size_t total_record_bytes = 0;  // Holds total bytes of all records minus headers

  for (size_t i = 0; i < page_count; i++) {
    const tsdb_level_page_t* pg = &pages[i];
    timeseries_page_header_t hdr;
    if (esp_partition_read(db->partition, pg->offset, &hdr, sizeof(hdr)) != ESP_OK) {
      ESP_LOGW(TAG, "Failed reading header @0x%08" PRIx32, pg->offset);
      continue;
    }
    // We need the page_seq for sorting merges in multi-iterator:
    uint32_t page_seq = hdr.sequence_num;

    // FieldData iterator on this page:
    timeseries_fielddata_iterator_t f_iter;
    if (!timeseries_fielddata_iterator_init(db, pg->offset, pg->size, &f_iter)) {
      ESP_LOGW(TAG, "Failed init fielddata iterator @0x%08" PRIx32, pg->offset);
      continue;
    }

    timeseries_field_data_header_t fd_hdr;
    unsigned int records_read = 0;

    while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
      if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
        // skip deleted records
        continue;
      }

      records_read++;
      total_record_bytes += fd_hdr.record_length;

      // Build descriptor
      tsdb_record_descriptor_t desc;
      memset(&desc, 0, sizeof(desc));
      desc.record_count = fd_hdr.record_count;
      desc.record_length = fd_hdr.record_length;
      desc.is_compressed = (fd_hdr.flags & TSDB_FIELDDATA_FLAG_COMPRESSED) == 0;
      // Figure out field_type from metadata
      timeseries_field_type_e ftype;
      if (!tsdb_lookup_series_type_in_metadata(db, fd_hdr.series_id, &ftype)) {
        // skip unknown series
        continue;
      }
      desc.field_type = ftype;
      // data_offset = page_offset + <this record offset> + sizeof(fd_hdr)
      desc.data_offset = pg->offset + f_iter.current_record_offset + sizeof(timeseries_field_data_header_t);
      desc.page_seq = page_seq;

      // Insert into series_map
      bool found = false;
      for (size_t s = 0; s < series_map_used; s++) {
        if (memcmp(series_map[s].series_id, fd_hdr.series_id, 16) == 0) {
          // Add to that vector
          tsdb_series_descriptors_t* S = &series_map[s];
          if (S->record_used == S->record_capacity) {
            size_t newcap = (S->record_capacity == 0) ? 4 : (S->record_capacity * 2);
            tsdb_record_descriptor_t* grow = realloc(S->records, newcap * sizeof(tsdb_record_descriptor_t));
            if (!grow) {
              goto series_map_cleanup;
            }
            S->records = grow;
            S->record_capacity = newcap;
          }
          S->records[S->record_used++] = desc;
          found = true;
          break;
        }
      }

      if (!found) {
        // Create a new entry in the series_map
        if (series_map_used == series_map_cap) {
          size_t newcap = (series_map_cap == 0) ? 4 : (series_map_cap * 2);
          tsdb_series_descriptors_t* grow = realloc(series_map, newcap * sizeof(tsdb_series_descriptors_t));
          if (!grow) {
            goto series_map_cleanup;
          }
          series_map = grow;
          series_map_cap = newcap;
        }
        memset(&series_map[series_map_used], 0, sizeof(series_map[series_map_used]));
        memcpy(series_map[series_map_used].series_id, fd_hdr.series_id, 16);
        series_map[series_map_used].record_used = 1;
        series_map[series_map_used].record_capacity = 4;
        series_map[series_map_used].records = calloc(4, sizeof(tsdb_record_descriptor_t));
        if (!series_map[series_map_used].records) {
          goto series_map_cleanup;
        }
        series_map[series_map_used].records[0] = desc;
        series_map_used++;
      }

      ESP_LOGV(TAG, "Added seriesId=%.2X%.2X%.2X%.2X... to map", fd_hdr.series_id[0], fd_hdr.series_id[1],
               fd_hdr.series_id[2], fd_hdr.series_id[3]);
    }  // end while

    ESP_LOGV(TAG, "Read %u records from page", records_read);
  }

  if (series_map_used == 0) {
    ESP_LOGV(TAG, "No data found among these pages; nothing to compact.");
    free(series_map);
    return true;  // no error
  }

  // printf("TWO free heap size:%ld\n", esp_get_free_heap_size());

  // ----------------------------------------------------------------
  // 2) Create one stream writer for a new page that will hold multiple series
  // ----------------------------------------------------------------
  timeseries_page_stream_writer_t writer;
  writer.batch_snapshot = batch;  // Set before init so it uses batch
  if (!timeseries_page_stream_writer_init(db, &writer, to_level, total_record_bytes)) {
    ESP_LOGE(TAG, "Failed to initialize stream-writer for multi-series page");
    for (size_t s = 0; s < series_map_used; s++) {
      free(series_map[s].records);
    }
    free(series_map);
    return false;
  }

  // printf("THREE free heap size:%ld\n", esp_get_free_heap_size());

  // ----------------------------------------------------------------
  // 3) For each series, stream its data into the same page
  //    But now, we do two passes:
  //      - Pass 1: Write timestamps
  //      - Reset multi-iterator
  //      - Pass 2: Write values
  // ----------------------------------------------------------------
  for (size_t s = 0; s < series_map_used; s++) {
    tsdb_series_descriptors_t* S = &series_map[s];
    if (S->record_used == 0) {
      continue;
    }

    // Sort records by page_seq (and data_offset as tie-breaker)
    qsort(S->records, S->record_used, sizeof(tsdb_record_descriptor_t), sort_by_seq_and_offset);

    // Build sub-iterators for each record
    timeseries_points_iterator_t** sub_iters = calloc(S->record_used, sizeof(*sub_iters));
    if (!sub_iters) {
      ESP_LOGE(TAG, "OOM allocating sub-iterators for series s=%zu", s);
      continue;
    }

    uint32_t* seqs = calloc(S->record_used, sizeof(uint32_t));
    if (!seqs) {
      free(sub_iters);
      ESP_LOGE(TAG, "OOM allocating seqs for series s=%zu", s);
      continue;
    }

    for (size_t r = 0; r < S->record_used; r++) {
      const tsdb_record_descriptor_t* RD = &S->records[r];
      sub_iters[r] = calloc(1, sizeof(timeseries_points_iterator_t));
      if (!sub_iters[r]) {
        ESP_LOGW(TAG, "OOM creating sub-iter for series s=%zu, record r=%zu", s, r);
        continue;
      }
      if (!timeseries_points_iterator_init(db, RD->data_offset, RD->record_length, RD->record_count, RD->field_type,
                                           RD->is_compressed, sub_iters[r])) {
        ESP_LOGW(TAG, "Failed init sub-iter offset=0x%08" PRIx32, RD->data_offset);
        free(sub_iters[r]);
        sub_iters[r] = NULL;
        continue;
      }
      seqs[r] = RD->page_seq;
    }

    // printf("PRE timeseries_multi_points_iterator_init free heap
    // size:%ld\n",esp_get_free_heap_size());

    ESP_LOGV(TAG, "Built %zu sub-iterators for series s=%zu", S->record_used, s);

    // Initialize the multi-iterator for pass #1 (timestamps)
    timeseries_multi_points_iterator_t multi_iter;
    if (!timeseries_multi_points_iterator_init(sub_iters, seqs, S->record_used, &multi_iter)) {
      ESP_LOGW(TAG, "Failed init multi-iter for series s=%zu (pass 1)", s);
      // Cleanup sub-iters
      for (size_t r = 0; r < S->record_used; r++) {
        if (sub_iters[r]) {
          timeseries_points_iterator_deinit(sub_iters[r]);
          free(sub_iters[r]);
        }
      }
      free(sub_iters);
      free(seqs);
      continue;
    }

    // printf("POST timeseries_multi_points_iterator_init free heap
    // size:%ld\n",esp_get_free_heap_size());

    // Begin a new series in the already-open page
    if (!timeseries_page_stream_writer_begin_series(&writer, S->series_id, S->records[0].field_type)) {
      ESP_LOGE(TAG, "Failed to begin series for s=%zu", s);
      timeseries_multi_points_iterator_deinit(&multi_iter);
      for (size_t r = 0; r < S->record_used; r++) {
        if (sub_iters[r]) {
          timeseries_points_iterator_deinit(sub_iters[r]);
          free(sub_iters[r]);
        }
      }
      free(sub_iters);
      free(seqs);
      continue;
    }

    // printf("TIMESTAMP WRITE free heap size:%ld\n",
    // esp_get_free_heap_size());

    // --- PASS #1: Write timestamps only ---
    {
      uint64_t ts;
      timeseries_field_value_t fv;
      while (timeseries_multi_points_iterator_next(&multi_iter, &ts, &fv)) {
        // Free string values yielded by the iterator (we only need timestamps)
        if (S->records[0].field_type == TIMESERIES_FIELD_TYPE_STRING &&
            fv.data.string_val.str) {
          free(fv.data.string_val.str);
          fv.data.string_val.str = NULL;
        }
        // Timestamps get written first
        if (!timeseries_page_stream_writer_write_timestamp(&writer, ts)) {
          ESP_LOGE(TAG, "Failed writing timestamp for s=%zu", s);
          break;
        }
      }
    }

    timeseries_page_stream_writer_finalize_timestamp(&writer);
    timeseries_multi_points_iterator_deinit(&multi_iter);

    // printf("SUBITERATORREBUILD free heap size:%ld\n",
    // esp_get_free_heap_size());

    // Re-init the multi-iterator for pass #2 (values)
    // We must re-init all sub-iterators so that they start from the
    // beginning.
    for (size_t r = 0; r < S->record_used; r++) {
      if (sub_iters[r]) {
        timeseries_points_iterator_deinit(sub_iters[r]);
        free(sub_iters[r]);
      }
    }

    // Build sub-iterators again:
    for (size_t r = 0; r < S->record_used; r++) {
      const tsdb_record_descriptor_t* RD = &S->records[r];
      sub_iters[r] = calloc(1, sizeof(timeseries_points_iterator_t));
      if (!sub_iters[r]) {
        ESP_LOGW(TAG, "OOM re-creating sub-iter for series s=%zu, record r=%zu", s, r);
        continue;
      }
      if (!timeseries_points_iterator_init(db, RD->data_offset, RD->record_length, RD->record_count, RD->field_type,
                                           RD->is_compressed, sub_iters[r])) {
        ESP_LOGW(TAG, "Failed re-init sub-iter offset=0x%08" PRIx32, RD->data_offset);
        free(sub_iters[r]);
        sub_iters[r] = NULL;
        continue;
      }
    }

    // printf("VALUE WRITE free heap size:%ld\n",
    // esp_get_free_heap_size());

    if (!timeseries_multi_points_iterator_init(sub_iters, seqs, S->record_used, &multi_iter)) {
      ESP_LOGW(TAG, "Failed init multi-iter for series s=%zu (pass 2)", s);
      // Cleanup sub-iters
      for (size_t r = 0; r < S->record_used; r++) {
        if (sub_iters[r]) {
          timeseries_points_iterator_deinit(sub_iters[r]);
          free(sub_iters[r]);
        }
      }
      free(sub_iters);
      free(seqs);
      // Try to end the series anyway
      timeseries_page_stream_writer_end_series(&writer);
      continue;
    }

    // --- PASS #2: Write values only ---
    {
      uint64_t ts;
      timeseries_field_value_t fv;
      while (timeseries_multi_points_iterator_next(&multi_iter, &ts, &fv)) {
        if (!timeseries_page_stream_writer_write_value(&writer, &fv)) {
          ESP_LOGE(TAG, "Failed writing value for s=%zu", s);
        }
        // Free string values yielded by the iterator after writing
        if (fv.type == TIMESERIES_FIELD_TYPE_STRING && fv.data.string_val.str) {
          free(fv.data.string_val.str);
          fv.data.string_val.str = NULL;
        }
      }

      // printf("free heap size:%ld\n", esp_get_free_heap_size());
    }
    timeseries_multi_points_iterator_deinit(&multi_iter);

    // Cleanup sub-iterators
    for (size_t r = 0; r < S->record_used; r++) {
      if (sub_iters[r]) {
        timeseries_points_iterator_deinit(sub_iters[r]);
        free(sub_iters[r]);
      }
    }
    free(sub_iters);
    free(seqs);

    // End the current series in the page
    if (!timeseries_page_stream_writer_end_series(&writer)) {
      ESP_LOGE(TAG, "Failed to end series in stream-writer for s=%zu", s);
      continue;
    }

    ESP_LOGV(TAG, "Finished writing series s=%zu in multi-series page", s);
  }

  // ----------------------------------------------------------------
  // 4) Finalize the page once all series have been written
  // ----------------------------------------------------------------
  if (!timeseries_page_stream_writer_finalize(&writer)) {
    ESP_LOGE(TAG, "Failed to finalize multi-series page");
    for (size_t s = 0; s < series_map_used; s++) {
      free(series_map[s].records);
    }
    free(series_map);
    return false;
  }

  // Cleanup series descriptors
  for (size_t s = 0; s < series_map_used; s++) {
    free(series_map[s].records);
  }
  free(series_map);

  // printf("free heap size:%ld\n", esp_get_free_heap_size());

  return true;

series_map_cleanup:
  for (size_t s = 0; s < series_map_used; s++) {
    free(series_map[s].records);
  }
  free(series_map);
  return false;
}

/**
 * @brief Rewrite a single old page (if it's an active field-data page)
 *        into a new page, omitting any deleted records.
 *
 * This function uses a two-pass approach. First it computes the total size
 * of all non-deleted field data records, then it allocates a new page using
 * that size. Finally, it copies each non-deleted record into the new page.
 *
 * @param db             Database handle
 * @param old_page_ofs   The offset of the old page
 * @return true on success, or if the page was skipped (not active field-data);
 *         false if there's a fatal error.
 */
static bool tsdb_rewrite_page_without_deleted(timeseries_db_t* db, uint32_t old_page_ofs) {
  // 1) Read the old page header.
  timeseries_page_header_t hdr;
  esp_err_t err = esp_partition_read(db->partition, old_page_ofs, &hdr, sizeof(hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to read old page header @0x%08X (err=0x%x)", (unsigned int)old_page_ofs, err);
    return false;
  }

  // 2) Check if it's an active field-data page.
  if (hdr.magic_number != TIMESERIES_MAGIC_NUM || hdr.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA ||
      hdr.page_state != TIMESERIES_PAGE_STATE_ACTIVE) {
    ESP_LOGW(TAG, "Page @0x%08X is not an active field-data page; skipping", (unsigned int)old_page_ofs);
    return true;
  }

  // Retrieve the old page size from the page cache (or header).
  uint32_t old_page_size = hdr.page_size;
  uint8_t level = hdr.field_data_level;

  ESP_LOGW(TAG, "Compacting page @0x%08X (level=%u) size=%u", (unsigned int)old_page_ofs, (unsigned int)level,
           (unsigned int)old_page_size);

  // 3) First pass: iterate over the field data records to compute the total
  // size of non-deleted records.
  uint32_t total_data_size = 0;
  uint32_t total_records = 0;
  timeseries_fielddata_iterator_t f_iter;
  if (!timeseries_fielddata_iterator_init(db, old_page_ofs, old_page_size, &f_iter)) {
    ESP_LOGE(TAG, "Failed to init fielddata iterator @0x%08X", (unsigned int)old_page_ofs);
    return false;
  }

  timeseries_field_data_header_t fd_hdr;
  while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
    // If DELETED bit is 0, the record is deleted: skip.
    if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
      ESP_LOGE(TAG, "Skipping deleted record in page=0x%08X header_abs_offset=0x%08X", (unsigned int)old_page_ofs,
               (unsigned int)f_iter.current_record_offset);
      continue;
    }
    total_data_size += sizeof(timeseries_field_data_header_t) + fd_hdr.record_length;
    total_records++;

    ESP_LOGW(TAG,
             "Record in page=0x%08X: record_count=%zu, record_length=%u, "
             "header_abs_offset=0x%08X",
             (unsigned int)old_page_ofs, (unsigned int)fd_hdr.record_count, (unsigned int)fd_hdr.record_length,
             (unsigned int)f_iter.current_record_offset);
  }

  ESP_LOGI(TAG, "Page @0x%08X: total_records=%u, total_data_size=%u", (unsigned int)old_page_ofs,
           (unsigned int)total_records, (unsigned int)total_data_size);

  // 4) Allocate a new page using the rewriter, passing the computed total size.
  //    Acquire flash_write_mutex for the entire rewrite+obsolete sequence to
  //    prevent interleaving with concurrent inserts.
  if (db->flash_write_mutex) { xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY); }

  if (total_records > 0) {
    timeseries_page_rewriter_t rewriter;
    memset(&rewriter, 0, sizeof(rewriter));  // Ensure batch_snapshot is NULL (not garbage)
    if (!timeseries_page_rewriter_start(db, level, total_data_size, &rewriter)) {
      ESP_LOGE(TAG, "Failed to initialize page rewriter");
      if (db->flash_write_mutex) { xSemaphoreGive(db->flash_write_mutex); }
      return false;
    }
    // The new page offset is now rewriter.base_offset.

    // 5) Second pass: reinitialize the iterator to copy over records.
    if (!timeseries_fielddata_iterator_init(db, old_page_ofs, old_page_size, &f_iter)) {
      ESP_LOGE(TAG, "Failed to re-init fielddata iterator @0x%08X", (unsigned int)old_page_ofs);
      timeseries_page_rewriter_abort(&rewriter);
      if (db->flash_write_mutex) { xSemaphoreGive(db->flash_write_mutex); }
      return false;
    }
    while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
      // Skip deleted records.
      if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
        continue;
      }
      // Write the record into the new page.
      if (!timeseries_page_rewriter_write_field_data(&rewriter, old_page_ofs, f_iter.current_record_offset, &fd_hdr)) {
        ESP_LOGE(TAG, "Failed writing field data to new page @0x%08X", (unsigned int)rewriter.base_offset);
        timeseries_page_rewriter_abort(&rewriter);
        if (db->flash_write_mutex) { xSemaphoreGive(db->flash_write_mutex); }
        return false;
      }
    }

    // 6) Finalize the new page.
    if (!timeseries_page_rewriter_finalize(&rewriter)) {
      ESP_LOGE(TAG, "Failed to finalize new page @0x%08X", (unsigned int)rewriter.base_offset);
      if (db->flash_write_mutex) { xSemaphoreGive(db->flash_write_mutex); }
      return false;
    }
  }

  // 7) Mark the OLD page as obsolete.
  hdr.page_state = TIMESERIES_PAGE_STATE_OBSOLETE;
  err = esp_partition_write(db->partition, old_page_ofs, &hdr, sizeof(hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed marking old page @0x%08X obsolete (err=0x%x)", (unsigned int)old_page_ofs, err);
    // Not strictly fatal, so continue.
  }

  if (db->flash_write_mutex) { xSemaphoreGive(db->flash_write_mutex); }

  // Remove the old page from the cache.
  tsdb_pagecache_remove_entry(db, old_page_ofs);

  ESP_LOGI(TAG, "Page @0x%08X compacted into new page", (unsigned int)old_page_ofs);
  return true;
}

bool timeseries_compact_page_list(timeseries_db_t* db, const uint32_t* page_offsets, size_t page_count) {
  if (!db || !page_offsets || page_count == 0) {
    // Nothing to do
    return true;
  }

  for (size_t i = 0; i < page_count; i++) {
    uint32_t old_page_ofs = page_offsets[i];
    // Use our new helper function
    if (!tsdb_rewrite_page_without_deleted(db, old_page_ofs)) {
      // If there's a fatal error, stop
      return false;
    }
  }

  return true;
}