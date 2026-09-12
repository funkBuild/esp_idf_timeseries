#include "timeseries_compaction.h"
#include "timeseries_compaction_merge.h"

#include "esp_err.h"
#include "esp_log.h"
#include "esp_partition.h"

#include "timeseries_data.h"  // for tsdb_write_levelX_page_dynamic()
#include "timeseries_internal.h"
#include "timeseries_iterator.h"  // for fielddata_iterator, points_iterator
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include "timeseries_page_rewriter.h"

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include "esp_timer.h"


static const char* TAG = "TimeseriesCompaction";

/* Maximum pages gathered per level per compaction run.
 * Must be <= TSDB_MAX_COMPACTION_CLAIMED_PAGES so all selected pages can be
 * registered in the claimed-pages array (enforced by the static_assert in
 * timeseries_compact_level_pages). */
#define MAX_PAGES_PER_LEVEL 32

/**
 * @brief Maximum level we support in ascending compaction. Adjust as needed.
 */
#define TSDB_MAX_LEVEL 4

// -----------------------------------------------------------------------------
// Forward Declarations
// -----------------------------------------------------------------------------

static bool compact_level_pages(timeseries_db_t* db, uint8_t from_level,
                                uint8_t to_level, size_t minimum_pages, size_t* selected_count);

static bool tsdb_find_level_pages(timeseries_db_t* db, uint8_t source_level, tsdb_level_page_t** out_pages,
                                  size_t* out_count);

static bool tsdb_mark_old_level_pages_obsolete(timeseries_db_t* db, uint8_t source_level,
                                               const tsdb_level_page_t* pages, size_t page_count,
                                               tsdb_page_cache_snapshot_t* batch);

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
    uint32_t elapsed_ms = (uint32_t)((end_time - start_time) / 1000);
    uint32_t elapsed_us = (uint32_t)((end_time - start_time) % 1000);
    ESP_LOGI(TAG, "Compaction from level %u to %u took %"PRIu32".%03"PRIu32" ms", from_level, to_level,
             elapsed_ms, elapsed_us);
  }

  // Count the highest level for diagnostic logging (non-fatal if it fails)
  tsdb_level_page_t* pages = NULL;
  size_t page_count = 0;

  if (!tsdb_find_level_pages(db, 4, &pages, &page_count)) {
    free(pages);
    ESP_LOGW(TAG, "Failed listing pages at level %u (diagnostic only, ignoring)", 4);
  } else {
    ESP_LOGV(TAG, "Highest level has %zu page(s)", page_count);
    free(pages);
  }

  uint32_t partition_size = db->partition->size;
  uint32_t used_space = tsdb_pagecache_get_total_active_size(db);
  uint32_t used_pct = (uint32_t)(used_space * 100 / partition_size);
  ESP_LOGI(TAG, "Total used space: %u KB, %"PRIu32"%%", (unsigned int)(used_space / 1024), used_pct);

  return true;
}

bool timeseries_compact_all_levels_force(timeseries_db_t* db) {
  if (!db) {
    return false;
  }

  for (uint8_t from_level = 0; from_level < TSDB_MAX_LEVEL; from_level++) {
    uint8_t to_level = (uint8_t)(from_level + 1);

    // A run claims at most MAX_PAGES_PER_LEVEL. Drain additional batches
    // before moving upward, including a final batch below the normal threshold.
    while (true) {
      size_t page_count = 0;
      int64_t start_time = esp_timer_get_time();
      if (!compact_level_pages(db, from_level, to_level, 1, &page_count)) {
        ESP_LOGE(TAG, "Force compaction from level %u to %u failed.", from_level, to_level);
        return false;
      }
      if (page_count == 0) break;
      int64_t elapsed = esp_timer_get_time() - start_time;
      ESP_LOGI(TAG, "Force compaction from level %u to %u took %"PRIu32".%03"PRIu32" ms",
               from_level, to_level, (uint32_t)(elapsed / 1000), (uint32_t)(elapsed % 1000));
      if (page_count < MAX_PAGES_PER_LEVEL) break;
    }
  }

  uint32_t used_space = tsdb_pagecache_get_total_active_size(db);
  uint32_t used_pct = (uint32_t)(used_space * 100 / db->partition->size);
  ESP_LOGI(TAG, "Total used space after force compact: %u KB, %"PRIu32"%%",
           (unsigned int)(used_space / 1024),
           used_pct);

  return true;
}

// Retire source pages and publish replacements as one operation for inserts.
// Reserve merge/removal storage before changing flash, so OOM leaves sources
// writable and visible. Encoding and output writes happen outside these locks.
static bool finish_compaction_batch(timeseries_db_t *db, uint8_t from_level,
    const tsdb_level_page_t *pages, size_t page_count,
    tsdb_page_cache_snapshot_t *batch, bool success) {
  if (db->flash_write_mutex) xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY);
  if (db->region_alloc_mutex) xSemaphoreTake(db->region_alloc_mutex, portMAX_DELAY);

  if (success) {
    tsdb_page_cache_snapshot_t *live = tsdb_snapshot_acquire_current(db);
    size_t live_count = live ? live->count : 0;
    tsdb_snapshot_release(live);
    if (live_count > SIZE_MAX / sizeof(*batch->entries) - batch->count ||
        page_count > SIZE_MAX / sizeof(*batch->removed_offsets) - batch->removed_count) {
      success = false;
    } else {
      size_t capacity = batch->count + live_count;
      if (capacity > batch->capacity) {
        timeseries_cached_page_t *entries = realloc(batch->entries, capacity * sizeof(*entries));
        if (!entries) success = false;
        else { batch->entries = entries; batch->capacity = capacity; }
      }
      size_t removed_capacity = batch->removed_count + page_count;
      if (success && removed_capacity > batch->removed_capacity) {
        uint32_t *removed = realloc(batch->removed_offsets, removed_capacity * sizeof(*removed));
        if (!removed) success = false;
        else { batch->removed_offsets = removed; batch->removed_capacity = removed_capacity; }
      }
    }
  }

  if (success)
    success = tsdb_mark_old_level_pages_obsolete(db, from_level, pages, page_count, batch);
  db->active_batch = NULL;
  if (success) {
    tsdb_pagecache_batch_sort(batch);
    success = tsdb_pagecache_commit_batch(db, batch);
  } else {
    tsdb_snapshot_release(batch);
  }
  db->compaction_claimed_count = 0;
  db->last_l0_cache_valid = false;
  if (db->region_alloc_mutex) xSemaphoreGive(db->region_alloc_mutex);
  if (db->flash_write_mutex) xSemaphoreGive(db->flash_write_mutex);
  return success;
}

bool timeseries_compact_level_pages(timeseries_db_t* db, uint8_t from_level, uint8_t to_level) {
  return compact_level_pages(db, from_level, to_level, MIN_PAGES_FOR_COMPACTION, NULL);
}

static bool compact_level_pages(timeseries_db_t* db, uint8_t from_level,
                                uint8_t to_level, size_t minimum_pages, size_t* selected_count) {
  if (selected_count) *selected_count = 0;
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
  if (page_count < minimum_pages) {
    // skip
    ESP_LOGV(TAG, "Level-%u has only %zu pages => skip", from_level, page_count);
    free(pages);
    return true;
  }

  if (selected_count) *selected_count = page_count;

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

  // Claim pages so inserts won't write to them during compaction.
  // page_count is guaranteed <= MAX_PAGES_PER_LEVEL (32) by tsdb_find_level_pages,
  // which matches TSDB_MAX_COMPACTION_CLAIMED_PAGES — so no truncation is needed.
  // The static_assert below enforces this invariant at compile time.
  static_assert(TSDB_MAX_COMPACTION_CLAIMED_PAGES >= MAX_PAGES_PER_LEVEL,
      "compaction_claimed_pages[] must be at least as large as MAX_PAGES_PER_LEVEL");
  if (db->flash_write_mutex) {
    xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY);
  }
  for (size_t i = 0; i < page_count; i++) {
    db->compaction_claimed_pages[i] = pages[i].offset;
  }
  db->compaction_claimed_count = page_count;
  // If the cached L0 page is being claimed, invalidate cache
  if (db->last_l0_cache_valid) {
    for (size_t i = 0; i < page_count; i++) {
      if (db->last_l0_page_offset == pages[i].offset) {
        db->last_l0_cache_valid = false;
        break;
      }
    }
  }
  if (db->flash_write_mutex) {
    xSemaphoreGive(db->flash_write_mutex);
  }

  bool success = tsdb_compact_stream_records(db, pages, page_count, to_level, batch);

  success = finish_compaction_batch(db, from_level, pages, page_count, batch, success);

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

  tsdb_level_page_t *temp = (tsdb_level_page_t *)malloc(MAX_PAGES_PER_LEVEL * sizeof(tsdb_level_page_t));
  if (!temp) {
    timeseries_page_cache_iterator_deinit(&page_iter);
    return false;
  }
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
    free(temp);
    return true;  // no pages => nothing to do
  }

  // Shrink to exact size needed
  tsdb_level_page_t* arr = (tsdb_level_page_t*)realloc(temp, found * sizeof(tsdb_level_page_t));
  if (!arr) {
    // realloc shrink failed (unlikely), but temp is still valid
    arr = temp;
  }
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

  // Caller holds flash_write_mutex and region_alloc_mutex until publication.
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

      // 4) Write updated header (flash_write_mutex held across the whole loop)
      err = esp_partition_write(db->partition, pofs, &hdr, sizeof(hdr));
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

      // Decrement tracked L0 page count
      if (source_level == 0) {
        atomic_fetch_sub(&db->l0_page_count, 1);
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
static bool tsdb_rewrite_page_without_deleted(timeseries_db_t* db, uint32_t old_page_ofs, uint8_t *window) {
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
  uint32_t total_data_size = sizeof(timeseries_page_header_t);
  uint32_t total_records = 0;
  timeseries_fielddata_iterator_t f_iter;
  if (!timeseries_fielddata_iterator_init_window(db, old_page_ofs, old_page_size, window, 4096, &f_iter)) {
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
    total_data_size += sizeof(timeseries_field_data_header_t) + fd_hdr.record_length +
        ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_REVISION) ? sizeof(uint64_t) : 0);
    total_records++;

    ESP_LOGW(TAG,
             "Record in page=0x%08X: record_count=%zu, record_length=%u, "
             "header_abs_offset=0x%08X",
             (unsigned int)old_page_ofs, (unsigned int)fd_hdr.record_count, (unsigned int)fd_hdr.record_length,
             (unsigned int)f_iter.current_record_offset);
  }
  bool read_ok = !f_iter.read_error;
  timeseries_fielddata_iterator_deinit(&f_iter);
  if (!read_ok) return false;

  ESP_LOGI(TAG, "Page @0x%08X: total_records=%u, total_data_size=%u", (unsigned int)old_page_ofs,
           (unsigned int)total_records, (unsigned int)total_data_size);

  // 4) Allocate a new page using the rewriter, passing the computed total size.
  //    Acquire flash_write_mutex for the entire rewrite+obsolete sequence to
  //    prevent interleaving with concurrent inserts.

  if (total_records > 0) {
    timeseries_page_rewriter_t rewriter;
    memset(&rewriter, 0, sizeof(rewriter));  // Ensure batch_snapshot is NULL (not garbage)
    if (!timeseries_page_rewriter_start(db, level, total_data_size, &rewriter)) {
      ESP_LOGE(TAG, "Failed to initialize page rewriter");

      return false;
    }
    // The new page offset is now rewriter.base_offset.
    // The entire rewrite holds flash_write_mutex, as do all insert users of
    // this buffer. Reuse it to copy ordinary records in one flash operation,
    // while the separate scan window retains the following record headers.
    rewriter.copy_buffer = db->write_buffer;
    rewriter.copy_buffer_capacity = db->write_buffer_capacity;

    // 5) Second pass: reinitialize the iterator to copy over records.
    if (!timeseries_fielddata_iterator_init_window(db, old_page_ofs, old_page_size, window, 4096, &f_iter)) {
      ESP_LOGE(TAG, "Failed to re-init fielddata iterator @0x%08X", (unsigned int)old_page_ofs);
      timeseries_page_rewriter_abort(&rewriter);

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
        timeseries_fielddata_iterator_deinit(&f_iter);
        timeseries_page_rewriter_abort(&rewriter);

        return false;
      }
    }
    read_ok = !f_iter.read_error;
    timeseries_fielddata_iterator_deinit(&f_iter);
    if (!read_ok) { timeseries_page_rewriter_abort(&rewriter); return false; }

    // 6) Finalize the new page.
    if (!timeseries_page_rewriter_finalize(&rewriter)) {
      ESP_LOGE(TAG, "Failed to finalize new page @0x%08X", (unsigned int)rewriter.base_offset);
      timeseries_page_rewriter_abort(&rewriter);

      return false;
    }
  }

  // 7) Mark the OLD page as obsolete.
  hdr.page_state = TIMESERIES_PAGE_STATE_OBSOLETE;
  err = esp_partition_write(db->partition, old_page_ofs, &hdr, sizeof(hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed marking old page @0x%08X obsolete (err=0x%x)", (unsigned int)old_page_ofs, err);
    return false;
  }

  // The append shortcut must not retain an obsolete source page.
  if (db->last_l0_cache_valid && db->last_l0_page_offset == old_page_ofs)
    db->last_l0_cache_valid = false;
  if (!tsdb_pagecache_remove_entry(db, old_page_ofs)) return false;

  // A nonempty L0 rewrite replaces one page with one page.
  if (level == 0 && total_records == 0)
    atomic_fetch_sub(&db->l0_page_count, 1);

  ESP_LOGI(TAG, "Page @0x%08X compacted into new page", (unsigned int)old_page_ofs);
  return true;
}

bool timeseries_compact_page_list(timeseries_db_t* db, const uint32_t* page_offsets, size_t page_count) {
  if (!db || !page_offsets || page_count == 0) {
    // Nothing to do
    return true;
  }

  uint8_t *window = malloc(4096);
  if (!window) return false;
  for (size_t i = 0; i < page_count; i++) {
    uint32_t old_page_ofs = page_offsets[i];
    // Use our new helper function
    if (db->flash_write_mutex) xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY);
    bool rewritten = tsdb_rewrite_page_without_deleted(db, old_page_ofs, window);
    if (db->flash_write_mutex) xSemaphoreGive(db->flash_write_mutex);
    if (!rewritten) {
      // If there's a fatal error, stop
      free(window);
      return false;
    }
  }

  free(window);
  return true;
}
