#include "timeseries_epoch.h"
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
#include "timeseries_flash_utils.h"
#include <stddef.h>

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

static void tsdb_mark_old_level_pages_obsolete(timeseries_db_t* db, uint8_t source_level,
                                               const tsdb_level_page_t* pages, size_t page_count,
                                               const tsdb_page_cache_snapshot_t* base,
                                               tsdb_page_cache_snapshot_t* next);

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

// A failed compaction gives back everything it reserved: the pages it wrote
// (journal entries) are marked OBSOLETE on flash, so a restart cannot load
// them next to their still-ACTIVE sources, and every region it held is
// removed from the live cache. Caller holds flash_write_mutex.
static void abandon_compaction_outputs(timeseries_db_t *db, const tsdb_page_cache_snapshot_t *batch) {
  for (size_t i = 0; i < batch->count; i++) {
    uint32_t off = batch->entries[i].offset;
    (void)tsdb_flash_write_byte(db->partition, off + offsetof(timeseries_page_header_t, page_state),
                                TIMESERIES_PAGE_STATE_OBSOLETE);
    (void)tsdb_pagecache_remove_entry(db, off);
  }
  // Regions it gave up while running (relocated or released) are OBSOLETE
  // on flash already; drop its placeholder if still listed live. A region
  // released earlier may already belong to another writer: leave that alone.
  for (size_t r = 0; r < batch->removed_count; r++)
    (void)tsdb_pagecache_remove_placeholder(db, batch->removed_offsets[r]);
}

bool tsdb_publish_rewrite_locked(timeseries_db_t *db, tsdb_page_cache_snapshot_t *batch, bool success) {
  if (!db || !batch) return false;
  tsdb_page_cache_snapshot_t *base = NULL, *next = NULL;
  if (success) {
    base = tsdb_snapshot_acquire_current(db);
    next = tsdb_pagecache_prepare_batch(base, batch, NULL, 0, 0);
    success = base && next;
  }
  if (success) {
    tsdb_pagecache_batch_sort(next);
    // Cannot fail: every other live-cache writer holds flash_write_mutex.
    success = tsdb_snapshot_compare_and_swap(db, base, next);
    if (success) next = NULL;
  }
  if (!success) abandon_compaction_outputs(db, batch);
  tsdb_snapshot_release(next);
  tsdb_snapshot_release(base);
  tsdb_snapshot_release(batch);
  return success;
}

// Retire source pages and publish replacements as one operation for inserts.
// Holding flash_write_mutex keeps the live cache unchanged throughout. Everything
// publication needs is allocated before any source changes on flash, so OOM
// leaves the sources ACTIVE and visible; flash writes cannot fail publication.
// Encoding and output writes happen before, outside these locks.
static bool finish_compaction_batch(timeseries_db_t *db, uint8_t from_level,
    const tsdb_level_page_t *pages, size_t page_count,
    tsdb_page_cache_snapshot_t *batch, bool success) {
  if (db->flash_write_mutex) tsdb_write_lock(db);

  tsdb_page_cache_snapshot_t *base = NULL, *next = NULL;
  if (success) {
    uint32_t sources[MAX_PAGES_PER_LEVEL];
    for (size_t i = 0; i < page_count; i++) sources[i] = pages[i].offset;
    base = tsdb_snapshot_acquire_current(db);
    // Room for every source to be put back if its flash state cannot change.
    next = tsdb_pagecache_prepare_batch(base, batch, sources, page_count, page_count);
    success = base && next;
  }
  if (success) {
    tsdb_mark_old_level_pages_obsolete(db, from_level, pages, page_count, base, next);
    db->active_batch = NULL;
    tsdb_pagecache_batch_sort(next);
    // Cannot fail: every other live-cache writer holds flash_write_mutex
    // (inserts, metadata writers, deletes, clear_all); this compaction's own
    // reservations are finished.
    success = tsdb_snapshot_compare_and_swap(db, base, next);
    if (success) {
      next = NULL;
    } else {
      ESP_LOGE(TAG, "Live page cache changed during compaction publication");
    }
  } else {
    db->active_batch = NULL;
    abandon_compaction_outputs(db, batch);
  }
  tsdb_snapshot_release(next);
  tsdb_snapshot_release(base);
  tsdb_snapshot_release(batch);
  db->compaction_claimed_count = 0;
  db->last_l0_cache_valid = false;
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

  // Quiescent point: nothing read before this call is used below, so pages
  // retired by earlier levels of this run can be reused.
  tsdb_epoch_quiesce(db);

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

  // Claim pages so inserts won't write to them during compaction.
  // page_count is guaranteed <= MAX_PAGES_PER_LEVEL (32) by tsdb_find_level_pages,
  // which matches TSDB_MAX_COMPACTION_CLAIMED_PAGES — so no truncation is needed.
  // The static_assert below enforces this invariant at compile time.
  static_assert(TSDB_MAX_COMPACTION_CLAIMED_PAGES >= MAX_PAGES_PER_LEVEL,
      "compaction_claimed_pages[] must be at least as large as MAX_PAGES_PER_LEVEL");
  if (db->flash_write_mutex) {
    tsdb_write_lock(db);
  }
  db->active_batch = batch;  // cleared when publication starts
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

// `next` is the snapshot about to be published, prepared without the source
// pages. A source that cannot be retired on flash is put back from `base`.
static void tsdb_mark_old_level_pages_obsolete(timeseries_db_t* db, uint8_t source_level,
                                               const tsdb_level_page_t* pages, size_t page_count,
                                               const tsdb_page_cache_snapshot_t* base,
                                               tsdb_page_cache_snapshot_t* next) {

  ESP_LOGI(TAG, "Marking %zu old level-%u pages as obsolete",
           page_count, source_level);

  // Caller holds flash_write_mutex until publication.
  for (size_t i = 0; i < page_count; i++) {
    uint32_t pofs = pages[i].offset;
    ESP_LOGI(TAG, "Marking level-%u page @0x%08" PRIx32 " as obsolete.", source_level, pofs);

    // 1) Read the existing page header
    timeseries_page_header_t hdr;
    esp_err_t err = esp_partition_read(db->partition, pofs, &hdr, sizeof(hdr));
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed to read page header @0x%08" PRIx32 " (err=0x%x)", pofs, err);
      tsdb_pagecache_restore_entry(next, base, pofs);
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
        tsdb_pagecache_restore_entry(next, base, pofs);
        continue;
      }

      // Decrement tracked L0 page count
      if (source_level == 0) {
        atomic_fetch_sub(&db->l0_page_count, 1);
      }

      ESP_LOGI(TAG, "Removed page @0x%08" PRIx32 " from cache", pofs);
    } else {
      ESP_LOGW(TAG, "Skipping page @0x%08" PRIx32 " (not an active level-%u field-data page).", pofs, source_level);
      tsdb_pagecache_restore_entry(next, base, pofs);
    }
  }

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
static bool tsdb_rewrite_page_without_deleted(timeseries_db_t* db, uint32_t old_page_ofs, uint8_t *window,
                                              bool salvage) {
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
  // Salvage (startup repair of a torn tail) keeps the well-formed prefix; the
  // caller has already established that the stop is a malformed record, not
  // a flash read failure.
  bool read_ok = salvage || !f_iter.read_error;
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
    read_ok = salvage || !f_iter.read_error;
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
    if (db->flash_write_mutex) tsdb_write_lock(db);
    bool rewritten = tsdb_rewrite_page_without_deleted(db, old_page_ofs, window, false);
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

bool timeseries_salvage_page_prefix(timeseries_db_t* db, uint32_t page_offset) {
  if (!db) return false;
  uint8_t *window = malloc(4096);
  if (!window) return false;
  if (db->flash_write_mutex) tsdb_write_lock(db);
  bool ok = tsdb_rewrite_page_without_deleted(db, page_offset, window, true);
  if (db->flash_write_mutex) xSemaphoreGive(db->flash_write_mutex);
  free(window);
  return ok;
}
