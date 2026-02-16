#include "timeseries_page_cache.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache_snapshot.h"

#include "esp_log.h"
#include <inttypes.h>
#include <string.h>

static const char* TAG = "TimeseriesPageCache";

/**
 * @brief Compare function for sorting timeseries_cached_page_t by ascending
 * offset. Used by qsort.
 */
static int page_offset_compare(const void* a, const void* b) {
  const timeseries_cached_page_t* pa = (const timeseries_cached_page_t*)a;
  const timeseries_cached_page_t* pb = (const timeseries_cached_page_t*)b;

  if (pa->offset < pb->offset) {
    return -1;
  } else if (pa->offset > pb->offset) {
    return 1;
  }
  return 0;  // equal
}

/**
 * @brief Build the page cache for the given database.
 * Scans the entire partition and builds a snapshot of all recognized pages.
 */
bool tsdb_build_page_cache(timeseries_db_t* db) {
  if (!db || !db->partition) {
    return false;
  }

  // Create a new snapshot to build into
  tsdb_page_cache_snapshot_t *snap = tsdb_snapshot_create(16);
  if (!snap) {
    return false;
  }

  // Use the page iterator to find all pages
  timeseries_page_iterator_t flash_iter;
  if (!timeseries_page_iterator_init(db, &flash_iter)) {
    tsdb_snapshot_release(snap);
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t offset = 0, size = 0;
  while (timeseries_page_iterator_next(&flash_iter, &hdr, &offset, &size)) {
    tsdb_pagecache_batch_add(snap, offset, &hdr);
  }

  // Sort the entries by ascending offset
  tsdb_pagecache_batch_sort(snap);

  ESP_LOGV(TAG, "Built page cache with %u entries.",
           (unsigned)snap->count);

  // Swap in the new snapshot
  tsdb_snapshot_swap(db, snap);
  return true;
}

/**
 * @brief Add a new entry to the page cache using CAS retry.
 * Thread-safe for concurrent readers and writers.
 */
void tsdb_pagecache_add_entry(timeseries_db_t* db, uint32_t offset, const timeseries_page_header_t* hdr) {
  if (!db || !hdr) {
    return;
  }

  for (int attempt = 0; attempt < 10; attempt++) {
    tsdb_page_cache_snapshot_t *old = tsdb_snapshot_acquire_current(db);
    tsdb_page_cache_snapshot_t *clone = tsdb_snapshot_clone(old);
    if (!clone) {
      tsdb_snapshot_release(old);
      ESP_LOGE(TAG, "OOM cloning snapshot for add_entry");
      return;
    }

    tsdb_pagecache_batch_add(clone, offset, hdr);
    tsdb_pagecache_batch_sort(clone);

    if (tsdb_snapshot_compare_and_swap(db, old, clone)) {
      tsdb_snapshot_release(old);
      return;  // Success
    }
    // CAS failed - someone else modified the snapshot, retry
    tsdb_snapshot_release(old);
    tsdb_snapshot_release(clone);
  }
  ESP_LOGE(TAG, "add_entry CAS failed after max retries, forcing swap");
  // Fallback: force swap (last resort)
  tsdb_page_cache_snapshot_t *old = tsdb_snapshot_acquire_current(db);
  tsdb_page_cache_snapshot_t *clone = tsdb_snapshot_clone(old);
  tsdb_snapshot_release(old);
  if (clone) {
    tsdb_pagecache_batch_add(clone, offset, hdr);
    tsdb_pagecache_batch_sort(clone);
    tsdb_snapshot_swap(db, clone);
  }
}

/**
 * @brief Remove a page cache entry using CAS retry.
 * Thread-safe for concurrent readers and writers.
 */
bool tsdb_pagecache_remove_entry(timeseries_db_t *db, uint32_t offset) {
  if (!db) {
    return false;
  }

  for (int attempt = 0; attempt < 10; attempt++) {
    tsdb_page_cache_snapshot_t *old = tsdb_snapshot_acquire_current(db);
    if (!old) {
      return false;
    }

    tsdb_page_cache_snapshot_t *clone = tsdb_snapshot_clone(old);
    if (!clone) {
      tsdb_snapshot_release(old);
      ESP_LOGE(TAG, "OOM cloning snapshot for remove_entry");
      return false;
    }

    bool found = tsdb_pagecache_batch_remove(clone, offset);
    if (!found) {
      tsdb_snapshot_release(old);
      tsdb_snapshot_release(clone);
      return false;
    }

    if (tsdb_snapshot_compare_and_swap(db, old, clone)) {
      tsdb_snapshot_release(old);
      return true;  // Success
    }
    // CAS failed - someone else modified the snapshot, retry
    tsdb_snapshot_release(old);
    tsdb_snapshot_release(clone);
  }
  ESP_LOGE(TAG, "remove_entry CAS failed after max retries, forcing swap");
  // Fallback: force swap (last resort)
  tsdb_page_cache_snapshot_t *old = tsdb_snapshot_acquire_current(db);
  if (!old) {
    return false;
  }
  tsdb_page_cache_snapshot_t *clone = tsdb_snapshot_clone(old);
  tsdb_snapshot_release(old);
  if (clone) {
    bool found = tsdb_pagecache_batch_remove(clone, offset);
    if (found) {
      tsdb_snapshot_swap(db, clone);
      return true;
    }
    tsdb_snapshot_release(clone);
  }
  return false;
}

uint32_t tsdb_pagecache_get_total_active_size(timeseries_db_t *db) {
  if (!db) {
    return 0;
  }

  tsdb_page_cache_snapshot_t *snap = tsdb_snapshot_acquire_current(db);
  if (!snap) {
    return 0;
  }

  uint32_t total_size = 0;
  for (size_t i = 0; i < snap->count; i++) {
    const timeseries_cached_page_t *entry = &snap->entries[i];
    if (entry->header.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
      total_size += entry->header.page_size;
    }
  }

  tsdb_snapshot_release(snap);
  return total_size;
}

uint32_t tsdb_pagecache_get_page_size(timeseries_db_t *db,
                                      uint32_t page_offset) {
  if (!db) {
    return 0;
  }

  tsdb_page_cache_snapshot_t *snap = tsdb_snapshot_acquire_current(db);
  if (!snap) {
    return 0;
  }

  uint32_t result = 0;
  for (size_t i = 0; i < snap->count; i++) {
    const timeseries_cached_page_t *entry = &snap->entries[i];
    if (entry->offset == page_offset) {
      result = entry->header.page_size;
      break;
    }
  }

  tsdb_snapshot_release(snap);
  return result;
}

void tsdb_pagecache_clear(timeseries_db_t *db) {
  if (!db) {
    return;
  }

  tsdb_page_cache_snapshot_t *empty = tsdb_snapshot_create(0);
  if (empty) {
    tsdb_snapshot_swap(db, empty);
  }
}

// ---------------------------------------------------------------------------
// Batch API (operates on a private mutable snapshot -- not shared)
// ---------------------------------------------------------------------------

tsdb_page_cache_snapshot_t *tsdb_pagecache_begin_batch(timeseries_db_t *db) {
  if (!db) {
    return NULL;
  }

  tsdb_page_cache_snapshot_t *current = tsdb_snapshot_acquire_current(db);
  tsdb_page_cache_snapshot_t *batch = tsdb_snapshot_clone(current);
  tsdb_snapshot_release(current);

  return batch;
}

void tsdb_pagecache_batch_add(tsdb_page_cache_snapshot_t *snap, uint32_t offset,
                              const timeseries_page_header_t *hdr) {
  if (!snap || !hdr) {
    return;
  }

  // Check if an entry at this offset already exists -- update it in place
  for (size_t i = 0; i < snap->count; i++) {
    if (snap->entries[i].offset == offset) {
      memcpy(&snap->entries[i].header, hdr, sizeof(timeseries_page_header_t));
      return;  // Updated existing entry
    }
  }

  // No existing entry -- grow array if needed and append
  if (snap->count == snap->capacity) {
    size_t newcap = (snap->capacity == 0) ? 8 : snap->capacity * 2;
    timeseries_cached_page_t *newarr =
        realloc(snap->entries, newcap * sizeof(*newarr));
    if (!newarr) {
      ESP_LOGE(TAG, "OOM expanding batch snapshot");
      return;
    }
    snap->entries = newarr;
    snap->capacity = newcap;
  }

  timeseries_cached_page_t *entry = &snap->entries[snap->count++];
  entry->offset = offset;
  memcpy(&entry->header, hdr, sizeof(timeseries_page_header_t));
}

bool tsdb_pagecache_batch_remove(tsdb_page_cache_snapshot_t *snap, uint32_t offset) {
  if (!snap || !snap->entries) {
    return false;
  }

  for (size_t i = 0; i < snap->count; i++) {
    if (snap->entries[i].offset == offset) {
      // Ensure removed_offsets has capacity BEFORE modifying entries,
      // so OOM doesn't leave entries array in an inconsistent state
      if (snap->removed_count == snap->removed_capacity) {
        size_t newcap = (snap->removed_capacity == 0) ? 8 : snap->removed_capacity * 2;
        uint32_t *newarr = realloc(snap->removed_offsets, newcap * sizeof(uint32_t));
        if (!newarr) {
          ESP_LOGE(TAG, "OOM tracking removed offset 0x%08" PRIx32 " - removal aborted", offset);
          return false;
        }
        snap->removed_offsets = newarr;
        snap->removed_capacity = newcap;
      }

      // Now safe to remove from entries
      size_t remaining = snap->count - (i + 1);
      if (remaining > 0) {
        memmove(&snap->entries[i], &snap->entries[i + 1],
                remaining * sizeof(snap->entries[i]));
      }
      snap->count--;

      snap->removed_offsets[snap->removed_count++] = offset;

      ESP_LOGV(TAG, "Batch removed page cache entry for offset=0x%08" PRIx32, offset);
      return true;
    }
  }

  return false;
}

void tsdb_pagecache_batch_sort(tsdb_page_cache_snapshot_t *snap) {
  if (!snap || snap->count < 2) {
    return;
  }
  qsort(snap->entries, snap->count, sizeof(timeseries_cached_page_t),
        page_offset_compare);
}

/**
 * @brief Commit a batch snapshot by merging with the current live snapshot.
 *
 * Any entries added to the live snapshot by other threads (e.g., inserts)
 * during the batch period are merged in, except those whose offset appears
 * in the batch's removed_offsets list. Uses CAS retry to avoid lost updates.
 */
void tsdb_pagecache_commit_batch(timeseries_db_t *db, tsdb_page_cache_snapshot_t *batch) {
  if (!db || !batch) {
    return;
  }

  // Save the batch base state so we can fully restore on CAS failure.
  // batch_add can overwrite existing entries in-place, so resetting count
  // alone is insufficient -- we must restore the original entry data too.
  size_t batch_base_count = batch->count;
  timeseries_cached_page_t *saved_entries = NULL;
  if (batch_base_count > 0) {
    saved_entries = malloc(batch_base_count * sizeof(timeseries_cached_page_t));
    if (saved_entries) {
      memcpy(saved_entries, batch->entries, batch_base_count * sizeof(timeseries_cached_page_t));
    }
  }

  for (int attempt = 0; attempt < 10; attempt++) {
    // Restore batch to its base state on retry
    if (attempt > 0) {
      batch->count = batch_base_count;
      if (saved_entries) {
        memcpy(batch->entries, saved_entries, batch_base_count * sizeof(timeseries_cached_page_t));
      }
    }

    // Acquire the current live snapshot
    tsdb_page_cache_snapshot_t *live = tsdb_snapshot_acquire_current(db);
    if (!live) {
      // No live snapshot, just swap in the batch
      tsdb_snapshot_swap(db, batch);
      free(saved_entries);
      return;
    }

    // Find entries in live that are NOT in batch and NOT in removed_offsets
    // These are entries that were added by other threads during the batch period
    for (size_t i = 0; i < live->count; i++) {
      uint32_t live_offset = live->entries[i].offset;

      // Check if this offset was removed by the batch
      bool was_removed = false;
      for (size_t r = 0; r < batch->removed_count; r++) {
        if (batch->removed_offsets[r] == live_offset) {
          was_removed = true;
          break;
        }
      }
      if (was_removed) {
        continue;
      }

      // Check if this offset already exists in the batch
      bool already_in_batch = false;
      for (size_t j = 0; j < batch->count; j++) {
        if (batch->entries[j].offset == live_offset) {
          already_in_batch = true;
          break;
        }
      }

      if (!already_in_batch) {
        // This entry was added by another thread during the batch -- merge it in
        tsdb_pagecache_batch_add(batch, live_offset, &live->entries[i].header);
      }
    }

    // Sort the merged result
    tsdb_pagecache_batch_sort(batch);

    // Attempt CAS swap
    if (tsdb_snapshot_compare_and_swap(db, live, batch)) {
      tsdb_snapshot_release(live);
      free(saved_entries);
      return;  // Success
    }
    // CAS failed - someone else modified the snapshot, retry
    tsdb_snapshot_release(live);
  }

  ESP_LOGE(TAG, "commit_batch CAS failed after max retries, forcing swap");
  // Fallback: force swap
  tsdb_pagecache_batch_sort(batch);
  tsdb_snapshot_swap(db, batch);
  free(saved_entries);
}
