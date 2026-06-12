#include "timeseries_page_cache.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache_snapshot.h"

#include "esp_log.h"
#include <string.h>

static const char *TAG = "TimeseriesPageCache";

/**
 * @brief Compare function for sorting timeseries_cached_page_t by ascending
 * offset. Used by qsort.
 */
static int page_offset_compare(const void *a, const void *b) {
  const timeseries_cached_page_t *pa = (const timeseries_cached_page_t *)a;
  const timeseries_cached_page_t *pb = (const timeseries_cached_page_t *)b;

  if (pa->offset < pb->offset) {
    return -1;
  } else if (pa->offset > pb->offset) {
    return 1;
  }
  return 0; // equal
}

/**
 * @brief Binary search for `offset` in the offset-sorted entries array.
 *
 * The entries array is maintained sorted by offset at all times (every
 * mutation goes through batch_add/batch_remove, which preserve order). On a
 * hit returns true and sets *idx to the matching index; on a miss returns
 * false and sets *idx to the lower-bound insertion point.
 */
static bool pagecache_find_index(const tsdb_page_cache_snapshot_t *snap,
                                 uint32_t offset, size_t *idx) {
  size_t lo = 0, hi = snap->count;  // search the half-open range [lo, hi)
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    uint32_t mo = snap->entries[mid].offset;
    if (mo == offset) {
      *idx = mid;
      return true;
    }
    if (mo < offset) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  *idx = lo;
  return false;
}

/**
 * @brief Build the page cache for the given database.
 * Scans the entire partition and builds a snapshot of all recognized pages.
 */
bool tsdb_build_page_cache(timeseries_db_t *db) {
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
    if (!tsdb_pagecache_batch_add(snap, offset, &hdr)) {
      ESP_LOGE(TAG, "OOM building page cache at offset 0x%08" PRIx32, offset);
      tsdb_snapshot_release(snap);
      return false;
    }
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
 * @brief Refresh a private clone from the current live snapshot.
 * Reuses the existing allocation when possible to avoid malloc/free churn.
 */
static bool snapshot_refresh_clone(tsdb_page_cache_snapshot_t *clone,
                                   const tsdb_page_cache_snapshot_t *src) {
  if (src->count > clone->capacity) {
    size_t newcap = src->count + 4; // small headroom for the add
    timeseries_cached_page_t *newarr =
        realloc(clone->entries, newcap * sizeof(*newarr));
    if (!newarr) return false;
    clone->entries = newarr;
    clone->capacity = newcap;
  }
  if (src->count > 0) {
    memcpy(clone->entries, src->entries,
           src->count * sizeof(timeseries_cached_page_t));
  }
  clone->count = src->count;
  return true;
}

/**
 * @brief Add a new entry to the page cache using CAS retry.
 * Thread-safe for concurrent readers and writers.
 * Reuses clone buffer on retry to reduce allocation overhead.
 */
bool tsdb_pagecache_add_entry(timeseries_db_t *db, uint32_t offset,
                              const timeseries_page_header_t *hdr) {
  if (!db || !hdr) {
    return false;
  }

  tsdb_page_cache_snapshot_t *clone = NULL;

  for (int attempt = 0; attempt < 10; attempt++) {
    tsdb_page_cache_snapshot_t *old = tsdb_snapshot_acquire_current(db);

    if (!clone) {
      clone = tsdb_snapshot_clone(old);
      if (!clone) {
        tsdb_snapshot_release(old);
        ESP_LOGE(TAG, "OOM cloning snapshot for add_entry");
        return false;
      }
    } else {
      // Reuse existing clone buffer — refresh from new current snapshot
      if (!snapshot_refresh_clone(clone, old)) {
        tsdb_snapshot_release(old);
        tsdb_snapshot_release(clone);
        ESP_LOGE(TAG, "OOM refreshing clone for add_entry retry");
        return false;
      }
    }

    if (!tsdb_pagecache_batch_add(clone, offset, hdr)) {
      tsdb_snapshot_release(old);
      tsdb_snapshot_release(clone);
      ESP_LOGE(TAG, "OOM in add_entry batch_add");
      return false;
    }
    tsdb_pagecache_batch_sort(clone);

    if (tsdb_snapshot_compare_and_swap(db, old, clone)) {
      tsdb_snapshot_release(old);
      return true;  // Success — clone is now the live snapshot
    }
    // CAS failed — retry with reused clone
    tsdb_snapshot_release(old);
  }

  ESP_LOGE(TAG, "add_entry CAS failed after max retries, forcing swap");
  // Fallback: force swap
  tsdb_page_cache_snapshot_t *old = tsdb_snapshot_acquire_current(db);
  if (old && clone) {
    if (!snapshot_refresh_clone(clone, old)) {
      tsdb_snapshot_release(old);
      tsdb_snapshot_release(clone);
      ESP_LOGE(TAG, "OOM refreshing clone for add_entry fallback");
      return false;
    }
    tsdb_snapshot_release(old);
    if (!tsdb_pagecache_batch_add(clone, offset, hdr)) {
      tsdb_snapshot_release(clone);
      ESP_LOGE(TAG, "OOM in add_entry fallback batch_add");
      return false;
    }
    tsdb_pagecache_batch_sort(clone);
    tsdb_snapshot_swap(db, clone);
    return true;
  } else {
    if (old) tsdb_snapshot_release(old);
    if (clone) tsdb_snapshot_release(clone);
    return false;
  }
}

/**
 * @brief Remove a page cache entry using CAS retry.
 * Thread-safe for concurrent readers and writers.
 * Reuses clone buffer on retry to reduce allocation overhead.
 */
bool tsdb_pagecache_remove_entry(timeseries_db_t *db, uint32_t offset) {
  if (!db) {
    return false;
  }

  tsdb_page_cache_snapshot_t *clone = NULL;

  for (int attempt = 0; attempt < 10; attempt++) {
    tsdb_page_cache_snapshot_t *old = tsdb_snapshot_acquire_current(db);
    if (!old) {
      if (clone) tsdb_snapshot_release(clone);
      return false;
    }

    if (!clone) {
      clone = tsdb_snapshot_clone(old);
      if (!clone) {
        tsdb_snapshot_release(old);
        ESP_LOGE(TAG, "OOM cloning snapshot for remove_entry");
        return false;
      }
    } else {
      if (!snapshot_refresh_clone(clone, old)) {
        tsdb_snapshot_release(old);
        tsdb_snapshot_release(clone);
        ESP_LOGE(TAG, "OOM refreshing clone for remove_entry retry");
        return false;
      }
    }

    bool found = tsdb_pagecache_batch_remove(clone, offset);
    if (!found) {
      tsdb_snapshot_release(old);
      tsdb_snapshot_release(clone);
      return false;
    }

    if (tsdb_snapshot_compare_and_swap(db, old, clone)) {
      tsdb_snapshot_release(old);
      return true;  // Success — clone is now the live snapshot
    }
    // CAS failed — retry with reused clone
    tsdb_snapshot_release(old);
  }

  ESP_LOGE(TAG, "remove_entry CAS failed after max retries, forcing swap");
  // Fallback: force swap
  tsdb_page_cache_snapshot_t *old = tsdb_snapshot_acquire_current(db);
  if (!old) {
    if (clone) tsdb_snapshot_release(clone);
    return false;
  }
  if (clone) {
    if (!snapshot_refresh_clone(clone, old)) {
      tsdb_snapshot_release(old);
      tsdb_snapshot_release(clone);
      ESP_LOGE(TAG, "OOM refreshing clone for remove_entry fallback");
      return false;
    }
    tsdb_snapshot_release(old);
    bool found = tsdb_pagecache_batch_remove(clone, offset);
    if (found) {
      tsdb_snapshot_swap(db, clone);
      return true;
    }
    tsdb_snapshot_release(clone);
  } else {
    tsdb_snapshot_release(old);
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

  // Entries are sorted by offset -- binary search instead of a linear scan.
  uint32_t result = 0;
  size_t idx;
  if (pagecache_find_index(snap, page_offset, &idx)) {
    result = snap->entries[idx].header.page_size;
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

bool tsdb_pagecache_batch_add(tsdb_page_cache_snapshot_t *snap, uint32_t offset,
                              const timeseries_page_header_t *hdr) {
  if (!snap || !hdr) {
    return false;
  }

  // Entries are kept sorted by offset: binary-search for an existing entry
  // (O(log n)) instead of a linear scan, and insert at the sorted position so
  // the array never needs a full qsort afterwards.
  size_t pos;
  if (pagecache_find_index(snap, offset, &pos)) {
    memcpy(&snap->entries[pos].header, hdr, sizeof(timeseries_page_header_t));
    return true;  // Updated existing entry in place
  }

  // No existing entry -- grow array if needed
  if (snap->count == snap->capacity) {
    size_t newcap = (snap->capacity == 0) ? 8 : snap->capacity * 2;
    timeseries_cached_page_t *newarr =
        realloc(snap->entries, newcap * sizeof(*newarr));
    if (!newarr) {
      ESP_LOGE(TAG, "OOM expanding batch snapshot");
      return false;
    }
    snap->entries = newarr;
    snap->capacity = newcap;
  }

  // Insert at the sorted position `pos`, shifting the tail one slot right.
  size_t tail = snap->count - pos;
  if (tail > 0) {
    memmove(&snap->entries[pos + 1], &snap->entries[pos],
            tail * sizeof(snap->entries[0]));
  }
  snap->entries[pos].offset = offset;
  memcpy(&snap->entries[pos].header, hdr, sizeof(timeseries_page_header_t));
  snap->count++;
  return true;
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
  // batch_add/batch_remove keep the array sorted by offset, so it is normally
  // already sorted here. Verify in O(n) and skip the O(n log n) qsort in that
  // (overwhelmingly common) case; the qsort remains as a safety net.
  bool sorted = true;
  for (size_t i = 1; i < snap->count; i++) {
    if (snap->entries[i - 1].offset > snap->entries[i].offset) {
      sorted = false;
      break;
    }
  }
  if (sorted) {
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
    if (!saved_entries) {
      // Cannot safely retry CAS without backup — force-swap immediately
      ESP_LOGW(TAG, "OOM saving batch state, skipping CAS merge — forcing swap");
      tsdb_pagecache_batch_sort(batch);
      tsdb_snapshot_swap(db, batch);
      return;
    }
    memcpy(saved_entries, batch->entries, batch_base_count * sizeof(timeseries_cached_page_t));
  }

  for (int attempt = 0; attempt < 10; attempt++) {
    // Restore batch to its base state on retry
    if (attempt > 0) {
      batch->count = batch_base_count;
      memcpy(batch->entries, saved_entries, batch_base_count * sizeof(timeseries_cached_page_t));
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
          // The batch inherited this offset when it was cloned. If the live
          // snapshot has since received a newer header for the same offset (a
          // concurrent in-place update, e.g. a metadata page header rewrite at
          // metadata.c), adopt it so the update isn't dropped on commit. Page
          // (re)allocations always bump sequence_num, and compaction only adds
          // new offsets or removes via removed_offsets (never rewrites an
          // existing offset in place), so a higher live seq unambiguously means
          // the batch's copy is stale.
          if (live->entries[i].header.sequence_num > batch->entries[j].header.sequence_num) {
            batch->entries[j].header = live->entries[i].header;
          }
          break;
        }
      }

      if (!already_in_batch) {
        // This entry was added by another thread during the batch -- merge it in
        if (!tsdb_pagecache_batch_add(batch, live_offset, &live->entries[i].header)) {
          ESP_LOGE(TAG, "OOM merging live entry in commit_batch, forcing swap with partial merge");
          tsdb_snapshot_release(live);
          free(saved_entries);
          // Force-swap the batch as-is rather than leaking it
          tsdb_pagecache_batch_sort(batch);
          tsdb_snapshot_swap(db, batch);
          return;
        }
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

  // Final merge with live snapshot before force-swap to avoid dropping
  // entries added by concurrent inserts since the last retry.
  batch->count = batch_base_count;
  if (saved_entries) {
    memcpy(batch->entries, saved_entries, batch_base_count * sizeof(timeseries_cached_page_t));
  }

  tsdb_page_cache_snapshot_t *live = tsdb_snapshot_acquire_current(db);
  if (live) {
    for (size_t i = 0; i < live->count; i++) {
      uint32_t live_offset = live->entries[i].offset;

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

      bool already_in_batch = false;
      for (size_t j = 0; j < batch->count; j++) {
        if (batch->entries[j].offset == live_offset) {
          already_in_batch = true;
          // The batch inherited this offset when it was cloned. If the live
          // snapshot has since received a newer header for the same offset (a
          // concurrent in-place update, e.g. a metadata page header rewrite at
          // metadata.c), adopt it so the update isn't dropped on commit. Page
          // (re)allocations always bump sequence_num, and compaction only adds
          // new offsets or removes via removed_offsets (never rewrites an
          // existing offset in place), so a higher live seq unambiguously means
          // the batch's copy is stale.
          if (live->entries[i].header.sequence_num > batch->entries[j].header.sequence_num) {
            batch->entries[j].header = live->entries[i].header;
          }
          break;
        }
      }

      if (!already_in_batch) {
        if (!tsdb_pagecache_batch_add(batch, live_offset, &live->entries[i].header)) {
          ESP_LOGE(TAG, "OOM merging live entry in force-swap fallback");
          break;
        }
      }
    }
    tsdb_snapshot_release(live);
  }

  tsdb_pagecache_batch_sort(batch);
  tsdb_snapshot_swap(db, batch);
  free(saved_entries);
}
