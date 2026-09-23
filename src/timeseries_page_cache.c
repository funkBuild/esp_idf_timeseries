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
                                   const tsdb_page_cache_snapshot_t *src, size_t additional) {
  size_t count = src ? src->count : 0;
  size_t maximum = SIZE_MAX / sizeof(*clone->entries);
  if (count > maximum || additional > maximum - count) return false;
  size_t needed = count + additional;
  if (needed > clone->capacity) {
    timeseries_cached_page_t *entries = realloc(clone->entries, needed * sizeof(*entries));
    if (!entries) return false;
    clone->entries = entries;
    clone->capacity = needed;
  }
  if (count) memcpy(clone->entries, src->entries, count * sizeof(*clone->entries));
  clone->count = count;
  return true;
}

// A full, unshared cache will otherwise copy its entire array on every add.
// Grow by 25% (at least eight entries) so later adds can reuse storage. The
// updater holds a reference throughout: readers arriving after this advisory
// refcount check still see an immutable old snapshot. Shared copies stay tight.
static size_t add_clone_room(const tsdb_page_cache_snapshot_t *old, uint32_t offset,
                              size_t *required) {
  size_t index;
  *required = old && pagecache_find_index(old, offset, &index) ? 0 : 1;
  if (!*required || !old || old->count != old->capacity ||
      atomic_load(&old->refcount) != 2) return *required;

  size_t maximum = SIZE_MAX / sizeof(*old->entries);
  if (old->count >= maximum) return *required; // clone rejects impossible size
  size_t extra = old->count / 4;
  if (extra < 8) extra = 8;
  if (extra > maximum - old->count) extra = maximum - old->count;
  return extra;
}

static void erase_entry(tsdb_page_cache_snapshot_t *snap, size_t index) {
  size_t tail = snap->count - index - 1;
  if (tail) memmove(snap->entries + index, snap->entries + index + 1, tail * sizeof(*snap->entries));
  snap->count--;
}

static void insert_entry(tsdb_page_cache_snapshot_t *snap, size_t index, uint32_t offset,
                         const timeseries_page_header_t *header) {
  size_t tail = snap->count - index;
  if (tail) memmove(snap->entries + index + 1, snap->entries + index, tail * sizeof(*snap->entries));
  snap->entries[index].offset = offset;
  memcpy(&snap->entries[index].header, header, sizeof(*header));
  snap->count++;
}

// Acquiring snapshot references uses this mutex too. A sole database reference
// lets us reuse storage safely; pinned readers always retain immutable arrays.
// Only mutations that fit existing storage run here, keeping allocation outside
// the normal snapshot-reader critical section.
static bool update_unshared(timeseries_db_t *db, uint32_t offset,
                             const timeseries_page_header_t *header, bool *handled) {
  *handled = false;
  bool ok = false;
  xSemaphoreTake(db->snapshot_mutex, portMAX_DELAY);
  tsdb_page_cache_snapshot_t *snap = db->current_snapshot;
  if (snap && atomic_load(&snap->refcount) == 1) {
    size_t index;
    bool found = pagecache_find_index(snap, offset, &index);
    if (!header) {
      *handled = true;
      if (found) { erase_entry(snap, index); ok = true; }
    } else if (found || snap->count < snap->capacity) {
      if (found) memcpy(&snap->entries[index].header, header, sizeof(*header));
      else insert_entry(snap, index, offset, header);
      *handled = ok = true;
    }
  }
  xSemaphoreGive(db->snapshot_mutex);
  return ok;
}

// A live removal has no batch to merge later and needs no removal-history array.
static bool remove_live_entry(tsdb_page_cache_snapshot_t *snap, uint32_t offset) {
  size_t index;
  if (!pagecache_find_index(snap, offset, &index)) return false;
  erase_entry(snap, index);
  return true;
}

// Rare contention fallback: refresh and publish under the same lock. Taking a
// snapshot, releasing the lock and force-swapping it could lose intervening adds.
static bool publish_contended(timeseries_db_t *db, tsdb_page_cache_snapshot_t *clone,
                               uint32_t offset, const timeseries_page_header_t *header) {
  xSemaphoreTake(db->snapshot_mutex, portMAX_DELAY);
  tsdb_page_cache_snapshot_t *old = db->current_snapshot;
  bool ok = snapshot_refresh_clone(clone, old, header ? 1 : 0);
  if (ok) ok = header ? tsdb_pagecache_batch_add(clone, offset, header) : remove_live_entry(clone, offset);
  if (ok) db->current_snapshot = clone;
  xSemaphoreGive(db->snapshot_mutex);
  if (ok) tsdb_snapshot_release(old);
  else tsdb_snapshot_release(clone);
  return ok;
}

/**
 * @brief Add a new entry to the page cache using CAS retry.
 * Thread-safe for concurrent readers and writers.
 * Reuses clone buffer on retry to reduce allocation overhead.
 */
bool tsdb_pagecache_add_entry(timeseries_db_t *db, uint32_t offset,
                              const timeseries_page_header_t *hdr) {
  if (!db || !db->snapshot_mutex || !hdr) {
    return false;
  }

  bool handled;
  bool updated = update_unshared(db, offset, hdr, &handled);
  if (handled) return updated;

  tsdb_page_cache_snapshot_t *clone = NULL;

  for (int attempt = 0; attempt < 10; attempt++) {
    tsdb_page_cache_snapshot_t *old = tsdb_snapshot_acquire_current(db);
    size_t required;
    size_t room = add_clone_room(old, offset, &required);

    if (!clone) {
      clone = tsdb_snapshot_clone_with_room(old, room);
      // Spare capacity is optional under memory pressure. Keep this allocation
      // outside snapshot_mutex, just like the initial clone attempt.
      if (!clone && room > required) clone = tsdb_snapshot_clone_with_room(old, required);
      if (!clone) {
        tsdb_snapshot_release(old);
        ESP_LOGE(TAG, "OOM cloning snapshot for add_entry");
        return false;
      }
    } else {
      // Reuse existing clone buffer — refresh from new current snapshot
      if (!snapshot_refresh_clone(clone, old, room) &&
          !(room > required && snapshot_refresh_clone(clone, old, required))) {
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

    if (tsdb_snapshot_compare_and_swap(db, old, clone)) {
      tsdb_snapshot_release(old);
      return true;  // Success — clone is now the live snapshot
    }
    // CAS failed — retry with reused clone
    tsdb_snapshot_release(old);
  }

  return publish_contended(db, clone, offset, hdr);
}

/**
 * @brief Remove a page cache entry using CAS retry.
 * Thread-safe for concurrent readers and writers.
 * Reuses clone buffer on retry to reduce allocation overhead.
 */
bool tsdb_pagecache_remove_entry(timeseries_db_t *db, uint32_t offset) {
  if (!db || !db->snapshot_mutex) {
    return false;
  }

  bool handled;
  bool updated = update_unshared(db, offset, NULL, &handled);
  if (handled) return updated;

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
      if (!snapshot_refresh_clone(clone, old, 0)) {
        tsdb_snapshot_release(old);
        tsdb_snapshot_release(clone);
        ESP_LOGE(TAG, "OOM refreshing clone for remove_entry retry");
        return false;
      }
    }

    bool found = remove_live_entry(clone, offset);
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

  return publish_contended(db, clone, offset, NULL);
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
  if (!batch) {
    return NULL;
  }

  // Remember what the batch started from (entries are sorted by offset), so
  // the commit can tell a live removal from a page the batch added itself.
  if (batch->count > 0) {
    batch->base_offsets = malloc(batch->count * sizeof(*batch->base_offsets));
    if (!batch->base_offsets) {
      tsdb_snapshot_release(batch);
      return NULL;
    }
    for (size_t i = 0; i < batch->count; i++) {
      batch->base_offsets[i] = batch->entries[i].offset;
    }
    batch->base_count = batch->count;
  }
  return batch;
}

static bool batch_base_contains(const tsdb_page_cache_snapshot_t *batch, uint32_t offset) {
  size_t lo = 0, hi = batch->base_count;
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    if (batch->base_offsets[mid] == offset) return true;
    if (batch->base_offsets[mid] < offset) lo = mid + 1;
    else hi = mid;
  }
  return false;
}

bool tsdb_pagecache_batch_reserve(tsdb_page_cache_snapshot_t *snap, size_t additional) {
  if (!snap) return false;
  size_t maximum = SIZE_MAX / sizeof(*snap->entries);
  if (snap->count > maximum || additional > maximum - snap->count) return false;
  size_t needed = snap->count + additional;
  if (needed <= snap->capacity) return true;
  size_t capacity = snap->capacity ? (snap->capacity > maximum / 2 ? maximum : snap->capacity * 2) : 8;
  if (capacity < needed) capacity = needed;
  timeseries_cached_page_t *entries = realloc(snap->entries, capacity * sizeof(*entries));
  if (!entries) return false;
  snap->entries = entries;
  snap->capacity = capacity;
  return true;
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

  if (!tsdb_pagecache_batch_reserve(snap, 1)) return false;

  insert_entry(snap, pos, offset, hdr);
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
 * Publish a private compaction batch together with concurrent live updates.
 * Serialize the in-memory merge with snapshot publication, not flash I/O.
 * This avoids retry copies and, crucially, never force-publishes a partial
 * merge after OOM. Both success and failure consume the caller's batch.
 */
bool tsdb_pagecache_commit_batch(timeseries_db_t *db, tsdb_page_cache_snapshot_t *batch) {
  if (!db || !db->snapshot_mutex || !batch) return false;

  xSemaphoreTake(db->snapshot_mutex, portMAX_DELAY);
  tsdb_page_cache_snapshot_t *live = db->current_snapshot;
  if (live) {
    // Pages removed from the live cache while the batch was open (for example
    // a metadata page retired by an insert's metadata compaction) are still in
    // the batch's copy. Drop them, or the commit would list them ACTIVE again
    // while their flash region is already free for reuse.
    size_t kept = 0;
    for (size_t i = 0; i < batch->count; i++) {
      uint32_t offset = batch->entries[i].offset;
      size_t pos;
      if (batch_base_contains(batch, offset) && !pagecache_find_index(live, offset, &pos)) {
        continue;
      }
      batch->entries[kept++] = batch->entries[i];
    }
    batch->count = kept;

    for (size_t i = 0; i < live->count; i++) {
      uint32_t offset = live->entries[i].offset;
      bool removed = false;
      for (size_t r = 0; r < batch->removed_count; r++) {
        if (batch->removed_offsets[r] == offset) {
          removed = true;
          break;
        }
      }
      if (removed) continue;

      size_t pos;
      if (pagecache_find_index(batch, offset, &pos)) {
        if (live->entries[i].header.sequence_num > batch->entries[pos].header.sequence_num)
          batch->entries[pos].header = live->entries[i].header;
      } else if (!tsdb_pagecache_batch_add(batch, offset, &live->entries[i].header)) {
        xSemaphoreGive(db->snapshot_mutex);
        tsdb_snapshot_release(batch);
        ESP_LOGE(TAG, "OOM merging compaction batch; keeping the live snapshot");
        return false;
      }
    }
  }
  db->current_snapshot = batch;
  xSemaphoreGive(db->snapshot_mutex);
  tsdb_snapshot_release(live);
  return true;
}
