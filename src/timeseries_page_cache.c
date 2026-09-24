#include "timeseries_page_cache.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache_snapshot.h"
#include "timeseries_epoch.h"
#include "timeseries_repair.h"

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
    tsdb_init_fault_transient(db);
    return false;
  }

  // Use the page iterator to find all pages
  timeseries_page_iterator_t flash_iter;
  if (!timeseries_page_iterator_init(db, &flash_iter)) {
    tsdb_snapshot_release(snap);
    return false;
  }

  // A header that cannot be read is recorded for init's recovery and the
  // scan resumes after it: a cache missing the pages behind it would let the
  // allocator erase them. The build then fails rather than publish a cache
  // with a hole in it.
  timeseries_page_header_t hdr;
  uint32_t offset = 0, size = 0;
  size_t unreadable = 0;
  for (;;) {
    if (!timeseries_page_iterator_next(&flash_iter, &hdr, &offset, &size)) {
      if (!flash_iter.read_error) break;
      tsdb_init_fault_bad_page(db, flash_iter.error_offset, 4096);
      unreadable++;
      continue;
    }
    if (!tsdb_pagecache_batch_add(snap, offset, &hdr)) {
      ESP_LOGE(TAG, "OOM building page cache at offset 0x%08" PRIx32, offset);
      tsdb_init_fault_transient(db);
      tsdb_snapshot_release(snap);
      return false;
    }
  }
  if (unreadable) {
    ESP_LOGE(TAG, "%zu page header(s) could not be read; page cache not built", unreadable);
    tsdb_snapshot_release(snap);
    return false;
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
      if (found) {
        // An older snapshot a reader still holds may list it.
        tsdb_retire_region_locked(db, offset, snap->entries[index].header.page_size);
        erase_entry(snap, index);
        ok = true;
      }
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
  if (ok) {
    tsdb_retire_diff_locked(db, old, clone);
    db->current_snapshot = clone;
  }
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
// Batch API: a compaction's journal of its own page-cache changes
// ---------------------------------------------------------------------------
//
// A batch is private to one compaction. It records only what that compaction
// intends: "set" entries (the pages it writes, kept sorted in entries[]) and
// removals (removed_offsets[]: its source pages and the regions it gave up).
// Per offset the last operation wins. At publication the journal is replayed
// onto the live snapshot current at that moment, so nothing another task did
// meanwhile can be lost or resurrected, and no merge rule is needed.

tsdb_page_cache_snapshot_t *tsdb_pagecache_begin_batch(timeseries_db_t *db) {
  if (!db) {
    return NULL;
  }
  return tsdb_snapshot_create(8);
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

static void forget_removal(tsdb_page_cache_snapshot_t *snap, uint32_t offset) {
  for (size_t r = 0; r < snap->removed_count; r++) {
    if (snap->removed_offsets[r] == offset) {
      snap->removed_offsets[r] = snap->removed_offsets[--snap->removed_count];
      return;
    }
  }
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
  // A journal that gave this region up and now uses it again keeps it.
  forget_removal(snap, offset);
  return true;
}

bool tsdb_pagecache_batch_remove(tsdb_page_cache_snapshot_t *snap, uint32_t offset) {
  if (!snap) {
    return false;
  }
  for (size_t r = 0; r < snap->removed_count; r++) {
    if (snap->removed_offsets[r] == offset) return true;
  }
  // Grow the removal list BEFORE dropping any pending entry, so OOM leaves
  // the journal unchanged.
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
  size_t pos;
  if (pagecache_find_index(snap, offset, &pos)) erase_entry(snap, pos);
  snap->removed_offsets[snap->removed_count++] = offset;
  ESP_LOGV(TAG, "Batch removed page cache entry for offset=0x%08" PRIx32, offset);
  return true;
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

bool tsdb_pagecache_is_placeholder(const timeseries_page_header_t *hdr) {
  return hdr && hdr->page_type == TSDB_PLACEHOLDER_PAGE_TYPE;
}

bool tsdb_pagecache_remove_placeholder(timeseries_db_t *db, uint32_t offset) {
  tsdb_page_cache_snapshot_t *live = tsdb_snapshot_acquire_current(db);
  size_t pos;
  bool placeholder = live && pagecache_find_index(live, offset, &pos) &&
                     tsdb_pagecache_is_placeholder(&live->entries[pos].header);
  tsdb_snapshot_release(live);
  return placeholder && tsdb_pagecache_remove_entry(db, offset);
}

tsdb_page_cache_snapshot_t *tsdb_pagecache_prepare_batch(const tsdb_page_cache_snapshot_t *base,
                                                         const tsdb_page_cache_snapshot_t *batch,
                                                         const uint32_t *also_remove, size_t also_remove_count,
                                                         size_t spare) {
  if (!batch) return NULL;
  size_t maximum = SIZE_MAX / sizeof(timeseries_cached_page_t);
  if (batch->count > maximum || spare > maximum - batch->count) return NULL;
  tsdb_page_cache_snapshot_t *next = tsdb_snapshot_clone_with_room(base, batch->count + spare);
  if (!next) return NULL;
  size_t pos;
  // A region the journal gave up is live, if at all, only as the journal's
  // own reservation placeholder (its pages are published by this replay). If
  // live lists anything else there, the region was released earlier and has
  // since been allocated and published by another writer: leave that alone.
  for (size_t r = 0; r < batch->removed_count; r++)
    if (pagecache_find_index(next, batch->removed_offsets[r], &pos) &&
        tsdb_pagecache_is_placeholder(&next->entries[pos].header))
      erase_entry(next, pos);
  for (size_t r = 0; r < also_remove_count; r++)
    if (pagecache_find_index(next, also_remove[r], &pos)) erase_entry(next, pos);
  for (size_t i = 0; i < batch->count; i++) {
    // Room was reserved above, so this cannot allocate or fail.
    (void)tsdb_pagecache_batch_add(next, batch->entries[i].offset, &batch->entries[i].header);
  }
  return next;
}

bool tsdb_pagecache_restore_entry(tsdb_page_cache_snapshot_t *next, const tsdb_page_cache_snapshot_t *base,
                                  uint32_t offset) {
  size_t pos;
  if (!next || !base || !pagecache_find_index(base, offset, &pos)) return false;
  return tsdb_pagecache_batch_add(next, offset, &base->entries[pos].header);
}

bool tsdb_pagecache_commit_batch(timeseries_db_t *db, tsdb_page_cache_snapshot_t *batch) {
  if (!db || !db->snapshot_mutex || !batch) return false;
  bool ok = false;
  // Replay onto whatever is live; if it changed meanwhile, replay again. The
  // allocation stays outside snapshot_mutex.
  for (int attempt = 0; attempt < 10 && !ok; attempt++) {
    tsdb_page_cache_snapshot_t *base = tsdb_snapshot_acquire_current(db);
    tsdb_page_cache_snapshot_t *next = tsdb_pagecache_prepare_batch(base, batch, NULL, 0, 0);
    if (!next) {
      tsdb_snapshot_release(base);
      ESP_LOGE(TAG, "OOM publishing compaction batch; keeping the live snapshot");
      break;
    }
    ok = tsdb_snapshot_compare_and_swap(db, base, next);
    if (!ok) tsdb_snapshot_release(next);
    tsdb_snapshot_release(base);
  }
  tsdb_snapshot_release(batch);
  return ok;
}
