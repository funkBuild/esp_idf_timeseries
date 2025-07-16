#include "timeseries_page_cache.h"
#include "timeseries_iterator.h"

#include "esp_log.h"
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
 *
 * This function scans the entire partition and builds a cache of all recognized
 * pages. This is useful for fast access to page headers and offsets.
 *
 * @param db The database context.
 * @return true if success, false if error.
 */
bool tsdb_build_page_cache(timeseries_db_t* db) {
  if (!db || !db->partition) {
    return false;
  }

  // Start fresh
  if (db->page_cache) {
    free(db->page_cache);
    db->page_cache = NULL;
  }
  db->page_cache_count = 0;
  db->page_cache_capacity = 0;
  db->total_active_size = 0;

  // Use the page iterator to find *all* pages (ACTIVE, OBSOLETE, etc.)
  timeseries_page_iterator_t flash_iter;
  if (!timeseries_page_iterator_init(db, &flash_iter)) {
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t offset = 0, size = 0;
  while (timeseries_page_iterator_next(&flash_iter, &hdr, &offset, &size)) {
    // For demonstration, assume timeseries_page_iterator_next is returning
    // all recognized pages. Then add them to the cache.
    tsdb_pagecache_add_entry(db, offset, &hdr);
  }

  // Now we have an unsorted array in db->page_cache.
  // Sort the page cache by ascending offset.
  qsort(db->page_cache, db->page_cache_count, sizeof(timeseries_cached_page_t), page_offset_compare);

  ESP_LOGV(TAG, "Built page cache with %u entries.", (unsigned)db->page_cache_count);
  return true;
}

/**
 * @brief Helper to add a new entry to db->page_cache dynamic array.
 *        Then we sort the cache so offsets remain ascending.
 */
void tsdb_pagecache_add_entry(timeseries_db_t* db, uint32_t offset, const timeseries_page_header_t* hdr) {
  if (!db || !hdr) {
    return;
  }

  // grow array if needed
  if (db->page_cache_count == db->page_cache_capacity) {
    size_t newcap = (db->page_cache_capacity == 0) ? 8 : db->page_cache_capacity * 2;
    timeseries_cached_page_t* newarr = realloc(db->page_cache, newcap * sizeof(*newarr));
    if (!newarr) {
      ESP_LOGE(TAG, "OOM expanding page cache");
      return;  // Or handle error
    }
    db->page_cache = newarr;
    db->page_cache_capacity = newcap;
  }

  // add the entry
  timeseries_cached_page_t* entry = &db->page_cache[db->page_cache_count++];
  entry->offset = offset;
  memcpy(&entry->header, hdr, sizeof(timeseries_page_header_t));

  /* Maintain running total */
  if (hdr->page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
    db->total_active_size += hdr->page_size;
  }

  // Re-sort the entire array to keep it in ascending offset order.
  // (If you'd rather do an insertion-based approach, that's another option.)
  qsort(db->page_cache, db->page_cache_count, sizeof(timeseries_cached_page_t), page_offset_compare);
}

/**
 * @brief Remove a page cache entry from db->page_cache by offset.
 *
 * @param db     Database context
 * @param offset Page offset to remove
 * @return true if an entry was found/removed, false otherwise
 */
bool tsdb_pagecache_remove_entry(timeseries_db_t* db, uint32_t offset) {
  if (!db || !db->page_cache) {
    return false;
  }

  for (size_t i = 0; i < db->page_cache_count; i++) {
    if (db->page_cache[i].offset == offset) {
      if (db->page_cache[i].header.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
        db->total_active_size -= db->page_cache[i].header.page_size;
      }

      // Shift down everything after i
      size_t remaining = db->page_cache_count - (i + 1);
      if (remaining > 0) {
        memmove(&db->page_cache[i], &db->page_cache[i + 1], remaining * sizeof(db->page_cache[i]));
      }
      db->page_cache_count--;

      ESP_LOGV(TAG, "Removed page cache entry for offset=0x%08" PRIx32, offset);
      return true;
    }
  }

  // Not found
  return false;
}

uint32_t tsdb_pagecache_get_total_active_size(const timeseries_db_t* db) { return (db) ? db->total_active_size : 0; }

uint32_t tsdb_pagecache_get_page_size(const timeseries_db_t* db, uint32_t page_offset) {
  if (!db || !db->page_cache) {
    return 0;
  }

  for (size_t i = 0; i < db->page_cache_count; i++) {
    const timeseries_cached_page_t* entry = &db->page_cache[i];
    if (entry->offset == page_offset) {
      return entry->header.page_size;
    }
  }

  return 0;  // not found
}

void tsdb_pagecache_clear(timeseries_db_t* db) {
  if (!db || !db->page_cache) {
    return;
  }

  free(db->page_cache);
  db->page_cache = NULL;
  db->page_cache_count = 0;
  db->page_cache_capacity = 0;
  db->total_active_size = 0;
}