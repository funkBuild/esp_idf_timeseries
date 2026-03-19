#include "timeseries_cache.h"
#include "esp_log.h"
#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#ifdef CONFIG_TIMESERIES_USE_SERIES_ID_CACHE

static const char *TAG = "TimeseriesCache";

/**
 * @brief Simple hash function for cache indexing
 */
static uint32_t hash_string(const char *str) {
  uint32_t hash = 5381;
  int c;
  while ((c = *str++)) {
    hash = ((hash << 5) + hash) + c; // hash * 33 + c
  }
  return hash;
}

bool tsdb_cache_init(timeseries_db_t *db) {
  if (!db) {
    return false;
  }

  // Allocate cache array
  size_t cache_size = SERIES_ID_CACHE_SIZE * sizeof(series_id_cache_entry_t);
  db->series_cache = (series_id_cache_entry_t *)calloc(1, cache_size);
  if (!db->series_cache) {
    ESP_LOGE(TAG, "Failed to allocate %zu bytes for series cache", cache_size);
    return false;
  }

  // Initialize all entries as empty
  for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
    db->series_cache[i].state = TSDB_CACHE_EMPTY;
    db->series_cache[i].last_access = 0;
  }

  db->cache_access_counter = 0;

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
  memset(&db->cache_stats, 0, sizeof(db->cache_stats));
#endif

  ESP_LOGI(TAG, "Initialized series ID cache with %d entries", SERIES_ID_CACHE_SIZE);
  return true;
}

bool tsdb_cache_lookup_series_id(timeseries_db_t *db, const char *key,
                                 unsigned char series_id[16]) {
  if (!db || !db->series_cache || !key || !series_id) {
    return false;
  }

  // Calculate hash index
  uint32_t hash = hash_string(key);
  uint32_t start_index = hash % SERIES_ID_CACHE_SIZE;

  // Linear probe with wraparound (handle collisions)
  for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
    uint32_t index = (start_index + i) % SERIES_ID_CACHE_SIZE;
    series_id_cache_entry_t *entry = &db->series_cache[index];

    if (entry->state == TSDB_CACHE_VALID && strcmp(entry->key, key) == 0) {
      // Cache hit!
      memcpy(series_id, entry->series_id, 16);

#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
      entry->last_access = ++db->cache_access_counter;
#endif

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
      db->cache_stats.hits++;
#endif

      ESP_LOGV(TAG, "Cache HIT for key: %.32s... (index=%" PRIu32 ")", key, index);
      return true;
    }

    // If we hit an empty slot, key is not in cache
    if (entry->state == TSDB_CACHE_EMPTY) {
      break;
    }
  }

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
  db->cache_stats.misses++;
#endif

  ESP_LOGV(TAG, "Cache MISS for key: %.32s...", key);
  return false;
}

void tsdb_cache_insert_series_id(timeseries_db_t *db, const char *key,
                                 const unsigned char series_id[16]) {
  if (!db || !db->series_cache || !key || !series_id) {
    return;
  }

  // Reject keys that would be truncated — silent truncation causes incorrect
  // cache hits when two long keys share the same prefix up to the limit.
  if (strlen(key) >= sizeof(((series_id_cache_entry_t *)0)->key)) {
    ESP_LOGW(TAG, "Cache key too long (%zu >= %zu), refusing to cache",
             strlen(key), sizeof(((series_id_cache_entry_t *)0)->key));
    return;
  }

  // Calculate hash index
  uint32_t hash = hash_string(key);
  uint32_t start_index = hash % SERIES_ID_CACHE_SIZE;

  // First, check if key already exists (update case)
  for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
    uint32_t index = (start_index + i) % SERIES_ID_CACHE_SIZE;
    series_id_cache_entry_t *entry = &db->series_cache[index];

    if (entry->state == TSDB_CACHE_VALID && strcmp(entry->key, key) == 0) {
      // Update existing entry
      memcpy(entry->series_id, series_id, 16);
#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
      entry->last_access = ++db->cache_access_counter;
#endif
      ESP_LOGV(TAG, "Cache UPDATE for key: %.32s... (index=%" PRIu32 ")", key, index);
      return;
    }

    // If we hit an empty slot, key doesn't exist in cache - stop searching
    if (entry->state == TSDB_CACHE_EMPTY) {
      break;
    }
  }

  // Not found, need to insert new entry
  // Try to find an empty slot
  for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
    uint32_t index = (start_index + i) % SERIES_ID_CACHE_SIZE;
    series_id_cache_entry_t *entry = &db->series_cache[index];

    if (entry->state == TSDB_CACHE_EMPTY) {
      // Found empty slot -- insert here
      strncpy(entry->key, key, sizeof(entry->key) - 1);
      entry->key[sizeof(entry->key) - 1] = '\0';
      memcpy(entry->series_id, series_id, 16);
      entry->state = TSDB_CACHE_VALID;
#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
      entry->last_access = ++db->cache_access_counter;
#endif

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
      db->cache_stats.insertions++;
#endif

      ESP_LOGV(TAG, "Cache INSERT for key: %.32s... (index=%" PRIu32 ")", key, index);
      return;
    }
  }

  // No empty slots, need to evict
#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
  // Find least recently used entry among VALID entries only.
  // Use unsigned distance (counter - last_access) so that wraparound of
  // cache_access_counter is handled correctly without renormalization.
  uint32_t lru_index = SERIES_ID_CACHE_SIZE;  // Sentinel value
  uint32_t lru_age = 0;                       // Largest age = least recently used

  for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
    if (db->series_cache[i].state == TSDB_CACHE_VALID) {
      uint32_t age = db->cache_access_counter - db->series_cache[i].last_access;
      if (age >= lru_age) {
        lru_index = i;
        lru_age = age;
      }
    }
  }

  // Safety check: if no valid entries found, use start_index as fallback
  if (lru_index == SERIES_ID_CACHE_SIZE) {
    ESP_LOGW(TAG, "No valid entries for LRU eviction, using start index %" PRIu32, start_index);
    lru_index = start_index;
  }
#else
  // Simple replacement at hash position
  uint32_t lru_index = start_index;
#endif

  // Sanity check to prevent heap corruption
  if (lru_index >= SERIES_ID_CACHE_SIZE) {
    ESP_LOGE(TAG, "ERROR: Invalid lru_index %lu (size %d), aborting insert",
             (unsigned long)lru_index, SERIES_ID_CACHE_SIZE);
    return;
  }

  // Evict the victim using backshift deletion to preserve probe chain invariants.
  // This avoids tombstones entirely: after deletion the slot is EMPTY and all
  // entries that were displaced past the victim are shifted back toward their
  // natural hash positions.
  ESP_LOGV(TAG, "Cache EVICT key: %.32s... from index=%" PRIu32,
           db->series_cache[lru_index].key, lru_index);
  db->series_cache[lru_index].state = TSDB_CACHE_EMPTY;

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
  db->cache_stats.evictions++;
#endif

  // Backshift: close the gap left by the evicted entry.  For each subsequent
  // occupied slot, if its natural hash position is at or before the gap
  // (accounting for wraparound), move it into the gap and advance the gap.
  {
    uint32_t gap = lru_index;
    for (size_t i = 1; i < SERIES_ID_CACHE_SIZE; i++) {
      uint32_t j = (lru_index + i) % SERIES_ID_CACHE_SIZE;
      series_id_cache_entry_t *entry = &db->series_cache[j];

      if (entry->state != TSDB_CACHE_VALID) {
        break;  // Hit an empty slot; probe chain is closed
      }

      // Compute this entry's natural (home) slot
      uint32_t home = hash_string(entry->key) % SERIES_ID_CACHE_SIZE;

      // Determine whether 'home' is "at or before" 'gap' in the circular
      // probe order starting from 'home' through 'j'.  Equivalently: would
      // a probe starting at 'home' pass through 'gap' on its way to 'j'?
      // Using the circular distance: dist(a,b) = (b - a) mod N
      uint32_t dist_home_to_gap = (gap - home + SERIES_ID_CACHE_SIZE) % SERIES_ID_CACHE_SIZE;
      uint32_t dist_home_to_j   = (j   - home + SERIES_ID_CACHE_SIZE) % SERIES_ID_CACHE_SIZE;

      if (dist_home_to_gap <= dist_home_to_j) {
        // This entry's home is between 'home' and 'j' inclusive, passing
        // through 'gap', so shift it back into the gap.
        db->series_cache[gap] = *entry;
        entry->state = TSDB_CACHE_EMPTY;
        gap = j;
      }
    }
  }

  // Now do a standard probe-based insert from the new key's natural hash
  // position.  There is at least one EMPTY slot after backshift deletion.
  for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
    uint32_t index = (start_index + i) % SERIES_ID_CACHE_SIZE;
    series_id_cache_entry_t *entry = &db->series_cache[index];

    if (entry->state == TSDB_CACHE_EMPTY) {
      strncpy(entry->key, key, sizeof(entry->key) - 1);
      entry->key[sizeof(entry->key) - 1] = '\0';
      memcpy(entry->series_id, series_id, 16);
      entry->state = TSDB_CACHE_VALID;
#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
      entry->last_access = ++db->cache_access_counter;
#endif

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
      db->cache_stats.insertions++;
#endif

      ESP_LOGV(TAG, "Cache INSERT after eviction for key: %.32s... (index=%" PRIu32 ")", key, index);
      return;
    }
  }

  // Should never reach here since backshift deletion freed a slot, but be safe
  ESP_LOGE(TAG, "ERROR: Failed to find slot after eviction for key: %.32s...", key);
}

void tsdb_cache_clear(timeseries_db_t *db) {
  if (!db || !db->series_cache) {
    return;
  }

  // Zero ALL entry data to prevent stale data from being used
  // Use single memset for efficiency
  memset(db->series_cache, 0, SERIES_ID_CACHE_SIZE * sizeof(series_id_cache_entry_t));

  db->cache_access_counter = 0;

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
  memset(&db->cache_stats, 0, sizeof(db->cache_stats));
#endif

  ESP_LOGI(TAG, "Cleared series ID cache");
}

void tsdb_cache_free(timeseries_db_t *db) {
  if (!db) {
    return;
  }

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
  // Log final cache stats before freeing
  tsdb_cache_log_stats(db);
#endif

  if (db->series_cache) {
    free(db->series_cache);
    db->series_cache = NULL;
  }

  ESP_LOGI(TAG, "Freed series ID cache resources");
}

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
void tsdb_cache_log_stats(const timeseries_db_t *db) {
  if (!db) {
    return;
  }

  uint32_t total_accesses = db->cache_stats.hits + db->cache_stats.misses;
  float hit_rate = (total_accesses > 0) ?
    (100.0f * db->cache_stats.hits / total_accesses) : 0.0f;

  ESP_LOGI(TAG, "Cache Statistics:");
  ESP_LOGI(TAG, "  Hits:       %" PRIu32, db->cache_stats.hits);
  ESP_LOGI(TAG, "  Misses:     %" PRIu32, db->cache_stats.misses);
  ESP_LOGI(TAG, "  Hit Rate:   %.1f%%", hit_rate);
  ESP_LOGI(TAG, "  Insertions: %" PRIu32, db->cache_stats.insertions);
  ESP_LOGI(TAG, "  Evictions:  %" PRIu32, db->cache_stats.evictions);

  // Count valid entries
  uint32_t valid_entries = 0;
  if (db->series_cache) {
    for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
      if (db->series_cache[i].state == TSDB_CACHE_VALID) {
        valid_entries++;
      }
    }
  }
  ESP_LOGI(TAG, "  Occupancy:  %" PRIu32 "/%d (%.1f%%)", valid_entries, SERIES_ID_CACHE_SIZE,
           100.0f * valid_entries / SERIES_ID_CACHE_SIZE);
}
#endif

#endif // CONFIG_TIMESERIES_USE_SERIES_ID_CACHE