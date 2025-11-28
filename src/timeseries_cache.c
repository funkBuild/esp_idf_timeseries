#include "timeseries_cache.h"
#include "esp_log.h"
#include <string.h>
#include <stdlib.h>

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

  // Initialize all entries as invalid
  for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
    db->series_cache[i].valid = false;
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

    if (entry->valid && strcmp(entry->key, key) == 0) {
      // Cache hit!
      memcpy(series_id, entry->series_id, 16);

#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
      entry->last_access = ++db->cache_access_counter;
#endif

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
      db->cache_stats.hits++;
#endif

      ESP_LOGV(TAG, "Cache HIT for key: %.32s... (index=%u)", key, index);
      return true;
    }

    // If we hit an empty slot, key is not in cache
    if (!entry->valid) {
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

  // Calculate hash index
  uint32_t hash = hash_string(key);
  uint32_t start_index = hash % SERIES_ID_CACHE_SIZE;

  // First, check if key already exists (update case)
  for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
    uint32_t index = (start_index + i) % SERIES_ID_CACHE_SIZE;
    series_id_cache_entry_t *entry = &db->series_cache[index];

    if (entry->valid && strcmp(entry->key, key) == 0) {
      // Update existing entry
      memcpy(entry->series_id, series_id, 16);
#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
      entry->last_access = ++db->cache_access_counter;
#endif
      ESP_LOGV(TAG, "Cache UPDATE for key: %.32s... (index=%u)", key, index);
      return;
    }
  }

  // Not found, need to insert new entry
  // First try to find an empty slot
  for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
    uint32_t index = (start_index + i) % SERIES_ID_CACHE_SIZE;
    series_id_cache_entry_t *entry = &db->series_cache[index];

    if (!entry->valid) {
      // Found empty slot
      strncpy(entry->key, key, sizeof(entry->key) - 1);
      entry->key[sizeof(entry->key) - 1] = '\0';
      memcpy(entry->series_id, series_id, 16);
      entry->valid = true;
#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
      entry->last_access = ++db->cache_access_counter;
#endif

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
      db->cache_stats.insertions++;
#endif

      ESP_LOGV(TAG, "Cache INSERT for key: %.32s... (index=%u)", key, index);
      return;
    }
  }

  // No empty slots, need to evict
#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
  // Find least recently used entry in the probe sequence
  uint32_t lru_index = start_index;
  uint32_t lru_access = db->series_cache[start_index].last_access;

  for (size_t i = 1; i < SERIES_ID_CACHE_SIZE && i < 8; i++) { // Check up to 8 slots
    uint32_t index = (start_index + i) % SERIES_ID_CACHE_SIZE;
    if (db->series_cache[index].last_access < lru_access) {
      lru_index = index;
      lru_access = db->series_cache[index].last_access;
    }
  }
#else
  // Simple replacement at hash position
  uint32_t lru_index = start_index;
#endif

  // Evict and replace
  series_id_cache_entry_t *entry = &db->series_cache[lru_index];
  ESP_LOGV(TAG, "Cache EVICT key: %.32s... from index=%u", entry->key, lru_index);

  strncpy(entry->key, key, sizeof(entry->key) - 1);
  entry->key[sizeof(entry->key) - 1] = '\0';
  memcpy(entry->series_id, series_id, 16);
  entry->valid = true;
#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
  entry->last_access = ++db->cache_access_counter;
#endif

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
  db->cache_stats.evictions++;
  db->cache_stats.insertions++;
#endif

  ESP_LOGV(TAG, "Cache INSERT after eviction for key: %.32s... (index=%u)", key, lru_index);
}

void tsdb_cache_clear(timeseries_db_t *db) {
  if (!db || !db->series_cache) {
    return;
  }

  for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
    db->series_cache[i].valid = false;
    db->series_cache[i].last_access = 0;
  }

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
  ESP_LOGI(TAG, "  Hits:       %u", db->cache_stats.hits);
  ESP_LOGI(TAG, "  Misses:     %u", db->cache_stats.misses);
  ESP_LOGI(TAG, "  Hit Rate:   %.1f%%", hit_rate);
  ESP_LOGI(TAG, "  Insertions: %u", db->cache_stats.insertions);
  ESP_LOGI(TAG, "  Evictions:  %u", db->cache_stats.evictions);

  // Count valid entries
  uint32_t valid_entries = 0;
  if (db->series_cache) {
    for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
      if (db->series_cache[i].valid) {
        valid_entries++;
      }
    }
  }
  ESP_LOGI(TAG, "  Occupancy:  %u/%d (%.1f%%)", valid_entries, SERIES_ID_CACHE_SIZE,
           100.0f * valid_entries / SERIES_ID_CACHE_SIZE);
}
#endif

#endif // CONFIG_TIMESERIES_USE_SERIES_ID_CACHE