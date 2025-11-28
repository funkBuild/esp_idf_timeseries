#ifndef TIMESERIES_CACHE_H
#define TIMESERIES_CACHE_H

#include "timeseries_internal.h"
#include <stdbool.h>
#include <stddef.h>

#ifdef CONFIG_TIMESERIES_USE_SERIES_ID_CACHE

/**
 * @brief Initialize the series ID cache
 *
 * @param db Database context
 * @return true on success, false on failure
 */
bool tsdb_cache_init(timeseries_db_t *db);

/**
 * @brief Lookup a series ID in the cache
 *
 * @param db Database context
 * @param key String key (measurement+tags+field)
 * @param series_id Output buffer for series ID (16 bytes)
 * @return true if found in cache, false if not found
 */
bool tsdb_cache_lookup_series_id(timeseries_db_t *db, const char *key,
                                 unsigned char series_id[16]);

/**
 * @brief Insert or update a series ID in the cache
 *
 * @param db Database context
 * @param key String key (measurement+tags+field)
 * @param series_id Series ID to store (16 bytes)
 */
void tsdb_cache_insert_series_id(timeseries_db_t *db, const char *key,
                                 const unsigned char series_id[16]);

/**
 * @brief Clear the series ID cache
 *
 * @param db Database context
 */
void tsdb_cache_clear(timeseries_db_t *db);

/**
 * @brief Free cache resources
 *
 * @param db Database context
 */
void tsdb_cache_free(timeseries_db_t *db);

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
/**
 * @brief Log cache statistics
 *
 * @param db Database context
 */
void tsdb_cache_log_stats(const timeseries_db_t *db);
#endif

#else // CONFIG_TIMESERIES_USE_SERIES_ID_CACHE not defined

// Empty inline stubs when cache is disabled
static inline bool tsdb_cache_init(timeseries_db_t *db) { return true; }
static inline bool tsdb_cache_lookup_series_id(timeseries_db_t *db, const char *key,
                                               unsigned char series_id[16]) { return false; }
static inline void tsdb_cache_insert_series_id(timeseries_db_t *db, const char *key,
                                               const unsigned char series_id[16]) {}
static inline void tsdb_cache_clear(timeseries_db_t *db) {}
static inline void tsdb_cache_free(timeseries_db_t *db) {}

#endif // CONFIG_TIMESERIES_USE_SERIES_ID_CACHE

#endif // TIMESERIES_CACHE_H