#ifndef TSDB_CACHE_BASE_H
#define TSDB_CACHE_BASE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "lru.h"

/* -------- key kinds we support / will support -------------------- */
typedef enum {
  K_MEASUREMENT_ID = 0,
  K_FIELD_NAMES = 1,
  K_FIELD_LIST = 2,
  K_MEASUREMENT_SER = 3,
  K_TAG_COMBO = 4,
} key_kind_e;

/* -------- opaque cache handle ------------------------------------ */
typedef struct ts_cache ts_cache_t;

/* -------- create / destroy / clear ------------------------------- */
ts_cache_t* ts_cache_create(size_t max_entries, size_t max_memory_bytes, uint32_t ttl_ms); /* 0 = no TTL */

void ts_cache_destroy(ts_cache_t* c);
void ts_cache_clear(ts_cache_t* c);

/* -------- generic look-up helpers --------------------------------
   (the public wrappers build a key + forward here)                 */
bool ts_cache_lookup_u32(ts_cache_t* c, key_kind_e kind, const char* str_key, /* NULL if not used   */
                         uint32_t meas_id,                                    /* 0 if not used      */
                         uint32_t* out_val);

bool ts_cache_insert_u32(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id, uint32_t val);

bool ts_cache_lookup_blob(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id,
                          void** out_data, /* malloc’ed copy     */
                          size_t* out_size);

bool ts_cache_insert_blob(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id, const void* data,
                          size_t size);

/* -------- stats -------------------------------------------------- */
typedef struct {
  uint32_t hits, misses, insertions, updates, evictions;
  size_t entries, bytes_in_use;
} ts_cache_stats_t;

void ts_cache_get_stats(const ts_cache_t* c, ts_cache_stats_t* out);

bool ts_cache_remove(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id);

bool ts_cache_remove_u32(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id);

#endif /* TSDB_CACHE_BASE_H */
