#include "timeseries_cache_measurement_id.h"

bool tsdb_measurement_cache_lookup(ts_cache_t* cache, const char* measurement_name, uint32_t* out_id) {
  return ts_cache_lookup_u32(cache, K_MEASUREMENT_ID, measurement_name, 0, /* no meas_id part */
                             out_id);
}

bool tsdb_measurement_cache_insert(ts_cache_t* cache, const char* measurement_name, uint32_t id) {
  return ts_cache_insert_u32(cache, K_MEASUREMENT_ID, measurement_name, 0, id);
}

bool tsdb_measurement_cache_delete(ts_cache_t* cache, const char* measurement_name) {
  return ts_cache_remove_u32(cache, K_MEASUREMENT_ID, measurement_name, 0);
}