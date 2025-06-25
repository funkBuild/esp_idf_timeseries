#ifndef TSDB_CACHE_MEASUREMENT_ID_H
#define TSDB_CACHE_MEASUREMENT_ID_H

#include <stdint.h>
#include <stdbool.h>
#include "timeseries_cache_base.h"

bool tsdb_measurement_cache_lookup(ts_cache_t* cache, const char* measurement_name, uint32_t* out_id);

bool tsdb_measurement_cache_insert(ts_cache_t* cache, const char* measurement_name, uint32_t id);

#endif