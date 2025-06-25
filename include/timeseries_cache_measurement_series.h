#ifndef TSDB_CACHE_MEASUREMENT_SERIES_H
#define TSDB_CACHE_MEASUREMENT_SERIES_H

#include <stdint.h>
#include <stdbool.h>
#include "timeseries_cache_base.h"
#include "timeseries_id_list.h" /* timeseries_series_id_list_t   */

/* measurement-id → series-id[]  (16-byte ids, variable length) */

/*  On hit: ids are appended to *out_list; returns true.
 *  On miss: returns false and leaves *out_list unchanged.      */
bool tsdb_measser_cache_lookup(ts_cache_t* cache, uint32_t measurement_id, timeseries_series_id_list_t* out_list);

/*  Inserts (or overwrites) the list in the cache. */
bool tsdb_measser_cache_insert(ts_cache_t* cache, uint32_t measurement_id, const timeseries_series_id_list_t* list);

#endif /* TSDB_CACHE_MEASUREMENT_SERIES_H */
