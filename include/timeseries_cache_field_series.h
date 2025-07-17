#ifndef TSDB_CACHE_FIELD_SERIES_H
#define TSDB_CACHE_FIELD_SERIES_H

#include <stdint.h>
#include <stdbool.h>
#include "timeseries_cache_base.h"
#include "timeseries_id_list.h" /* for timeseries_series_id_list_t   */

/*  Look-up:
 *  --------
 *  On hit, all ids are appended to *out_list (uniqueness handled by caller).
 *  Returns true if the key existed in cache, false on miss.
 */
bool tsdb_fieldseries_cache_lookup(ts_cache_t* cache, uint32_t measurement_id, const char* field_name,
                                   timeseries_series_id_list_t* out_list);

/*  Insert:
 *  -------
 *  Packs the series-id list (count×16 bytes) into one blob and stores it.
 */
bool tsdb_fieldseries_cache_insert(ts_cache_t* cache, uint32_t measurement_id, const char* field_name,
                                   const timeseries_series_id_list_t* list);

bool tsdb_fieldseries_cache_delete(ts_cache_t* cache, uint32_t measurement_id, const char* field_name);

#endif /* TSDB_CACHE_FIELD_SERIES_H */
