#ifndef TSDB_CACHE_TAGCOMBO_SERIES_H
#define TSDB_CACHE_TAGCOMBO_SERIES_H

#include <stdint.h>
#include <stdbool.h>
#include "timeseries_cache_base.h"
#include "timeseries_id_list.h" /* timeseries_series_id_list_t   */

/*  Cache a whole *tag-set* (k=v pairs) for one measurement:
 *      (measurement_id , {k1=v1 … kn=vn})  →  series-id[]
 *
 *  The tag set is passed as two parallel arrays; the function itself
 *  builds a canonical key internally (sorted, “mid|k=v|k=v”).
 */

/* ––––– LOOK-UP –––––
 *  On hit: all ids are appended to *out_list.
 *  Returns true if the tag-set exists in cache, false on miss.
 */
bool tsdb_tagcombo_cache_lookup(ts_cache_t* cache, uint32_t measurement_id, size_t num_tags,
                                const char* const* tag_keys, const char* const* tag_values,
                                timeseries_series_id_list_t* out_list);

/* ––––– INSERT –––––
 *  Stores (or overwrites) the mapping.
 */
bool tsdb_tagcombo_cache_insert(ts_cache_t* cache, uint32_t measurement_id, size_t num_tags,
                                const char* const* tag_keys, const char* const* tag_values,
                                const timeseries_series_id_list_t* list);

#endif /* TSDB_CACHE_TAGCOMBO_SERIES_H */
