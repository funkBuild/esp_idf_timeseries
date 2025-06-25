#ifndef TSDB_CACHE_FIELD_NAMES_H
#define TSDB_CACHE_FIELD_NAMES_H

#include <stdint.h>
#include <stdbool.h>
#include "timeseries_cache_base.h"
#include "timeseries_string_list.h"

/* ------------------------------------------------------------------
 * measurement-id  ⇄  list-of-field-names (unique, NUL-terminated)
 * ------------------------------------------------------------------*/

/*  LOOK-UP
 *  --------
 *  On hit:  field names are appended (unique) to *out_list and
 *           the function returns true.
 *  On miss: returns false, leaving *out_list unchanged.
 */
bool tsdb_fieldnames_cache_lookup(ts_cache_t* cache, uint32_t meas_id, timeseries_string_list_t* out_list);

/*  INSERT
 *  ------
 *  Packs every string in the list into one blob and stores it.
 *  Returns false on OOM or cache disabled.
 */
bool tsdb_fieldnames_cache_insert(ts_cache_t* cache, uint32_t meas_id, const timeseries_string_list_t* list);

#endif /* TSDB_CACHE_FIELD_NAMES_H */
