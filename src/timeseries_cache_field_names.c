#include "timeseries_cache_field_names.h"
#include <string.h>
#include <stdlib.h>

/* pack: Σ(len+1) bytes, each name followed by '\0' */
static bool pack_list(const timeseries_string_list_t* lst, uint8_t** out_blob, size_t* out_sz) {
  size_t sz = 0;
  for (size_t i = 0; i < lst->count; ++i) sz += strlen(lst->items[i]) + 1;

  uint8_t* buf = malloc(sz ? sz : 1); /* allow empty list -> 1 byte */
  if (!buf) return false;

  uint8_t* p = buf;
  for (size_t i = 0; i < lst->count; ++i) {
    size_t len = strlen(lst->items[i]);
    memcpy(p, lst->items[i], len);
    p[len] = '\0';
    p += len + 1;
  }

  *out_blob = buf;
  *out_sz = sz;
  return true;
}

/* unpack: append every string to out_list (keeps uniqueness) */
static bool unpack_blob(const uint8_t* blob, size_t sz, timeseries_string_list_t* out_list) {
  if (!blob || sz == 0) return false;
  bool any = false;
  const uint8_t *p = blob, *end = blob + sz;
  while (p < end) {
    size_t len = strnlen((const char*)p, end - p);
    if (len == 0 || p + len >= end) break; /* corrupt guard          */
    if (tsdb_string_list_append_unique(out_list, (const char*)p)) any = true;
    p += len + 1;
  }
  return any;
}

/* --------------------------------------------------------------------------
 *  Public wrappers
 * --------------------------------------------------------------------------*/

bool tsdb_fieldnames_cache_lookup(ts_cache_t* cache, uint32_t meas_id, timeseries_string_list_t* out_list) {
  if (!cache || !out_list) return false;

  void* blob = NULL;
  size_t sz = 0;
  if (!ts_cache_lookup_blob(cache, K_FIELD_NAMES, NULL /* no string key */, meas_id, &blob, &sz))
    return false; /* miss */

  bool ok = unpack_blob((uint8_t*)blob, sz, out_list);
  free(blob);
  return ok;
}

bool tsdb_fieldnames_cache_insert(ts_cache_t* cache, uint32_t meas_id, const timeseries_string_list_t* list) {
  if (!cache || !list) return false;

  uint8_t* blob = NULL;
  size_t sz = 0;
  if (!pack_list(list, &blob, &sz)) return false; /* OOM */

  bool ok = ts_cache_insert_blob(cache, K_FIELD_NAMES, NULL, meas_id, blob, sz);
  free(blob);
  return ok;
}
