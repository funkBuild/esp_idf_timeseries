#include "timeseries_cache_field_series.h"
#include <string.h>
#include <stdlib.h>

#define SERIES_ID_BYTES 16

/* ------------ helpers ------------------------------------------------- */
static bool pack_ids(const timeseries_series_id_list_t* lst, uint8_t** blob, size_t* sz) {
  *sz = lst->count * SERIES_ID_BYTES;
  *blob = (*sz) ? (uint8_t*)malloc(*sz) : NULL;
  if (*sz && !*blob) return false;

  for (size_t i = 0; i < lst->count; ++i) memcpy(*blob + i * SERIES_ID_BYTES, lst->ids[i].bytes, SERIES_ID_BYTES);
  return true;
}

static void unpack_ids(const uint8_t* blob, size_t sz, timeseries_series_id_list_t* out) {
  for (size_t off = 0; off + SERIES_ID_BYTES <= sz; off += SERIES_ID_BYTES) tsdb_series_id_list_append(out, blob + off);
}

/* ------------ public wrappers ----------------------------------------- */
bool tsdb_fieldseries_cache_lookup(ts_cache_t* cache, uint32_t measurement_id, const char* field_name,
                                   timeseries_series_id_list_t* out_list) {
  if (!cache || !field_name || !out_list) return false;

  void* blob = NULL;
  size_t sz = 0;
  bool hit = ts_cache_lookup_blob(cache, K_FIELD_LIST, field_name, measurement_id, &blob, &sz);
  if (hit && blob) {
    unpack_ids((const uint8_t*)blob, sz, out_list);
    free(blob);
  }
  return hit;
}

bool tsdb_fieldseries_cache_insert(ts_cache_t* cache, uint32_t measurement_id, const char* field_name,
                                   const timeseries_series_id_list_t* list) {
  if (!cache || !field_name || !list) return false;

  uint8_t* blob;
  size_t sz;
  if (!pack_ids(list, &blob, &sz)) return false;

  bool ok = ts_cache_insert_blob(cache, K_FIELD_LIST, field_name, measurement_id, blob, sz);
  free(blob);
  return ok;
}
