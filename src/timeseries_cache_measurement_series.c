#include "timeseries_cache_measurement_series.h"
#include <stdlib.h>
#include <string.h>

#define SID_BYTES 16

/* ---------- helpers ------------------------------------------------- */
static bool pack(const timeseries_series_id_list_t* lst, uint8_t** blob, size_t* sz) {
  *sz = lst->count * SID_BYTES;
  *blob = (*sz) ? (uint8_t*)malloc(*sz) : NULL;
  if (*sz && !*blob) return false;

  for (size_t i = 0; i < lst->count; ++i) memcpy(*blob + i * SID_BYTES, lst->ids[i].bytes, SID_BYTES);
  return true;
}
static void unpack(const uint8_t* blob, size_t sz, timeseries_series_id_list_t* out) {
  for (size_t off = 0; off + SID_BYTES <= sz; off += SID_BYTES) tsdb_series_id_list_append(out, blob + off);
}

/* ---------- API wrappers ------------------------------------------- */
bool tsdb_measser_cache_lookup(ts_cache_t* cache, uint32_t meas_id, timeseries_series_id_list_t* out) {
  if (!cache || !out) return false;

  void* blob = NULL;
  size_t sz = 0;
  if (!ts_cache_lookup_blob(cache, K_MEASUREMENT_SER, NULL, meas_id, &blob, &sz)) return false; /* miss */

  unpack((uint8_t*)blob, sz, out);
  free(blob);
  return true; /* hit  */
}

bool tsdb_measser_cache_insert(ts_cache_t* cache, uint32_t meas_id, const timeseries_series_id_list_t* lst) {
  if (!cache || !lst) return false;

  uint8_t* blob;
  size_t sz;
  if (!pack(lst, &blob, &sz)) return false;

  bool ok = ts_cache_insert_blob(cache, K_MEASUREMENT_SER, NULL, meas_id, blob, sz);
  free(blob);
  return ok;
}
