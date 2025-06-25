#include "timeseries_cache_tagcombo_series.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SID_BYTES 16
#define MAX_KEY 256 /* safety guard for combo key            */

/* ------------------------------------------------------------------ */
/*  helpers: build canonical key, pack/unpack id blobs                */
/* ------------------------------------------------------------------ */
static bool build_combo_key(uint32_t mid, size_t n, const char* const* k, const char* const* v, char* buf,
                            size_t buf_sz) {
  /* indices to sort deterministically */
  size_t idx[n];
  for (size_t i = 0; i < n; ++i) idx[i] = i;

  for (size_t i = 0; i < n - 1; ++i)
    for (size_t j = i + 1; j < n; ++j) {
      int c = strcmp(k[idx[i]], k[idx[j]]);
      if (c > 0 || (c == 0 && strcmp(v[idx[i]], v[idx[j]]) > 0)) {
        size_t t = idx[i];
        idx[i] = idx[j];
        idx[j] = t;
      }
    }

  int w = snprintf(buf, buf_sz, "%u", (unsigned)mid);
  if (w < 0 || (size_t)w >= buf_sz) return false;
  size_t off = (size_t)w;

  for (size_t p = 0; p < n; ++p) {
    w = snprintf(buf + off, buf_sz - off, "|%s=%s", k[idx[p]], v[idx[p]]);
    if (w < 0 || off + (size_t)w >= buf_sz) return false;
    off += (size_t)w;
  }
  return true;
}

static bool pack_ids(const timeseries_series_id_list_t* lst, uint8_t** blob, size_t* sz) {
  *sz = lst->count * SID_BYTES;
  *blob = (*sz) ? (uint8_t*)malloc(*sz) : NULL;
  if (*sz && !*blob) return false;

  for (size_t i = 0; i < lst->count; ++i) memcpy(*blob + i * SID_BYTES, lst->ids[i].bytes, SID_BYTES);
  return true;
}

static void unpack_ids(const uint8_t* blob, size_t sz, timeseries_series_id_list_t* out) {
  for (size_t o = 0; o + SID_BYTES <= sz; o += SID_BYTES) tsdb_series_id_list_append(out, blob + o);
}

/* ------------------------------------------------------------------ */
/*  public wrappers                                                   */
/* ------------------------------------------------------------------ */
bool tsdb_tagcombo_cache_lookup(ts_cache_t* cache, uint32_t mid, size_t n_tags, const char* const* keys,
                                const char* const* vals, timeseries_series_id_list_t* out) {
  if (!cache || !keys || !vals || !out || n_tags == 0) return false;

  char key_buf[MAX_KEY];
  if (!build_combo_key(mid, n_tags, keys, vals, key_buf, sizeof key_buf)) return false; /* key overflow */

  void* blob = NULL;
  size_t sz = 0;
  bool hit = ts_cache_lookup_blob(cache, K_TAG_COMBO, key_buf, mid, &blob, &sz);
  if (hit && blob) {
    unpack_ids((uint8_t*)blob, sz, out);
    free(blob);
  }
  return hit;
}

bool tsdb_tagcombo_cache_insert(ts_cache_t* cache, uint32_t mid, size_t n_tags, const char* const* keys,
                                const char* const* vals, const timeseries_series_id_list_t* lst) {
  if (!cache || !keys || !vals || !lst || n_tags == 0) return false;

  char key_buf[MAX_KEY];
  if (!build_combo_key(mid, n_tags, keys, vals, key_buf, sizeof key_buf)) return false;

  uint8_t* blob;
  size_t sz;
  if (!pack_ids(lst, &blob, &sz)) return false;

  bool ok = ts_cache_insert_blob(cache, K_TAG_COMBO, key_buf, mid, blob, sz);
  free(blob);
  return ok;
}
