/*--------------------------------------------------------------------
 *  Generic intrusive-LRU hash cache   (no external deps)
 *------------------------------------------------------------------*/
#include "timeseries_cache_base.h"
#include <string.h>
#include <stdlib.h>
#include "esp_timer.h" /* or stub on non-ESP builds */

#define DEFAULT_MAX_ENTRIES 256
#define DEFAULT_MAX_MEMORY (128 * 1024) /* 128 KB */

typedef struct entry {
  /* --- intrusive LRU & bucket chain --- */
  lru_node_t lru;
  struct entry* next;

  /* --- bookkeeping --- */
  uint32_t hash;
  key_kind_e kind;
  uint32_t last_access_ms;

  /* --- key --- */
  char* str_key;    /* dup’ed or NULL   */
  uint32_t meas_id; /* 0 if unused      */

  /* --- value --- */
  uint32_t u32;   /* for fixed width values         */
  size_t blob_sz; /* 0 for u32 values               */
  uint8_t* blob;  /* malloc’ed blob for var width   */
} entry_t;

struct ts_cache {
  entry_t** buckets;
  size_t bucket_cnt; /* power of two */

  size_t entry_cap;
  size_t mem_cap;

  uint32_t ttl_ms;

  size_t entry_cnt;
  size_t mem_used;

  lru_list_t lru;

  ts_cache_stats_t st;
};

/* ---------- helpers ---------------------------------------------------- */

static uint32_t djb2(const char* s) {
  uint32_t h = 5381;
  int c;
  while ((c = *s++)) h = ((h << 5) + h) + (uint32_t)c;
  return h;
}
static inline size_t bucket_of(uint32_t h, size_t n) { return h & (n - 1); }

static size_t entry_mem(const entry_t* e) { return sizeof *e + (e->str_key ? strlen(e->str_key) + 1 : 0) + e->blob_sz; }

/* ---------- eviction --------------------------------------------------- */
static void unlink_from_bucket(ts_cache_t* c, entry_t* vict) {
  size_t b = bucket_of(vict->hash, c->bucket_cnt);
  entry_t** pp = &c->buckets[b];
  while (*pp && *pp != vict) pp = &(*pp)->next;
  if (*pp) *pp = vict->next;
}
static void evict_one(ts_cache_t* c) {
  entry_t* vict = (entry_t*)lru_pop_tail(&c->lru);
  if (!vict) return;

  unlink_from_bucket(c, vict);

  c->mem_used -= entry_mem(vict);
  c->entry_cnt -= 1;
  c->st.evictions++;

  free(vict->str_key);
  free(vict->blob);
  free(vict);
}
static void ensure_capacity(ts_cache_t* c, size_t extra) {
  while ((c->entry_cnt >= c->entry_cap) || (c->mem_used + extra > c->mem_cap)) evict_one(c);
}

/* ---------- core create / destroy ------------------------------------- */
ts_cache_t* ts_cache_create(size_t max_entries, size_t max_mem, uint32_t ttl_ms) {
  ts_cache_t* c = calloc(1, sizeof *c);
  if (!c) return NULL;

  if (!max_entries) max_entries = DEFAULT_MAX_ENTRIES;
  if (!max_mem) max_mem = DEFAULT_MAX_MEMORY;

  size_t buckets = 1;
  while (buckets < max_entries * 2) buckets <<= 1;

  c->buckets = calloc(buckets, sizeof(entry_t*));
  if (!c->buckets) {
    free(c);
    return NULL;
  }

  c->bucket_cnt = buckets;
  c->entry_cap = max_entries;
  c->mem_cap = max_mem;
  c->ttl_ms = ttl_ms;

  lru_list_init(&c->lru);
  return c;
}
void ts_cache_destroy(ts_cache_t* c) {
  if (!c) return;
  ts_cache_clear(c);
  free(c->buckets);
  free(c);
}
void ts_cache_clear(ts_cache_t* c) {
  if (!c) return;
  for (size_t b = 0; b < c->bucket_cnt; ++b) {
    entry_t* e = c->buckets[b];
    while (e) {
      entry_t* n = e->next;
      free(e->str_key);
      free(e->blob);
      free(e);
      e = n;
    }
    c->buckets[b] = NULL;
  }
  lru_list_init(&c->lru);
  memset(&c->st, 0, sizeof c->st);
  c->entry_cnt = c->mem_used = 0;
}

/* ---------- fast path: common look-up routine ------------------------- */
static entry_t* find_entry(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id, uint32_t hash,
                           entry_t** prev_out) {
  size_t b = bucket_of(hash, c->bucket_cnt);
  entry_t *e = c->buckets[b], *prev = NULL;
  while (e) {
    if (e->hash == hash && e->kind == kind && e->meas_id == meas_id &&
        ((str_key == NULL && e->str_key == NULL) || (str_key && e->str_key && strcmp(e->str_key, str_key) == 0))) {
      if (prev_out) *prev_out = prev;
      return e;
    }
    prev = e;
    e = e->next;
  }
  return NULL;
}

/* ---------- expiry helper --------------------------------------------- */
static bool expired(const ts_cache_t* c, const entry_t* e) {
  if (!c->ttl_ms) return false;
  uint32_t now = (uint32_t)(esp_timer_get_time() / 1000ULL);
  return now - e->last_access_ms > c->ttl_ms;
}

/* ---------- u32 API ---------------------------------------------------- */
bool ts_cache_lookup_u32(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id, uint32_t* out_val) {
  if (!c || !out_val) return false;

  uint32_t h = str_key ? djb2(str_key) : meas_id; /* cheap hash */
  entry_t* prev = NULL;
  entry_t* e = find_entry(c, kind, str_key, meas_id, h, &prev);

  if (!e) {
    c->st.misses++;
    return false;
  }

  if (expired(c, e)) {
    /* unlink & evict */
    if (prev)
      prev->next = e->next;
    else
      c->buckets[bucket_of(h, c->bucket_cnt)] = e->next;
    lru_unlink(&c->lru, &e->lru);
    c->mem_used -= entry_mem(e);
    c->entry_cnt--;
    c->st.evictions++;
    free(e->str_key);
    free(e);
    c->st.misses++;
    return false;
  }

  *out_val = e->u32;
  e->last_access_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);
  lru_move_front(&c->lru, &e->lru);
  c->st.hits++;
  return true;
}

bool ts_cache_insert_u32(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id, uint32_t val) {
  if (!c) return false;
  uint32_t h = str_key ? djb2(str_key) : meas_id;

  entry_t* e = find_entry(c, kind, str_key, meas_id, h, NULL);
  if (e) {
    /* update in place */
    e->u32 = val;
    e->last_access_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);
    lru_move_front(&c->lru, &e->lru);
    c->st.updates++;
    return true;
  }

  size_t need = sizeof *e + (str_key ? strlen(str_key) + 1 : 0);
  ensure_capacity(c, need);

  e = calloc(1, sizeof *e);
  if (!e) return false;
  if (str_key) {
    e->str_key = strdup(str_key);
    if (!e->str_key) {
      free(e);
      return false;
    }
  }
  e->u32 = val;
  e->kind = kind;
  e->meas_id = meas_id;
  e->hash = h;
  e->last_access_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);

  size_t b = bucket_of(h, c->bucket_cnt);
  e->next = c->buckets[b];
  c->buckets[b] = e;
  lru_link_front(&c->lru, &e->lru);

  c->entry_cnt++;
  c->mem_used += entry_mem(e);
  c->st.insertions++;
  return true;
}

/* ---------- blob API ---------------------------------------------------- */
bool ts_cache_lookup_blob(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id, void** out_data,
                          size_t* out_sz) {
  if (!c || !out_data || !out_sz) return false;
  *out_data = NULL;
  *out_sz = 0;

  uint32_t h = str_key ? djb2(str_key) : meas_id;
  entry_t* prev = NULL;
  entry_t* e = find_entry(c, kind, str_key, meas_id, h, &prev);

  if (!e) {
    c->st.misses++;
    return false;
  }

  if (expired(c, e)) {
    if (prev)
      prev->next = e->next;
    else
      c->buckets[bucket_of(h, c->bucket_cnt)] = e->next;
    lru_unlink(&c->lru, &e->lru);
    c->mem_used -= entry_mem(e);
    c->entry_cnt--;
    c->st.evictions++;
    free(e->str_key);
    free(e->blob);
    free(e);
    c->st.misses++;
    return false;
  }

  uint8_t* copy = malloc(e->blob_sz);
  if (!copy) return false;
  memcpy(copy, e->blob, e->blob_sz);

  *out_data = copy;
  *out_sz = e->blob_sz;
  e->last_access_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);
  lru_move_front(&c->lru, &e->lru);
  c->st.hits++;
  return true;
}

bool ts_cache_insert_blob(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id, const void* data,
                          size_t size) {
  if (!c || !data || !size) return false;
  uint32_t h = str_key ? djb2(str_key) : meas_id;

  entry_t* e = find_entry(c, kind, str_key, meas_id, h, NULL);
  if (e) {
    /* resize if needed */
    if (e->blob_sz != size) {
      uint8_t* nb = realloc(e->blob, size);
      if (!nb) return false;
      c->mem_used += (size - e->blob_sz);
      e->blob = nb;
      e->blob_sz = size;
    }
    memcpy(e->blob, data, size);
    e->last_access_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);
    lru_move_front(&c->lru, &e->lru);
    c->st.updates++;
    return true;
  }

  size_t need = sizeof *e + (str_key ? strlen(str_key) + 1 : 0) + size;
  ensure_capacity(c, need);

  e = calloc(1, sizeof *e);
  if (!e) return false;
  if (str_key) {
    e->str_key = strdup(str_key);
    if (!e->str_key) {
      free(e);
      return false;
    }
  }
  e->blob = malloc(size);
  if (!e->blob) {
    free(e->str_key);
    free(e);
    return false;
  }
  memcpy(e->blob, data, size);
  e->blob_sz = size;

  e->kind = kind;
  e->meas_id = meas_id;
  e->hash = h;
  e->last_access_ms = (uint32_t)(esp_timer_get_time() / 1000ULL);

  size_t b = bucket_of(h, c->bucket_cnt);
  e->next = c->buckets[b];
  c->buckets[b] = e;
  lru_link_front(&c->lru, &e->lru);

  c->entry_cnt++;
  c->mem_used += entry_mem(e);
  c->st.insertions++;
  return true;
}

/* ---------- stats ------------------------------------------------------- */
void ts_cache_get_stats(const ts_cache_t* c, ts_cache_stats_t* out) {
  if (!c || !out) return;
  *out = c->st;
  out->entries = c->entry_cnt;
  out->bytes_in_use = c->mem_used;
}

bool ts_cache_remove(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id) {
  if (!c) return false;

  uint32_t h = str_key ? djb2(str_key) : meas_id;
  size_t b = bucket_of(h, c->bucket_cnt);
  entry_t* e = c->buckets[b];
  entry_t* prev = NULL;

  while (e) {
    if (e->hash == h && e->kind == kind && e->meas_id == meas_id &&
        ((str_key == NULL && e->str_key == NULL) || (str_key && e->str_key && strcmp(e->str_key, str_key) == 0))) {
      // Unlink from bucket chain
      if (prev)
        prev->next = e->next;
      else
        c->buckets[b] = e->next;

      // Unlink from LRU list
      lru_unlink(&c->lru, &e->lru);

      // Update counters
      c->mem_used -= entry_mem(e);
      c->entry_cnt--;
      c->st.evictions++;  // Count as eviction for consistency

      // Free memory
      free(e->str_key);
      free(e->blob);
      free(e);

      return true;
    }
    prev = e;
    e = e->next;
  }
  return false;  // Key not found
}

/* ---------- u32 removal API ------------------------------------------- */
bool ts_cache_remove_u32(ts_cache_t* c, key_kind_e kind, const char* str_key, uint32_t meas_id) {
  if (!c) return false;

  uint32_t h = str_key ? djb2(str_key) : meas_id;
  size_t b = bucket_of(h, c->bucket_cnt);

  entry_t* e = c->buckets[b];
  entry_t* prev = NULL;

  while (e) {
    if (e->hash == h && e->kind == kind && e->meas_id == meas_id &&
        ((str_key == NULL && e->str_key == NULL) || (str_key && e->str_key && strcmp(e->str_key, str_key) == 0))) {
      /* --- unlink from bucket chain --- */
      if (prev)
        prev->next = e->next;
      else
        c->buckets[b] = e->next;

      /* --- unlink from LRU list --- */
      lru_unlink(&c->lru, &e->lru);

      /* --- update accounting --- */
      c->mem_used -= entry_mem(e);
      c->entry_cnt -= 1;
      c->st.evictions++; /* reuse eviction counter */

      /* --- free resources --- */
      free(e->str_key);
      free(e->blob);
      free(e);
      return true; /* removed! */
    }
    prev = e;
    e = e->next;
  }
  return false; /* not found */
}
