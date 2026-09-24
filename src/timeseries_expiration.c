#include "timeseries_epoch.h"
#include "timeseries_expiration.h"

#include "esp_err.h"
#include "esp_log.h"
#include "esp_partition.h"

#include "timeseries_compaction.h"        // timeseries_compact_page_list(), tsdb_publish_rewrite_locked()
#include "timeseries_compaction_merge.h"  // tsdb_trim_stream_records()
#include "timeseries_flash_utils.h"       // tsdb_flash_write_byte()
#include "timeseries_data.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"        // tsdb_pagecache_get_total_active_size(), batches
#include "timeseries_page_cache_snapshot.h"

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "TimeseriesExpiration";

/*
 * Expiration by horizon.
 *
 * Guarantee: after a run there is one horizon T such that no point older
 * than T survives and no point at or after T is removed, for every series,
 * whatever the record layout. (A point inserted while the run is in progress
 * may be older than T; it is expired by a later run.)
 *
 * A run picks the smallest T whose removal is estimated to free the
 * requested bytes. T is found by a TSDB_EXPIRE_EDGES-way search: each pass
 * evaluates TSDB_EXPIRE_EDGES candidate horizons over a window, and the next
 * pass searches the part of the chosen bucket where the estimate can change
 * (inside some record's span). Empty time between records is never
 * subdivided, so one outlier timestamp far from the rest (2^62, microseconds
 * among milliseconds, UINT64_MAX) cannot make the steps coarser than the
 * data: the search ends at a 1 ms step or at a record boundary, so a run
 * removes at most the target plus the points at the chosen timestamp (and
 * the conservative trim charge below).
 *   - a record entirely before T is soft-deleted (as before);
 *   - a record entirely before T is soft-deleted (as before);
 *   - a record straddling T is trimmed: its points at or after T are written,
 *     with their original revisions, into one new page through the compaction
 *     writer and journal (existing record format), then the original is
 *     soft-deleted;
 *   - a record at or after T is untouched.
 * Trims are written and published first; nothing is deleted unless every trim
 * succeeded, so a failed trim leaves every record intact (its outputs are
 * given back as for a failed compaction). The pages that lost records are
 * then rewritten without them (timeseries_compact_page_list), which is what
 * returns space. The usage threshold only decides whether a run starts; a run
 * frees the requested bytes (reduction_threshold of the partition), as it
 * always has. If a round frees less than that (records appended meanwhile,
 * or the trim bound), a later horizon is chosen for the rest, at most
 * TSDB_EXPIRE_MAX_ROUNDS times per run.
 *
 * Estimates are conservative: a trimmed record is charged a new record of
 * its kept fraction plus 25% and a fixed overhead. At most
 * TSDB_EXPIRE_MAX_TRIMS records are trimmed per round; if the horizon that
 * frees enough would need more, the largest horizon within that bound is
 * used and the next round continues.
 *
 * The horizon is not stored: every run recomputes it from the records.
 */
#define TSDB_EXPIRE_EDGES 128
#define TSDB_EXPIRE_MAX_TRIMS 64
#define TSDB_EXPIRE_MAX_ROUNDS 4
#define TSDB_EXPIRE_TRIM_OVERHEAD 64
// Each refinement shrinks the step at least TSDB_EXPIRE_EDGES-fold; 128^10 > 2^64.
#define TSDB_EXPIRE_MAX_REFINES 10

typedef bool (*record_visit_t)(void *ctx, uint32_t page_offset, const timeseries_page_header_t *page,
                               uint32_t record_offset, const timeseries_field_data_header_t *record);

// Visit every live (not soft-deleted) record of every ACTIVE field-data page.
// With `locked`, flash_write_mutex is held per page, so visitors may write.
static bool visit_live_records(timeseries_db_t *db, bool locked, record_visit_t visit, void *ctx) {
  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) return false;
  uint8_t *window = malloc(4096);
  bool ok = window != NULL;
  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;
  while (ok && timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset, &page_size)) {
    if (hdr.magic_number != TIMESERIES_MAGIC_NUM || hdr.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA ||
        hdr.page_state != TIMESERIES_PAGE_STATE_ACTIVE)
      continue;
    if (locked) tsdb_write_lock(db);
    timeseries_fielddata_iterator_t it;
    if (!timeseries_fielddata_iterator_init_window(db, page_offset, page_size, window, 4096, &it)) {
      ok = false;
    } else {
      timeseries_field_data_header_t rec;
      while (ok && timeseries_fielddata_iterator_next(&it, &rec)) {
        if (!(rec.flags & TSDB_FIELDDATA_FLAG_DELETED)) continue;  // bit cleared = deleted
        if (!rec.record_count || rec.start_time > rec.end_time) continue;  // not a usable record
        ok = visit(ctx, page_offset, &hdr, it.current_record_offset, &rec);
      }
      if (it.read_error) ok = false;
      timeseries_fielddata_iterator_deinit(&it);
    }
    if (locked && db->flash_write_mutex) xSemaphoreGive(db->flash_write_mutex);
  }
  timeseries_page_cache_iterator_deinit(&page_iter);
  free(window);
  return ok;
}

// ---------------------------------------------------------------------------
// Choosing the horizon
// ---------------------------------------------------------------------------

typedef struct {
  uint64_t min_start, max_end;
  size_t records;
} bounds_ctx_t;

static bool visit_bounds(void *p, uint32_t po, const timeseries_page_header_t *ph, uint32_t ro,
                         const timeseries_field_data_header_t *r) {
  (void)po;
  (void)ph;
  (void)ro;
  bounds_ctx_t *b = p;
  if (!b->records || r->start_time < b->min_start) b->min_start = r->start_time;
  if (!b->records || r->end_time > b->max_end) b->max_end = r->end_time;
  b->records++;
  return true;
}

// Candidate horizons edge[i] = min(lo + (i+1)*width, hi), i < TSDB_EXPIRE_EDGES.
// Bucket i holds the values v with edge[i-1] <= v < edge[i] (edge[-1] = lo).
typedef struct {
  uint64_t lo, hi, width;
  int64_t full_from[TSDB_EXPIRE_EDGES + 1];  // difference array: bytes of records ending before edge
  int64_t net[TSDB_EXPIRE_EDGES];            // straddling records: bytes freed minus trim output
  uint32_t straddlers[TSDB_EXPIRE_EDGES];    // records with start < edge[i] <= end
  uint32_t lo_straddlers;                    // records with start < lo <= end
  // Where the estimate can change inside bucket i: the lowest record start
  // in it and the highest end + 1 of a record ending in it.
  uint64_t first_start[TSDB_EXPIRE_EDGES], last_end[TSDB_EXPIRE_EDGES];
  bool any_start[TSDB_EXPIRE_EDGES], any_end[TSDB_EXPIRE_EDGES];
} edges_t;

static uint64_t edge_at(const edges_t *e, size_t i) {
  uint64_t span = e->hi - e->lo;
  // (i+1)*width > span, tested without forming the product (it can wrap).
  if (e->width > span / (i + 1)) return e->hi;
  return e->lo + (uint64_t)(i + 1) * e->width;
}

// First edge index whose value is > x (TSDB_EXPIRE_EDGES if none).
static size_t first_edge_above(const edges_t *e, uint64_t x) {
  if (x < e->lo) return 0;
  if (x >= e->hi) return TSDB_EXPIRE_EDGES;  // edges are <= hi (clamped ones equal it)
  return (size_t)((x - e->lo) / e->width);  // < TSDB_EXPIRE_EDGES since x - lo < hi - lo
}

static uint32_t record_bytes(const timeseries_field_data_header_t *r) {
  return (uint32_t)sizeof(*r) + r->record_length;
}

// Conservative size of the record a trim at horizon t (start < t <= end) writes.
static int64_t trim_output_estimate(const timeseries_field_data_header_t *r, uint64_t t) {
  // Kept fraction of the time span, (a+1)/(b+1). Both spans can be 2^64 wide
  // (a record over [0, UINT64_MAX]), so they are scaled below 2^32 first,
  // rounding the fraction up; the product with the count then fits.
  uint64_t a = r->end_time - t, b = r->end_time - r->start_time, num, den;
  if (b < (1ULL << 32)) {
    num = a + 1;
    den = b + 1;
  } else {
    unsigned s = 0;
    while ((b >> s) >= (1ULL << 32)) s++;
    num = (a >> s) + 1;  // (a+1)/(b+1) <= ((a>>s)+1)/(b>>s), and b>>s >= 2^31
    den = b >> s;
  }
  uint64_t kept = (num * (uint64_t)r->record_count + den - 1) / den;
  if (kept > r->record_count) kept = r->record_count;
  uint64_t bytes = ((uint64_t)r->record_length * kept + r->record_count - 1) / r->record_count;
  return (int64_t)(sizeof(*r) + TSDB_EXPIRE_TRIM_OVERHEAD + bytes + bytes / 4);
}

static bool visit_edges(void *p, uint32_t po, const timeseries_page_header_t *ph, uint32_t ro,
                        const timeseries_field_data_header_t *r) {
  (void)po;
  (void)ph;
  (void)ro;
  edges_t *e = p;
  if (r->start_time < e->lo && r->end_time >= e->lo) e->lo_straddlers++;
  if (r->start_time >= e->lo) {
    size_t b = first_edge_above(e, r->start_time);
    if (b < TSDB_EXPIRE_EDGES && (!e->any_start[b] || r->start_time < e->first_start[b])) {
      e->first_start[b] = r->start_time;
      e->any_start[b] = true;
    }
  }
  if (r->end_time >= e->lo) {
    size_t b = first_edge_above(e, r->end_time);  // b < EDGES: end < hi, so end + 1 cannot wrap
    if (b < TSDB_EXPIRE_EDGES && (!e->any_end[b] || r->end_time + 1 > e->last_end[b])) {
      e->last_end[b] = r->end_time + 1;
      e->any_end[b] = true;
    }
  }
  // Records entirely before edge i: every edge above end_time.
  e->full_from[first_edge_above(e, r->end_time)] += record_bytes(r);
  // Records straddling edge i: start < edge <= end.
  for (size_t i = first_edge_above(e, r->start_time); i < TSDB_EXPIRE_EDGES; i++) {
    uint64_t t = edge_at(e, i);
    if (t > r->end_time) break;
    if (t <= r->start_time) continue;
    e->straddlers[i]++;
    e->net[i] += (int64_t)record_bytes(r) - trim_output_estimate(r, t);
  }
  return true;
}

// Evaluate the candidate horizons in (lo, hi]. freed[i] is the estimated
// bytes a run at edge i returns.
static bool evaluate_edges(timeseries_db_t *db, edges_t *e, uint64_t lo, uint64_t hi, int64_t *freed) {
  memset(e, 0, sizeof(*e));
  e->lo = lo;
  e->hi = hi;
  uint64_t span = hi - lo;  // ceil(span / EDGES) without the wrapping add
  e->width = span / TSDB_EXPIRE_EDGES + (span % TSDB_EXPIRE_EDGES != 0);
  if (!e->width) e->width = 1;
  if (!visit_live_records(db, false, visit_edges, e)) return false;
  int64_t full = 0;
  for (size_t i = 0; i < TSDB_EXPIRE_EDGES; i++) {
    full += e->full_from[i];
    freed[i] = full + e->net[i];
  }
  return true;
}

// Smallest edge freeing >= target within the trim bound; else the largest
// edge within the bound that frees anything. False when none frees anything.
static bool pick_edge(const edges_t *e, const int64_t *freed, int64_t target, size_t *out, bool *met) {
  for (size_t i = 0; i < TSDB_EXPIRE_EDGES; i++) {
    if (e->straddlers[i] <= TSDB_EXPIRE_MAX_TRIMS && freed[i] >= target) {
      *out = i;
      *met = true;
      return true;
    }
  }
  *met = false;
  for (size_t i = TSDB_EXPIRE_EDGES; i-- > 0;) {
    if (e->straddlers[i] <= TSDB_EXPIRE_MAX_TRIMS && freed[i] > 0) {
      *out = i;
      return true;
    }
  }
  return false;
}

// The part (*lo, *hi] of bucket i (edge[i-1], edge[i]] worth searching. The
// estimate only changes at a horizon inside some record's (start, end + 1],
// so the smallest horizon in the bucket that frees what edge[i] frees lies
// there; empty time around the records is dropped. *hi frees what edge[i]
// frees: no record straddles or ends in [*hi, edge[i]).
static void narrow_bucket(const edges_t *e, size_t i, uint64_t *lo, uint64_t *hi) {
  uint64_t a = i ? edge_at(e, i - 1) : e->lo, b = edge_at(e, i);
  bool crosses_a = i ? e->straddlers[i - 1] > 0 : e->lo_straddlers > 0;
  *lo = (!crosses_a && e->any_start[i]) ? e->first_start[i] : a;
  *hi = (!e->straddlers[i] && e->any_end[i]) ? e->last_end[i] : b;
  if (*lo >= *hi) {
    *lo = a;
    *hi = b;
  }
}

// Choose the horizon for this round. False with *horizon = 0 when there is
// nothing to expire.
static bool choose_horizon(timeseries_db_t *db, int64_t target, uint64_t *horizon) {
  *horizon = 0;
  bounds_ctx_t b = {0};
  if (!visit_live_records(db, false, visit_bounds, &b)) return false;
  if (!b.records) return true;
  // The last edge (max_end + 1) removes everything and needs no trims. At
  // UINT64_MAX it saturates: a point there is never expired, the rest can be.
  uint64_t lo = b.min_start, hi = b.max_end == UINT64_MAX ? UINT64_MAX : b.max_end + 1;
  if (lo >= hi) return true;  // every point is at UINT64_MAX
  edges_t *e = malloc(sizeof(*e));
  int64_t *freed = malloc(TSDB_EXPIRE_EDGES * sizeof(*freed));
  bool ok = e && freed;
  size_t i = 0;
  bool met = false;
  if (ok) ok = evaluate_edges(db, e, lo, hi, freed);
  if (ok && pick_edge(e, freed, target, &i, &met)) {
    uint64_t t = edge_at(e, i);
    // When the target decided the edge, search its bucket again, narrowed to
    // the records in it, until the step is 1 ms. A failed pass keeps t.
    for (int pass = 0; met && e->width > 1 && pass < TSDB_EXPIRE_MAX_REFINES; pass++) {
      uint64_t a, z;
      narrow_bucket(e, i, &a, &z);
      if (z - a <= 1) {
        t = z;  // the only candidate left
        break;
      }
      size_t j;
      bool met2 = false;
      if (!evaluate_edges(db, e, a, z, freed) || !pick_edge(e, freed, target, &j, &met2) || !met2) break;
      i = j;
      t = edge_at(e, i);
    }
    *horizon = t;
  }
  free(e);
  free(freed);
  return ok;
}

// ---------------------------------------------------------------------------
// Applying the horizon
// ---------------------------------------------------------------------------

typedef struct {
  uint64_t horizon;
  tsdb_record_ref_t *refs;
  size_t count, capacity;
  uint8_t level;
  bool overflow;
} straddle_ctx_t;

static bool visit_straddlers(void *p, uint32_t po, const timeseries_page_header_t *ph, uint32_t ro,
                             const timeseries_field_data_header_t *r) {
  straddle_ctx_t *s = p;
  if (!(r->start_time < s->horizon && r->end_time >= s->horizon)) return true;
  if (s->count == s->capacity) {
    s->overflow = true;
    return false;
  }
  s->refs[s->count++] = (tsdb_record_ref_t){po, ph->sequence_num, ro, *r};
  if (ph->field_data_level > s->level) s->level = ph->field_data_level;
  return true;
}

typedef struct {
  uint32_t *pages;
  size_t count, capacity;
  bool oom;
} page_list_t;

static bool page_list_add(page_list_t *l, uint32_t page) {
  for (size_t i = 0; i < l->count; i++)
    if (l->pages[i] == page) return true;
  if (l->count == l->capacity) {
    size_t cap = l->capacity ? l->capacity * 2 : 16;
    uint32_t *grown = realloc(l->pages, cap * sizeof(*grown));
    if (!grown) {
      l->oom = true;
      return false;
    }
    l->pages = grown;
    l->capacity = cap;
  }
  l->pages[l->count++] = page;
  return true;
}

// Soft-delete one record: clear its DELETED bit (1 -> 0). Caller holds
// flash_write_mutex. The record must still be the one that was found.
static bool soft_delete(timeseries_db_t *db, const tsdb_record_ref_t *ref) {
  uint32_t abs = ref->page_offset + ref->record_offset;
  timeseries_page_header_t page;
  timeseries_field_data_header_t now;
  if (esp_partition_read(db->partition, ref->page_offset, &page, sizeof(page)) != ESP_OK ||
      esp_partition_read(db->partition, abs, &now, sizeof(now)) != ESP_OK)
    return false;
  if (page.magic_number != TIMESERIES_MAGIC_NUM || page.page_state != TIMESERIES_PAGE_STATE_ACTIVE ||
      page.sequence_num != ref->page_sequence || memcmp(now.series_id, ref->header.series_id, 16) ||
      now.start_time != ref->header.start_time || now.record_length != ref->header.record_length) {
    ESP_LOGW(TAG, "Record @0x%08" PRIx32 " changed since it was found; not deleted", abs);
    return false;
  }
  if (!(now.flags & TSDB_FIELDDATA_FLAG_DELETED)) return true;
  return tsdb_flash_write_byte(db->partition, abs + offsetof(timeseries_field_data_header_t, flags),
                               (uint8_t)(now.flags & ~TSDB_FIELDDATA_FLAG_DELETED)) == ESP_OK;
}

typedef struct {
  timeseries_db_t *db;
  uint64_t horizon;
  page_list_t *modified;
  size_t deleted;
  uint64_t freed;  // bytes of the records deleted
  bool write_failed;
} expire_ctx_t;

// Visited with flash_write_mutex held for the page.
static bool visit_expire(void *p, uint32_t po, const timeseries_page_header_t *ph, uint32_t ro,
                         const timeseries_field_data_header_t *r) {
  expire_ctx_t *x = p;
  if (r->end_time >= x->horizon) return true;
  tsdb_record_ref_t ref = {po, ph->sequence_num, ro, *r};
  if (!soft_delete(x->db, &ref)) {
    x->write_failed = true;
    return true;  // keep going; the run reports failure
  }
  x->deleted++;
  x->freed += record_bytes(r);
  return page_list_add(x->modified, po);
}

// One round at `horizon`. Returns false on any failure; nothing is deleted if
// a trim cannot be written and published. *freed: bytes the round returns
// (deleted records, less a conservative estimate of the trim outputs).
static bool expire_before(timeseries_db_t *db, uint64_t horizon, page_list_t *modified, size_t *trimmed,
                          size_t *deleted, int64_t *freed) {
  *trimmed = *deleted = 0;
  *freed = 0;
  straddle_ctx_t s = {.horizon = horizon, .capacity = TSDB_EXPIRE_MAX_TRIMS, .level = 1};
  s.refs = malloc(s.capacity * sizeof(*s.refs));
  if (!s.refs) return false;
  bool ok = visit_live_records(db, false, visit_straddlers, &s) || s.overflow;
  if (s.overflow) {
    // Records were appended since the horizon was chosen; the next round
    // chooses again.
    ESP_LOGW(TAG, "More than %d records straddle the horizon; skipping this round", TSDB_EXPIRE_MAX_TRIMS);
    free(s.refs);
    return false;
  }
  if (ok && s.count) {
    tsdb_page_cache_snapshot_t *batch = tsdb_pagecache_begin_batch(db);
    bool written = batch && tsdb_trim_stream_records(db, s.refs, s.count, horizon, s.level, batch);
    if (!batch) {
      ok = false;
    } else {
      tsdb_write_lock(db);
      ok = tsdb_publish_rewrite_locked(db, batch, written);
      // The kept points are live in the new page; now drop the originals.
      for (size_t i = 0; ok && i < s.count; i++) {
        if (!soft_delete(db, &s.refs[i]) || !page_list_add(modified, s.refs[i].page_offset)) ok = false;
        else *freed += (int64_t)record_bytes(&s.refs[i].header) - trim_output_estimate(&s.refs[i].header, horizon);
      }
      if (db->flash_write_mutex) xSemaphoreGive(db->flash_write_mutex);
      if (ok) *trimmed = s.count;
    }
  }
  free(s.refs);
  if (!ok) return false;
  expire_ctx_t x = {.db = db, .horizon = horizon, .modified = modified};
  ok = visit_live_records(db, true, visit_expire, &x) && !x.write_failed && !modified->oom;
  *deleted = x.deleted;
  *freed += (int64_t)x.freed;
  return ok;
}

/**
 * @brief Return usage as a fraction (0.0..1.0).
 */
static bool get_usage_ratio(timeseries_db_t *db, float *out_ratio) {
  if (!db || !out_ratio) {
    return false;
  }
  uint32_t partition_size = db->partition->size;
  uint32_t used_space = tsdb_pagecache_get_total_active_size(db);
  *out_ratio = (float)used_space / (float)partition_size;
  return true;
}

static bool expire_to_horizon(timeseries_db_t *db, uint32_t bytes_to_free) {
  bool ok = true;
  uint64_t previous = 0;
  int64_t remaining = bytes_to_free;
  for (int round = 0; round < TSDB_EXPIRE_MAX_ROUNDS && remaining > 0; round++) {
    uint64_t horizon = 0;
    if (!choose_horizon(db, remaining, &horizon)) return false;
    if (!horizon || horizon <= previous) break;  // nothing (more) to expire
    page_list_t modified = {0};
    size_t trimmed = 0, deleted = 0;
    int64_t freed = 0;
    ok = expire_before(db, horizon, &modified, &trimmed, &deleted, &freed);
    remaining -= freed;
    ESP_LOGI(TAG, "Expiration round %d: horizon %" PRIu64 ", %zu record(s) trimmed, %zu deleted", round, horizon,
             trimmed, deleted);
    // Reclaim what was deleted, even after a partial failure.
    if (modified.count && !timeseries_compact_page_list(db, modified.pages, modified.count)) ok = false;
    free(modified.pages);
    if (!ok) break;
    previous = horizon;
  }
  return ok;
}

bool timeseries_expiration_run(timeseries_db_t *db, float usage_threshold,
                               float reduction_threshold) {
  // Positive comparisons also reject NaN, before the float-to-integer conversion.
  if (!db || !db->partition || !db->partition->size ||
      !(usage_threshold > 0.0f && usage_threshold < 1.0f) ||
      !(reduction_threshold > 0.0f && reduction_threshold < 1.0f)) return false;
  float used_ratio;
  if (!get_usage_ratio(db, &used_ratio)) return false;
  if (used_ratio < usage_threshold) return true;

  // Own the slot throughout discovery, deletion and rewriting. A streaming
  // compactor may already have decoded a source record; deleting that source
  // while it runs would let its output resurrect the expired data.
  bool expected = false;
  if (!atomic_compare_exchange_strong(&db->compaction_in_progress, &expected, true)) return false;
  uint32_t bytes_to_free = (uint32_t)((float)db->partition->size * reduction_threshold);
  if (!bytes_to_free) bytes_to_free = 1;
  bool ok = expire_to_horizon(db, bytes_to_free);
  atomic_store(&db->compaction_in_progress, false);
  return ok;
}
