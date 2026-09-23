/*
 * Retention phases (TSDB_REVIEW §8): expiration must leave, after every run,
 * a horizon T such that no point older than T survives and no point at or
 * after T is removed, for every series, whatever the record layout.
 *
 * Run with the other deterministic phases (run_tests.sh), or one at a time
 * with REVIEW=<id>.
 */
#include <inttypes.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "timeseries.h"
#include "timeseries_compaction.h"
#include "timeseries_data.h"
#include "timeseries_expiration.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "mbedtls/md.h"

extern timeseries_db_t* review_db(void);

#define RS_SERIES 3
static const char* const rs_fields[RS_SERIES] = {"a", "b", "c"};
#define RS_TS0 ((uint64_t)1000000000000)
#define RS_STEP ((uint64_t)1000)

/* Every timestamp inserted into "ret" (all fields share them), in order. */
static uint64_t* rs_ts;
static size_t rs_count, rs_cap;

static void rs_reset(void) {
  free(rs_ts);
  rs_ts = NULL;
  rs_count = rs_cap = 0;
}

/* Insert n points per field at timestamps first, first+STEP, ... */
static bool rs_put(uint64_t first, size_t n) {
  timeseries_field_value_t* fv = calloc(n * RS_SERIES, sizeof(*fv));
  uint64_t* ts = calloc(n, sizeof(*ts));
  if (!fv || !ts) abort();
  for (size_t i = 0; i < n; i++) {
    ts[i] = first + i * RS_STEP;
    for (size_t f = 0; f < RS_SERIES; f++) {
      fv[f * n + i].type = TIMESERIES_FIELD_TYPE_FLOAT;
      fv[f * n + i].data.float_val = (double)((ts[i] - RS_TS0) / RS_STEP) + 0.25 * (double)f;
    }
  }
  timeseries_insert_data_t d = {.measurement_name = "ret", .field_names = (const char**)rs_fields,
                                .field_values = fv, .num_fields = RS_SERIES, .timestamps_ms = ts, .num_points = n};
  bool ok = timeseries_insert(&d);
  if (ok) {
    if (rs_count + n > rs_cap) {
      rs_cap = (rs_count + n) * 2;
      rs_ts = realloc(rs_ts, rs_cap * sizeof(*rs_ts));
      if (!rs_ts) abort();
    }
    memcpy(rs_ts + rs_count, ts, n * sizeof(*ts));
    rs_count += n;
  }
  free(fv);
  free(ts);
  return ok;
}

static int cmp_u64(const void* a, const void* b) {
  uint64_t x = *(const uint64_t*)a, y = *(const uint64_t*)b;
  return x < y ? -1 : x > y;
}

static size_t rs_level_pages(uint8_t level) {
  tsdb_usage_summary_t s;
  memset(&s, 0, sizeof s);
  timeseries_get_usage_summary(&s);
  return level < 5 ? s.page_summaries[level].num_pages : 0;
}

/*
 * The retention guarantee over everything inserted with rs_put(): per series
 * the surviving points are exactly the inserted points at or after one
 * horizon, and that horizon is the same for every series. Every returned
 * point must also be an inserted one, with its own value. Returns failures;
 * *removed gets the number of points expired (all series).
 */
static int rs_check(const char* tag, size_t* removed) {
  int bad = 0;
  *removed = 0;
  uint64_t* inserted = malloc((rs_count ? rs_count : 1) * sizeof(*inserted));
  if (!inserted) abort();
  memcpy(inserted, rs_ts, rs_count * sizeof(*inserted));
  qsort(inserted, rs_count, sizeof(*inserted), cmp_u64);
  uint64_t max_removed = 0, min_surviving = UINT64_MAX;
  bool any_removed = false;
  for (size_t f = 0; f < RS_SERIES; f++) {
    const char* fn[1] = {rs_fields[f]};
    timeseries_query_t q = {.measurement_name = "ret", .field_names = fn, .num_fields = 1};
    timeseries_query_result_t r;
    memset(&r, 0, sizeof r);
    if (!timeseries_query(&q, &r)) {
      printf("%s: FAIL query of ret.%s failed\n", tag, rs_fields[f]);
      bad++;
      continue;
    }
    // Merge-walk the sorted inserted list against the sorted result.
    size_t i = 0, surviving = 0, removed_here = 0;
    uint64_t series_max_removed = 0, series_min_surviving = UINT64_MAX;
    for (size_t p = 0; p < r.num_points; p++) {
      uint64_t t = r.timestamps[p];
      while (i < rs_count && inserted[i] < t) {
        removed_here++;
        if (inserted[i] > series_max_removed) series_max_removed = inserted[i];
        i++;
      }
      double want = (double)((t - RS_TS0) / RS_STEP) + 0.25 * (double)f;
      if (i >= rs_count || inserted[i] != t || r.columns[0].values[p].data.float_val != want) {
        if (bad++ < 5) printf("%s: FAIL ret.%s returned ts=%" PRIu64 " that was not inserted (or wrong value)\n", tag,
                              rs_fields[f], t);
        continue;
      }
      i++;
      surviving++;
      if (t < series_min_surviving) series_min_surviving = t;
    }
    for (; i < rs_count; i++) {
      removed_here++;
      if (inserted[i] > series_max_removed) series_max_removed = inserted[i];
    }
    timeseries_query_free_result(&r);
    if (removed_here && surviving && series_max_removed > series_min_surviving) {
      if (bad++ < 5)
        printf("%s: FAIL ret.%s is not a newest suffix: removed ts=%" PRIu64 " is newer than surviving ts=%" PRIu64
               "\n", tag, rs_fields[f], series_max_removed, series_min_surviving);
    }
    if (removed_here) {
      any_removed = true;
      if (series_max_removed > max_removed) max_removed = series_max_removed;
    }
    if (series_min_surviving < min_surviving) min_surviving = series_min_surviving;
    *removed += removed_here;
    printf("%s: ret.%s surviving=%zu removed=%zu oldest surviving k=%" PRId64 "\n", tag, rs_fields[f], surviving,
           removed_here, series_min_surviving == UINT64_MAX ? -1 : (int64_t)((series_min_surviving - RS_TS0) / RS_STEP));
  }
  if (any_removed && min_surviving != UINT64_MAX && max_removed >= min_surviving) {
    if (bad++ < 6)
      printf("%s: FAIL no single horizon: a series lost ts=%" PRIu64 " while another kept ts=%" PRIu64 "\n", tag,
             max_removed, min_surviving);
  }
  free(inserted);
  return bad;
}

/*
 * Build records of one series whose time ranges interleave within a level:
 * more than 32 L0 pages whose offsets are not in time order (newer data in
 * lower offsets), then two L0->L1 compactions. Auto-compaction is held off
 * while the pages are arranged.
 */
static void rs_build_interleaved(const char* tag) {
  timeseries_db_t* db = review_db();
  timeseries_clear_all();
  rs_reset();
  atomic_store(&db->compaction_in_progress, true);
  static uint32_t holes[48];
  size_t nholes = 0;
  uint32_t o, sz;
  while (nholes < 48 && tsdb_reserve_blank_region(db, TIMESERIES_FIELD_DATA_PAGE_SIZE, NULL, &o, &sz))
    holes[nholes++] = o;
  uint64_t ts = RS_TS0;
  // Older data lands above the holes, in time order.
  while (rs_level_pages(0) < 40) {
    if (!rs_put(ts, 300)) break;
    ts += 300 * RS_STEP;
  }
  for (size_t i = 0; i < nholes; i++) tsdb_release_reserved_region(db, holes[i], NULL);
  // Newer data lands in the (lower) holes.
  while (rs_level_pages(0) < 80) {
    if (!rs_put(ts, 300)) break;
    ts += 300 * RS_STEP;
  }
  size_t l0 = rs_level_pages(0);
  bool c1 = timeseries_compact_level0_pages(db);
  bool c2 = timeseries_compact_level0_pages(db);
  atomic_store(&db->compaction_in_progress, false);
  printf("%s: %zu points per series in %zu L0 pages; two L0->L1 compactions -> %d %d; L0=%zu L1=%zu\n", tag,
         rs_count, l0, c1, c2, rs_level_pages(0), rs_level_pages(1));
}

/* Time ranges of every live record of ret.a at a level, sorted by start. */
static size_t rs_record_ranges(uint8_t level, uint64_t (*out)[2], size_t cap);

/* ---- R14: expiration over interleaved records keeps one horizon ---------- */
static int t_expiration_horizon_interleaved(void) {
  timeseries_db_t* db = review_db();
  rs_build_interleaved("R14");
  uint64_t ranges[64][2];
  size_t n = rs_record_ranges(1, ranges, 64);
  for (size_t i = 0; i < n; i++)
    printf("R14: L1 record of ret.a: k[%" PRIu64 "..%" PRIu64 "]\n", (ranges[i][0] - RS_TS0) / RS_STEP,
           (ranges[i][1] - RS_TS0) / RS_STEP);
  // Expire a tenth of the used space: usage is over any positive threshold.
  float used = (float)tsdb_pagecache_get_total_active_size(db) / (float)db->partition->size;
  bool ok = timeseries_expiration_run(db, 0.001f, used / 10.0f);
  size_t removed = 0;
  int bad = rs_check("R14", &removed);
  printf("R14: expiration -> %d, removed %zu points\n", ok, removed);
  if (!ok || !removed) {
    printf("R14: FAIL expiration removed nothing\n");
    bad++;
  }
  rs_reset();
  return bad;
}

static size_t rs_record_ranges(uint8_t level, uint64_t (*out)[2], size_t cap) {
  timeseries_db_t* db = review_db();
  static const char key[] = "ret:a";  // series key: measurement[:tags]:field
  unsigned char sid[16];
  mbedtls_md(mbedtls_md_info_from_type(MBEDTLS_MD_MD5), (const unsigned char*)key, strlen(key), sid);
  size_t n = 0;
  timeseries_page_cache_iterator_t it;
  if (!timeseries_page_cache_iterator_init(db, &it)) return 0;
  timeseries_page_header_t h;
  uint32_t off, size;
  while (timeseries_page_cache_iterator_next(&it, &h, &off, &size)) {
    if (h.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA || h.field_data_level != level) continue;
    timeseries_fielddata_iterator_t f;
    if (!timeseries_fielddata_iterator_init(db, off, size, &f)) continue;
    timeseries_field_data_header_t fh;
    while (timeseries_fielddata_iterator_next(&f, &fh)) {
      if (!(fh.flags & TSDB_FIELDDATA_FLAG_DELETED) || memcmp(fh.series_id, sid, 16) || n >= cap) continue;
      out[n][0] = fh.start_time;
      out[n][1] = fh.end_time;
      n++;
    }
    timeseries_fielddata_iterator_deinit(&f);
  }
  timeseries_page_cache_iterator_deinit(&it);
  qsort(out, n, sizeof(out[0]), cmp_u64);
  return n;
}

/* ---- R15: compaction merges in time order ------------------------------ */
// In-order inserts whose L0 pages are not in offset order: after the L0->L1
// compactions, the level-1 records of a series must not overlap in time.
static int t_compaction_time_contiguous(void) {
  int bad = 0;
  rs_build_interleaved("R15");
  uint64_t ranges[64][2];
  size_t n = rs_record_ranges(1, ranges, 64);
  for (size_t i = 0; i < n; i++) {
    printf("R15: L1 record of ret.a: k[%" PRIu64 "..%" PRIu64 "]\n", (ranges[i][0] - RS_TS0) / RS_STEP,
           (ranges[i][1] - RS_TS0) / RS_STEP);
    if (i && ranges[i][0] <= ranges[i - 1][1]) {
      printf("R15: FAIL L1 records k[..%" PRIu64 "] and k[%" PRIu64 "..] of ret.a overlap in time\n",
             (ranges[i - 1][1] - RS_TS0) / RS_STEP, (ranges[i][0] - RS_TS0) / RS_STEP);
      bad++;
    }
  }
  if (!n) {
    printf("R15: FAIL no level-1 records\n");
    bad++;
  }
  rs_reset();
  return bad;
}

int retention_main(const char* which) {
  struct {
    const char* name;
    int (*fn)(void);
  } t[] = {{"R14", t_expiration_horizon_interleaved}, {"R15", t_compaction_time_contiguous}};
  int bad = 0;
  for (size_t i = 0; i < sizeof t / sizeof t[0]; i++) {
    if (strcmp(which, "all") && strcmp(which, t[i].name)) continue;
    int b = t[i].fn();
    printf("%s %s\n", b ? "FAIL" : "PASS", t[i].name);
    fflush(stdout);
    bad += b;
  }
  return bad;
}
