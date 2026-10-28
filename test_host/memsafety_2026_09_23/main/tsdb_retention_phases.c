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
#include "esp_timer.h"
#include "timeseries_compaction.h"
#include "timeseries_data.h"
#include "timeseries_expiration.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
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

static float rs_usage(void) {
  timeseries_db_t* db = review_db();
  return (float)tsdb_pagecache_get_total_active_size(db) / (float)db->partition->size;
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
  // Expire a tenth of the used space; the threshold is just under the usage,
  // so the run stops once that is freed.
  float used = rs_usage();
  bool ok = timeseries_expiration_run(db, used - 0.001f, used / 10.0f);
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

/* Oldest surviving timestamp of ret.a (UINT64_MAX when none). */
static uint64_t rs_oldest(void) {
  const char* fn[1] = {"a"};
  timeseries_query_t q = {.measurement_name = "ret", .field_names = fn, .num_fields = 1, .limit = 1};
  timeseries_query_result_t r;
  memset(&r, 0, sizeof r);
  uint64_t t = UINT64_MAX;
  if (timeseries_query(&q, &r) && r.num_points) t = r.timestamps[0];
  timeseries_query_free_result(&r);
  return t;
}

/* Expire about a tenth of the used space in one run. An insert may have
 * started a background compaction; the run needs the maintenance slot, so
 * wait for that first (a busy slot makes the run return false, retryably). */
static bool rs_expire_tenth(void) {
  for (int i = 0; i < 3000 && atomic_load(&review_db()->compaction_in_progress); i++) vTaskDelay(pdMS_TO_TICKS(10));
  float used = rs_usage();
  return timeseries_expiration_run(review_db(), used - 0.001f, used / 10.0f);
}

/* ---- R16: back-filled points older than the horizon ---------------------- */
// Points inserted after a run with timestamps before its horizon are stored
// (the horizon is not a write barrier) and are the first to go next run.
static int t_backfill_before_horizon(void) {
  int bad = 0;
  rs_build_interleaved("R16");
  bool e1 = rs_expire_tenth();
  size_t removed = 0;
  bad += rs_check("R16 run 1", &removed);
  uint64_t horizon = rs_oldest();
  // Back-fill 50 points per series well before the horizon.
  uint64_t back = horizon - 1000 * RS_STEP;
  bool put = rs_put(back, 50);
  size_t n_before = 0;
  {
    const char* fn[1] = {"a"};
    timeseries_query_t q = {.measurement_name = "ret", .field_names = fn, .num_fields = 1, .end_ms = (int64_t)horizon - 1};
    timeseries_query_result_t r;
    memset(&r, 0, sizeof r);
    if (timeseries_query(&q, &r)) n_before = r.num_points;
    timeseries_query_free_result(&r);
  }
  printf("R16: run 1 -> %d, horizon k=%" PRIu64 "; back-filled 50 points at k=%" PRIu64 " -> %d, stored before the horizon=%zu (want 50)\n",
         e1, (horizon - RS_TS0) / RS_STEP, (back - RS_TS0) / RS_STEP, put, n_before);
  if (!e1 || !put || n_before != 50) {
    printf("R16: FAIL back-filled points were not stored\n");
    bad++;
  }
  bool e2 = rs_expire_tenth();
  bad += rs_check("R16 run 2", &removed);  // back-fill included: it must be gone with everything older
  uint64_t horizon2 = rs_oldest();
  printf("R16: run 2 -> %d, horizon k=%" PRIu64 "\n", e2, (horizon2 - RS_TS0) / RS_STEP);
  if (!e2 || horizon2 <= back) {
    printf("R16: FAIL the next run did not expire the back-filled points\n");
    bad++;
  }
  rs_reset();
  return bad;
}

/* ---- R17: restart between runs --------------------------------------------- */
// The horizon is not stored: after a restart the next run recomputes it from
// the records and the guarantee still holds.
static int t_restart_between_runs(void) {
  int bad = 0;
  rs_build_interleaved("R17");
  bool e1 = rs_expire_tenth();
  size_t removed = 0;
  bad += rs_check("R17 run 1", &removed);
  uint64_t h1 = rs_oldest();
  timeseries_deinit();
  if (!timeseries_init()) {
    printf("R17: FAIL re-init\n");
    return bad + 1;
  }
  uint64_t h1_after = rs_oldest();
  bool e2 = rs_expire_tenth();
  bad += rs_check("R17 run 2", &removed);
  uint64_t h2 = rs_oldest();
  printf("R17: run 1 -> %d horizon k=%" PRIu64 "; after restart oldest k=%" PRIu64 "; run 2 -> %d horizon k=%" PRIu64 "\n",
         e1, (h1 - RS_TS0) / RS_STEP, (h1_after - RS_TS0) / RS_STEP, e2, (h2 - RS_TS0) / RS_STEP);
  if (!e1 || !e2 || h1_after != h1 || h2 <= h1) {
    printf("R17: FAIL restart changed the data or the next run did not advance the horizon\n");
    bad++;
  }
  rs_reset();
  return bad;
}

/* ---- R18: a trim that fails leaves every record intact ------------------- */
extern bool review_arm_fail_after_finalize(bool arm);
static size_t rs_points(void) {
  const char* fn[1] = {"a"};
  timeseries_query_t q = {.measurement_name = "ret", .field_names = fn, .num_fields = 1};
  timeseries_query_result_t r;
  memset(&r, 0, sizeof r);
  size_t n = 0;
  if (timeseries_query(&q, &r)) n = r.num_points;
  timeseries_query_free_result(&r);
  return n;
}
static size_t rs_placeholders(void) {
  timeseries_page_cache_iterator_t it;
  size_t n = 0;
  if (!timeseries_page_cache_iterator_init(review_db(), &it)) return 0;
  timeseries_page_header_t h;
  uint32_t o, s;
  while (timeseries_page_cache_iterator_next(&it, &h, &o, &s))
    if (h.page_type == 0xFF) n++;
  timeseries_page_cache_iterator_deinit(&it);
  return n;
}
static int t_trim_failure(void) {
  int bad = 0;
  rs_build_interleaved("R18");
  size_t before = rs_points();
  uint32_t used_before = tsdb_pagecache_get_total_active_size(review_db());
  // The trim writes its page, then the allocation that publishes it fails.
  review_arm_fail_after_finalize(true);
  bool e1 = rs_expire_tenth();
  bool fired = !review_arm_fail_after_finalize(false);
  size_t after = rs_points();
  uint32_t used_after = tsdb_pagecache_get_total_active_size(review_db());
  size_t leaked = rs_placeholders();
  printf("R18: run with a failing trim publication -> %d (want 0), injected=%d; points %zu -> %zu; used %" PRIu32
         " -> %" PRIu32 " B; reserved regions left=%zu\n", e1, fired, before, after, used_before, used_after, leaked);
  if (!fired) {
    printf("R18: FAIL the run trimmed nothing (no straddling record)\n");
    bad++;
  } else if (e1 || after != before || used_after != used_before || leaked) {
    printf("R18: FAIL a failed trim changed the data or leaked its output\n");
    bad++;
  }
  timeseries_deinit();  // restart: the abandoned output must not come back
  if (!timeseries_init()) return bad + 1;
  size_t restarted = rs_points();
  bool e2 = rs_expire_tenth();
  size_t removed = 0;
  bad += rs_check("R18 retry", &removed);
  printf("R18: after restart points=%zu (want %zu); retry -> %d removed %zu\n", restarted, before, e2, removed);
  if (restarted != before || !e2 || !removed) {
    printf("R18: FAIL the abandoned trim output survived a restart, or the retry failed\n");
    bad++;
  }
  rs_reset();
  return bad;
}

/* ---- Heartbeat load (B1, R19, R20) -------------------------------------- */
// A heartbeat-like load: inserts of 12 fields x 1 point, expiration every 100
// inserts (free 5% of the partition once usage passes `threshold`) after a
// synchronous compaction, background compaction as in the product.
extern _Atomic uint64_t review_write_bytes, review_erase_bytes;
extern atomic_uint review_erase_calls;
extern atomic_uint review_sector_erases[];
extern const unsigned review_max_sectors;
extern unsigned review_write_calls(void);

typedef struct {
  unsigned failed, exp_calls;
  uint64_t exp_us, exp_max;
  uint32_t min_free_run;  // smallest largest-free-run seen once the partition has filled
} hb_result_t;

// The largest gap between live pages.
static uint32_t largest_free_run(void) {
  timeseries_db_t* db = review_db();
  tsdb_page_cache_snapshot_t* sn = tsdb_snapshot_acquire_current(db);
  uint32_t at = 0, big = 0;
  for (size_t k = 0; sn && k <= sn->count; k++) {
    uint32_t next = k < sn->count ? sn->entries[k].offset : db->partition->size;
    if (next > at && next - at > big) big = next - at;
    if (k < sn->count) at = next + sn->entries[k].header.page_size;
  }
  tsdb_snapshot_release(sn);
  return big;
}

// One heartbeat: 12 float fields at `ts`, values from the xorshift state *s.
static bool heartbeat(uint64_t ts, uint32_t* s) {
  static const char* names[12] = {"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11"};
  timeseries_field_value_t fv[12];
  for (int f = 0; f < 12; f++) {
    *s ^= *s << 13;
    *s ^= *s >> 17;
    *s ^= *s << 5;
    fv[f].type = TIMESERIES_FIELD_TYPE_FLOAT;
    fv[f].data.float_val = (double)(*s % 100000) / 7.0;
  }
  timeseries_insert_data_t d = {.measurement_name = "bench", .field_names = names, .field_values = fv,
                                .num_fields = 12, .timestamps_ms = &ts, .num_points = 1};
  return timeseries_insert(&d);
}

static hb_result_t heartbeat_load(unsigned n, float threshold) {
  hb_result_t r = {.min_free_run = UINT32_MAX};
  uint64_t ts = RS_TS0;
  uint32_t s = 12345;
  for (unsigned i = 0; i < n; i++) {
    ts += 1000;
    if (!heartbeat(ts, &s)) r.failed++;
    if (getenv("B1_PROGRESS") && i % 10000 == 9999) {
      printf("progress i=%u failed=%u used=%" PRIu32 " KiB usable_free=%" PRIu32 " KiB\n", i + 1, r.failed,
             tsdb_pagecache_get_total_active_size(review_db()) / 1024, tsdb_usable_free(review_db()) / 1024);
      fflush(stdout);
    }
    if (i % 100 == 99) {
      // Settle compaction first: expiration needs the maintenance slot and
      // returns "busy" (doing nothing) while a compaction holds it.
      timeseries_compact_sync();
      int64_t t0 = esp_timer_get_time();
      timeseries_expiration_run(review_db(), threshold, 0.05f);
      uint64_t dt = (uint64_t)(esp_timer_get_time() - t0);
      r.exp_us += dt;
      r.exp_calls++;
      if (dt > r.exp_max) r.exp_max = dt;
    }
    // Contiguous space only matters once usage reaches the threshold.
    if (i % 1000 == 999 && i >= n / 4) {
      uint32_t run = largest_free_run();
      if (run < r.min_free_run) r.min_free_run = run;
    }
  }
  timeseries_compact_sync();
  return r;
}

// Erase counts per sector since the last reset: hottest / mean.
static double wear_report(const char* tag) {
  const esp_partition_t* part = review_db()->partition;
  unsigned n = part->size / 4096 < review_max_sectors ? part->size / 4096 : review_max_sectors, mx = 0, never = 0;
  uint64_t sum = 0;
  for (unsigned i = 0; i < n; i++) {
    unsigned v = atomic_load(&review_sector_erases[i]);
    sum += v;
    if (v > mx) mx = v;
    if (!v) never++;
  }
  double mean = (double)sum / n, ratio = sum ? mx / mean : 0;
  printf("%s: erases per 4 KiB sector: mean %.1f, hottest %u (%.2fx mean), never erased %u of %u\n", tag, mean, mx,
         ratio, never, n);
  return ratio;
}

static void wear_reset(void) {
  for (unsigned i = 0; i < review_db()->partition->size / 4096 && i < review_max_sectors; i++)
    atomic_store(&review_sector_erases[i], 0);
}

/* ---- B1: flash wear and expiration latency (benchmark, not in "all") ---- */
// 8000 heartbeat inserts at a 35% usage threshold (the product's 5% at a
// threshold this load reaches). Reports flash writes and erases per 1000
// inserts and the time spent in each expiration call.
static int t_bench_wear_latency(void) {
  timeseries_clear_all();
  wear_reset();
  unsigned w0 = review_write_calls(), e0 = atomic_load(&review_erase_calls);
  uint64_t wb0 = atomic_load(&review_write_bytes), eb0 = atomic_load(&review_erase_bytes);
  const char* env = getenv("B1_N");
  const unsigned N = env ? (unsigned)atoi(env) : 8000;
  // B1_THRESH=0.999: no application expiration, as in the product.
  const char* thresh = getenv("B1_THRESH");
  hb_result_t r = heartbeat_load(N, thresh ? (float)atof(thresh) : 0.35f);
  unsigned failed = r.failed;
  unsigned writes = review_write_calls() - w0, erases = atomic_load(&review_erase_calls) - e0;
  uint64_t wbytes = atomic_load(&review_write_bytes) - wb0, ebytes = atomic_load(&review_erase_bytes) - eb0;
  tsdb_usage_summary_t us;
  timeseries_get_usage_summary(&us);
  printf("B1: %u inserts (%u failed); per 1000 successful inserts: %.1f writes, %.1f KiB written, %.1f erases, %.1f KiB erased; "
         "expire: %u calls, mean %.2f ms, max %.2f ms; final usage %" PRIu32 " KiB\n",
         N, failed, writes * 1000.0 / (N - failed), wbytes / 1024.0 * 1000.0 / (N - failed),
         erases * 1000.0 / (N - failed), ebytes / 1024.0 * 1000.0 / (N - failed), r.exp_calls,
         r.exp_us / 1000.0 / r.exp_calls, r.exp_max / 1000.0, us.used_space_bytes / 1024);
  wear_report("B1");
  return failed ? 1 : 0;
}

/* ---- R19: erases spread over the partition ------------------------------- */
// First-fit from offset 0 put most erases on the first few hundred KiB: the
// hottest sector took ~7.6x the mean on this load, and the flash wears out at
// its hottest sector. The wear-leveling allocator (timeseries_data.h) keeps it
// near 1.5x.
#define R19_INSERTS 100000
#define R19_MAX_RATIO 2.0
static int t_wear_spread(void) {
  timeseries_clear_all();
  wear_reset();
  hb_result_t r = heartbeat_load(R19_INSERTS, 0.35f);
  double ratio = wear_report("R19");
  printf("R19: %u inserts, %u failed; hottest sector %.2fx the mean (limit %.1f)\n", R19_INSERTS, r.failed, ratio,
         R19_MAX_RATIO);
  if (r.failed || ratio > R19_MAX_RATIO) {
    printf("R19: FAIL erases concentrate on part of the partition\n");
    return 1;
  }
  return 0;
}

/* ---- R20: compaction never needs one large contiguous region -------------- */
// At 70% usage free space is fragmented, more so with a rotating allocation
// origin. A compaction that reserved its whole source size in one region
// (120 KiB here) then found none and every insert failed; outputs now
// continue in pages of at most TSDB_STREAM_PAGE_MAX_SIZE.
#define R20_INSERTS 100000
static int t_fill_seventy(void) {
  timeseries_clear_all();
  hb_result_t r = heartbeat_load(R20_INSERTS, 0.70f);
  printf("R20: %u inserts at a 70%% usage threshold, %u failed; smallest largest-free-run %" PRIu32 " KiB\n",
         R20_INSERTS, r.failed, r.min_free_run / 1024);
  if (r.failed) {
    printf("R20: FAIL inserts failed for lack of contiguous space\n");
    return 1;
  }
  return 0;
}

/* ---- R21: a full partition keeps recording -------------------------------- */
// The product never expires data itself. Compaction and expiration write
// before they free, so a full, fragmented partition wedged: an expiration
// could not place its trims and so deleted nothing, a compaction could not
// place its outputs, and every insert failed. Inserts now leave free space
// for maintenance (timeseries_data.h), the background task clears the oldest
// data before space runs out, and an insert that still finds none drops the
// oldest page, which allocates nothing. 250k inserts span several turnovers
// of the partition (the wedge appeared from about 184k inserts).
#define R21_INSERTS 250000
static uint64_t newest_points(uint64_t from_ts) {
  const char* fn[1] = {"f0"};
  timeseries_query_t q = {.measurement_name = "bench", .field_names = fn, .num_fields = 1,
                          .start_ms = (int64_t)from_ts};
  timeseries_query_result_t r;
  memset(&r, 0, sizeof r);
  if (!timeseries_query(&q, &r)) return 0;
  uint64_t n = r.num_points;
  timeseries_query_free_result(&r);
  return n;
}
static int t_full_keeps_recording(void) {
  timeseries_clear_all();
  wear_reset();
  hb_result_t r = heartbeat_load(R21_INSERTS, 0.999f);
  uint64_t last = RS_TS0 + (uint64_t)R21_INSERTS * 1000;
  uint64_t recent = newest_points(last - 999 * 1000);
  uint32_t used = tsdb_pagecache_get_total_active_size(review_db());
  double ratio = wear_report("R21");
  printf("R21: %u inserts without expiration, %u failed; newest 1000 points stored=%" PRIu64 " (want 1000); used "
         "%" PRIu32 " KiB of %" PRIu32 " KiB; hottest sector %.2fx the mean\n",
         R21_INSERTS, r.failed, recent, used / 1024, review_db()->partition->size / 1024, ratio);
  if (r.failed || recent != 1000) {
    printf("R21: FAIL a full partition stopped recording\n");
    return 1;
  }
  return 0;
}

/* ---- R22: "stop when full" recovers after expiration ------------------------ */
// With drop_oldest off, inserts fail once the partition is full. The
// reserves keep space for the application's expiration, which then makes
// inserts possible again: freeing 10% (about 200 KiB) makes room for some
// 1500 heartbeats of this load (about 150 bytes each once compacted).
#define R22_AFTER 500
static int t_full_stop_recovers(void) {
  timeseries_db_t* db = review_db();
  typeof(db->space) space = db->space;
  db->space.drop_oldest = false;
  db->space.clear_free = 0;
  db->space.max_usage_percent = 0;
  timeseries_clear_all();
  uint64_t ts = RS_TS0;
  uint32_t s = 12345;
  unsigned n = 0;
  bool filled = false;
  // Settle compaction every 100 inserts, as heartbeat_load does: the host
  // scheduler barely runs the background task while this task never blocks.
  for (; n < 400000 && !filled; n++) {
    ts += 1000;
    filled = !heartbeat(ts, &s);
    if (n % 100 == 99) timeseries_compact_sync();
  }
  for (int i = 0; i < 3000 && atomic_load(&db->compaction_in_progress); i++) vTaskDelay(pdMS_TO_TICKS(10));
  uint32_t used0 = tsdb_pagecache_get_total_active_size(db), usable0 = tsdb_usable_free(db);
  bool expired = timeseries_expiration_run(db, 0.01f, 0.10f);
  uint32_t used1 = tsdb_pagecache_get_total_active_size(db), usable1 = tsdb_usable_free(db);
  printf("R22: full: used %" PRIu32 " KiB, usable free %" PRIu32 " KiB; after expiration: used %" PRIu32
         " KiB, usable free %" PRIu32 " KiB\n", used0 / 1024, usable0 / 1024, used1 / 1024, usable1 / 1024);
  unsigned after = 0;
  for (; after < R22_AFTER; after++) {
    ts += 1000;
    if (!heartbeat(ts, &s)) break;
    if (after % 100 == 99) timeseries_compact_sync();
  }
  db->space = space;
  printf("R22: first failed insert after %u inserts (filled=%d); expiration -> %d; then %u of %d inserts stored\n",
         n, filled, expired, after, R22_AFTER);
  if (!filled || !expired || after != R22_AFTER) {
    printf("R22: FAIL a full partition could not expire its way back to recording\n");
    return 1;
  }
  return 0;
}

/* ---- R23: dropping a page never brings back an overwritten value ---------- */
// Points 0..999 are compacted into one L1 record; point 500 is then
// overwritten (a newer revision, in an L0 page whose newest point is older
// than the L1 page's). Dropping that L0 page alone would make the old value
// of point 500 visible again, so the overlapping L1 record goes with it.
static int t_drop_keeps_newest_revision(void) {
  timeseries_db_t* db = review_db();
  timeseries_clear_all();
  const char* fn[1] = {"v"};
  static uint64_t ts[1000];
  static timeseries_field_value_t v[1000];
  for (int k = 0; k < 1000; k++) {
    ts[k] = RS_TS0 + (uint64_t)k * 1000;
    v[k] = (timeseries_field_value_t){.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 1.0};
  }
  timeseries_insert_data_t d = {.measurement_name = "rv", .field_names = fn, .field_values = v, .num_fields = 1,
                                .timestamps_ms = ts, .num_points = 1000};
  bool ok = timeseries_insert(&d) && timeseries_compact_force_sync();
  timeseries_field_value_t newer = {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 2.0};
  timeseries_insert_data_t o = {.measurement_name = "rv", .field_names = fn, .field_values = &newer,
                                .num_fields = 1, .timestamps_ms = &ts[500], .num_points = 1};
  ok = ok && timeseries_insert(&o);
  for (int i = 0; i < 3000 && atomic_load(&db->compaction_in_progress); i++) vTaskDelay(pdMS_TO_TICKS(10));
  bool expected = false, dropped = false;
  bool slot = atomic_compare_exchange_strong(&db->compaction_in_progress, &expected, true);
  bool drop_ok = slot && tsdb_drop_oldest_page_held(db, &dropped);
  if (slot) atomic_store(&db->compaction_in_progress, false);
  timeseries_query_t q = {.measurement_name = "rv", .field_names = fn, .num_fields = 1};
  timeseries_query_result_t r;
  memset(&r, 0, sizeof r);
  bool q_ok = timeseries_query(&q, &r);
  int stale = 0;
  for (size_t p = 0; q_ok && p < r.num_points; p++)
    if (r.timestamps[p] == ts[500] && r.columns[0].values[p].data.float_val == 1.0) stale++;
  size_t left = q_ok ? r.num_points : 0;
  timeseries_query_free_result(&r);
  printf("R23: setup -> %d; drop -> %d (dropped %d); points left %zu; overwritten value visible again %d (want 0)\n",
         ok, drop_ok, dropped, left, stale);
  if (!ok || !drop_ok || !dropped || !q_ok || stale || left >= 1000) {
    printf("R23: FAIL dropping a page exposed an overwritten value (or dropped nothing)\n");
    return 1;
  }
  return 0;
}

/* ---- R24: a full partition holds data, not deleted records ---------------- */
// 1000 random doubles x 4 fields per insert (they barely compress), as the
// QEMU test full_drop: the partition must hold many inserts' worth of live
// data, with deleted records reclaimed rather than accumulating.
static void space_report(const char* tag) {
  timeseries_db_t* db = review_db();
  uint32_t pages[6] = {0}, page_bytes[6] = {0}, live[6] = {0}, dead[6] = {0};
  timeseries_page_cache_iterator_t it;
  timeseries_page_header_t h;
  uint32_t off, size;
  timeseries_page_cache_iterator_init(db, &it);
  while (timeseries_page_cache_iterator_next(&it, &h, &off, &size)) {
    if (h.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA || h.page_state != TIMESERIES_PAGE_STATE_ACTIVE) continue;
    int l = h.field_data_level < 5 ? h.field_data_level : 5;
    pages[l]++;
    page_bytes[l] += size;
    timeseries_fielddata_iterator_t f;
    timeseries_field_data_header_t r;
    if (!timeseries_fielddata_iterator_init(db, off, size, &f)) continue;
    while (timeseries_fielddata_iterator_next(&f, &r)) {
      uint32_t b = sizeof(r) + r.record_length;
      if (r.flags & TSDB_FIELDDATA_FLAG_DELETED) live[l] += b; else dead[l] += b;
    }
    timeseries_fielddata_iterator_deinit(&f);
  }
  timeseries_page_cache_iterator_deinit(&it);
  for (int l = 0; l < 6; l++)
    if (pages[l])
      printf("%s: L%d %3" PRIu32 " pages %5" PRIu32 " KiB: live %5" PRIu32 " KiB, deleted %5" PRIu32 " KiB\n", tag, l,
             pages[l], page_bytes[l] / 1024, live[l] / 1024, dead[l] / 1024);
}
#define R24_POINTS 1000
static int t_full_random_batches(void) {
  static const char* fields[4] = {"a", "b", "c", "d"};
  static uint64_t ts[R24_POINTS];
  static timeseries_field_value_t v[4 * R24_POINTS];
  timeseries_clear_all();
  uint32_t s = 12345, failed = 0;
  const int inserts = (int)(review_db()->partition->size / 12000);
  uint64_t t = 1000000;
  for (int i = 0; i < inserts; i++, t += R24_POINTS * 1000) {
    for (size_t k = 0; k < R24_POINTS; k++) ts[k] = t + k * 1000;
    for (size_t k = 0; k < 4 * R24_POINTS; k++) {
      s ^= s << 13;
      s ^= s >> 17;
      s ^= s << 5;
      v[k].type = TIMESERIES_FIELD_TYPE_FLOAT;
      v[k].data.float_val = (double)s / 7.0;
    }
    timeseries_insert_data_t d = {.measurement_name = "full", .field_names = fields, .field_values = v,
                                  .num_fields = 4, .timestamps_ms = ts, .num_points = R24_POINTS};
    if (!timeseries_insert(&d)) failed++;
  }
  for (int i = 0; i < 6000 && atomic_load(&review_db()->compaction_in_progress); i++) vTaskDelay(pdMS_TO_TICKS(10));
  const char* fn[1] = {"a"};
  timeseries_query_t q = {.measurement_name = "full", .field_names = fn, .num_fields = 1};
  timeseries_query_result_t r;
  memset(&r, 0, sizeof r);
  size_t stored = timeseries_query(&q, &r) ? r.num_points : 0;
  uint64_t last = t - R24_POINTS * 1000;
  size_t newest = 0;
  for (size_t p = 0; p < stored; p++) newest += r.timestamps[p] >= last;
  timeseries_query_free_result(&r);
  space_report("R24");
  printf("R24: %d inserts, %" PRIu32 " failed; field a holds %zu points (%zu of the newest insert's %d)\n", inserts,
         failed, stored, newest, R24_POINTS);
  // The partition holds roughly 50 inserts of this data.
  if (failed || newest != R24_POINTS || stored < 20 * R24_POINTS) {
    printf("R24: FAIL a full partition lost recent data\n");
    return 1;
  }
  return 0;
}

int retention_main(const char* which) {
  struct {
    const char* name;
    int (*fn)(void);
  } t[] = {{"R14", t_expiration_horizon_interleaved}, {"R15", t_compaction_time_contiguous},
           {"R16", t_backfill_before_horizon},        {"R17", t_restart_between_runs},
           {"R18", t_trim_failure},                   {"R19", t_wear_spread},
           {"R20", t_fill_seventy},                   {"R21", t_full_keeps_recording},
           {"R22", t_full_stop_recovers},             {"R23", t_drop_keeps_newest_revision},
           {"R24", t_full_random_batches},
           {"B1", t_bench_wear_latency}};
  int bad = 0;
  for (size_t i = 0; i < sizeof t / sizeof t[0]; i++) {
    if (strcmp(which, "all") ? strcmp(which, t[i].name) : t[i].name[0] == 'B') continue;  // benchmarks on request
    int b = t[i].fn();
    printf("%s %s\n", b ? "FAIL" : "PASS", t[i].name);
    fflush(stdout);
    bad += b;
  }
  return bad;
}
