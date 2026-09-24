/*
 * Golden behaviour of the read paths, captured 2026-09-24 before the Tier-2
 * refactors of the metadata page walks, the query executor, the points
 * iterator failure paths and timeseries_get_timestamp_range.
 *
 * One deterministic database (float / int / bool / string series, several
 * tag sets, compacted and raw records, an ALP-streaming series, a deleted
 * measurement and a deleted field) is summarised as a signature string: the
 * measurement / field / tag listings, the metadata series-ID lookups, the
 * timestamp ranges, and a set of queries (full, ranged, limited, tag and
 * field filtered, every rollup aggregation) with a hash of every value.
 *
 * On the host build the same operations are also run under
 *   - every single allocation failure (1st, 2nd, ... until none is hit), and
 *   - a read failure injected at sub-offsets of every live page,
 * and the per-case outcome tokens are pinned by length + FNV-1a. Error
 * handling that changes what a caller sees under a fault changes the digest.
 */
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "timeseries.h"
#include "timeseries_id_list.h"
#include "timeseries_internal.h"
#include "timeseries_metadata.h"
#include "timeseries_page_cache_snapshot.h"
#include "timeseries_string_list.h"
#include "unity.h"

extern timeseries_db_t *timeseries_get_db_handle(void);
#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_allocation_probe(size_t, bool);
bool tsdb_audit_allocation_hit(void);
void tsdb_test_arm_read_fault(size_t start, size_t end);
void tsdb_test_disarm_read_fault(void);
#endif

/* ---- signature builder --------------------------------------------------- */
typedef struct {
  char *buf;
  size_t len, cap;
} golden_sig_t;

static void sig_printf(golden_sig_t *s, const char *fmt, ...) {
  char tmp[512];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
  va_end(ap);
  if (n < 0) return;
  size_t add = (size_t)n < sizeof(tmp) ? (size_t)n : sizeof(tmp) - 1;
  if (s->len + add + 1 > s->cap) {
    size_t cap = s->cap ? s->cap * 2 : 4096;
    while (cap < s->len + add + 1) cap *= 2;
    char *grown = realloc(s->buf, cap);
    TEST_ASSERT_NOT_NULL(grown);
    s->buf = grown;
    s->cap = cap;
  }
  memcpy(s->buf + s->len, tmp, add);
  s->len += add;
  s->buf[s->len] = '\0';
}

static uint64_t fnv_bytes(uint64_t h, const void *p, size_t n) {
  const unsigned char *b = p;
  for (size_t i = 0; i < n; i++) {
    h ^= b[i];
    h *= 1099511628211ULL;
  }
  return h;
}
#define FNV_INIT 1469598103934665603ULL

/* Grow once up front so a sweep's own bookkeeping never allocates while an
 * allocation failure is armed. */
static void sig_reserve(golden_sig_t *s, size_t cap) {
  s->buf = malloc(cap);
  TEST_ASSERT_NOT_NULL(s->buf);
  s->buf[0] = '\0';
  s->len = 0;
  s->cap = cap;
}

/* ---- database ------------------------------------------------------------ */
static void insert_series(const char *m, const char *tag_value, const char *field, timeseries_field_type_e type,
                          size_t count, uint64_t t0, uint64_t step, int salt) {
  uint64_t *ts = calloc(count, sizeof(*ts));
  timeseries_field_value_t *v = calloc(count, sizeof(*v));
  char (*strs)[24] = calloc(count, sizeof(*strs));
  TEST_ASSERT_TRUE(ts && v && strs);
  for (size_t i = 0; i < count; i++) {
    ts[i] = t0 + i * step;
    v[i].type = type;
    switch (type) {
      case TIMESERIES_FIELD_TYPE_FLOAT: v[i].data.float_val = (double)((int)i * 3 - 70 + salt) * 0.25; break;
      case TIMESERIES_FIELD_TYPE_INT: v[i].data.int_val = (int64_t)i * (int64_t)i - 50 + salt; break;
      case TIMESERIES_FIELD_TYPE_BOOL: v[i].data.bool_val = (i + (size_t)salt) % 3 == 0; break;
      case TIMESERIES_FIELD_TYPE_STRING:
        snprintf(strs[i], sizeof(strs[i]), "s%d-%.*s", (int)i + salt, (int)(i % 9), "abcdefghi");
        v[i].data.string_val.str = strs[i];
        v[i].data.string_val.length = strlen(strs[i]);
        break;
    }
  }
  const char *tag_key = "site";
  timeseries_insert_data_t in = {.measurement_name = m,
                                 .tag_keys = &tag_key,
                                 .tag_values = &tag_value,
                                 .num_tags = tag_value ? 1 : 0,
                                 .field_names = &field,
                                 .field_values = v,
                                 .num_fields = 1,
                                 .timestamps_ms = ts,
                                 .num_points = count};
  TEST_ASSERT_TRUE(timeseries_insert(&in));
  free(ts);
  free(v);
  free(strs);
}

static void build_golden_db(void) {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  /* Reopen the empty database so measurement IDs restart at 1 whatever ran
   * before: the on-flash layout (key lengths) is then the same every time. */
  timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_init());
  insert_series("g_float", "a", "f", TIMESERIES_FIELD_TYPE_FLOAT, 300, 1000, 10, 0);
  insert_series("g_float", "b", "f", TIMESERIES_FIELD_TYPE_FLOAT, 50, 2005, 20, 7);
  insert_series("g_float", "a", "g", TIMESERIES_FIELD_TYPE_INT, 40, 1500, 30, 3);
  insert_series("g_int", "a", "i", TIMESERIES_FIELD_TYPE_INT, 100, 500, 7, 0);
  insert_series("g_bool", "c", "b", TIMESERIES_FIELD_TYPE_BOOL, 60, 800, 11, 1);
  insert_series("g_str", "a", "s", TIMESERIES_FIELD_TYPE_STRING, 25, 900, 13, 2);
  insert_series("g_gone", "a", "x", TIMESERIES_FIELD_TYPE_INT, 10, 100, 1, 0);
  insert_series("g_int", "a", "dropme", TIMESERIES_FIELD_TYPE_INT, 5, 520, 1, 0);
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());
  /* Raw (level-0) records on top of the compacted ones */
  insert_series("g_float", "a", "f", TIMESERIES_FIELD_TYPE_FLOAT, 20, 10000, 5, 11);
  insert_series("g_int", "b", "i", TIMESERIES_FIELD_TYPE_INT, 10, 600, 3, 5);
  insert_series("g_str", "a", "s", TIMESERIES_FIELD_TYPE_STRING, 5, 5000, 1, 40);
  insert_series("g_bool", "c", "b", TIMESERIES_FIELD_TYPE_BOOL, 7, 700, 2, 2);
  TEST_ASSERT_TRUE(timeseries_delete_measurement("g_gone"));
  TEST_ASSERT_TRUE(timeseries_delete_measurement_and_field("g_int", "dropme"));
}

/* ---- operations ------------------------------------------------------------ */
static void op_query(golden_sig_t *s, const char *m, uint64_t start, uint64_t end, size_t limit,
                     timeseries_aggregation_method_e agg, uint32_t rollup, const char *tag_value, const char *field) {
  const char *tag_key = "site";
  timeseries_query_t q = {.measurement_name = m,
                          .tag_keys = tag_value ? &tag_key : NULL,
                          .tag_values = tag_value ? &tag_value : NULL,
                          .num_tags = tag_value ? 1 : 0,
                          .field_names = field ? &field : NULL,
                          .num_fields = field ? 1 : 0,
                          .start_ms = start,
                          .end_ms = end,
                          .limit = limit,
                          .rollup_interval = rollup,
                          .aggregate_method = agg};
  timeseries_query_result_t r = {0};
  bool ok = timeseries_query(&q, &r);
  sig_printf(s, "Q %s %" PRIu64 "-%" PRIu64 " l%zu a%d r%" PRIu32 " t%s f%s: ok=%d np=%zu nc=%zu", m, start, end,
             limit, (int)agg, rollup, tag_value ? tag_value : "-", field ? field : "-", ok, r.num_points,
             r.num_columns);
  if (ok) {
    uint64_t h = fnv_bytes(FNV_INIT, r.timestamps, r.num_points * sizeof(uint64_t));
    sig_printf(s, " ts=%016" PRIx64, h);
    for (size_t c = 0; c < r.num_columns; c++) {
      uint64_t vh = FNV_INIT;
      for (size_t i = 0; i < r.num_points; i++) {
        const timeseries_field_value_t *v = &r.columns[c].values[i];
        vh = fnv_bytes(vh, &v->type, sizeof(v->type));
        switch (v->type) {
          case TIMESERIES_FIELD_TYPE_FLOAT: vh = fnv_bytes(vh, &v->data.float_val, 8); break;
          case TIMESERIES_FIELD_TYPE_INT: vh = fnv_bytes(vh, &v->data.int_val, 8); break;
          case TIMESERIES_FIELD_TYPE_BOOL: vh = fnv_bytes(vh, &v->data.bool_val, 1); break;
          case TIMESERIES_FIELD_TYPE_STRING:
            if (v->data.string_val.str) vh = fnv_bytes(vh, v->data.string_val.str, v->data.string_val.length);
            break;
        }
      }
      sig_printf(s, " [%s:%d %016" PRIx64 "]", r.columns[c].name ? r.columns[c].name : "?", (int)r.columns[c].type,
                 vh);
    }
  }
  sig_printf(s, "\n");
  timeseries_query_free_result(&r);
}

static void op_measurements(golden_sig_t *s) {
  char **names = NULL;
  size_t n = 0;
  bool ok = timeseries_get_measurements(&names, &n);
  sig_printf(s, "M ok=%d n=%zu:", ok, n);
  for (size_t i = 0; i < n; i++) {
    sig_printf(s, " %s", names[i]);
    free(names[i]);
  }
  free(names);
  sig_printf(s, "\n");
}

static void op_fields(golden_sig_t *s, const char *m) {
  char **names = NULL;
  size_t n = 0;
  bool ok = timeseries_get_fields_for_measurement(m, &names, &n);
  sig_printf(s, "F %s ok=%d n=%zu:", m, ok, n);
  for (size_t i = 0; i < n; i++) {
    sig_printf(s, " %s", names[i]);
    free(names[i]);
  }
  free(names);
  sig_printf(s, "\n");
}

static void op_tags(golden_sig_t *s, const char *m) {
  tsdb_tag_pair_t *pairs = NULL;
  size_t n = 0;
  bool ok = timeseries_get_tags_for_measurement(m, &pairs, &n);
  sig_printf(s, "T %s ok=%d n=%zu:", m, ok, n);
  for (size_t i = 0; i < n; i++) {
    sig_printf(s, " %s=%s", pairs[i].key, pairs[i].val);
    free(pairs[i].key);
    free(pairs[i].val);
  }
  free(pairs);
  sig_printf(s, "\n");
}

static void op_range(golden_sig_t *s, const char *m) {
  uint64_t lo = 1, hi = 2;
  uint32_t count = 3;
  bool ok = timeseries_get_timestamp_range(m, &lo, &hi, &count);
  sig_printf(s, "R %s ok=%d %" PRIu64 "-%" PRIu64 " n=%" PRIu32 "\n", m, ok, lo, hi, count);
}

static void sig_id_list(golden_sig_t *s, const char *what, bool ok, timeseries_series_id_list_t *l) {
  uint64_t h = FNV_INIT;
  for (size_t i = 0; i < l->count; i++) h = fnv_bytes(h, l->ids[i].bytes, 16);
  sig_printf(s, "%s ok=%d n=%zu %016" PRIx64 "\n", what, ok, l->count, h);
  tsdb_series_id_list_free(l);
}

static void op_meta_walks(golden_sig_t *s, const char *m) {
  timeseries_db_t *db = timeseries_get_db_handle();
  uint32_t id = 0;
  bool have = tsdb_find_measurement_id(db, m, &id);
  /* The numeric ID depends on what earlier tests created; only its presence is pinned. */
  sig_printf(s, "W %s id-ok=%d\n", m, have);
  timeseries_series_id_list_t l;
  tsdb_series_id_list_init(&l);
  sig_id_list(s, "Wtag-a", tsdb_find_series_ids_for_tag(db, id, "site", "a", &l), &l);
  tsdb_series_id_list_init(&l);
  sig_id_list(s, "Wtag-zz", tsdb_find_series_ids_for_tag(db, id, "site", "zz", &l), &l);
  static const char *const fields[] = {"f", "g", "i", "b", "s", "dropme", "nope"};
  for (size_t f = 0; f < sizeof(fields) / sizeof(fields[0]); f++) {
    tsdb_series_id_list_init(&l);
    char what[32];
    snprintf(what, sizeof(what), "Wfield-%s", fields[f]);
    sig_id_list(s, what, tsdb_find_series_ids_for_field(db, id, fields[f], &l), &l);
  }
  tsdb_series_id_list_init(&l);
  sig_id_list(s, "Wall", tsdb_find_all_series_ids_for_measurement(db, id, &l), &l);
  timeseries_string_list_t names;
  tsdb_string_list_init(&names);
  bool ok = tsdb_list_fields_for_measurement(db, id, &names);
  sig_printf(s, "Wfields ok=%d n=%zu:", ok, names.count);
  for (size_t i = 0; i < names.count; i++) sig_printf(s, " %s", names.items[i]);
  sig_printf(s, "\n");
  tsdb_string_list_free(&names);
  tsdb_string_list_init(&names);
  ok = tsdb_list_all_measurements(db, &names);
  sig_printf(s, "Wmeas ok=%d n=%zu:", ok, names.count);
  for (size_t i = 0; i < names.count; i++) sig_printf(s, " %s", names.items[i]);
  sig_printf(s, "\n");
  tsdb_string_list_free(&names);
  tsdb_tag_pair_t *pairs = NULL;
  size_t n = 0;
  ok = timeseries_metadata_get_tags_for_measurement(db, id, &pairs, &n);
  sig_printf(s, "Wtags ok=%d n=%zu:", ok, n);
  for (size_t i = 0; i < n; i++) {
    sig_printf(s, " %s=%s", pairs[i].key, pairs[i].val);
    free(pairs[i].key);
    free(pairs[i].val);
  }
  free(pairs);
  sig_printf(s, "\n");
}

typedef void (*golden_op_t)(golden_sig_t *s);
static void op_q_float_full(golden_sig_t *s) { op_query(s, "g_float", 0, 0, 0, TSDB_AGGREGATION_NONE, 0, NULL, NULL); }
static void op_q_float_range(golden_sig_t *s) {
  op_query(s, "g_float", 1500, 2600, 0, TSDB_AGGREGATION_NONE, 0, "a", "f");
}
static void op_q_float_rollup(golden_sig_t *s) { op_query(s, "g_float", 0, 0, 0, TSDB_AGGREGATION_AVG, 500, NULL, "f"); }
static void op_q_int(golden_sig_t *s) { op_query(s, "g_int", 520, 900, 30, TSDB_AGGREGATION_NONE, 0, NULL, NULL); }
static void op_q_int_rollup(golden_sig_t *s) { op_query(s, "g_int", 0, 0, 0, TSDB_AGGREGATION_SUM, 100, NULL, NULL); }
static void op_q_bool(golden_sig_t *s) { op_query(s, "g_bool", 0, 0, 0, TSDB_AGGREGATION_COUNT, 50, NULL, NULL); }
static void op_q_str(golden_sig_t *s) { op_query(s, "g_str", 0, 0, 0, TSDB_AGGREGATION_NONE, 0, "a", NULL); }
static void op_ranges(golden_sig_t *s) {
  op_range(s, "g_float");
  op_range(s, "g_int");
  op_range(s, "g_str");
  op_range(s, "g_gone");
  op_range(s, "missing");
}
static void op_listings(golden_sig_t *s) {
  op_measurements(s);
  op_fields(s, "g_float");
  op_fields(s, "g_int");
  op_tags(s, "g_float");
  op_tags(s, "g_int");
}
static void op_walks(golden_sig_t *s) {
  op_meta_walks(s, "g_float");
  op_meta_walks(s, "g_int");
}

static const golden_op_t golden_ops[] = {op_listings,      op_walks,        op_ranges, op_q_float_full,
                                         op_q_float_range, op_q_float_rollup, op_q_int, op_q_int_rollup,
                                         op_q_bool,        op_q_str};
#define GOLDEN_OP_COUNT (sizeof(golden_ops) / sizeof(golden_ops[0]))

static void full_signature(golden_sig_t *s) {
  for (size_t i = 0; i < GOLDEN_OP_COUNT; i++) golden_ops[i](s);
}

static void expect_digest(const char *what, const golden_sig_t *s, const char *want) {
  char got[64];
  snprintf(got, sizeof(got), "#%zu:%016" PRIx64, s->len, fnv_bytes(FNV_INIT, s->buf, s->len));
  if (strcmp(got, want) != 0) {
    printf("golden %s digest %s (want %s)\n%s\n", what, got, want, s->buf ? s->buf : "");
    TEST_FAIL_MESSAGE("golden digest differs");
  }
}

static const char GOLDEN_SIGNATURE[] =
    "M ok=1 n=4: g_float g_int g_bool g_str\n"
    "F g_float ok=1 n=2: f g\n"
    "F g_int ok=1 n=1: i\n"
    "T g_float ok=1 n=2: site=b site=a\n"
    "T g_int ok=1 n=2: site=b site=a\n"
    "W g_float id-ok=1\n"
    "Wtag-a ok=1 n=2 937c244f6c7b7f34\n"
    "Wtag-zz ok=0 n=0 14650fb0739d0383\n"
    "Wfield-f ok=1 n=2 cba896b355ff9e6f\n"
    "Wfield-g ok=1 n=1 561184af16c9697f\n"
    "Wfield-i ok=0 n=0 14650fb0739d0383\n"
    "Wfield-b ok=0 n=0 14650fb0739d0383\n"
    "Wfield-s ok=0 n=0 14650fb0739d0383\n"
    "Wfield-dropme ok=0 n=0 14650fb0739d0383\n"
    "Wfield-nope ok=0 n=0 14650fb0739d0383\n"
    "Wall ok=1 n=3 1b822e4946f4fb5b\n"
    "Wfields ok=1 n=2: f g\n"
    "Wmeas ok=1 n=4: g_float g_int g_bool g_str\n"
    "Wtags ok=1 n=2: site=b site=a\n"
    "W g_int id-ok=1\n"
    "Wtag-a ok=1 n=1 5139e5c4d4cfe3dc\n"
    "Wtag-zz ok=0 n=0 14650fb0739d0383\n"
    "Wfield-f ok=0 n=0 14650fb0739d0383\n"
    "Wfield-g ok=0 n=0 14650fb0739d0383\n"
    "Wfield-i ok=1 n=2 2427d959ec13147e\n"
    "Wfield-b ok=0 n=0 14650fb0739d0383\n"
    "Wfield-s ok=0 n=0 14650fb0739d0383\n"
    "Wfield-dropme ok=0 n=0 14650fb0739d0383\n"
    "Wfield-nope ok=0 n=0 14650fb0739d0383\n"
    "Wall ok=1 n=2 2427d959ec13147e\n"
    "Wfields ok=1 n=1: i\n"
    "Wmeas ok=1 n=4: g_float g_int g_bool g_str\n"
    "Wtags ok=1 n=2: site=b site=a\n"
    "R g_float ok=1 1000-10095 n=410\n"
    "R g_int ok=1 500-1193 n=110\n"
    "R g_str ok=1 900-5004 n=30\n"
    "R g_gone ok=0 0-0 n=0\n"
    "R missing ok=0 0-0 n=0\n"
    "Q g_float 0-0 l0 a0 r0 t- f-: ok=1 np=370 nc=2 ts=a27313f7311397e3 [f:0 0d98025248206f94] [g:1 2e51c324819d3561]\n"
    "Q g_float 1500-2600 l0 a0 r0 ta ff: ok=1 np=111 nc=1 ts=5e420cbdd69a3a4d [f:0 5fe75586021fd937]\n"
    "Q g_float 0-0 l0 a3 r500 t- ff: ok=1 np=7 nc=1 ts=9b85739ab006f1f4 [f:0 54e4de119250c792]\n"
    "Q g_int 520-900 l30 a0 r0 t- f-: ok=1 np=30 nc=1 ts=25cf34ef9da6740a [i:1 5798ec86bd87ecd7]\n"
    "Q g_int 0-0 l0 a5 r100 t- f-: ok=1 np=7 nc=1 ts=853a2236957728ed [i:1 49c3840971212dbf]\n"
    "Q g_bool 0-0 l0 a6 r50 t- f-: ok=1 np=14 nc=1 ts=99d7a599b9f2c5db [b:2 96811766a851ad20]\n"
    "Q g_str 0-0 l0 a0 r0 ta f-: ok=1 np=30 nc=1 ts=f6d9a42366fd955e [s:3 ddb33e1db4371f09]\n";

TEST_CASE("golden: read paths over a mixed database", "[golden_readpaths]") {
  build_golden_db();
  golden_sig_t s = {0};
  full_signature(&s);
  if (strcmp(s.buf, GOLDEN_SIGNATURE) != 0) {
    printf("golden signature:\n%s\n", s.buf);
    TEST_FAIL_MESSAGE("read-path signature differs");
  }
  free(s.buf);
  timeseries_deinit();
}

#if CONFIG_IDF_TARGET_LINUX
TEST_CASE("golden: read paths under every single allocation failure", "[golden_readpaths][golden_oom]") {
  build_golden_db();
  golden_sig_t base = {0};
  full_signature(&base);
  golden_sig_t sweep = {0};
  for (size_t op = 0; op < GOLDEN_OP_COUNT; op++) {
    for (size_t nth = 1; nth < 100000; nth++) {
      golden_sig_t one;
      sig_reserve(&one, 64 * 1024);
      tsdb_audit_allocation_probe(nth, false);
      golden_ops[op](&one);
      bool hit = tsdb_audit_allocation_hit();
      tsdb_audit_allocation_probe(0, false);
      sig_printf(&sweep, "%zu/%zu %016" PRIx64 "\n", op, nth, fnv_bytes(FNV_INIT, one.buf, one.len));
      free(one.buf);
      if (!hit) break;
    }
  }
  /* The faults leave the database intact */
  golden_sig_t after = {0};
  full_signature(&after);
  TEST_ASSERT_EQUAL_STRING(base.buf, after.buf);
  expect_digest("oom", &sweep, "#11471:ab15f895a91fcd01");
  free(base.buf);
  free(after.buf);
  free(sweep.buf);
  timeseries_deinit();
}

TEST_CASE("golden: read paths with a read failure inside each live page", "[golden_readpaths][golden_readfault]") {
  build_golden_db();
  timeseries_db_t *db = timeseries_get_db_handle();
  golden_sig_t base = {0};
  full_signature(&base);
  tsdb_page_cache_snapshot_t *snap = tsdb_snapshot_acquire_current(db);
  TEST_ASSERT_NOT_NULL(snap);
  size_t pages = snap->count;
  uint32_t *offsets = calloc(pages, sizeof(*offsets));
  uint32_t *sizes = calloc(pages, sizeof(*sizes));
  for (size_t p = 0; p < pages; p++) {
    offsets[p] = snap->entries[p].offset;
    sizes[p] = snap->entries[p].header.page_size;
  }
  tsdb_snapshot_release(snap);
  golden_sig_t sweep = {0};
  static const uint32_t subs[] = {0, 40, 64, 200, 1000, 2048, 4095, 4100, 8000};
  for (size_t p = 0; p < pages; p++) {
    for (size_t k = 0; k < sizeof(subs) / sizeof(subs[0]); k++) {
      if (subs[k] >= sizes[p]) continue;
      golden_sig_t one = {0};
      tsdb_test_arm_read_fault(offsets[p] + subs[k], offsets[p] + subs[k] + 1);
      full_signature(&one);
      tsdb_test_disarm_read_fault();
      sig_printf(&sweep, "page%zu+%" PRIu32 " %016" PRIx64 "\n", p, subs[k], fnv_bytes(FNV_INIT, one.buf, one.len));
      free(one.buf);
    }
  }
  free(offsets);
  free(sizes);
  golden_sig_t after = {0};
  full_signature(&after);
  TEST_ASSERT_EQUAL_STRING(base.buf, after.buf);
  expect_digest("read-fault", &sweep, "#920:55ce9d644f4c6944");
  free(base.buf);
  free(after.buf);
  free(sweep.buf);
  timeseries_deinit();
}
#endif
