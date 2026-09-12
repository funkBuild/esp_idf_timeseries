#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_compaction.h"
#include "timeseries_iterator.h"
#include "esp_timer.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

extern timeseries_db_t *timeseries_get_db_handle(void);
#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_allocation_probe(size_t, bool);
bool tsdb_audit_allocation_hit(void);
size_t tsdb_audit_allocation_count(void);
size_t tsdb_audit_largest_allocation(void);
#endif

static timeseries_db_t *prepare(void) {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  atomic_store(&db->compaction_in_progress, true);
  return db;
}
static void cleanup(timeseries_db_t *db) {
  atomic_store(&db->compaction_in_progress, false);
  timeseries_deinit();
}
static bool query_matches(const char *field, const uint64_t *ts, const timeseries_field_value_t *v,
                          size_t n, uint64_t first, uint64_t last) {
  timeseries_query_t q = {.measurement_name = "seventh", .field_names = &field, .num_fields = 1,
                          .start_ms = first, .end_ms = last};
  timeseries_query_result_t r = {0};
  bool ok = timeseries_query(&q, &r) && r.num_points == n && r.num_columns == 1;
  for (size_t i = 0; ok && i < n; i++) {
    const timeseries_field_value_t *got = &r.columns[0].values[i];
    ok = r.timestamps[i] == ts[i] && got->type == v[i].type;
    if (!ok) break;
    switch (v[i].type) {
      case TIMESERIES_FIELD_TYPE_INT: ok = got->data.int_val == v[i].data.int_val; break;
      case TIMESERIES_FIELD_TYPE_FLOAT: ok = got->data.float_val == v[i].data.float_val; break;
      case TIMESERIES_FIELD_TYPE_BOOL: ok = got->data.bool_val == v[i].data.bool_val; break;
      case TIMESERIES_FIELD_TYPE_STRING:
        ok = got->data.string_val.length == v[i].data.string_val.length &&
             (!v[i].data.string_val.length ||
              memcmp(got->data.string_val.str, v[i].data.string_val.str, v[i].data.string_val.length) == 0);
        break;
    }
  }
  timeseries_query_free_result(&r);
  return ok;
}
static void mixed_batch(bool narrow, bool small_chunks) {
  timeseries_db_t *db = prepare();
  size_t old_chunk = db->chunk_size;
  if (small_chunks) db->chunk_size = 2;
  uint64_t ts[] = {9, 1, 5, 1, 3, 9}, saved_ts[6];
  memcpy(saved_ts, ts, sizeof(ts));
  const char *fields[] = {"int", "float", "bool", "string"};
  char *strings[] = {"old", "a", "b\0c", "", "new", "last"};
  size_t lengths[] = {3, 1, 3, 0, 3, 4};
  timeseries_field_value_t values[24] = {0}, saved_values[24];
  for (size_t i = 0; i < 6; i++) {
    values[i].type = TIMESERIES_FIELD_TYPE_INT; values[i].data.int_val = 100 + i;
    values[6+i].type = TIMESERIES_FIELD_TYPE_FLOAT; values[6+i].data.float_val = i + .25;
    values[12+i].type = TIMESERIES_FIELD_TYPE_BOOL; values[12+i].data.bool_val = i % 2;
    values[18+i].type = TIMESERIES_FIELD_TYPE_STRING;
    values[18+i].data.string_val.str = strings[i]; values[18+i].data.string_val.length = lengths[i];
  }
  memcpy(saved_values, values, sizeof(values));
  timeseries_insert_data_t insert = {.measurement_name = "seventh", .field_names = fields,
      .field_values = values, .num_fields = 4, .timestamps_ms = ts, .num_points = 6};
  bool inserted = timeseries_insert(&insert);
  db->chunk_size = old_chunk;
  uint64_t expected_ts[] = {1, 3, 5, 9};
  size_t selected[] = {3, 4, 2, 5};
  bool before = inserted, after = inserted;
  for (int pass = 0; pass < 2; pass++) {
    if (pass) after &= timeseries_compact_all_levels_force(db);
    for (size_t f = 0; f < 4; f++) {
      timeseries_field_value_t expected[4];
      for (size_t i = 0; i < 4; i++) expected[i] = values[f*6+selected[i]];
      bool matched = query_matches(fields[f], expected_ts, expected, narrow ? 3 : 4, 1, narrow ? 5 : 9);
      if (pass) after &= matched; else before &= matched;
    }
  }
  bool unchanged = !memcmp(ts, saved_ts, sizeof(ts)) && !memcmp(values, saved_values, sizeof(values));
  cleanup(db);
  TEST_ASSERT_TRUE(inserted);
  TEST_ASSERT_TRUE_MESSAGE(before, "unordered batch query before compaction");
  TEST_ASSERT_TRUE_MESSAGE(after, "unordered batch query after compaction");
  TEST_ASSERT_TRUE(unchanged);
}
TEST_CASE("seventh: unordered batch preserves all field types", "[seventh][seventh_regression]") {
  mixed_batch(false, false);
}
TEST_CASE("seventh: unordered batch supports narrow ranges", "[seventh][seventh_regression]") {
  mixed_batch(true, false);
}
TEST_CASE("seventh: normalize before splitting into small records", "[seventh][seventh_regression]") {
  mixed_batch(false, true);
}
TEST_CASE("seventh: repeated timestamp stores only the final supplied value", "[seventh][seventh_regression]") {
  timeseries_db_t *db = prepare();
  const char *field = "v";
  uint64_t ts[] = {1, 1, 1};
  timeseries_field_value_t values[3] = {0};
  for (int i = 0; i < 3; i++) { values[i].type = TIMESERIES_FIELD_TYPE_INT; values[i].data.int_val = i; }
  timeseries_insert_data_t insert = {.measurement_name = "seventh", .field_names = &field,
      .field_values = values, .num_fields = 1, .timestamps_ms = ts, .num_points = 3};
  bool inserted = timeseries_insert(&insert);
  bool before = query_matches(field, ts, values+2, 1, 1, 1);
  bool compacted = timeseries_compact_all_levels_force(db);
  bool after = query_matches(field, ts, values+2, 1, 1, 1);
  cleanup(db);
  TEST_ASSERT_TRUE(inserted);
  TEST_ASSERT_TRUE(before);
  TEST_ASSERT_TRUE(compacted);
  TEST_ASSERT_TRUE(after);
}
TEST_CASE("seventh: reversed multi-page insert retains value pairing", "[seventh][seventh_regression]") {
  timeseries_db_t *db = prepare();
  const size_t n = 1200;
  uint64_t *ts = malloc(n * sizeof(*ts));
  timeseries_field_value_t *v = calloc(n, sizeof(*v));
  TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(v);
  for (size_t i = 0; i < n; i++) { ts[i] = n-i; v[i].type = TIMESERIES_FIELD_TYPE_INT; v[i].data.int_val = (n-i)*7; }
  const char *field = "v";
  timeseries_insert_data_t insert = {.measurement_name = "seventh", .field_names = &field,
      .field_values = v, .num_fields = 1, .timestamps_ms = ts, .num_points = n};
  bool inserted = timeseries_insert(&insert), unchanged = true;
  for (size_t i = 0; i < n; i++) unchanged &= ts[i] == n-i && v[i].data.int_val == (n-i)*7;
  for (size_t i = 0; i < n; i++) { ts[i] = i+1; v[i].data.int_val = (i+1)*7; }
  bool before = query_matches(field, ts+400, v+400, 400, 401, 800);
  bool compacted = timeseries_compact_all_levels_force(db);
  bool after = query_matches(field, ts+400, v+400, 400, 401, 800);
  free(ts); free(v); cleanup(db);
  TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE(unchanged);
  TEST_ASSERT_TRUE(before); TEST_ASSERT_TRUE(compacted); TEST_ASSERT_TRUE(after);
}
TEST_CASE("seventh: random batches match independent latest-value model", "[seventh][seventh_regression]") {
  timeseries_db_t *db = prepare();
  uint32_t state = 12345;
  uint64_t ts[31], expected_ts[11];
  timeseries_field_value_t values[31] = {0}, expected[11] = {0};
  bool seen[11] = {0}, ok = true;
  const char *field = "v";
  timeseries_insert_data_t insert = {.measurement_name = "seventh", .field_names = &field,
      .field_values = values, .num_fields = 1, .timestamps_ms = ts, .num_points = 31};
  for (int trial = 0; trial < 64 && ok; trial++) {
    for (size_t i = 0; i < 31; i++) {
      state = state * 1664525u + 1013904223u;
      size_t key = state % 11;
      ts[i] = key+1; values[i].type = TIMESERIES_FIELD_TYPE_INT; values[i].data.int_val = trial*31+i;
      seen[key] = true; expected[key] = values[i];
    }
    ok = timeseries_insert(&insert);
    if (trial % 8 == 7) ok &= timeseries_compact_all_levels_force(db);
    timeseries_field_value_t selected[11]; size_t count = 0;
    for (size_t i = 0; i < 11; i++) if (seen[i]) { expected_ts[count] = i+1; selected[count++] = expected[i]; }
    ok &= query_matches(field, expected_ts, selected, count, 1, 11);
  }
  cleanup(db);
  TEST_ASSERT_TRUE(ok);
}
#if CONFIG_IDF_TARGET_LINUX
TEST_CASE("seventh: ordering allocation failure leaves the database unchanged", "[seventh][seventh_alloc]") {
  timeseries_db_t *db = prepare();
  const char *field = "v";
  uint64_t ts[] = {2, 1};
  timeseries_field_value_t v[2] = {{.type = TIMESERIES_FIELD_TYPE_INT}, {.type = TIMESERIES_FIELD_TYPE_INT}};
  timeseries_insert_data_t insert = {.measurement_name = "seventh", .field_names = &field,
      .field_values = v, .num_fields = 1, .timestamps_ms = ts, .num_points = 1};
  TEST_ASSERT_TRUE(timeseries_insert(&insert));
  insert.num_points = 2;
  tsdb_audit_allocation_probe(1, true);
  bool inserted = timeseries_insert(&insert), hit = tsdb_audit_allocation_hit();
  tsdb_audit_allocation_probe(0, false);
  bool unchanged = query_matches(field, ts, v, 1, 1, 2);
  cleanup(db);
  TEST_ASSERT_TRUE(hit); TEST_ASSERT_FALSE(inserted); TEST_ASSERT_TRUE(unchanged);
}
TEST_CASE("seventh: sorted batches allocate no ordering storage and fields share one index", "[seventh][seventh_alloc]") {
  timeseries_db_t *db = prepare();
  const char *fields[] = {"a", "b", "c", "d"};
  uint64_t ts[] = {1, 2, 3, 4};
  timeseries_field_value_t v[16] = {0};
  for (int i = 0; i < 16; i++) v[i].type = TIMESERIES_FIELD_TYPE_INT;
  timeseries_insert_data_t insert = {.measurement_name = "seventh", .field_names = fields,
      .field_values = v, .num_fields = 4, .timestamps_ms = ts, .num_points = 4};
  TEST_ASSERT_TRUE(timeseries_insert(&insert));
  tsdb_audit_allocation_probe(0, true);
  bool sorted = timeseries_insert(&insert); size_t sorted_allocs = tsdb_audit_allocation_count();
  tsdb_audit_allocation_probe(0, false);
  ts[0] = 4; ts[1] = 3; ts[2] = 2; ts[3] = 1;
  tsdb_audit_allocation_probe(0, true);
  bool reversed = timeseries_insert(&insert); size_t reversed_allocs = tsdb_audit_allocation_count();
  tsdb_audit_allocation_probe(0, false);
  cleanup(db);
  printf("seventh allocations: sorted=%zu reversed=%zu\n", sorted_allocs, reversed_allocs);
  TEST_ASSERT_TRUE(sorted); TEST_ASSERT_TRUE(reversed);
  TEST_ASSERT_EQUAL(0, sorted_allocs); TEST_ASSERT_EQUAL(1, reversed_allocs);
}
TEST_CASE("seventh: ordered duplicate runs allocate one index per unique timestamp", "[seventh][seventh_alloc]") {
  timeseries_db_t *db = prepare();
  uint64_t ts[64];
  timeseries_field_value_t values[64] = {0};
  for (size_t i = 0; i < 64; i++) {
    ts[i] = 1; values[i].type = TIMESERIES_FIELD_TYPE_INT; values[i].data.int_val = i;
  }
  const char *field = "v";
  timeseries_insert_data_t insert = {.measurement_name = "seventh", .field_names = &field,
      .field_values = values, .num_fields = 1, .timestamps_ms = ts, .num_points = 1};
  TEST_ASSERT_TRUE(timeseries_insert(&insert));
  insert.num_points = 64;
  tsdb_audit_allocation_probe(0, true);
  bool inserted = timeseries_insert(&insert);
  size_t largest = tsdb_audit_largest_allocation(), allocations = tsdb_audit_allocation_count();
  tsdb_audit_allocation_probe(0, false);
  bool matched = query_matches(field, ts, values+63, 1, 1, 1);
  cleanup(db);
  TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE(matched);
  TEST_ASSERT_EQUAL(1, allocations); TEST_ASSERT_EQUAL(sizeof(size_t), largest);
}

static size_t stored_points(timeseries_db_t *db) {
  timeseries_page_cache_iterator_t it;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &it));
  timeseries_page_header_t h; uint32_t offset, size; size_t count = 0;
  while (timeseries_page_cache_iterator_next(&it, &h, &offset, &size)) {
    if (h.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA) continue;
    timeseries_fielddata_iterator_t fields;
    TEST_ASSERT_TRUE(timeseries_fielddata_iterator_init(db, offset, size, &fields));
    timeseries_field_data_header_t f;
    while (timeseries_fielddata_iterator_next(&fields, &f))
      if (f.flags & TSDB_FIELDDATA_FLAG_DELETED) count += f.record_count;
    timeseries_fielddata_iterator_deinit(&fields);
  }
  timeseries_page_cache_iterator_deinit(&it);
  return count;
}
static void benchmark_batch(bool duplicates) {
  timeseries_db_t *db = prepare();
  const size_t n = 512, unique = duplicates ? 32 : n;
  uint64_t *ts = malloc(n * sizeof(*ts)), *expected_ts = malloc(unique * sizeof(*expected_ts));
  timeseries_field_value_t *v = calloc(n, sizeof(*v)), *expected = calloc(unique, sizeof(*expected));
  TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(v); TEST_ASSERT_NOT_NULL(expected_ts); TEST_ASSERT_NOT_NULL(expected);
  const char *field = "v";
  ts[0] = 999999; v[0].type = TIMESERIES_FIELD_TYPE_INT;
  timeseries_insert_data_t insert = {.measurement_name = "seventh", .field_names = &field,
      .field_values = v, .num_fields = 1, .timestamps_ms = ts, .num_points = 1};
  TEST_ASSERT_TRUE(timeseries_insert(&insert));
  size_t old_chunk = db->chunk_size; db->chunk_size = 1;
  for (size_t i = 0; i < n; i++) {
    size_t key = duplicates ? i/16 : i;
    ts[i] = key+1; v[i].type = TIMESERIES_FIELD_TYPE_INT; v[i].data.int_val = i;
    expected_ts[key] = key+1; expected[key] = v[i];
  }
  insert.num_points = n;
  tsdb_audit_allocation_probe(0, true);
  int64_t start = esp_timer_get_time();
  bool inserted = timeseries_insert(&insert);
  int64_t insert_us = esp_timer_get_time() - start;
  size_t largest = tsdb_audit_largest_allocation();
  tsdb_audit_allocation_probe(0, false);
  db->chunk_size = old_chunk;
  bool matched = true;
  start = esp_timer_get_time();
  for (int repeat = 0; repeat < 21; repeat++)
    matched &= query_matches(field, expected_ts, expected, unique, 1, unique);
  int64_t query_us = esp_timer_get_time() - start;
  size_t stored = stored_points(db);
  free(ts); free(v); free(expected_ts); free(expected); cleanup(db);
  printf("seventh benchmark: duplicates=%d insert_us=%lld query21_us=%lld largest=%zu stored=%zu\n",
         duplicates, (long long)insert_us, (long long)query_us, largest, stored);
  TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE(matched);
  TEST_ASSERT_EQUAL(unique+1, stored);
}
TEST_CASE("seventh: duplicate batch insert and query benchmark", "[seventh][seventh_bench]") { benchmark_batch(true); }
TEST_CASE("seventh: unique batch insert and query control", "[seventh][seventh_bench]") { benchmark_batch(false); }
#endif
