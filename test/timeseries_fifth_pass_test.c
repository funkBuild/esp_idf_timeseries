#include "unity.h"
#include "timeseries_multi_series_iterator.h"
#include "timeseries_internal.h"
#include "esp_timer.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdio.h>

static timeseries_field_value_t aggregate_ints(const int64_t* values, size_t n,
                                               timeseries_aggregation_method_e method) {
  timeseries_points_iterator_t it = {0};
  it.valid = true;
  it.series_type = TIMESERIES_FIELD_TYPE_INT;
  it.ts_count = n;
  it.ts_array = malloc(n * sizeof(uint64_t));
  it.int_array = malloc(n * sizeof(int64_t));
  TEST_ASSERT_NOT_NULL(it.ts_array);
  TEST_ASSERT_NOT_NULL(it.int_array);
  for (size_t i = 0; i < n; i++) it.ts_array[i] = i + 1;
  memcpy(it.int_array, values, n * sizeof(int64_t));
  timeseries_points_iterator_t* ptr = &it;
  uint32_t priority = 1;
  timeseries_multi_points_iterator_t m;
  TEST_ASSERT_TRUE(timeseries_multi_points_iterator_init(&ptr, &priority, 1, &m));
  timeseries_multi_points_iterator_t* mptr = &m;
  timeseries_multi_series_iterator_t agg;
  TEST_ASSERT_TRUE(timeseries_multi_series_iterator_init(&mptr, 1, n + 1, method, &agg));
  timeseries_field_value_t value;
  TEST_ASSERT_TRUE(timeseries_multi_series_iterator_next(&agg, NULL, &value));
  TEST_ASSERT_FALSE(timeseries_multi_series_iterator_next(&agg, NULL, NULL));
  timeseries_multi_series_iterator_deinit(&agg);
  timeseries_multi_points_iterator_deinit(&m);
  timeseries_points_iterator_deinit(&it);
  return value;
}

static void check_int(const int64_t* values, size_t n, timeseries_aggregation_method_e method, int64_t expected) {
  timeseries_field_value_t v = aggregate_ints(values, n, method);
  TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, v.type);
  TEST_ASSERT_EQUAL_INT64(expected, v.data.int_val);
}

TEST_CASE("fifth: SUM promotes positive overflow to FLOAT", "[fifth][fifth_sum_positive]") {
  const int64_t values[] = {INT64_MAX, INT64_MAX};
  timeseries_field_value_t v = aggregate_ints(values, 2, TSDB_AGGREGATION_SUM);
  TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_FLOAT, v.type);
  TEST_ASSERT_DOUBLE_WITHIN(1.0, 0x1p64, v.data.float_val);
}
TEST_CASE("fifth: SUM promotes negative overflow to FLOAT", "[fifth][fifth_sum_negative]") {
  const int64_t values[] = {INT64_MIN, INT64_MIN};
  timeseries_field_value_t v = aggregate_ints(values, 2, TSDB_AGGREGATION_SUM);
  TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_FLOAT, v.type);
  TEST_ASSERT_DOUBLE_WITHIN(1.0, -0x1p64, v.data.float_val);
}
TEST_CASE("fifth: SUM recovers exact integers after intermediate overflow", "[fifth][fifth_cancellation]") {
  const int64_t pos[] = {INT64_MAX, INT64_MAX, INT64_MIN, -INT64_MAX};
  const int64_t neg[] = {INT64_MIN, INT64_MIN, INT64_MAX, INT64_MAX};
  check_int(pos, 4, TSDB_AGGREGATION_SUM, -1);
  check_int(neg, 4, TSDB_AGGREGATION_SUM, -2);
  check_int(neg, 4, TSDB_AGGREGATION_NONE, -2);
}
TEST_CASE("fifth: AVG at signed limits remains exact", "[fifth][fifth_avg_limits]") {
  const int64_t hi[] = {INT64_MAX, INT64_MAX, INT64_MAX};
  const int64_t lo[] = {INT64_MIN, INT64_MIN, INT64_MIN};
  check_int(hi, 3, TSDB_AGGREGATION_AVG, INT64_MAX);
  check_int(lo, 3, TSDB_AGGREGATION_AVG, INT64_MIN);
}
TEST_CASE("fifth: AVG does not double a signed sum", "[fifth][fifth_avg_finalize]") {
  const int64_t hi[] = {INT64_MAX};
  const int64_t lo[] = {INT64_MIN};
  check_int(hi, 1, TSDB_AGGREGATION_AVG, INT64_MAX);
  check_int(lo, 1, TSDB_AGGREGATION_AVG, INT64_MIN);
}
TEST_CASE("fifth: negative AVG rounds nearest and preserves exact values", "[fifth][fifth_avg_negative]") {
  const int64_t vals[] = {-10, -20, 5};
  const int64_t same[] = {-8, -8};
  check_int(vals, 3, TSDB_AGGREGATION_AVG, -8);
  check_int(same, 2, TSDB_AGGREGATION_AVG, -8);
}
TEST_CASE("fifth: AVG rounds half ties away from zero", "[fifth][fifth_avg_ties]") {
  const int64_t cases[][2] = {
      {1, 2}, {-1, -2}, {INT64_MAX, INT64_MIN}, {INT64_MAX, INT64_MAX - 1}, {INT64_MIN, INT64_MIN + 1}};
  const int64_t expected[] = {2, -2, -1, INT64_MAX, INT64_MIN};
  for (size_t i = 0; i < 5; i++) check_int(cases[i], 2, TSDB_AGGREGATION_AVG, expected[i]);
}

#ifdef CONFIG_IDF_TARGET_LINUX
/* Independent reference: native host 128-bit arithmetic, never used by production. */
TEST_CASE("fifth: integer rollups match a 128-bit reference", "[fifth][fifth_reference]") {
  uint64_t seed = 0x178459;
  int64_t values[31];
  for (size_t trial = 0; trial < 500; trial++) {
    size_t n = 1 + trial % 31;
    __int128 sum = 0;
    for (size_t i = 0; i < n; i++) {
      seed ^= seed << 13;
      seed ^= seed >> 7;
      seed ^= seed << 17;
      memcpy(&values[i], &seed, sizeof(seed));
      if (trial % 4 == 0) values[i] = INT64_MAX - (seed % 100);
      if (trial % 4 == 1) values[i] = INT64_MIN + (seed % 100);
      sum += values[i];
    }
    timeseries_field_value_t v = aggregate_ints(values, n, TSDB_AGGREGATION_SUM);
    if (sum >= INT64_MIN && sum <= INT64_MAX) {
      TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, v.type);
      TEST_ASSERT_EQUAL_INT64((int64_t)sum, v.data.int_val);
    } else {
      TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_FLOAT, v.type);
      TEST_ASSERT_DOUBLE_WITHIN(fabs((double)sum) * 3e-16, (double)sum, v.data.float_val);
    }
    __int128 magnitude = sum < 0 ? -sum : sum;
    __int128 avg = (magnitude + n / 2) / n;
    if (sum < 0) avg = -avg;
    check_int(values, n, TSDB_AGGREGATION_AVG, (int64_t)avg);
  }
}
#endif

TEST_CASE("fifth: public SUM and AVG preserve extremes before and after compaction", "[fifth][fifth_public]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  uint64_t timestamps[] = {1, 2, 101, 102, 201, 202};
  int64_t ints[] = {INT64_MAX, INT64_MAX, INT64_MIN, INT64_MIN, -1, -2};
  timeseries_field_value_t values[6];
  for (size_t i = 0; i < 6; i++) {
    values[i].type = TIMESERIES_FIELD_TYPE_INT;
    values[i].data.int_val = ints[i];
  }
  const char* fields[] = {"v"};
  timeseries_insert_data_t insert = {.measurement_name = "fifth",
                                     .field_names = fields,
                                     .field_values = values,
                                     .num_fields = 1,
                                     .timestamps_ms = timestamps,
                                     .num_points = 6};
  TEST_ASSERT_TRUE(timeseries_insert(&insert));
  for (int pass = 0; pass < 2; pass++) {
    if (pass) {
      TEST_ASSERT_TRUE(timeseries_compact_force_sync());
      tsdb_usage_summary_t usage;
      TEST_ASSERT_TRUE(timeseries_get_usage_summary(&usage));
      for (size_t i = 0; i < 4; i++) TEST_ASSERT_EQUAL(0, usage.page_summaries[i].num_pages);
      TEST_ASSERT_EQUAL(1, usage.page_summaries[4].num_pages);
    }
    timeseries_query_t q = {
        .measurement_name = "fifth", .rollup_interval = 50, .aggregate_method = TSDB_AGGREGATION_SUM};
    timeseries_query_result_t r = {0};
    TEST_ASSERT_TRUE(timeseries_query(&q, &r));
    TEST_ASSERT_EQUAL(3, r.num_points);
    TEST_ASSERT_EQUAL(1, r.num_columns);
    TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, r.columns[0].type);
    TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_FLOAT, r.columns[0].values[0].type);
    TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_FLOAT, r.columns[0].values[1].type);
    TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, r.columns[0].values[2].type);
    TEST_ASSERT_DOUBLE_WITHIN(1.0, 0x1p64, r.columns[0].values[0].data.float_val);
    TEST_ASSERT_DOUBLE_WITHIN(1.0, -0x1p64, r.columns[0].values[1].data.float_val);
    TEST_ASSERT_EQUAL_INT64(-3, r.columns[0].values[2].data.int_val);
    timeseries_query_free_result(&r);
    q.aggregate_method = TSDB_AGGREGATION_AVG;
    TEST_ASSERT_TRUE(timeseries_query(&q, &r));
    TEST_ASSERT_EQUAL(3, r.num_points);
    const int64_t expected[] = {INT64_MAX, INT64_MIN, -2};
    for (size_t i = 0; i < 3; i++) {
      TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, r.columns[0].values[i].type);
      TEST_ASSERT_EQUAL_INT64(expected[i], r.columns[0].values[i].data.int_val);
    }
    timeseries_query_free_result(&r);
  }
  timeseries_deinit();
}

TEST_CASE("fifth: wide AVG division works for power-of-two and other counts", "[fifth][fifth_division]") {
  const size_t counts[] = {2, 3, 4, 7, 8, 16, 31};
  int64_t values[31];
  for (size_t k = 0; k < sizeof(counts) / sizeof(counts[0]); k++) {
    size_t n = counts[k];
    for (size_t i = 0; i < n; i++) values[i] = INT64_MAX;
    values[n - 1] -= n;
    check_int(values, n, TSDB_AGGREGATION_AVG, INT64_MAX - 1);
    for (size_t i = 0; i < n; i++) values[i] = INT64_MIN;
    values[n - 1] += n;
    check_int(values, n, TSDB_AGGREGATION_AVG, INT64_MIN + 1);
  }
  const int64_t at_max[] = {INT64_MAX, 1, -1};
  const int64_t at_min[] = {INT64_MIN, -1, 1};
  check_int(at_max, 3, TSDB_AGGREGATION_SUM, INT64_MAX);
  check_int(at_min, 3, TSDB_AGGREGATION_SUM, INT64_MIN);
}

TEST_CASE("fifth: AVG throughput with regular windows", "[fifth][fifth_benchmark]") {
  const size_t n = 4096;
  const size_t widths[] = {1, 15, 16};
  for (size_t k = 0; k < 3; k++) {
    timeseries_points_iterator_t it = {0};
    it.valid = true;
    it.series_type = TIMESERIES_FIELD_TYPE_INT;
    it.ts_count = n;
    it.ts_array = malloc(n * sizeof(uint64_t));
    it.int_array = malloc(n * sizeof(int64_t));
    TEST_ASSERT_NOT_NULL(it.ts_array);
    TEST_ASSERT_NOT_NULL(it.int_array);
    for (size_t i = 0; i < n; i++) {
      it.ts_array[i] = i + 1;
      it.int_array[i] = i;
    }
    timeseries_points_iterator_t* ptr = &it;
    uint32_t priority = 1;
    timeseries_multi_points_iterator_t m;
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_init(&ptr, &priority, 1, &m));
    timeseries_multi_points_iterator_t* mptr = &m;
    timeseries_multi_series_iterator_t agg;
    TEST_ASSERT_TRUE(timeseries_multi_series_iterator_init(&mptr, 1, widths[k], TSDB_AGGREGATION_AVG, &agg));
    size_t count = 0;
    int64_t checksum = 0, timestamp_sum = 0;
    timeseries_field_value_t v;
    uint64_t ts;
    int64_t start = esp_timer_get_time();
    while (timeseries_multi_series_iterator_next(&agg, &ts, &v)) {
      count++;
      checksum += v.data.int_val;
      timestamp_sum += ts;
    }
    int64_t elapsed = esp_timer_get_time() - start;
    timeseries_multi_series_iterator_deinit(&agg);
    timeseries_multi_points_iterator_deinit(&m);
    timeseries_points_iterator_deinit(&it);
    int64_t expected = 0, expected_ts = 0;
    for (size_t i = 0; i < n; i += widths[k]) {
      size_t len = n - i < widths[k] ? n - i : widths[k];
      expected += i + len / 2;
      expected_ts += i + 1;
    }
    TEST_ASSERT_EQUAL((n + widths[k] - 1) / widths[k], count);
    TEST_ASSERT_EQUAL_INT64(expected, checksum);
    TEST_ASSERT_EQUAL_INT64(expected_ts, timestamp_sum);
    printf("FIFTH_AVG width=%u n=%u us=%lld\n", (unsigned)widths[k], (unsigned)n, (long long)elapsed);
  }
}

TEST_CASE("fifth: forced compaction bypasses the normal page threshold", "[fifth][fifth_force]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  uint64_t ts = 1;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_INT, .data.int_val = 42};
  const char* field = "v";
  timeseries_insert_data_t insert = {.measurement_name = "fifth_force",
                                     .field_names = &field,
                                     .field_values = &value,
                                     .num_fields = 1,
                                     .timestamps_ms = &ts,
                                     .num_points = 1};
  TEST_ASSERT_TRUE(timeseries_insert(&insert));
  TEST_ASSERT_TRUE(timeseries_compact_sync());
  tsdb_usage_summary_t before, after;
  TEST_ASSERT_TRUE(timeseries_get_usage_summary(&before));
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());
  TEST_ASSERT_TRUE(timeseries_get_usage_summary(&after));
  timeseries_deinit();
  TEST_ASSERT_EQUAL(1, before.page_summaries[0].num_pages);
  for (size_t i = 0; i < 4; i++) TEST_ASSERT_EQUAL(0, after.page_summaries[i].num_pages);
  TEST_ASSERT_EQUAL(1, after.page_summaries[4].num_pages);
}

TEST_CASE("fifth: forced compaction drains more than one page batch", "[fifth][fifth_force_batches]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  extern timeseries_db_t* timeseries_get_db_handle(void);
  timeseries_db_t* db = timeseries_get_db_handle();
  atomic_store(&db->compaction_in_progress, true);  // Defer automatic compaction while arranging 33 pages.
  uint64_t* ts = malloc(500 * sizeof(*ts));
  timeseries_field_value_t* values = calloc(500, sizeof(*values));
  TEST_ASSERT_NOT_NULL(ts);
  TEST_ASSERT_NOT_NULL(values);
  const char* field = "v";
  timeseries_insert_data_t insert = {.measurement_name = "fifth_batches",
                                     .field_names = &field,
                                     .field_values = values,
                                     .num_fields = 1,
                                     .timestamps_ms = ts,
                                     .num_points = 500};
  for (size_t page = 0; page < 33; page++) {
    for (size_t i = 0; i < 500; i++) {
      ts[i] = page * 500 + i + 1;
      values[i].type = TIMESERIES_FIELD_TYPE_INT;
      values[i].data.int_val = ts[i];
    }
    TEST_ASSERT_TRUE(timeseries_insert(&insert));
  }
  free(ts);
  free(values);
  atomic_store(&db->compaction_in_progress, false);
  tsdb_usage_summary_t before, after;
  TEST_ASSERT_TRUE(timeseries_get_usage_summary(&before));
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());
  TEST_ASSERT_TRUE(timeseries_get_usage_summary(&after));
  timeseries_query_t q = {.measurement_name = "fifth_batches", .start_ms = 16498, .end_ms = 16500};
  timeseries_query_result_t r = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  TEST_ASSERT_EQUAL(3, r.num_points);
  for (size_t i = 0; i < 3; i++) TEST_ASSERT_EQUAL_INT64(16498 + i, r.columns[0].values[i].data.int_val);
  timeseries_query_free_result(&r);
  timeseries_deinit();
  TEST_ASSERT_EQUAL(33, before.page_summaries[0].num_pages);
  for (size_t i = 0; i < 4; i++) TEST_ASSERT_EQUAL(0, after.page_summaries[i].num_pages);
  TEST_ASSERT_GREATER_THAN(0, after.page_summaries[4].num_pages);
}
