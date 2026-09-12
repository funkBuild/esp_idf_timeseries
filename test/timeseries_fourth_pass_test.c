#include "unity.h"
#include "timeseries_multi_series_iterator.h"
#include "esp_timer.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

static void make_points(timeseries_points_iterator_t* it, const uint64_t* ts, const int64_t* values, size_t n) {
  memset(it, 0, sizeof(*it));
  it->valid = true;
  it->ts_count = n;
  it->series_type = TIMESERIES_FIELD_TYPE_INT;
  it->ts_array = malloc(n * sizeof(*ts));
  it->int_array = malloc(n * sizeof(*values));
  TEST_ASSERT_NOT_NULL(it->ts_array);
  TEST_ASSERT_NOT_NULL(it->int_array);
  memcpy(it->ts_array, ts, n * sizeof(*ts));
  memcpy(it->int_array, values, n * sizeof(*values));
}
static void make_multi(timeseries_points_iterator_t* it, timeseries_multi_points_iterator_t* m) {
  timeseries_points_iterator_t* ptr = it;
  uint32_t priority = 1;
  TEST_ASSERT_TRUE(timeseries_multi_points_iterator_init(&ptr, &priority, 1, m));
}

TEST_CASE("fourth: sparse rollup skips empty windows", "[fourth][fourth_sparse]") {
  uint64_t ts[] = {10, 1000011};
  int64_t values[] = {7, 9};
  timeseries_points_iterator_t it;
  make_points(&it, ts, values, 2);
  timeseries_multi_points_iterator_t m;
  make_multi(&it, &m);
  timeseries_multi_points_iterator_t* ptr = &m;
  timeseries_multi_series_iterator_t agg;
  TEST_ASSERT_TRUE(timeseries_multi_series_iterator_init(&ptr, 1, 3, TSDB_AGGREGATION_SUM, &agg));
  uint64_t t;
  timeseries_field_value_t value;
  int64_t start = esp_timer_get_time();
  TEST_ASSERT_TRUE(timeseries_multi_series_iterator_next(&agg, &t, &value));
  TEST_ASSERT_EQUAL_UINT64(10, t);
  TEST_ASSERT_EQUAL_INT64(7, value.data.int_val);
  TEST_ASSERT_TRUE(timeseries_multi_series_iterator_next(&agg, &t, &value));
  int64_t elapsed = esp_timer_get_time() - start;
  TEST_ASSERT_EQUAL_UINT64(1000009, t);
  TEST_ASSERT_EQUAL_INT64(9, value.data.int_val);
  TEST_ASSERT_FALSE(timeseries_multi_series_iterator_next(&agg, NULL, NULL));
  timeseries_multi_series_iterator_deinit(&agg);
  timeseries_multi_points_iterator_deinit(&m);
  timeseries_points_iterator_deinit(&it);
  printf("FOURTH_SPARSE gap=1000001 interval=3 us=%lld\n", (long long)elapsed);
  TEST_ASSERT_LESS_THAN(10000, elapsed);
}

static void max_timestamp_test(uint64_t start, uint64_t interval) {
  uint64_t ts[] = {start, UINT64_MAX};
  int64_t values[] = {7, 9};
  timeseries_points_iterator_t it;
  make_points(&it, ts, values, start == UINT64_MAX ? 1 : 2);
  timeseries_multi_points_iterator_t m;
  make_multi(&it, &m);
  timeseries_multi_points_iterator_t* ptrs[] = {NULL, &m};
  timeseries_multi_series_iterator_t agg;
  TEST_ASSERT_TRUE(timeseries_multi_series_iterator_init(ptrs, 2, interval, TSDB_AGGREGATION_SUM, &agg));
  uint64_t t = 0;
  timeseries_field_value_t value;
  bool got = timeseries_multi_series_iterator_next(&agg, &t, &value);
  bool done = !timeseries_multi_series_iterator_next(&agg, NULL, NULL);
  timeseries_multi_series_iterator_deinit(&agg);
  timeseries_multi_points_iterator_deinit(&m);
  timeseries_points_iterator_deinit(&it);
  TEST_ASSERT_TRUE(got);
  TEST_ASSERT_TRUE(done);
  TEST_ASSERT_EQUAL_UINT64(start, t);
  TEST_ASSERT_EQUAL_INT64(start == UINT64_MAX ? 7 : 16, value.data.int_val);
}
TEST_CASE("fourth: maximum timestamp is a valid rollup point", "[fourth][fourth_max]") {
  max_timestamp_test(UINT64_MAX, 10);
}
TEST_CASE("fourth: maximum timestamp is valid without rollup", "[fourth][fourth_max_raw]") {
  max_timestamp_test(UINT64_MAX, 0);
}
TEST_CASE("fourth: overflowing last window includes maximum timestamp", "[fourth][fourth_tail]") {
  max_timestamp_test(UINT64_MAX - 5, 10);
}

TEST_CASE("fourth: LAST selects the latest timestamp across series", "[fourth][fourth_last]") {
  uint64_t ts[2] = {9, 1};
  int64_t values[2] = {90, 10};
  timeseries_points_iterator_t it[2];
  timeseries_multi_points_iterator_t m[2];
  timeseries_multi_points_iterator_t* ptrs[] = {&m[0], &m[1]};
  for (int i = 0; i < 2; i++) {
    make_points(&it[i], &ts[i], &values[i], 1);
    make_multi(&it[i], &m[i]);
  }
  timeseries_multi_series_iterator_t agg;
  TEST_ASSERT_TRUE(timeseries_multi_series_iterator_init(ptrs, 2, 10, TSDB_AGGREGATION_LAST, &agg));
  uint64_t t;
  timeseries_field_value_t value;
  TEST_ASSERT_TRUE(timeseries_multi_series_iterator_next(&agg, &t, &value));
  timeseries_multi_series_iterator_deinit(&agg);
  for (int i = 0; i < 2; i++) {
    timeseries_multi_points_iterator_deinit(&m[i]);
    timeseries_points_iterator_deinit(&it[i]);
  }
  TEST_ASSERT_EQUAL_UINT64(1, t);
  TEST_ASSERT_EQUAL_INT64(90, value.data.int_val);
}

static void large_float_test(bool positive) {
  uint64_t ts[] = {1, 2};
  int64_t dummy[] = {0, 0};
  timeseries_points_iterator_t it;
  make_points(&it, ts, dummy, 2);
  it.series_type = TIMESERIES_FIELD_TYPE_FLOAT;
  it.float_array[0] = positive ? 1e100 : -1e100;
  it.float_array[1] = positive ? 2e100 : -2e100;
  timeseries_multi_points_iterator_t m;
  make_multi(&it, &m);
  timeseries_multi_points_iterator_t* ptr = &m;
  timeseries_multi_series_iterator_t agg;
  TEST_ASSERT_TRUE(
      timeseries_multi_series_iterator_init(&ptr, 1, 10, positive ? TSDB_AGGREGATION_MIN : TSDB_AGGREGATION_MAX, &agg));
  uint64_t t;
  timeseries_field_value_t value;
  TEST_ASSERT_TRUE(timeseries_multi_series_iterator_next(&agg, &t, &value));
  timeseries_multi_series_iterator_deinit(&agg);
  timeseries_multi_points_iterator_deinit(&m);
  timeseries_points_iterator_deinit(&it);
  TEST_ASSERT_DOUBLE_WITHIN(1e85, positive ? 1e100 : -1e100, value.data.float_val);
}
TEST_CASE("fourth: float MIN supports values above integer range", "[fourth][fourth_float_min]") {
  large_float_test(true);
}
TEST_CASE("fourth: float MAX supports values below integer range", "[fourth][fourth_float_max]") {
  large_float_test(false);
}

TEST_CASE("fourth: non-sum rollups do not overflow an unused sum", "[fourth][fourth_unused_sum]") {
  uint64_t ts[] = {1, 2};
  int64_t values[] = {INT64_MAX, INT64_MAX};
  timeseries_aggregation_method_e methods[] = {TSDB_AGGREGATION_MIN, TSDB_AGGREGATION_MAX, TSDB_AGGREGATION_LAST,
                                               TSDB_AGGREGATION_COUNT};
  for (size_t i = 0; i < sizeof(methods) / sizeof(methods[0]); i++) {
    timeseries_points_iterator_t it;
    make_points(&it, ts, values, 2);
    timeseries_multi_points_iterator_t m;
    make_multi(&it, &m);
    timeseries_multi_points_iterator_t* ptr = &m;
    timeseries_multi_series_iterator_t agg;
    TEST_ASSERT_TRUE(timeseries_multi_series_iterator_init(&ptr, 1, 10, methods[i], &agg));
    uint64_t t;
    timeseries_field_value_t value;
    TEST_ASSERT_TRUE(timeseries_multi_series_iterator_next(&agg, &t, &value));
    TEST_ASSERT_EQUAL_INT64(methods[i] == TSDB_AGGREGATION_COUNT ? 2 : INT64_MAX, value.data.int_val);
    timeseries_multi_series_iterator_deinit(&agg);
    timeseries_multi_points_iterator_deinit(&m);
    timeseries_points_iterator_deinit(&it);
  }
}

TEST_CASE("fourth: rollup grid handles enormous gaps and exact boundaries", "[fourth][fourth_grid]") {
  const uint64_t intervals[] = {1, 3, UINT64_MAX / 2, UINT64_MAX};
  const uint64_t timestamps[] = {0, 1, 3, UINT64_MAX / 2, UINT64_MAX - 1, UINT64_MAX};
  const int64_t values[] = {1, 2, 4, 8, 16, 32};
  for (size_t k = 0; k < sizeof(intervals) / sizeof(intervals[0]); k++) {
    timeseries_points_iterator_t it;
    make_points(&it, timestamps, values, 6);
    timeseries_multi_points_iterator_t m;
    make_multi(&it, &m);
    timeseries_multi_points_iterator_t* ptr = &m;
    timeseries_multi_series_iterator_t agg;
    TEST_ASSERT_TRUE(timeseries_multi_series_iterator_init(&ptr, 1, intervals[k], TSDB_AGGREGATION_SUM, &agg));
    size_t i = 0;
    while (i < 6) {
      uint64_t expected_ts = (timestamps[i] / intervals[k]) * intervals[k];
      int64_t expected_sum = 0;
      do {
        expected_sum += values[i++];
      } while (i < 6 && timestamps[i] - expected_ts < intervals[k]);
      uint64_t ts;
      timeseries_field_value_t value;
      TEST_ASSERT_TRUE(timeseries_multi_series_iterator_next(&agg, &ts, &value));
      TEST_ASSERT_EQUAL_UINT64(expected_ts, ts);
      TEST_ASSERT_EQUAL_INT64(expected_sum, value.data.int_val);
    }
    TEST_ASSERT_FALSE(timeseries_multi_series_iterator_next(&agg, NULL, NULL));
    timeseries_multi_series_iterator_deinit(&agg);
    timeseries_multi_points_iterator_deinit(&m);
    timeseries_points_iterator_deinit(&it);
  }
}

TEST_CASE("fourth: string LAST and COUNT release skipped and replaced values", "[fourth][fourth_strings]") {
  const uint64_t timestamps[] = {9, 1, 9, 8};
  const char* strings[] = {"first", "older", "tie winner", "older again"};
  for (int mode = 0; mode < 3; mode++) {
    timeseries_points_iterator_t it[4] = {0};
    timeseries_multi_points_iterator_t m[4];
    timeseries_multi_points_iterator_t* ptrs[4];
    for (int i = 0; i < 4; i++) {
      it[i].valid = true;
      it[i].series_type = TIMESERIES_FIELD_TYPE_STRING;
      it[i].ts_count = it[i].string_array_count = 1;
      it[i].ts_array = malloc(sizeof(uint64_t));
      it[i].string_array = calloc(1, sizeof(string_array_t));
      TEST_ASSERT_NOT_NULL(it[i].ts_array);
      TEST_ASSERT_NOT_NULL(it[i].string_array);
      it[i].ts_array[0] = timestamps[i];
      it[i].string_array[0].str = strdup(strings[i]);
      TEST_ASSERT_NOT_NULL(it[i].string_array[0].str);
      it[i].string_array[0].length = strlen(strings[i]);
      make_multi(&it[i], &m[i]);
      ptrs[i] = &m[i];
    }
    timeseries_multi_series_iterator_t agg;
    TEST_ASSERT_TRUE(timeseries_multi_series_iterator_init(
        ptrs, 4, 10, mode == 2 ? TSDB_AGGREGATION_COUNT : TSDB_AGGREGATION_LAST, &agg));
    timeseries_field_value_t value;
    TEST_ASSERT_TRUE(timeseries_multi_series_iterator_next(&agg, NULL, mode == 1 ? NULL : &value));
    if (mode == 0) {
      TEST_ASSERT_EQUAL_STRING("tie winner", value.data.string_val.str);
      free(value.data.string_val.str);
    } else if (mode == 2) {
      TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, value.type);
      TEST_ASSERT_EQUAL_INT64(4, value.data.int_val);
    }
    TEST_ASSERT_FALSE(timeseries_multi_series_iterator_next(&agg, NULL, NULL));
    timeseries_multi_series_iterator_deinit(&agg);
    for (int i = 0; i < 4; i++) {
      timeseries_multi_points_iterator_deinit(&m[i]);
      timeseries_points_iterator_deinit(&it[i]);
    }
  }
}

TEST_CASE("fourth: dense rollup throughput", "[fourth][fourth_dense]") {
  const size_t n = 4096;
  uint64_t* timestamps = malloc(n * sizeof(*timestamps));
  int64_t* values = malloc(n * sizeof(*values));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);
  for (size_t i = 0; i < n; i++) {
    timestamps[i] = i;
    values[i] = i;
  }
  const timeseries_aggregation_method_e methods[] = {TSDB_AGGREGATION_SUM, TSDB_AGGREGATION_COUNT,
                                                     TSDB_AGGREGATION_LAST};
  for (size_t k = 0; k < 3; k++) {
    timeseries_points_iterator_t it;
    make_points(&it, timestamps, values, n);
    timeseries_multi_points_iterator_t m;
    make_multi(&it, &m);
    timeseries_multi_points_iterator_t* ptr = &m;
    timeseries_multi_series_iterator_t agg;
    TEST_ASSERT_TRUE(timeseries_multi_series_iterator_init(&ptr, 1, 1, methods[k], &agg));
    uint64_t checksum = 0, timestamp_sum = 0;
    size_t count = 0;
    timeseries_field_value_t value;
    uint64_t ts;
    int64_t start = esp_timer_get_time();
    while (timeseries_multi_series_iterator_next(&agg, &ts, &value)) {
      count++;
      checksum += value.data.int_val;
      timestamp_sum += ts;
    }
    int64_t elapsed = esp_timer_get_time() - start;
    timeseries_multi_series_iterator_deinit(&agg);
    timeseries_multi_points_iterator_deinit(&m);
    timeseries_points_iterator_deinit(&it);
    TEST_ASSERT_EQUAL(n, count);
    TEST_ASSERT_EQUAL_UINT64(k == 1 ? n : n * (n - 1) / 2, checksum);
    TEST_ASSERT_EQUAL_UINT64(n * (n - 1) / 2, timestamp_sum);
    printf("FOURTH_DENSE method=%u n=%u us=%lld\n", (unsigned)methods[k], (unsigned)n, (long long)elapsed);
  }
  free(timestamps);
  free(values);
}
