/**
 * @file timeseries_aggregation_edge_test.c
 * @brief Edge case tests for aggregation: compaction boundaries, identical
 *        values, large values, and FLOAT precision.
 */

#include "esp_log.h"
#include "timeseries.h"
#include "unity.h"
#include <inttypes.h>
#include <math.h>
#include <string.h>

static const char *TAG = "aggregation_edge_test";

static bool s_db_initialized = false;

static void ensure_init(void) {
  if (!s_db_initialized) {
    TEST_ASSERT_TRUE_MESSAGE(timeseries_init(), "Failed to init timeseries");
    s_db_initialized = true;
  }
  TEST_ASSERT_TRUE(timeseries_clear_all());
}

// Helper: insert N int points with specific values
static bool insert_ints(const char *measurement, const char *field,
                        const int64_t *int_vals, const uint64_t *timestamps,
                        size_t count) {
  timeseries_field_value_t *vals =
      malloc(count * sizeof(timeseries_field_value_t));
  if (!vals)
    return false;
  for (size_t i = 0; i < count; i++) {
    vals[i].type = TIMESERIES_FIELD_TYPE_INT;
    vals[i].data.int_val = int_vals[i];
  }
  const char *fields[] = {field};
  timeseries_insert_data_t data = {
      .measurement_name = measurement,
      .field_names = fields,
      .field_values = vals,
      .num_fields = 1,
      .timestamps_ms = (uint64_t *)timestamps,
      .num_points = count,
  };
  bool ok = timeseries_insert(&data);
  free(vals);
  return ok;
}

// Helper: insert N float points with specific values
static bool insert_float_vals(const char *measurement, const char *field,
                              const double *float_vals,
                              const uint64_t *timestamps, size_t count) {
  timeseries_field_value_t *vals =
      malloc(count * sizeof(timeseries_field_value_t));
  if (!vals)
    return false;
  for (size_t i = 0; i < count; i++) {
    vals[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    vals[i].data.float_val = float_vals[i];
  }
  const char *fields[] = {field};
  timeseries_insert_data_t data = {
      .measurement_name = measurement,
      .field_names = fields,
      .field_values = vals,
      .num_fields = 1,
      .timestamps_ms = (uint64_t *)timestamps,
      .num_points = count,
  };
  bool ok = timeseries_insert(&data);
  free(vals);
  return ok;
}

static bool query_agg(const char *measurement,
                      timeseries_aggregation_method_e method,
                      uint32_t rollup_interval,
                      timeseries_query_result_t *result) {
  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = measurement;
  q.start_ms = 0;
  q.end_ms = INT64_MAX;
  q.rollup_interval = rollup_interval;
  q.aggregate_method = method;
  memset(result, 0, sizeof(*result));
  return timeseries_query(&q, result);
}

// ============================================================================
// Aggregation across compaction boundaries (mixed L0 + L1)
// ============================================================================

TEST_CASE("aggregation: query spans L0 and compacted L1 data",
          "[aggregation][compaction]") {
  ensure_init();

  // Insert first batch of data, then compact to L1
  int64_t batch1_vals[] = {10, 20, 30, 40, 50};
  uint64_t batch1_ts[] = {1000, 1100, 1200, 1300, 1400};
  TEST_ASSERT_TRUE(insert_ints("agg_compact", "v", batch1_vals, batch1_ts, 5));

  // Force compact to push L0 → L1
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());

  // Insert second batch that stays in L0
  int64_t batch2_vals[] = {60, 70, 80};
  uint64_t batch2_ts[] = {1500, 1600, 1700};
  TEST_ASSERT_TRUE(insert_ints("agg_compact", "v", batch2_vals, batch2_ts, 3));

  // Query aggregation spanning both L0 and L1 data
  timeseries_query_result_t r;

  // Single large window covering everything: SUM = 10+20+30+40+50+60+70+80 = 360
  TEST_ASSERT_TRUE(query_agg("agg_compact", TSDB_AGGREGATION_SUM, 10000, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_EQUAL_INT64(360, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);

  // COUNT = 8
  TEST_ASSERT_TRUE(
      query_agg("agg_compact", TSDB_AGGREGATION_COUNT, 10000, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_EQUAL_INT64(8, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);

  // MIN = 10
  TEST_ASSERT_TRUE(query_agg("agg_compact", TSDB_AGGREGATION_MIN, 10000, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_EQUAL_INT64(10, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);

  // MAX = 80
  TEST_ASSERT_TRUE(query_agg("agg_compact", TSDB_AGGREGATION_MAX, 10000, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_EQUAL_INT64(80, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);

  // AVG = 360/8 = 45
  TEST_ASSERT_TRUE(query_agg("agg_compact", TSDB_AGGREGATION_AVG, 10000, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_EQUAL_INT64(45, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);

  // LAST = 80 (highest timestamp)
  TEST_ASSERT_TRUE(query_agg("agg_compact", TSDB_AGGREGATION_LAST, 10000, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_EQUAL_INT64(80, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);

  ESP_LOGI(TAG, "Aggregation across L0+L1 boundary: all methods verified");
}

// ============================================================================
// All identical values
// ============================================================================

TEST_CASE("aggregation: all identical INT values",
          "[aggregation][edge]") {
  ensure_init();

  int64_t vals[] = {42, 42, 42, 42, 42};
  uint64_t ts[] = {1000, 1100, 1200, 1300, 1400};
  TEST_ASSERT_TRUE(insert_ints("agg_same", "v", vals, ts, 5));

  timeseries_query_result_t r;

  // AVG of identical = same value
  TEST_ASSERT_TRUE(query_agg("agg_same", TSDB_AGGREGATION_AVG, 10000, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_EQUAL_INT64(42, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);

  // MIN = MAX = 42
  TEST_ASSERT_TRUE(query_agg("agg_same", TSDB_AGGREGATION_MIN, 10000, &r));
  TEST_ASSERT_EQUAL_INT64(42, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);

  TEST_ASSERT_TRUE(query_agg("agg_same", TSDB_AGGREGATION_MAX, 10000, &r));
  TEST_ASSERT_EQUAL_INT64(42, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);

  // SUM = 42 * 5 = 210
  TEST_ASSERT_TRUE(query_agg("agg_same", TSDB_AGGREGATION_SUM, 10000, &r));
  TEST_ASSERT_EQUAL_INT64(210, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);

  // COUNT = 5
  TEST_ASSERT_TRUE(query_agg("agg_same", TSDB_AGGREGATION_COUNT, 10000, &r));
  TEST_ASSERT_EQUAL_INT64(5, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);
}

TEST_CASE("aggregation: all identical FLOAT values",
          "[aggregation][edge]") {
  ensure_init();

  double vals[] = {3.14, 3.14, 3.14};
  uint64_t ts[] = {1000, 1100, 1200};
  TEST_ASSERT_TRUE(insert_float_vals("agg_same_f", "v", vals, ts, 3));

  timeseries_query_result_t r;

  TEST_ASSERT_TRUE(query_agg("agg_same_f", TSDB_AGGREGATION_AVG, 10000, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_DOUBLE_WITHIN(0.001, 3.14,
                            r.columns[0].values[0].data.float_val);
  timeseries_query_free_result(&r);

  TEST_ASSERT_TRUE(query_agg("agg_same_f", TSDB_AGGREGATION_MIN, 10000, &r));
  TEST_ASSERT_DOUBLE_WITHIN(0.001, 3.14,
                            r.columns[0].values[0].data.float_val);
  timeseries_query_free_result(&r);

  TEST_ASSERT_TRUE(query_agg("agg_same_f", TSDB_AGGREGATION_MAX, 10000, &r));
  TEST_ASSERT_DOUBLE_WITHIN(0.001, 3.14,
                            r.columns[0].values[0].data.float_val);
  timeseries_query_free_result(&r);
}

// ============================================================================
// Large INT values — potential SUM overflow
// ============================================================================

TEST_CASE("aggregation: large INT values in SUM",
          "[aggregation][edge]") {
  ensure_init();

  // Values near INT32_MAX to stress 64-bit accumulation
  int64_t vals[] = {2000000000LL, 2000000000LL, 2000000000LL};
  uint64_t ts[] = {1000, 1100, 1200};
  TEST_ASSERT_TRUE(insert_ints("agg_big", "v", vals, ts, 3));

  timeseries_query_result_t r;

  // SUM = 6,000,000,000 (exceeds INT32_MAX but fits INT64)
  TEST_ASSERT_TRUE(query_agg("agg_big", TSDB_AGGREGATION_SUM, 10000, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_EQUAL_INT64(6000000000LL, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);

  // AVG = 2,000,000,000
  TEST_ASSERT_TRUE(query_agg("agg_big", TSDB_AGGREGATION_AVG, 10000, &r));
  TEST_ASSERT_EQUAL_INT64(2000000000LL, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);
}

// ============================================================================
// FLOAT precision edge cases
// ============================================================================

TEST_CASE("aggregation: FLOAT AVG precision with small differences",
          "[aggregation][edge]") {
  ensure_init();

  // Values with small differences that test floating point precision
  double vals[] = {1.0000001, 1.0000002, 1.0000003};
  uint64_t ts[] = {1000, 1100, 1200};
  TEST_ASSERT_TRUE(insert_float_vals("agg_prec", "v", vals, ts, 3));

  timeseries_query_result_t r;

  TEST_ASSERT_TRUE(query_agg("agg_prec", TSDB_AGGREGATION_AVG, 10000, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_DOUBLE_WITHIN(1e-6, 1.0000002,
                            r.columns[0].values[0].data.float_val);
  timeseries_query_free_result(&r);

  TEST_ASSERT_TRUE(query_agg("agg_prec", TSDB_AGGREGATION_MIN, 10000, &r));
  TEST_ASSERT_DOUBLE_WITHIN(1e-6, 1.0000001,
                            r.columns[0].values[0].data.float_val);
  timeseries_query_free_result(&r);

  TEST_ASSERT_TRUE(query_agg("agg_prec", TSDB_AGGREGATION_MAX, 10000, &r));
  TEST_ASSERT_DOUBLE_WITHIN(1e-6, 1.0000003,
                            r.columns[0].values[0].data.float_val);
  timeseries_query_free_result(&r);
}

// ============================================================================
// Raw query across compaction boundary (no aggregation)
// ============================================================================

TEST_CASE("aggregation: raw query returns all points across L0+L1",
          "[aggregation][compaction]") {
  ensure_init();

  // Insert + compact first batch
  int64_t batch1[] = {100, 200, 300};
  uint64_t ts1[] = {1000, 2000, 3000};
  TEST_ASSERT_TRUE(insert_ints("raw_mixed", "v", batch1, ts1, 3));
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());

  // Insert second batch (stays L0)
  int64_t batch2[] = {400, 500};
  uint64_t ts2[] = {4000, 5000};
  TEST_ASSERT_TRUE(insert_ints("raw_mixed", "v", batch2, ts2, 2));

  // Raw query (no aggregation) should return all 5 points
  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = "raw_mixed";
  q.start_ms = 0;
  q.end_ms = INT64_MAX;

  timeseries_query_result_t r = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  TEST_ASSERT_EQUAL(5, r.num_points);

  // Verify timestamps are sorted
  for (size_t i = 1; i < r.num_points; i++) {
    TEST_ASSERT_TRUE(r.timestamps[i] > r.timestamps[i - 1]);
  }

  // Verify values
  TEST_ASSERT_EQUAL_INT64(100, r.columns[0].values[0].data.int_val);
  TEST_ASSERT_EQUAL_INT64(200, r.columns[0].values[1].data.int_val);
  TEST_ASSERT_EQUAL_INT64(300, r.columns[0].values[2].data.int_val);
  TEST_ASSERT_EQUAL_INT64(400, r.columns[0].values[3].data.int_val);
  TEST_ASSERT_EQUAL_INT64(500, r.columns[0].values[4].data.int_val);

  timeseries_query_free_result(&r);

  ESP_LOGI(TAG, "Raw query across L0+L1: all 5 points returned in order");
}
