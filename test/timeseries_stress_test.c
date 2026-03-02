/**
 * @file timeseries_stress_test.c
 * @brief Stress tests: rapid insert/query/compact cycles, many small series,
 *        insert-compact interleaving, and repeated clear/re-init.
 */

#include "esp_log.h"
#include "esp_timer.h"
#include "timeseries.h"
#include "unity.h"
#include <inttypes.h>
#include <string.h>

static const char *TAG = "stress_test";

static bool s_db_initialized = false;

static void ensure_init(void) {
  if (!s_db_initialized) {
    TEST_ASSERT_TRUE(timeseries_init());
    s_db_initialized = true;
  }
  TEST_ASSERT_TRUE(timeseries_clear_all());
}

static bool insert_n(const char *measurement, const char *field, size_t count,
                     uint64_t start_ts) {
  uint64_t *ts = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *vals =
      malloc(count * sizeof(timeseries_field_value_t));
  if (!ts || !vals) {
    free(ts);
    free(vals);
    return false;
  }
  for (size_t i = 0; i < count; i++) {
    ts[i] = start_ts + i * 1000;
    vals[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    vals[i].data.float_val = (double)i * 0.5;
  }
  const char *fields[] = {field};
  timeseries_insert_data_t data = {
      .measurement_name = measurement,
      .field_names = fields,
      .field_values = vals,
      .num_fields = 1,
      .timestamps_ms = ts,
      .num_points = count,
  };
  bool ok = timeseries_insert(&data);
  free(ts);
  free(vals);
  return ok;
}

// ============================================================================
// Rapid insert → query → compact cycles
// ============================================================================

TEST_CASE("stress: insert-query-compact cycle 10 rounds",
          "[stress][cycle]") {
  ensure_init();

  int64_t start = esp_timer_get_time();

  for (int round = 0; round < 10; round++) {
    // Insert 50 points per round
    TEST_ASSERT_TRUE(
        insert_n("cycle_test", "v", 50, (uint64_t)round * 100000));

    // Query to verify data is there
    timeseries_query_t q;
    memset(&q, 0, sizeof(q));
    q.measurement_name = "cycle_test";
    q.start_ms = 0;
    q.end_ms = INT64_MAX;

    timeseries_query_result_t r = {0};
    TEST_ASSERT_TRUE(timeseries_query(&q, &r));
    TEST_ASSERT_EQUAL((round + 1) * 50, r.num_points);
    timeseries_query_free_result(&r);

    // Compact every other round
    if (round % 2 == 1) {
      TEST_ASSERT_TRUE(timeseries_compact_sync());
    }
  }

  int64_t elapsed = esp_timer_get_time() - start;
  ESP_LOGI(TAG, "10 insert-query-compact cycles: %" PRId64 " ms",
           elapsed / 1000);
}

// ============================================================================
// Many small series
// ============================================================================

TEST_CASE("stress: 10 different measurements", "[stress][series]") {
  ensure_init();

  for (int s = 0; s < 10; s++) {
    char name[32];
    snprintf(name, sizeof(name), "sensor_%d", s);
    TEST_ASSERT_TRUE(insert_n(name, "value", 20, 1000));
  }

  // Verify all measurements exist
  char **measurements = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(timeseries_get_measurements(&measurements, &count));
  TEST_ASSERT_EQUAL(10, count);

  for (size_t i = 0; i < count; i++) {
    free(measurements[i]);
  }
  free(measurements);

  // Compact and re-verify
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());

  // Query each measurement
  for (int s = 0; s < 10; s++) {
    char name[32];
    snprintf(name, sizeof(name), "sensor_%d", s);

    timeseries_query_t q;
    memset(&q, 0, sizeof(q));
    q.measurement_name = name;
    q.start_ms = 0;
    q.end_ms = INT64_MAX;

    timeseries_query_result_t r = {0};
    TEST_ASSERT_TRUE(timeseries_query(&q, &r));
    TEST_ASSERT_EQUAL(20, r.num_points);
    timeseries_query_free_result(&r);
  }

  ESP_LOGI(TAG, "10 measurements × 20 points: all verified after compaction");
}

// ============================================================================
// Insert-compact interleaving
// ============================================================================

TEST_CASE("stress: interleave inserts and compactions",
          "[stress][interleave]") {
  ensure_init();

  // Insert batch 1
  TEST_ASSERT_TRUE(insert_n("interleave", "v", 100, 0));

  // Compact L0→L1
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());

  // Insert batch 2 (new L0 pages)
  TEST_ASSERT_TRUE(insert_n("interleave", "v", 100, 200000));

  // Compact again (L0→L1, L1→L2)
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());

  // Insert batch 3
  TEST_ASSERT_TRUE(insert_n("interleave", "v", 100, 400000));

  // Final query — all 300 points should be present
  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = "interleave";
  q.start_ms = 0;
  q.end_ms = INT64_MAX;

  timeseries_query_result_t r = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  TEST_ASSERT_EQUAL(300, r.num_points);

  // Verify timestamps are sorted
  for (size_t i = 1; i < r.num_points; i++) {
    TEST_ASSERT_TRUE(r.timestamps[i] > r.timestamps[i - 1]);
  }

  timeseries_query_free_result(&r);
  ESP_LOGI(TAG, "Interleaved insert+compact: 300 points verified");
}

// ============================================================================
// Repeated clear + re-populate
// ============================================================================

TEST_CASE("stress: clear and re-populate 5 times", "[stress][clear]") {
  ensure_init();

  for (int cycle = 0; cycle < 5; cycle++) {
    TEST_ASSERT_TRUE(timeseries_clear_all());

    // Insert fresh data
    TEST_ASSERT_TRUE(
        insert_n("clear_cycle", "v", 30, (uint64_t)cycle * 100000));

    // Verify correct count
    timeseries_query_t q;
    memset(&q, 0, sizeof(q));
    q.measurement_name = "clear_cycle";
    q.start_ms = 0;
    q.end_ms = INT64_MAX;

    timeseries_query_result_t r = {0};
    TEST_ASSERT_TRUE(timeseries_query(&q, &r));
    TEST_ASSERT_EQUAL(30, r.num_points);
    timeseries_query_free_result(&r);
  }

  ESP_LOGI(TAG, "5 clear+re-populate cycles completed");
}

// ============================================================================
// Multi-field insert stress
// ============================================================================

TEST_CASE("stress: 5 fields in single measurement", "[stress][fields]") {
  ensure_init();

  size_t num_points = 50;
  uint64_t *ts = malloc(num_points * sizeof(uint64_t));
  timeseries_field_value_t *vals =
      malloc(5 * num_points * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(ts);
  TEST_ASSERT_NOT_NULL(vals);

  for (size_t i = 0; i < num_points; i++) {
    ts[i] = 1000 + i * 1000;
    for (int f = 0; f < 5; f++) {
      vals[f * num_points + i].type = TIMESERIES_FIELD_TYPE_FLOAT;
      vals[f * num_points + i].data.float_val = (double)(f * 100 + i);
    }
  }

  const char *fields[] = {"f0", "f1", "f2", "f3", "f4"};
  timeseries_insert_data_t data = {
      .measurement_name = "multi_field_stress",
      .field_names = fields,
      .field_values = vals,
      .num_fields = 5,
      .timestamps_ms = ts,
      .num_points = num_points,
  };
  TEST_ASSERT_TRUE(timeseries_insert(&data));
  free(ts);
  free(vals);

  // Verify all fields exist
  char **field_names = NULL;
  size_t fcount = 0;
  TEST_ASSERT_TRUE(timeseries_get_fields_for_measurement("multi_field_stress",
                                                         &field_names, &fcount));
  TEST_ASSERT_EQUAL(5, fcount);
  for (size_t i = 0; i < fcount; i++) {
    free(field_names[i]);
  }
  free(field_names);

  // Query and verify point count
  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = "multi_field_stress";
  q.start_ms = 0;
  q.end_ms = INT64_MAX;

  timeseries_query_result_t r = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  TEST_ASSERT_EQUAL(50, r.num_points);
  TEST_ASSERT_EQUAL(5, r.num_columns);
  timeseries_query_free_result(&r);

  ESP_LOGI(TAG, "5 fields × 50 points verified");
}
