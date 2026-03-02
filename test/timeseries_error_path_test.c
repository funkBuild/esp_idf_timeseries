/**
 * @file timeseries_error_path_test.c
 * @brief Tests for error paths: double-init, double-deinit, operations on
 *        cleared DB, insert validation, and edge case robustness.
 */

#include "timeseries.h"
#include "unity.h"
#include <string.h>

static bool s_db_initialized = false;

static void ensure_init(void) {
  if (!s_db_initialized) {
    TEST_ASSERT_TRUE(timeseries_init());
    s_db_initialized = true;
  }
}

static void ensure_clean(void) {
  ensure_init();
  TEST_ASSERT_TRUE(timeseries_clear_all());
}

// ============================================================================
// Lifecycle error paths
// ============================================================================

TEST_CASE("error: double init does not crash", "[error][lifecycle]") {
  ensure_init();
  // Second init should either succeed or fail gracefully, not crash
  bool ok = timeseries_init();
  (void)ok; // we don't care about the return, just that it doesn't crash
  s_db_initialized = true;
}

TEST_CASE("error: deinit then operations do not crash",
          "[error][lifecycle]") {
  ensure_init();
  timeseries_deinit();
  s_db_initialized = false;

  // Operations after deinit should not crash (return value is undefined)
  uint64_t ts = 1000;
  timeseries_field_value_t val = {.type = TIMESERIES_FIELD_TYPE_INT,
                                  .data.int_val = 42};
  const char *fields[] = {"temp"};
  timeseries_insert_data_t data = {
      .measurement_name = "test",
      .field_names = fields,
      .field_values = &val,
      .num_fields = 1,
      .timestamps_ms = &ts,
      .num_points = 1,
  };
  timeseries_insert(&data); // may succeed or fail, must not crash

  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = "test";
  q.start_ms = 0;
  q.end_ms = INT64_MAX;
  timeseries_query_result_t r = {0};
  bool query_ok = timeseries_query(&q, &r);
  if (query_ok) {
    timeseries_query_free_result(&r);
  }

  // Re-init for other tests
  TEST_ASSERT_TRUE(timeseries_init());
  s_db_initialized = true;
}

// ============================================================================
// Insert validation edge cases
// ============================================================================

TEST_CASE("error: insert NULL data pointer", "[error][insert]") {
  ensure_init();
  TEST_ASSERT_FALSE(timeseries_insert(NULL));
}

TEST_CASE("error: insert with 0 points", "[error][insert]") {
  ensure_clean();

  uint64_t ts = 1000;
  timeseries_field_value_t val = {.type = TIMESERIES_FIELD_TYPE_INT,
                                  .data.int_val = 42};
  const char *fields[] = {"v"};
  timeseries_insert_data_t data = {
      .measurement_name = "test",
      .field_names = fields,
      .field_values = &val,
      .num_fields = 1,
      .timestamps_ms = &ts,
      .num_points = 0, // zero points
  };
  // Should either succeed as no-op or return false, not crash
  timeseries_insert(&data);
}

TEST_CASE("error: insert with 0 fields", "[error][insert]") {
  ensure_clean();

  uint64_t ts = 1000;
  timeseries_insert_data_t data = {
      .measurement_name = "test",
      .field_names = NULL,
      .field_values = NULL,
      .num_fields = 0, // zero fields
      .timestamps_ms = &ts,
      .num_points = 1,
  };
  // Should either succeed as no-op or return false, not crash
  timeseries_insert(&data);
}

TEST_CASE("error: insert with NULL measurement name", "[error][insert]") {
  ensure_clean();

  uint64_t ts = 1000;
  timeseries_field_value_t val = {.type = TIMESERIES_FIELD_TYPE_INT,
                                  .data.int_val = 42};
  const char *fields[] = {"v"};
  timeseries_insert_data_t data = {
      .measurement_name = NULL,
      .field_names = fields,
      .field_values = &val,
      .num_fields = 1,
      .timestamps_ms = &ts,
      .num_points = 1,
  };
  TEST_ASSERT_FALSE(timeseries_insert(&data));
}

TEST_CASE("error: insert with empty measurement name", "[error][insert]") {
  ensure_clean();

  uint64_t ts = 1000;
  timeseries_field_value_t val = {.type = TIMESERIES_FIELD_TYPE_INT,
                                  .data.int_val = 42};
  const char *fields[] = {"v"};
  timeseries_insert_data_t data = {
      .measurement_name = "",
      .field_names = fields,
      .field_values = &val,
      .num_fields = 1,
      .timestamps_ms = &ts,
      .num_points = 1,
  };
  // Should either succeed or fail, not crash
  timeseries_insert(&data);
}

// ============================================================================
// Clear then query
// ============================================================================

TEST_CASE("error: query after clear_all returns empty", "[error][query]") {
  ensure_clean();

  // Insert data
  uint64_t ts[] = {1000, 2000, 3000};
  timeseries_field_value_t vals[3];
  for (int i = 0; i < 3; i++) {
    vals[i].type = TIMESERIES_FIELD_TYPE_INT;
    vals[i].data.int_val = i * 10;
  }
  const char *fields[] = {"v"};
  timeseries_insert_data_t data = {
      .measurement_name = "clear_test",
      .field_names = fields,
      .field_values = vals,
      .num_fields = 1,
      .timestamps_ms = ts,
      .num_points = 3,
  };
  TEST_ASSERT_TRUE(timeseries_insert(&data));

  // Clear
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Query should return empty
  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = "clear_test";
  q.start_ms = 0;
  q.end_ms = INT64_MAX;
  timeseries_query_result_t r = {0};
  bool ok = timeseries_query(&q, &r);
  // Either returns false (measurement gone) or ok with 0 points
  if (ok) {
    TEST_ASSERT_EQUAL(0, r.num_points);
    timeseries_query_free_result(&r);
  }
}

// ============================================================================
// Compact on empty DB
// ============================================================================

TEST_CASE("error: compact on empty db does not crash", "[error][compact]") {
  ensure_clean();

  // Compact with no data — should succeed or no-op
  bool ok = timeseries_compact_sync();
  (void)ok;
}

TEST_CASE("error: force compact on empty db does not crash",
          "[error][compact]") {
  ensure_clean();

  bool ok = timeseries_compact_force_sync();
  (void)ok;
}

// ============================================================================
// Free result edge cases
// ============================================================================

TEST_CASE("error: free_result on zeroed result", "[error][memory]") {
  timeseries_query_result_t r;
  memset(&r, 0, sizeof(r));
  // Should not crash
  timeseries_query_free_result(&r);
}

TEST_CASE("error: free_result NULL pointer", "[error][memory]") {
  // Should not crash
  timeseries_query_free_result(NULL);
}

// ============================================================================
// Metadata on empty DB
// ============================================================================

TEST_CASE("error: get_measurements on empty db", "[error][metadata]") {
  ensure_clean();

  char **measurements = NULL;
  size_t count = 0;
  bool ok = timeseries_get_measurements(&measurements, &count);
  if (ok) {
    TEST_ASSERT_EQUAL(0, count);
    free(measurements);
  }
}

TEST_CASE("error: get_fields for nonexistent measurement",
          "[error][metadata]") {
  ensure_clean();

  char **fields = NULL;
  size_t count = 0;
  bool ok =
      timeseries_get_fields_for_measurement("nonexistent", &fields, &count);
  // Should either return false or ok with 0 fields
  if (ok) {
    TEST_ASSERT_EQUAL(0, count);
    free(fields);
  }
}

TEST_CASE("error: get_tags for nonexistent measurement",
          "[error][metadata]") {
  ensure_clean();

  tsdb_tag_pair_t *tags = NULL;
  size_t count = 0;
  bool ok = timeseries_get_tags_for_measurement("nonexistent", &tags, &count);
  if (ok) {
    TEST_ASSERT_EQUAL(0, count);
    free(tags);
  }
}

// ============================================================================
// Expire on empty DB
// ============================================================================

TEST_CASE("error: expire on empty db does not crash", "[error][expire]") {
  ensure_clean();

  bool ok = timeseries_expire();
  (void)ok;
}
