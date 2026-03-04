/**
 * @file timeseries_api_test.c
 * @brief Tests for the public API functions ported from the feature branch:
 *        - timeseries_get_measurements()
 *        - timeseries_get_fields_for_measurement()
 *        - timeseries_get_tags_for_measurement()
 *        - timeseries_get_usage_summary()
 *        - timeseries_delete_measurement()
 *        - timeseries_delete_measurement_and_field()
 */

#include "esp_log.h"
#include "timeseries.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

static const char *TAG = "api_test";

static bool s_db_initialized = false;

static void ensure_init(void) {
  if (!s_db_initialized) {
    TEST_ASSERT_TRUE(timeseries_init());
    s_db_initialized = true;
  }
}

static void clear_db(void) {
  ensure_init();
  TEST_ASSERT_TRUE(timeseries_clear_all());
}

static void insert_measurement(const char *measurement, const char **tag_keys,
                               const char **tag_values, size_t num_tags,
                               const char **field_names, size_t num_fields,
                               size_t num_points) {
  uint64_t *timestamps = malloc(num_points * sizeof(uint64_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  timeseries_field_value_t *values =
      malloc(num_fields * num_points * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < num_points; i++) {
    timestamps[i] = 1000 * i;
    for (size_t f = 0; f < num_fields; f++) {
      values[f * num_points + i].type = TIMESERIES_FIELD_TYPE_FLOAT;
      values[f * num_points + i].data.float_val = (double)(f * 100 + i);
    }
  }

  timeseries_insert_data_t data = {
      .measurement_name = measurement,
      .tag_keys = tag_keys,
      .tag_values = tag_values,
      .num_tags = num_tags,
      .field_names = field_names,
      .field_values = values,
      .num_fields = num_fields,
      .timestamps_ms = timestamps,
      .num_points = num_points,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&data));
  free(timestamps);
  free(values);
}

// ============================================================================
// timeseries_get_measurements
// ============================================================================

TEST_CASE("api: get_measurements on empty db returns false",
          "[api][measurements]") {
  clear_db();

  char **measurements = NULL;
  size_t count = 0;
  bool ok = timeseries_get_measurements(&measurements, &count);
  TEST_ASSERT_FALSE(ok);
  TEST_ASSERT_EQUAL(0, count);
  TEST_ASSERT_NULL(measurements);
}

TEST_CASE("api: get_measurements returns single measurement",
          "[api][measurements]") {
  clear_db();

  const char *fields[] = {"temp"};
  insert_measurement("weather", NULL, NULL, 0, fields, 1, 5);

  char **measurements = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(timeseries_get_measurements(&measurements, &count));
  TEST_ASSERT_EQUAL(1, count);
  TEST_ASSERT_NOT_NULL(measurements);
  TEST_ASSERT_EQUAL_STRING("weather", measurements[0]);

  for (size_t i = 0; i < count; i++) free(measurements[i]);
  free(measurements);
}

TEST_CASE("api: get_measurements returns multiple measurements",
          "[api][measurements]") {
  clear_db();

  const char *fields[] = {"value"};
  insert_measurement("alpha", NULL, NULL, 0, fields, 1, 3);
  insert_measurement("beta", NULL, NULL, 0, fields, 1, 3);
  insert_measurement("gamma", NULL, NULL, 0, fields, 1, 3);

  char **measurements = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(timeseries_get_measurements(&measurements, &count));
  TEST_ASSERT_EQUAL(3, count);

  // Check all three are present (order may vary)
  bool found_alpha = false, found_beta = false, found_gamma = false;
  for (size_t i = 0; i < count; i++) {
    if (strcmp(measurements[i], "alpha") == 0) found_alpha = true;
    if (strcmp(measurements[i], "beta") == 0) found_beta = true;
    if (strcmp(measurements[i], "gamma") == 0) found_gamma = true;
    free(measurements[i]);
  }
  free(measurements);

  TEST_ASSERT_TRUE(found_alpha);
  TEST_ASSERT_TRUE(found_beta);
  TEST_ASSERT_TRUE(found_gamma);
}

TEST_CASE("api: get_measurements NULL params", "[api][measurements]") {
  ensure_init();
  TEST_ASSERT_FALSE(timeseries_get_measurements(NULL, NULL));

  size_t count = 0;
  TEST_ASSERT_FALSE(timeseries_get_measurements(NULL, &count));

  char **measurements = NULL;
  TEST_ASSERT_FALSE(timeseries_get_measurements(&measurements, NULL));
}

// ============================================================================
// timeseries_get_fields_for_measurement
// ============================================================================

TEST_CASE("api: get_fields for nonexistent measurement returns false",
          "[api][fields]") {
  clear_db();

  char **fields = NULL;
  size_t count = 0;
  bool ok =
      timeseries_get_fields_for_measurement("nonexistent", &fields, &count);
  TEST_ASSERT_FALSE(ok);
  TEST_ASSERT_EQUAL(0, count);
}

TEST_CASE("api: get_fields returns single field", "[api][fields]") {
  clear_db();

  const char *field_names[] = {"temperature"};
  insert_measurement("sensor", NULL, NULL, 0, field_names, 1, 5);

  char **fields = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(
      timeseries_get_fields_for_measurement("sensor", &fields, &count));
  TEST_ASSERT_EQUAL(1, count);
  TEST_ASSERT_EQUAL_STRING("temperature", fields[0]);

  for (size_t i = 0; i < count; i++) free(fields[i]);
  free(fields);
}

TEST_CASE("api: get_fields returns multiple fields", "[api][fields]") {
  clear_db();

  const char *field_names[] = {"temp", "humidity", "pressure"};
  insert_measurement("weather", NULL, NULL, 0, field_names, 3, 5);

  char **fields = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(
      timeseries_get_fields_for_measurement("weather", &fields, &count));
  TEST_ASSERT_EQUAL(3, count);

  bool found_temp = false, found_hum = false, found_pres = false;
  for (size_t i = 0; i < count; i++) {
    if (strcmp(fields[i], "temp") == 0) found_temp = true;
    if (strcmp(fields[i], "humidity") == 0) found_hum = true;
    if (strcmp(fields[i], "pressure") == 0) found_pres = true;
    free(fields[i]);
  }
  free(fields);

  TEST_ASSERT_TRUE(found_temp);
  TEST_ASSERT_TRUE(found_hum);
  TEST_ASSERT_TRUE(found_pres);
}

TEST_CASE("api: get_fields NULL params", "[api][fields]") {
  ensure_init();
  TEST_ASSERT_FALSE(
      timeseries_get_fields_for_measurement(NULL, NULL, NULL));
  TEST_ASSERT_FALSE(
      timeseries_get_fields_for_measurement("x", NULL, NULL));
}

// ============================================================================
// timeseries_get_tags_for_measurement
// ============================================================================

TEST_CASE("api: get_tags for measurement with no tags returns false",
          "[api][tags]") {
  clear_db();

  const char *fields[] = {"val"};
  insert_measurement("no_tags", NULL, NULL, 0, fields, 1, 3);

  tsdb_tag_pair_t *tags = NULL;
  size_t count = 0;
  bool ok = timeseries_get_tags_for_measurement("no_tags", &tags, &count);
  TEST_ASSERT_FALSE(ok);
  TEST_ASSERT_EQUAL(0, count);
}

TEST_CASE("api: get_tags returns tags for measurement", "[api][tags]") {
  clear_db();

  const char *tag_keys[] = {"location", "device"};
  const char *tag_values[] = {"office", "sensor1"};
  const char *fields[] = {"temp"};

  insert_measurement("tagged_m", tag_keys, tag_values, 2, fields, 1, 5);

  tsdb_tag_pair_t *tags = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(
      timeseries_get_tags_for_measurement("tagged_m", &tags, &count));
  TEST_ASSERT_EQUAL(2, count);

  bool found_location = false, found_device = false;
  for (size_t i = 0; i < count; i++) {
    ESP_LOGI(TAG, "Tag: %s=%s", tags[i].key, tags[i].val);
    if (strcmp(tags[i].key, "location") == 0 &&
        strcmp(tags[i].val, "office") == 0)
      found_location = true;
    if (strcmp(tags[i].key, "device") == 0 &&
        strcmp(tags[i].val, "sensor1") == 0)
      found_device = true;
    free(tags[i].key);
    free(tags[i].val);
  }
  free(tags);

  TEST_ASSERT_TRUE(found_location);
  TEST_ASSERT_TRUE(found_device);
}

TEST_CASE("api: get_tags for nonexistent measurement returns false",
          "[api][tags]") {
  clear_db();

  tsdb_tag_pair_t *tags = NULL;
  size_t count = 0;
  bool ok = timeseries_get_tags_for_measurement("ghost", &tags, &count);
  TEST_ASSERT_FALSE(ok);
  TEST_ASSERT_EQUAL(0, count);
}

TEST_CASE("api: get_tags NULL params", "[api][tags]") {
  ensure_init();
  TEST_ASSERT_FALSE(timeseries_get_tags_for_measurement(NULL, NULL, NULL));
  TEST_ASSERT_FALSE(timeseries_get_tags_for_measurement("x", NULL, NULL));
}

// ============================================================================
// timeseries_get_usage_summary
// ============================================================================

TEST_CASE("api: get_usage_summary on empty db", "[api][usage]") {
  clear_db();

  tsdb_usage_summary_t summary;
  TEST_ASSERT_TRUE(timeseries_get_usage_summary(&summary));

  // Should have at least the metadata page
  TEST_ASSERT_GREATER_THAN(0, summary.metadata_summary.num_pages);
  TEST_ASSERT_GREATER_THAN(0, summary.total_space_bytes);
  TEST_ASSERT_GREATER_THAN(0, summary.used_space_bytes);

  ESP_LOGI(TAG, "Empty DB: metadata_pages=%" PRIu32 " used=%" PRIu32
                " total=%" PRIu32,
           summary.metadata_summary.num_pages, summary.used_space_bytes,
           summary.total_space_bytes);
}

TEST_CASE("api: get_usage_summary after inserts", "[api][usage]") {
  clear_db();

  const char *fields[] = {"value"};
  insert_measurement("usage_test", NULL, NULL, 0, fields, 1, 100);

  tsdb_usage_summary_t summary;
  TEST_ASSERT_TRUE(timeseries_get_usage_summary(&summary));

  // Should have metadata + at least one field data page
  TEST_ASSERT_GREATER_THAN(0, summary.metadata_summary.num_pages);
  TEST_ASSERT_GREATER_THAN(0, summary.page_summaries[0].num_pages);

  ESP_LOGI(TAG, "After inserts: L0_pages=%" PRIu32 " metadata=%" PRIu32
                " used=%" PRIu32,
           summary.page_summaries[0].num_pages,
           summary.metadata_summary.num_pages, summary.used_space_bytes);
}

TEST_CASE("api: get_usage_summary NULL param", "[api][usage]") {
  ensure_init();
  TEST_ASSERT_FALSE(timeseries_get_usage_summary(NULL));
}

// ============================================================================
// timeseries_delete_measurement
// ============================================================================

TEST_CASE("api: delete_measurement removes data", "[api][delete]") {
  clear_db();

  const char *fields[] = {"temp"};
  insert_measurement("to_delete", NULL, NULL, 0, fields, 1, 20);

  // Verify data exists
  timeseries_query_t query = {
      .measurement_name = "to_delete",
      .start_ms = 0,
      .end_ms = INT64_MAX,
      .limit = 0,
  };
  timeseries_query_result_t result = {0};
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));
  TEST_ASSERT_EQUAL(20, result.num_points);
  timeseries_query_free_result(&result);

  // Delete
  TEST_ASSERT_TRUE(timeseries_delete_measurement("to_delete"));

  // Verify measurement is gone from listings
  char **measurements = NULL;
  size_t mcount = 0;
  bool found = timeseries_get_measurements(&measurements, &mcount);
  if (found) {
    bool still_listed = false;
    for (size_t i = 0; i < mcount; i++) {
      if (strcmp(measurements[i], "to_delete") == 0) still_listed = true;
      free(measurements[i]);
    }
    free(measurements);
    TEST_ASSERT_FALSE(still_listed);
  }
}

TEST_CASE("api: delete_measurement nonexistent is not an error",
          "[api][delete]") {
  clear_db();

  // Deleting something that doesn't exist should succeed (no-op)
  TEST_ASSERT_TRUE(timeseries_delete_measurement("nonexistent_m"));
}

TEST_CASE("api: delete_measurement does not affect other measurements",
          "[api][delete]") {
  clear_db();

  const char *fields[] = {"val"};
  insert_measurement("keep_this", NULL, NULL, 0, fields, 1, 10);
  insert_measurement("remove_this", NULL, NULL, 0, fields, 1, 10);

  TEST_ASSERT_TRUE(timeseries_delete_measurement("remove_this"));

  // "keep_this" should still have data
  timeseries_query_t query = {
      .measurement_name = "keep_this",
      .start_ms = 0,
      .end_ms = INT64_MAX,
      .limit = 0,
  };
  timeseries_query_result_t result = {0};
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));
  TEST_ASSERT_EQUAL(10, result.num_points);
  timeseries_query_free_result(&result);
}

TEST_CASE("api: delete_measurement NULL param", "[api][delete]") {
  ensure_init();
  TEST_ASSERT_FALSE(timeseries_delete_measurement(NULL));
}

// ============================================================================
// timeseries_delete_measurement_and_field
// ============================================================================

TEST_CASE("api: delete_measurement_and_field removes specific field",
          "[api][delete_field]") {
  clear_db();

  const char *field_names[] = {"temp", "humidity"};
  insert_measurement("multi_field", NULL, NULL, 0, field_names, 2, 10);

  // Verify both fields exist
  char **fields = NULL;
  size_t fcount = 0;
  TEST_ASSERT_TRUE(
      timeseries_get_fields_for_measurement("multi_field", &fields, &fcount));
  TEST_ASSERT_EQUAL(2, fcount);
  for (size_t i = 0; i < fcount; i++) free(fields[i]);
  free(fields);

  // Delete just "temp"
  TEST_ASSERT_TRUE(
      timeseries_delete_measurement_and_field("multi_field", "temp"));

  // Verify "humidity" still queryable
  const char *remaining_field[] = {"humidity"};
  timeseries_query_t query = {
      .measurement_name = "multi_field",
      .field_names = remaining_field,
      .num_fields = 1,
      .start_ms = 0,
      .end_ms = INT64_MAX,
      .limit = 0,
  };
  timeseries_query_result_t result = {0};
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));
  TEST_ASSERT_EQUAL(10, result.num_points);
  timeseries_query_free_result(&result);
}

TEST_CASE("api: delete_measurement_and_field nonexistent is not error",
          "[api][delete_field]") {
  clear_db();

  // Nonexistent measurement
  TEST_ASSERT_TRUE(
      timeseries_delete_measurement_and_field("ghost", "field"));

  // Existing measurement but nonexistent field
  const char *fields[] = {"temp"};
  insert_measurement("real_m", NULL, NULL, 0, fields, 1, 5);
  // This should not crash; may return true (no-op) or false
  timeseries_delete_measurement_and_field("real_m", "nonexistent_field");
}

TEST_CASE("api: delete_measurement_and_field NULL params",
          "[api][delete_field]") {
  ensure_init();
  TEST_ASSERT_FALSE(timeseries_delete_measurement_and_field(NULL, NULL));
  TEST_ASSERT_FALSE(timeseries_delete_measurement_and_field("x", NULL));
  TEST_ASSERT_FALSE(timeseries_delete_measurement_and_field(NULL, "y"));
}

// ============================================================================
// Type conversion (float <-> int) and incompatible conflict tests
// ============================================================================

static void insert_typed(const char *measurement, const char *field_name,
                         timeseries_field_type_e type, size_t num_points) {
  uint64_t *timestamps = malloc(num_points * sizeof(uint64_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  timeseries_field_value_t *values =
      malloc(num_points * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < num_points; i++) {
    timestamps[i] = 1000 * i;
    values[i].type = type;
    switch (type) {
    case TIMESERIES_FIELD_TYPE_FLOAT:
      values[i].data.float_val = 1.5 + (double)i;
      break;
    case TIMESERIES_FIELD_TYPE_INT:
      values[i].data.int_val = 100 + (int64_t)i;
      break;
    case TIMESERIES_FIELD_TYPE_BOOL:
      values[i].data.bool_val = (i % 2 == 0);
      break;
    case TIMESERIES_FIELD_TYPE_STRING:
      values[i].data.string_val.str = "test";
      values[i].data.string_val.length = 4;
      break;
    }
  }

  const char *field_names[] = {field_name};
  timeseries_insert_data_t data = {
      .measurement_name = measurement,
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = num_points,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&data));
  free(timestamps);
  free(values);
}

static bool try_insert_typed(const char *measurement, const char *field_name,
                             timeseries_field_type_e type, size_t num_points) {
  uint64_t *timestamps = malloc(num_points * sizeof(uint64_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  timeseries_field_value_t *values =
      malloc(num_points * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < num_points; i++) {
    timestamps[i] = 100000 + 1000 * i;  // Offset to avoid overlapping with insert_typed
    values[i].type = type;
    switch (type) {
    case TIMESERIES_FIELD_TYPE_FLOAT:
      values[i].data.float_val = 2.5 + (double)i;
      break;
    case TIMESERIES_FIELD_TYPE_INT:
      values[i].data.int_val = 200 + (int64_t)i;
      break;
    case TIMESERIES_FIELD_TYPE_BOOL:
      values[i].data.bool_val = (i % 2 != 0);
      break;
    case TIMESERIES_FIELD_TYPE_STRING:
      values[i].data.string_val.str = "test2";
      values[i].data.string_val.length = 5;
      break;
    }
  }

  const char *field_names[] = {field_name};
  timeseries_insert_data_t data = {
      .measurement_name = measurement,
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = num_points,
  };

  bool ok = timeseries_insert(&data);
  free(timestamps);
  free(values);
  return ok;
}

TEST_CASE("api: int-to-float conversion succeeds and stores as float",
          "[api][type_conversion]") {
  clear_db();

  // Insert as float first — establishes the metadata type
  insert_typed("conv_test", "value", TIMESERIES_FIELD_TYPE_FLOAT, 5);

  // Insert as int — should auto-convert to float
  bool ok = try_insert_typed("conv_test", "value", TIMESERIES_FIELD_TYPE_INT, 5);
  TEST_ASSERT_TRUE(ok);

  // Query back and verify type is float and values are correct
  timeseries_query_t query = {
      .measurement_name = "conv_test",
      .start_ms = 0,
      .end_ms = INT64_MAX,
      .limit = 0,
  };
  timeseries_query_result_t result = {0};
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));
  TEST_ASSERT_EQUAL(10, result.num_points);
  TEST_ASSERT_EQUAL(1, result.num_columns);
  TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_FLOAT, result.columns[0].type);

  // The second batch (int 200..204) should have been converted to float
  // Values are stored as doubles: 200.0, 201.0, etc.
  for (size_t i = 5; i < 10; i++) {
    double val = result.columns[0].values[i].data.float_val;
    double expected = 200.0 + (double)(i - 5);
    TEST_ASSERT_DOUBLE_WITHIN(0.001, expected, val);
  }

  timeseries_query_free_result(&result);
}

TEST_CASE("api: float-to-int conversion succeeds and stores as int",
          "[api][type_conversion]") {
  clear_db();

  // Insert as int first — establishes the metadata type
  insert_typed("conv_test2", "count", TIMESERIES_FIELD_TYPE_INT, 5);

  // Insert as float — should auto-convert to int (truncated)
  bool ok = try_insert_typed("conv_test2", "count", TIMESERIES_FIELD_TYPE_FLOAT, 5);
  TEST_ASSERT_TRUE(ok);

  // Query back and verify type is int
  timeseries_query_t query = {
      .measurement_name = "conv_test2",
      .start_ms = 0,
      .end_ms = INT64_MAX,
      .limit = 0,
  };
  timeseries_query_result_t result = {0};
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));
  TEST_ASSERT_EQUAL(10, result.num_points);
  TEST_ASSERT_EQUAL(1, result.num_columns);
  TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, result.columns[0].type);

  // The second batch (float 2.5, 3.5, ...) should have been truncated to int
  for (size_t i = 5; i < 10; i++) {
    int64_t val = result.columns[0].values[i].data.int_val;
    int64_t expected = (int64_t)(2.5 + (double)(i - 5));
    TEST_ASSERT_EQUAL_INT64(expected, val);
  }

  timeseries_query_free_result(&result);
}

TEST_CASE("api: incompatible type conflict fails (float vs string)",
          "[api][type_conversion]") {
  clear_db();

  // Insert as float first
  insert_typed("conflict_test", "val", TIMESERIES_FIELD_TYPE_FLOAT, 5);

  // Insert as string — should fail (incompatible)
  bool ok = try_insert_typed("conflict_test", "val", TIMESERIES_FIELD_TYPE_STRING, 3);
  TEST_ASSERT_FALSE(ok);
}

TEST_CASE("api: incompatible type conflict fails (int vs bool)",
          "[api][type_conversion]") {
  clear_db();

  insert_typed("conflict_test2", "flag", TIMESERIES_FIELD_TYPE_INT, 5);

  bool ok = try_insert_typed("conflict_test2", "flag", TIMESERIES_FIELD_TYPE_BOOL, 3);
  TEST_ASSERT_FALSE(ok);
}

// ============================================================================
// Delete + recreate with different type
// ============================================================================

TEST_CASE("api: delete measurement then recreate with different type",
          "[api][delete][type_conversion]") {
  clear_db();

  // Insert as float
  insert_typed("recreate_m", "val", TIMESERIES_FIELD_TYPE_FLOAT, 5);

  // Verify float type
  timeseries_query_t query = {
      .measurement_name = "recreate_m",
      .start_ms = 0,
      .end_ms = INT64_MAX,
      .limit = 0,
  };
  timeseries_query_result_t result = {0};
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));
  TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_FLOAT, result.columns[0].type);
  timeseries_query_free_result(&result);

  // Delete the measurement
  TEST_ASSERT_TRUE(timeseries_delete_measurement("recreate_m"));

  // Recreate with int — should succeed (old type metadata was cleaned up)
  insert_typed("recreate_m", "val", TIMESERIES_FIELD_TYPE_INT, 5);

  // Verify int type
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));
  TEST_ASSERT_EQUAL(5, result.num_points);
  TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, result.columns[0].type);
  timeseries_query_free_result(&result);
}

TEST_CASE("api: delete field then recreate with different type",
          "[api][delete_field][type_conversion]") {
  clear_db();

  // Insert two fields, one float and one int
  insert_typed("recreate_f", "temp", TIMESERIES_FIELD_TYPE_FLOAT, 5);
  insert_typed("recreate_f", "count", TIMESERIES_FIELD_TYPE_INT, 5);

  // Delete just "temp"
  TEST_ASSERT_TRUE(
      timeseries_delete_measurement_and_field("recreate_f", "temp"));

  // Recreate "temp" as int — should succeed
  insert_typed("recreate_f", "temp", TIMESERIES_FIELD_TYPE_INT, 5);

  // Verify "temp" is now int
  const char *field_names[] = {"temp"};
  timeseries_query_t query = {
      .measurement_name = "recreate_f",
      .field_names = field_names,
      .num_fields = 1,
      .start_ms = 0,
      .end_ms = INT64_MAX,
      .limit = 0,
  };
  timeseries_query_result_t result = {0};
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));
  TEST_ASSERT_EQUAL(5, result.num_points);
  TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, result.columns[0].type);
  timeseries_query_free_result(&result);

  // Verify "count" is still intact and still int
  const char *count_field[] = {"count"};
  timeseries_query_t query2 = {
      .measurement_name = "recreate_f",
      .field_names = count_field,
      .num_fields = 1,
      .start_ms = 0,
      .end_ms = INT64_MAX,
      .limit = 0,
  };
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query2, &result));
  TEST_ASSERT_EQUAL(5, result.num_points);
  TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, result.columns[0].type);
  timeseries_query_free_result(&result);
}

// ============================================================================
// Log hook
// ============================================================================

static int s_log_hook_call_count;
static int s_log_hook_last_level;
static char s_log_hook_last_message[256];

static void test_log_hook(int level, const char *message) {
  s_log_hook_call_count++;
  s_log_hook_last_level = level;
  strncpy(s_log_hook_last_message, message, sizeof(s_log_hook_last_message) - 1);
  s_log_hook_last_message[sizeof(s_log_hook_last_message) - 1] = '\0';
}

TEST_CASE("api: log hook fires warning on type conversion",
          "[api][log_hook]") {
  clear_db();

  timeseries_set_log_hook(test_log_hook);
  s_log_hook_call_count = 0;
  s_log_hook_last_level = 0;
  s_log_hook_last_message[0] = '\0';

  // Insert as float first
  insert_typed("hook_test", "val", TIMESERIES_FIELD_TYPE_FLOAT, 3);

  // Insert as int — triggers conversion warning
  bool ok = try_insert_typed("hook_test", "val", TIMESERIES_FIELD_TYPE_INT, 3);
  TEST_ASSERT_TRUE(ok);
  TEST_ASSERT_GREATER_THAN(0, s_log_hook_call_count);
  TEST_ASSERT_EQUAL(ESP_LOG_WARN, s_log_hook_last_level);
  TEST_ASSERT_NOT_NULL(strstr(s_log_hook_last_message, "val"));
  TEST_ASSERT_NOT_NULL(strstr(s_log_hook_last_message, "converting"));

  timeseries_set_log_hook(NULL);
}

TEST_CASE("api: log hook fires error on incompatible conflict",
          "[api][log_hook]") {
  clear_db();

  timeseries_set_log_hook(test_log_hook);
  s_log_hook_call_count = 0;
  s_log_hook_last_level = 0;
  s_log_hook_last_message[0] = '\0';

  // Insert as float first
  insert_typed("hook_err_test", "val", TIMESERIES_FIELD_TYPE_FLOAT, 3);

  // Insert as bool — triggers error
  bool ok = try_insert_typed("hook_err_test", "val", TIMESERIES_FIELD_TYPE_BOOL, 3);
  TEST_ASSERT_FALSE(ok);
  TEST_ASSERT_GREATER_THAN(0, s_log_hook_call_count);
  TEST_ASSERT_EQUAL(ESP_LOG_ERROR, s_log_hook_last_level);
  TEST_ASSERT_NOT_NULL(strstr(s_log_hook_last_message, "conflict"));

  timeseries_set_log_hook(NULL);
}
