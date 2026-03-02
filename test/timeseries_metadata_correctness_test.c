/**
 * @file timeseries_metadata_correctness_test.c
 * @brief Metadata correctness tests: consistency through insert/delete/compact
 *        cycles, tag deduplication, and multi-field/multi-tag scenarios.
 */

#include "timeseries.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

static bool s_db_initialized = false;

static void ensure_init(void) {
  if (!s_db_initialized) {
    TEST_ASSERT_TRUE(timeseries_init());
    s_db_initialized = true;
  }
  TEST_ASSERT_TRUE(timeseries_clear_all());
}

static bool insert_tagged(const char *measurement, const char *field,
                          const char *tag_key, const char *tag_value,
                          size_t count) {
  uint64_t *ts = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *vals =
      malloc(count * sizeof(timeseries_field_value_t));
  if (!ts || !vals) {
    free(ts);
    free(vals);
    return false;
  }
  for (size_t i = 0; i < count; i++) {
    ts[i] = 1000 + i * 100;
    vals[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    vals[i].data.float_val = (double)i;
  }
  const char *fields[] = {field};
  const char *tk[] = {tag_key};
  const char *tv[] = {tag_value};
  timeseries_insert_data_t data = {
      .measurement_name = measurement,
      .tag_keys = tag_key ? tk : NULL,
      .tag_values = tag_value ? tv : NULL,
      .num_tags = tag_key ? 1 : 0,
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
// Metadata consistency through insert → compact → verify
// ============================================================================

TEST_CASE("metadata: fields survive compaction", "[metadata][compaction]") {
  ensure_init();

  // Insert 2 fields
  TEST_ASSERT_TRUE(insert_tagged("mc_fields", "temperature", NULL, NULL, 20));
  TEST_ASSERT_TRUE(insert_tagged("mc_fields", "humidity", NULL, NULL, 20));

  // Compact
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());

  // Fields should still be listed
  char **fields = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(
      timeseries_get_fields_for_measurement("mc_fields", &fields, &count));
  TEST_ASSERT_EQUAL(2, count);

  // Verify both field names present (order may vary)
  bool found_temp = false, found_humid = false;
  for (size_t i = 0; i < count; i++) {
    if (strcmp(fields[i], "temperature") == 0)
      found_temp = true;
    if (strcmp(fields[i], "humidity") == 0)
      found_humid = true;
    free(fields[i]);
  }
  free(fields);
  TEST_ASSERT_TRUE(found_temp);
  TEST_ASSERT_TRUE(found_humid);
}

TEST_CASE("metadata: tags survive compaction", "[metadata][compaction]") {
  ensure_init();

  TEST_ASSERT_TRUE(
      insert_tagged("mc_tags", "temp", "location", "office", 20));

  TEST_ASSERT_TRUE(timeseries_compact_force_sync());

  tsdb_tag_pair_t *tags = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(
      timeseries_get_tags_for_measurement("mc_tags", &tags, &count));
  TEST_ASSERT_GREATER_THAN(0, count);

  bool found = false;
  for (size_t i = 0; i < count; i++) {
    if (strcmp(tags[i].key, "location") == 0 &&
        strcmp(tags[i].val, "office") == 0) {
      found = true;
    }
    free(tags[i].key);
    free(tags[i].val);
  }
  free(tags);
  TEST_ASSERT_TRUE(found);
}

TEST_CASE("metadata: measurements survive compaction",
          "[metadata][compaction]") {
  ensure_init();

  TEST_ASSERT_TRUE(insert_tagged("mc_meas_a", "v", NULL, NULL, 10));
  TEST_ASSERT_TRUE(insert_tagged("mc_meas_b", "v", NULL, NULL, 10));

  TEST_ASSERT_TRUE(timeseries_compact_force_sync());

  char **measurements = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(timeseries_get_measurements(&measurements, &count));
  TEST_ASSERT_EQUAL(2, count);

  bool found_a = false, found_b = false;
  for (size_t i = 0; i < count; i++) {
    if (strcmp(measurements[i], "mc_meas_a") == 0)
      found_a = true;
    if (strcmp(measurements[i], "mc_meas_b") == 0)
      found_b = true;
    free(measurements[i]);
  }
  free(measurements);
  TEST_ASSERT_TRUE(found_a);
  TEST_ASSERT_TRUE(found_b);
}

// ============================================================================
// Metadata after delete
// ============================================================================

TEST_CASE("metadata: delete measurement removes from get_measurements",
          "[metadata][delete]") {
  ensure_init();

  TEST_ASSERT_TRUE(insert_tagged("md_keep", "v", NULL, NULL, 10));
  TEST_ASSERT_TRUE(insert_tagged("md_delete", "v", NULL, NULL, 10));

  TEST_ASSERT_TRUE(timeseries_delete_measurement("md_delete"));

  char **measurements = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(timeseries_get_measurements(&measurements, &count));

  bool found_keep = false, found_delete = false;
  for (size_t i = 0; i < count; i++) {
    if (strcmp(measurements[i], "md_keep") == 0)
      found_keep = true;
    if (strcmp(measurements[i], "md_delete") == 0)
      found_delete = true;
    free(measurements[i]);
  }
  free(measurements);
  TEST_ASSERT_TRUE(found_keep);
  TEST_ASSERT_FALSE(found_delete);
}

TEST_CASE("metadata: delete field updates get_fields",
          "[metadata][delete]") {
  ensure_init();

  TEST_ASSERT_TRUE(insert_tagged("md_field", "temp", NULL, NULL, 10));
  TEST_ASSERT_TRUE(insert_tagged("md_field", "humidity", NULL, NULL, 10));

  TEST_ASSERT_TRUE(
      timeseries_delete_measurement_and_field("md_field", "temp"));

  char **fields = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(
      timeseries_get_fields_for_measurement("md_field", &fields, &count));
  TEST_ASSERT_EQUAL(1, count);
  TEST_ASSERT_EQUAL_STRING("humidity", fields[0]);
  free(fields[0]);
  free(fields);
}

// ============================================================================
// Multiple tag values for same measurement
// ============================================================================

TEST_CASE("metadata: multiple tag values listed correctly",
          "[metadata][tags]") {
  ensure_init();

  TEST_ASSERT_TRUE(
      insert_tagged("multi_tag", "temp", "location", "office", 5));
  TEST_ASSERT_TRUE(
      insert_tagged("multi_tag", "temp", "location", "warehouse", 5));

  tsdb_tag_pair_t *tags = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(
      timeseries_get_tags_for_measurement("multi_tag", &tags, &count));

  // Should have both tag values
  bool found_office = false, found_warehouse = false;
  for (size_t i = 0; i < count; i++) {
    if (strcmp(tags[i].key, "location") == 0) {
      if (strcmp(tags[i].val, "office") == 0)
        found_office = true;
      if (strcmp(tags[i].val, "warehouse") == 0)
        found_warehouse = true;
    }
    free(tags[i].key);
    free(tags[i].val);
  }
  free(tags);
  TEST_ASSERT_TRUE(found_office);
  TEST_ASSERT_TRUE(found_warehouse);
}

// ============================================================================
// Metadata consistency through delete → compact cycle
// ============================================================================

TEST_CASE("metadata: delete then compact cleans up metadata",
          "[metadata][compaction]") {
  ensure_init();

  TEST_ASSERT_TRUE(insert_tagged("dc_test", "v", NULL, NULL, 20));

  // Verify measurement exists
  char **measurements = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(timeseries_get_measurements(&measurements, &count));
  bool found = false;
  for (size_t i = 0; i < count; i++) {
    if (strcmp(measurements[i], "dc_test") == 0)
      found = true;
    free(measurements[i]);
  }
  free(measurements);
  TEST_ASSERT_TRUE(found);

  // Delete then compact
  TEST_ASSERT_TRUE(timeseries_delete_measurement("dc_test"));
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());

  // Should no longer appear
  measurements = NULL;
  count = 0;
  bool ok = timeseries_get_measurements(&measurements, &count);
  if (ok && count > 0) {
    found = false;
    for (size_t i = 0; i < count; i++) {
      if (strcmp(measurements[i], "dc_test") == 0)
        found = true;
      free(measurements[i]);
    }
    free(measurements);
    TEST_ASSERT_FALSE(found);
  }
}

// ============================================================================
// Re-insert after delete preserves correct metadata
// ============================================================================

TEST_CASE("metadata: re-insert after delete has correct metadata",
          "[metadata][lifecycle]") {
  ensure_init();

  // Insert, delete, re-insert with different tags
  TEST_ASSERT_TRUE(
      insert_tagged("reinst", "temp", "site", "A", 5));
  TEST_ASSERT_TRUE(timeseries_delete_measurement("reinst"));

  // Re-insert with different tag value
  TEST_ASSERT_TRUE(
      insert_tagged("reinst", "pressure", "site", "B", 5));

  // Measurement should exist
  char **measurements = NULL;
  size_t count = 0;
  TEST_ASSERT_TRUE(timeseries_get_measurements(&measurements, &count));
  bool found = false;
  for (size_t i = 0; i < count; i++) {
    if (strcmp(measurements[i], "reinst") == 0)
      found = true;
    free(measurements[i]);
  }
  free(measurements);
  TEST_ASSERT_TRUE(found);

  // Field should be "pressure" not "temp"
  char **fields = NULL;
  size_t fcount = 0;
  TEST_ASSERT_TRUE(
      timeseries_get_fields_for_measurement("reinst", &fields, &fcount));
  TEST_ASSERT_EQUAL(1, fcount);
  TEST_ASSERT_EQUAL_STRING("pressure", fields[0]);
  free(fields[0]);
  free(fields);
}
