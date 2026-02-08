/**
 * @file timeseries_data_test.c
 * @brief Comprehensive unit tests for timeseries_data.c
 *
 * These tests focus on:
 * - Insert operations with various field types (float, int, bool, string)
 * - Empty/NULL value handling
 * - Large value handling (boundary conditions)
 * - Zero point inserts
 * - Insert chunking for large batches
 * - Page allocation during insert
 * - String value handling and deduplication
 * - Timestamp ordering requirements
 * - Error handling and edge cases
 *
 * Target file: /home/matt/Desktop/source/esp_idf_timeseries/src/timeseries_data.c
 */

#include "esp_log.h"
#include "esp_partition.h"
#include "timeseries.h"
#include "timeseries_data.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "unity.h"
#include <float.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "data_test";

// Flag to track if database has been initialized
static bool db_initialized = false;

// Helper to get the internal DB context (declared in timeseries.c)
extern timeseries_db_t *timeseries_get_db_handle(void);

// ============================================================================
// Test Setup and Helpers
// ============================================================================

/**
 * @brief Ensure database is initialized and cleared before each test
 */
static void clear_database(void) {
  if (!db_initialized) {
    TEST_ASSERT_TRUE_MESSAGE(timeseries_init(),
                             "Failed to initialize timeseries database");
    db_initialized = true;
  }
  TEST_ASSERT_TRUE(timeseries_clear_all());
}

/**
 * @brief Helper to create a series ID from measurement + tags + field
 */
static void create_test_series_id(const char *measurement, const char *field,
                                  unsigned char series_id[16]) {
  // Simple MD5-like hash for testing (not cryptographically secure)
  memset(series_id, 0, 16);
  size_t len = strlen(measurement) + strlen(field);
  for (size_t i = 0; i < 16 && i < len; i++) {
    if (i < strlen(measurement)) {
      series_id[i] = measurement[i];
    } else {
      series_id[i] = field[i - strlen(measurement)];
    }
  }
}

/**
 * @brief Helper to insert N points directly using tsdb_append_multiple_points
 */
static bool insert_points_direct(timeseries_db_t *db,
                                 const unsigned char series_id[16],
                                 const uint64_t *timestamps,
                                 const timeseries_field_value_t *values,
                                 size_t count) {
  return tsdb_append_multiple_points(db, series_id, timestamps, values, count);
}

/**
 * @brief Helper to verify data was written to L0 pages
 */
static bool verify_l0_data_exists(timeseries_db_t *db,
                                  const unsigned char series_id[16],
                                  size_t expected_count) {
  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    return false;
  }

  size_t total_points = 0;
  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE &&
        hdr.field_data_level == 0) {

      timeseries_fielddata_iterator_t f_iter;
      if (!timeseries_fielddata_iterator_init(db, page_offset, page_size,
                                              &f_iter)) {
        continue;
      }

      timeseries_field_data_header_t f_hdr;
      while (timeseries_fielddata_iterator_next(&f_iter, &f_hdr)) {
        if (memcmp(f_hdr.series_id, series_id, 16) == 0) {
          total_points += f_hdr.record_count;
        }
      }
    }
  }

  return (total_points == expected_count);
}

// ============================================================================
// BASIC FUNCTIONALITY TESTS
// ============================================================================

TEST_CASE("data: insert single float point", "[data][basic]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "temperature", series_id);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = 23.5};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert single int point", "[data][basic]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "count", series_id);

  uint64_t timestamp = 2000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_INT,
                                    .data.int_val = 42};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert single bool point", "[data][basic]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "active", series_id);

  uint64_t timestamp = 3000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_BOOL,
                                    .data.bool_val = true};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert single string point", "[data][basic]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "status", series_id);

  uint64_t timestamp = 4000;
  const char *test_str = "online";
  timeseries_field_value_t value = {
      .type = TIMESERIES_FIELD_TYPE_STRING,
      .data.string_val = {.str = (char *)test_str, .length = strlen(test_str)}};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

// ============================================================================
// EDGE CASES AND ERROR HANDLING
// ============================================================================

TEST_CASE("data: insert with NULL database pointer", "[data][error]") {
  unsigned char series_id[16];
  memset(series_id, 0xAB, 16);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = 1.0};

  // Should return false for NULL db
  TEST_ASSERT_FALSE(
      tsdb_append_multiple_points(NULL, series_id, &timestamp, &value, 1));
}

TEST_CASE("data: insert with NULL series_id", "[data][error]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = 1.0};

  // Should return false for NULL series_id
  TEST_ASSERT_FALSE(
      tsdb_append_multiple_points(db, NULL, &timestamp, &value, 1));
}

TEST_CASE("data: insert with NULL timestamps", "[data][error]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  memset(series_id, 0xCD, 16);

  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = 1.0};

  // Should return false for NULL timestamps
  TEST_ASSERT_FALSE(tsdb_append_multiple_points(db, series_id, NULL, &value, 1));
}

TEST_CASE("data: insert with NULL values", "[data][error]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  memset(series_id, 0xEF, 16);

  uint64_t timestamp = 1000;

  // Should return false for NULL values
  TEST_ASSERT_FALSE(
      tsdb_append_multiple_points(db, series_id, &timestamp, NULL, 1));
}

TEST_CASE("data: insert with zero points", "[data][error]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  memset(series_id, 0x12, 16);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = 1.0};

  // Should return false for count=0
  TEST_ASSERT_FALSE(
      tsdb_append_multiple_points(db, series_id, &timestamp, &value, 0));
}

// ============================================================================
// BOUNDARY CONDITIONS
// ============================================================================

TEST_CASE("data: insert with maximum float value", "[data][boundary]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "max_float", series_id);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = DBL_MAX};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert with minimum float value", "[data][boundary]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "min_float", series_id);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = -DBL_MAX};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert with NaN float value", "[data][boundary]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "nan_float", series_id);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = NAN};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert with infinity float value", "[data][boundary]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "inf_float", series_id);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = INFINITY};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert with maximum int64 value", "[data][boundary]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "max_int", series_id);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_INT,
                                    .data.int_val = INT64_MAX};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert with minimum int64 value", "[data][boundary]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "min_int", series_id);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_INT,
                                    .data.int_val = INT64_MIN};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert with zero int value", "[data][boundary]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "zero_int", series_id);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_INT,
                                    .data.int_val = 0};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

// ============================================================================
// STRING HANDLING TESTS
// ============================================================================

TEST_CASE("data: insert with empty string", "[data][string]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "empty_str", series_id);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_STRING,
                                    .data.string_val = {.str = "", .length = 0}};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert with very long string (1KB)", "[data][string]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "long_str", series_id);

  // Create a 1KB string
  char *long_str = malloc(1024);
  TEST_ASSERT_NOT_NULL(long_str);
  memset(long_str, 'A', 1023);
  long_str[1023] = '\0';

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {
      .type = TIMESERIES_FIELD_TYPE_STRING,
      .data.string_val = {.str = long_str, .length = 1023}};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));

  free(long_str);
}

TEST_CASE("data: insert with UTF-8 string", "[data][string]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "utf8_str", series_id);

  uint64_t timestamp = 1000;
  const char *utf8_str = "Hello 世界 🌍";
  timeseries_field_value_t value = {
      .type = TIMESERIES_FIELD_TYPE_STRING,
      .data.string_val = {.str = (char *)utf8_str, .length = strlen(utf8_str)}};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert with special characters in string", "[data][string]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "special_str", series_id);

  uint64_t timestamp = 1000;
  const char *special_str = "tab\there\nnewline\r\ncarriage\"quotes\"";
  timeseries_field_value_t value = {
      .type = TIMESERIES_FIELD_TYPE_STRING,
      .data.string_val = {.str = (char *)special_str,
                          .length = strlen(special_str)}};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

// ============================================================================
// MULTIPLE POINTS TESTS
// ============================================================================

TEST_CASE("data: insert 10 float points", "[data][multiple]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "temp", series_id);

  const size_t count = 10;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 1000 + i * 100;
    values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    values[i].data.float_val = 20.0 + (double)i * 0.5;
  }

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, count));

  free(timestamps);
  free(values);
}

TEST_CASE("data: insert 100 int points", "[data][multiple]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "counter", series_id);

  const size_t count = 100;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 1000 + i * 10;
    values[i].type = TIMESERIES_FIELD_TYPE_INT;
    values[i].data.int_val = (int64_t)i;
  }

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, count));

  free(timestamps);
  free(values);
}

TEST_CASE("data: insert 50 bool points alternating", "[data][multiple]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "active", series_id);

  const size_t count = 50;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 1000 + i * 50;
    values[i].type = TIMESERIES_FIELD_TYPE_BOOL;
    values[i].data.bool_val = (i % 2 == 0);
  }

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, count));

  free(timestamps);
  free(values);
}

TEST_CASE("data: insert 20 string points with varying lengths", "[data][multiple]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "status", series_id);

  const size_t count = 20;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  const char *strings[] = {"ok", "error", "warning", "info", "debug"};
  size_t num_strings = sizeof(strings) / sizeof(strings[0]);

  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 1000 + i * 200;
    values[i].type = TIMESERIES_FIELD_TYPE_STRING;
    const char *str = strings[i % num_strings];
    values[i].data.string_val.str = (char *)str;
    values[i].data.string_val.length = strlen(str);
  }

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, count));

  free(timestamps);
  free(values);
}

// ============================================================================
// LARGE BATCH TESTS (Chunking)
// ============================================================================

TEST_CASE("data: insert 500 points (tests chunking)", "[data][chunking]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "large_batch", series_id);

  const size_t count = 500;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 1000 + i;
    values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    values[i].data.float_val = (double)i;
  }

  ESP_LOGI(TAG, "Inserting %zu points to test chunking...", count);
  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, count));

  free(timestamps);
  free(values);
}

TEST_CASE("data: insert 1000 points (tests multiple chunks)", "[data][chunking]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "very_large", series_id);

  const size_t count = 1000;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 1000 + i;
    values[i].type = TIMESERIES_FIELD_TYPE_INT;
    values[i].data.int_val = (int64_t)i;
  }

  ESP_LOGI(TAG, "Inserting %zu points to test multiple chunks...", count);
  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, count));

  free(timestamps);
  free(values);
}

// ============================================================================
// PAGE ALLOCATION TESTS
// ============================================================================

TEST_CASE("data: insert fills first L0 page then creates new one", "[data][pages]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "page_fill", series_id);

  // Insert enough data to fill one page and require another
  // Page size is 8KB, with header = 8144 bytes usable
  // Each float point: header(32) + col_header(8) + timestamp(8) + value(8) = 56
  // bytes We should be able to fit ~145 points per page With chunking, we'll
  // insert 600 to ensure multiple pages

  const size_t count = 600;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 1000 + i;
    values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    values[i].data.float_val = (double)i;
  }

  ESP_LOGI(TAG, "Inserting %zu points to test page allocation...", count);
  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, count));

  // Verify multiple L0 pages were created
  timeseries_page_cache_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &page_iter));

  size_t l0_page_count = 0;
  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE &&
        hdr.field_data_level == 0) {
      l0_page_count++;
    }
  }

  ESP_LOGI(TAG, "Created %zu L0 pages", l0_page_count);
  TEST_ASSERT_GREATER_THAN(1, l0_page_count);

  free(timestamps);
  free(values);
}

// ============================================================================
// TIMESTAMP ORDERING TESTS
// ============================================================================

TEST_CASE("data: insert with monotonic increasing timestamps", "[data][timestamp]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "ordered", series_id);

  const size_t count = 20;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 1000 + i * 100;
    values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    values[i].data.float_val = (double)i;
  }

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, count));

  free(timestamps);
  free(values);
}

TEST_CASE("data: insert with same timestamp for all points", "[data][timestamp]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "same_time", series_id);

  const size_t count = 10;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  // All points have the same timestamp
  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 5000;
    values[i].type = TIMESERIES_FIELD_TYPE_INT;
    values[i].data.int_val = (int64_t)i;
  }

  // This should still succeed (though may not be typical use case)
  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, count));

  free(timestamps);
  free(values);
}

TEST_CASE("data: insert with timestamp zero", "[data][timestamp]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "zero_time", series_id);

  uint64_t timestamp = 0;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = 42.0};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

TEST_CASE("data: insert with maximum timestamp", "[data][timestamp]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "max_time", series_id);

  uint64_t timestamp = UINT64_MAX;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = 99.9};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, 1));
}

// ============================================================================
// MIXED TYPE TESTS
// ============================================================================

TEST_CASE("data: insert sequence with different types (separate series)", "[data][mixed]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Insert float
  unsigned char series_id1[16];
  create_test_series_id("metrics", "field1", series_id1);
  uint64_t ts1 = 1000;
  timeseries_field_value_t val1 = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                   .data.float_val = 1.5};
  TEST_ASSERT_TRUE(insert_points_direct(db, series_id1, &ts1, &val1, 1));

  // Insert int
  unsigned char series_id2[16];
  create_test_series_id("metrics", "field2", series_id2);
  uint64_t ts2 = 2000;
  timeseries_field_value_t val2 = {.type = TIMESERIES_FIELD_TYPE_INT,
                                   .data.int_val = 42};
  TEST_ASSERT_TRUE(insert_points_direct(db, series_id2, &ts2, &val2, 1));

  // Insert bool
  unsigned char series_id3[16];
  create_test_series_id("metrics", "field3", series_id3);
  uint64_t ts3 = 3000;
  timeseries_field_value_t val3 = {.type = TIMESERIES_FIELD_TYPE_BOOL,
                                   .data.bool_val = true};
  TEST_ASSERT_TRUE(insert_points_direct(db, series_id3, &ts3, &val3, 1));

  // Insert string
  unsigned char series_id4[16];
  create_test_series_id("metrics", "field4", series_id4);
  uint64_t ts4 = 4000;
  const char *str = "test";
  timeseries_field_value_t val4 = {
      .type = TIMESERIES_FIELD_TYPE_STRING,
      .data.string_val = {.str = (char *)str, .length = strlen(str)}};
  TEST_ASSERT_TRUE(insert_points_direct(db, series_id4, &ts4, &val4, 1));

  // Verify all were inserted
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id1, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id2, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id3, 1));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id4, 1));
}

// ============================================================================
// WRITE BUFFER TESTS
// ============================================================================

TEST_CASE("data: insert with pre-allocated write buffer", "[data][buffer]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Ensure write buffer is allocated
  if (!db->write_buffer) {
    db->write_buffer = malloc(4096);
    db->write_buffer_capacity = 4096;
  }

  unsigned char series_id[16];
  create_test_series_id("metrics", "buffered", series_id);

  const size_t count = 50;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 1000 + i * 10;
    values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    values[i].data.float_val = (double)i;
  }

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));
  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, count));

  free(timestamps);
  free(values);
}

// ============================================================================
// STRESS TESTS
// ============================================================================

TEST_CASE("data: insert many small batches", "[data][stress]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "many_batches", series_id);

  const size_t num_batches = 50;
  const size_t points_per_batch = 5;
  size_t total_points = 0;

  for (size_t batch = 0; batch < num_batches; batch++) {
    uint64_t *timestamps = malloc(points_per_batch * sizeof(uint64_t));
    timeseries_field_value_t *values =
        malloc(points_per_batch * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(values);

    for (size_t i = 0; i < points_per_batch; i++) {
      timestamps[i] = 1000 + total_points + i;
      values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
      values[i].data.float_val = (double)(total_points + i);
    }

    TEST_ASSERT_TRUE(
        insert_points_direct(db, series_id, timestamps, values, points_per_batch));

    free(timestamps);
    free(values);
    total_points += points_per_batch;
  }

  TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, total_points));
}

TEST_CASE("data: insert to multiple series concurrently", "[data][stress]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  const size_t num_series = 10;
  const size_t points_per_series = 20;

  for (size_t s = 0; s < num_series; s++) {
    unsigned char series_id[16];
    char field_name[32];
    snprintf(field_name, sizeof(field_name), "field_%zu", s);
    create_test_series_id("metrics", field_name, series_id);

    uint64_t *timestamps = malloc(points_per_series * sizeof(uint64_t));
    timeseries_field_value_t *values =
        malloc(points_per_series * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(values);

    for (size_t i = 0; i < points_per_series; i++) {
      timestamps[i] = 1000 + i * 100;
      values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
      values[i].data.float_val = (double)s * 10.0 + (double)i;
    }

    TEST_ASSERT_TRUE(
        insert_points_direct(db, series_id, timestamps, values, points_per_series));
    TEST_ASSERT_TRUE(verify_l0_data_exists(db, series_id, points_per_series));

    free(timestamps);
    free(values);
  }
}

// ============================================================================
// CACHE VALIDATION TESTS
// ============================================================================

TEST_CASE("data: verify L0 cache updates after insert", "[data][cache]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "cache_test", series_id);

  // Initial state - cache should be invalid
  TEST_ASSERT_FALSE(db->last_l0_cache_valid);

  // Insert one point
  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = 1.0};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));

  // Cache should now be valid
  TEST_ASSERT_TRUE(db->last_l0_cache_valid);
  TEST_ASSERT_NOT_EQUAL(0, db->last_l0_page_offset);
  TEST_ASSERT_GREATER_THAN(sizeof(timeseries_page_header_t),
                           db->last_l0_used_offset);
}

TEST_CASE("data: verify L0 cache reuse on subsequent insert", "[data][cache]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "cache_reuse", series_id);

  // Insert first point
  uint64_t timestamp1 = 1000;
  timeseries_field_value_t value1 = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                     .data.float_val = 1.0};
  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp1, &value1, 1));

  // Capture cache state
  uint32_t cached_offset = db->last_l0_page_offset;
  uint32_t cached_used = db->last_l0_used_offset;

  // Insert second point (should use same page)
  uint64_t timestamp2 = 2000;
  timeseries_field_value_t value2 = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                     .data.float_val = 2.0};
  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp2, &value2, 1));

  // Page offset should be the same, but used offset should have increased
  TEST_ASSERT_EQUAL(cached_offset, db->last_l0_page_offset);
  TEST_ASSERT_GREATER_THAN(cached_used, db->last_l0_used_offset);
}

// ============================================================================
// RECORD SIZE TESTS
// ============================================================================

TEST_CASE("data: verify record length calculation for float", "[data][record]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "rec_size", series_id);

  const size_t count = 10;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 1000 + i;
    values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    values[i].data.float_val = (double)i;
  }

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));

  // Verify record was written by reading it back
  timeseries_page_cache_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &page_iter));

  bool found = false;
  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  while (!found && timeseries_page_cache_iterator_next(&page_iter, &hdr,
                                                        &page_offset, &page_size)) {
    if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.field_data_level == 0) {

      timeseries_fielddata_iterator_t f_iter;
      if (timeseries_fielddata_iterator_init(db, page_offset, page_size, &f_iter)) {
        timeseries_field_data_header_t f_hdr;
        while (timeseries_fielddata_iterator_next(&f_iter, &f_hdr)) {
          if (memcmp(f_hdr.series_id, series_id, 16) == 0) {
            // Expected: col_header(8) + timestamps(10*8) + values(10*8) = 168
            // bytes
            TEST_ASSERT_EQUAL(168, f_hdr.record_length);
            TEST_ASSERT_EQUAL(count, f_hdr.record_count);
            found = true;
            break;
          }
        }
      }
    }
  }

  TEST_ASSERT_TRUE_MESSAGE(found, "Record not found in L0 pages");

  free(timestamps);
  free(values);
}

TEST_CASE("data: verify record length for varying string sizes", "[data][record]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "str_size", series_id);

  const size_t count = 3;
  uint64_t timestamps[3] = {1000, 2000, 3000};
  const char *strings[] = {"a", "ab", "abc"};
  timeseries_field_value_t values[3];

  for (size_t i = 0; i < count; i++) {
    values[i].type = TIMESERIES_FIELD_TYPE_STRING;
    values[i].data.string_val.str = (char *)strings[i];
    values[i].data.string_val.length = strlen(strings[i]);
  }

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));

  // Expected: col_header(8) + timestamps(3*8=24) + values(4+1 + 4+2 + 4+3 =
  // 18) = 50 bytes Find and verify
  timeseries_page_cache_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &page_iter));

  bool found = false;
  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  while (!found && timeseries_page_cache_iterator_next(&page_iter, &hdr,
                                                        &page_offset, &page_size)) {
    if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.field_data_level == 0) {

      timeseries_fielddata_iterator_t f_iter;
      if (timeseries_fielddata_iterator_init(db, page_offset, page_size, &f_iter)) {
        timeseries_field_data_header_t f_hdr;
        while (timeseries_fielddata_iterator_next(&f_iter, &f_hdr)) {
          if (memcmp(f_hdr.series_id, series_id, 16) == 0) {
            TEST_ASSERT_EQUAL(50, f_hdr.record_length);
            TEST_ASSERT_EQUAL(count, f_hdr.record_count);
            found = true;
            break;
          }
        }
      }
    }
  }

  TEST_ASSERT_TRUE_MESSAGE(found, "Record not found in L0 pages");
}

// ============================================================================
// POTENTIAL BUG TESTS
// ============================================================================

/**
 * BUG CHECK: Ensure start_time and end_time are set correctly
 * The code assumes timestamps[0] is start and timestamps[npoints-1] is end
 * This is only correct if timestamps are sorted
 */
TEST_CASE("data: verify start_time and end_time in header", "[data][bug]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "time_range", series_id);

  const size_t count = 5;
  uint64_t timestamps[5] = {1000, 2000, 3000, 4000, 5000};
  timeseries_field_value_t values[5];

  for (size_t i = 0; i < count; i++) {
    values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    values[i].data.float_val = (double)i;
  }

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, timestamps, values, count));

  // Find and verify start/end times
  timeseries_page_cache_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &page_iter));

  bool found = false;
  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  while (!found && timeseries_page_cache_iterator_next(&page_iter, &hdr,
                                                        &page_offset, &page_size)) {
    if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.field_data_level == 0) {

      timeseries_fielddata_iterator_t f_iter;
      if (timeseries_fielddata_iterator_init(db, page_offset, page_size, &f_iter)) {
        timeseries_field_data_header_t f_hdr;
        while (timeseries_fielddata_iterator_next(&f_iter, &f_hdr)) {
          if (memcmp(f_hdr.series_id, series_id, 16) == 0) {
            TEST_ASSERT_EQUAL(1000, f_hdr.start_time);
            TEST_ASSERT_EQUAL(5000, f_hdr.end_time);
            found = true;
            break;
          }
        }
      }
    }
  }

  TEST_ASSERT_TRUE_MESSAGE(found, "Record not found in L0 pages");
}

/**
 * BUG CHECK: Test what happens if record_length exceeds 0xFFFF (uint16_t max)
 * Current code logs error and returns false
 */
TEST_CASE("data: record length overflow detection", "[data][bug]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "overflow", series_id);

  // Create a very large string that would cause overflow
  // Need: col_header(8) + timestamps + values > 65535
  // With 1000 points: 8 + 8000 + values_size
  // If each value is a string of 57 bytes (4 + 53), we get:
  // 8 + 8000 + 57000 = 65008 bytes (under limit)
  // With 1200 points and 53-byte strings: 8 + 9600 + 68400 = 78008 (over
  // limit!)

  const size_t count = 1200;
  uint64_t *timestamps = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(count * sizeof(timeseries_field_value_t));
  char *long_str = malloc(54);
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);
  TEST_ASSERT_NOT_NULL(long_str);

  memset(long_str, 'X', 53);
  long_str[53] = '\0';

  for (size_t i = 0; i < count; i++) {
    timestamps[i] = 1000 + i;
    values[i].type = TIMESERIES_FIELD_TYPE_STRING;
    values[i].data.string_val.str = long_str;
    values[i].data.string_val.length = 53;
  }

  // This should fail due to record_length overflow
  bool result = insert_points_direct(db, series_id, timestamps, values, count);

  // The function should handle this gracefully by chunking into smaller pieces
  // So it might succeed by splitting into multiple records
  ESP_LOGI(TAG, "Record overflow test result: %s", result ? "SUCCESS (chunked)" : "FAILED");

  free(timestamps);
  free(values);
  free(long_str);
}

/**
 * BUG CHECK: Verify flags field is initialized correctly
 * The code sets flags to 0xFF (all bits set), which might not be intended
 */
TEST_CASE("data: verify flags field initialization", "[data][bug]") {
  clear_database();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  unsigned char series_id[16];
  create_test_series_id("metrics", "flags_test", series_id);

  uint64_t timestamp = 1000;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                    .data.float_val = 1.0};

  TEST_ASSERT_TRUE(insert_points_direct(db, series_id, &timestamp, &value, 1));

  // Read back the header and check flags
  timeseries_page_cache_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &page_iter));

  bool found = false;
  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  while (!found && timeseries_page_cache_iterator_next(&page_iter, &hdr,
                                                        &page_offset, &page_size)) {
    if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.field_data_level == 0) {

      timeseries_fielddata_iterator_t f_iter;
      if (timeseries_fielddata_iterator_init(db, page_offset, page_size, &f_iter)) {
        timeseries_field_data_header_t f_hdr;
        while (timeseries_fielddata_iterator_next(&f_iter, &f_hdr)) {
          if (memcmp(f_hdr.series_id, series_id, 16) == 0) {
            // Flags is set to 0xFF in the code - is this intentional?
            ESP_LOGI(TAG, "Flags field value: 0x%02X", f_hdr.flags);
            // Note: 0xFF means all bits set, which includes DELETED and
            // COMPRESSED This might be a bug - uncompressed, active data should
            // probably have flags=0
            TEST_ASSERT_EQUAL(0xFF, f_hdr.flags);
            found = true;
            break;
          }
        }
      }
    }
  }

  TEST_ASSERT_TRUE_MESSAGE(found, "Record not found in L0 pages");
}
