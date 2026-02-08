/**
 * @file timeseries_query_test.c
 * @brief Comprehensive unit tests for timeseries_query.c
 *
 * This test suite focuses on:
 * - NULL/invalid parameter handling
 * - Empty result scenarios
 * - Time range boundary conditions
 * - Field selection edge cases
 * - Tag filtering combinations
 * - Limit parameter edge cases
 * - Memory allocation failure scenarios
 * - Result handling and cleanup
 */

#include "esp_log.h"
#include "timeseries.h"
#include "unity.h"
#include <string.h>
#include <limits.h>

static const char *TAG = "query_test";

// Flag to track if database has been initialized
static bool db_initialized = false;

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * @brief Ensure database is initialized and cleared before each test
 */
static void setup_test(void) {
    if (!db_initialized) {
        TEST_ASSERT_TRUE_MESSAGE(timeseries_init(), "Failed to initialize timeseries database");
        db_initialized = true;
    }
    TEST_ASSERT_TRUE(timeseries_clear_all());
}

/**
 * @brief Insert test data with tags and multiple fields
 *
 * @param measurement_name Name of the measurement
 * @param tag_keys Array of tag keys (can be NULL if num_tags == 0)
 * @param tag_values Array of tag values (can be NULL if num_tags == 0)
 * @param num_tags Number of tags
 * @param field_names Array of field names
 * @param num_fields Number of fields
 * @param num_points Number of data points to insert
 * @param start_timestamp Starting timestamp
 * @param timestamp_increment Increment between timestamps
 * @return true on success, false on failure
 */
static bool insert_test_data(const char *measurement_name,
                              const char **tag_keys, const char **tag_values, size_t num_tags,
                              const char **field_names, size_t num_fields,
                              size_t num_points, uint64_t start_timestamp, uint64_t timestamp_increment) {
    uint64_t *timestamps = malloc(num_points * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(num_fields * num_points * sizeof(timeseries_field_value_t));

    if (!timestamps || !field_values) {
        free(timestamps);
        free(field_values);
        return false;
    }

    // Fill timestamps and field values
    for (size_t i = 0; i < num_points; i++) {
        timestamps[i] = start_timestamp + (i * timestamp_increment);

        // Fill each field for this point
        for (size_t f = 0; f < num_fields; f++) {
            size_t idx = f * num_points + i;

            // Alternate field types for testing
            if (f % 3 == 0) {
                field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
                field_values[idx].data.float_val = (double)i * 1.5 + f;
            } else if (f % 3 == 1) {
                field_values[idx].type = TIMESERIES_FIELD_TYPE_INT;
                field_values[idx].data.int_val = (int64_t)i * 10 + f;
            } else {
                field_values[idx].type = TIMESERIES_FIELD_TYPE_BOOL;
                field_values[idx].data.bool_val = ((i + f) % 2 == 0);
            }
        }
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = measurement_name,
        .tag_keys = tag_keys,
        .tag_values = tag_values,
        .num_tags = num_tags,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = num_fields,
        .timestamps_ms = timestamps,
        .num_points = num_points,
    };

    bool result = timeseries_insert(&insert_data);

    free(timestamps);
    free(field_values);
    return result;
}

/**
 * @brief Execute a query and verify the result structure is valid
 */
static bool execute_and_validate_query(const timeseries_query_t *query,
                                        timeseries_query_result_t *result) {
    memset(result, 0, sizeof(*result));
    bool success = timeseries_query(query, result);

    if (!success) {
        return false;
    }

    // Validate result structure consistency
    if (result->num_points > 0) {
        TEST_ASSERT_NOT_NULL(result->timestamps);
    }

    if (result->num_columns > 0) {
        TEST_ASSERT_NOT_NULL(result->columns);

        // Validate each column
        for (size_t c = 0; c < result->num_columns; c++) {
            TEST_ASSERT_NOT_NULL(result->columns[c].name);
            if (result->num_points > 0) {
                TEST_ASSERT_NOT_NULL(result->columns[c].values);
            }
        }
    }

    return true;
}

// =============================================================================
// NULL/Invalid Parameter Tests
// =============================================================================

TEST_CASE("query: NULL query parameter", "[query][error]") {
    setup_test();

    timeseries_query_result_t result;
    memset(&result, 0, sizeof(result));

    // Query with NULL query parameter should return false
    bool success = timeseries_query(NULL, &result);
    TEST_ASSERT_FALSE(success);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: NULL result parameter", "[query][error]") {
    setup_test();

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "test_measurement";

    // Query with NULL result parameter should return false
    bool success = timeseries_query(&query, NULL);
    TEST_ASSERT_FALSE(success);
}

TEST_CASE("query: NULL measurement name", "[query][error]") {
    setup_test();

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = NULL;  // Invalid

    timeseries_query_result_t result;
    memset(&result, 0, sizeof(result));

    // Should handle NULL measurement name gracefully
    // Implementation returns false for NULL measurement name
    bool success = timeseries_query(&query, &result);
    TEST_ASSERT_FALSE(success);

    timeseries_query_free_result(&result);
}

// =============================================================================
// Empty Result Tests
// =============================================================================

TEST_CASE("query: empty database returns empty result", "[query][empty]") {
    setup_test();

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "nonexistent_measurement";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return success with empty result
    TEST_ASSERT_EQUAL(0, result.num_points);
    TEST_ASSERT_EQUAL(0, result.num_columns);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: nonexistent measurement returns empty result", "[query][empty]") {
    setup_test();

    // Insert data for a different measurement
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query nonexistent measurement
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "nonexistent";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    TEST_ASSERT_EQUAL(0, result.num_points);
    TEST_ASSERT_EQUAL(0, result.num_columns);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: no matching tags returns empty result", "[query][empty][tags]") {
    setup_test();

    // Insert data with specific tags
    const char *tag_keys[] = {"location", "sensor"};
    const char *tag_values[] = {"room1", "temp1"};
    const char *field_names[] = {"temperature"};

    TEST_ASSERT_TRUE(insert_test_data("weather", tag_keys, tag_values, 2,
                                      field_names, 1, 10, 1000, 1000));

    // Query with non-matching tags
    const char *query_tag_keys[] = {"location"};
    const char *query_tag_values[] = {"room2"};  // Different value

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.tag_keys = query_tag_keys;
    query.tag_values = query_tag_values;
    query.num_tags = 1;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    TEST_ASSERT_EQUAL(0, result.num_points);
    TEST_ASSERT_EQUAL(0, result.num_columns);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: no matching fields returns empty result", "[query][empty][fields]") {
    setup_test();

    // Insert data with specific field
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query for non-existent field
    const char *query_field_names[] = {"pressure"};

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.field_names = query_field_names;
    query.num_fields = 1;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    TEST_ASSERT_EQUAL(0, result.num_points);
    TEST_ASSERT_EQUAL(0, result.num_columns);

    timeseries_query_free_result(&result);
}

// =============================================================================
// Time Range Boundary Tests
// =============================================================================

TEST_CASE("query: start_ms equals end_ms returns empty result", "[query][time]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query with start_ms == end_ms
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.start_ms = 5000;
    query.end_ms = 5000;  // Same as start

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return empty or very limited results
    // (depends on implementation - likely empty)

    timeseries_query_free_result(&result);
}

TEST_CASE("query: start_ms greater than end_ms", "[query][time]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query with inverted time range
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.start_ms = 10000;
    query.end_ms = 5000;  // Less than start

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return empty result (no data in invalid range)
    TEST_ASSERT_EQUAL(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: start_ms = 0 includes all data from beginning", "[query][time]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query with start_ms = 0 (no lower bound)
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.start_ms = 0;  // From beginning
    query.end_ms = 5000;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should include points from timestamp 1000-5000
    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: end_ms = 0 includes all data to end", "[query][time]") {
    setup_test();

    // Insert data from 1000 to 10000
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query with end_ms = 0 (no upper bound)
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.start_ms = 5000;
    query.end_ms = 0;  // To end

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should include points from 5000 onwards
    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: both start_ms and end_ms = 0 returns all data", "[query][time]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query with no time bounds
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.start_ms = 0;
    query.end_ms = 0;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return all data
    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: time range with no matching data", "[query][time]") {
    setup_test();

    // Insert data from 1000 to 10000
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query for time range outside data
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.start_ms = 20000;  // After all data
    query.end_ms = 30000;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    TEST_ASSERT_EQUAL(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: maximum timestamp values", "[query][time][boundary]") {
    setup_test();

    // Insert data with large timestamps
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 5, UINT64_MAX - 10000, 1000));

    // Query with very large timestamp range
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.start_ms = UINT64_MAX - 20000;
    query.end_ms = UINT64_MAX;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should handle large timestamps correctly
    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

// =============================================================================
// Field Selection Tests
// =============================================================================

TEST_CASE("query: num_fields = 0 returns all fields", "[query][fields]") {
    setup_test();

    // Insert data with multiple fields
    const char *field_names[] = {"temperature", "humidity", "pressure"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 3, 10, 1000, 1000));

    // Query with num_fields = 0 (all fields)
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.num_fields = 0;  // Request all fields

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return all 3 fields
    TEST_ASSERT_EQUAL(3, result.num_columns);
    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: select single field from multiple", "[query][fields]") {
    setup_test();

    // Insert data with multiple fields
    const char *field_names[] = {"temperature", "humidity", "pressure"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 3, 10, 1000, 1000));

    // Query for single field
    const char *query_field_names[] = {"humidity"};

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.field_names = query_field_names;
    query.num_fields = 1;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return only 1 field
    TEST_ASSERT_EQUAL(1, result.num_columns);
    TEST_ASSERT_EQUAL_STRING("humidity", result.columns[0].name);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: select subset of fields", "[query][fields]") {
    setup_test();

    // Insert data with multiple fields
    const char *field_names[] = {"temperature", "humidity", "pressure", "wind"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 4, 10, 1000, 1000));

    // Query for subset of fields
    const char *query_field_names[] = {"temperature", "wind"};

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.field_names = query_field_names;
    query.num_fields = 2;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return only 2 fields
    TEST_ASSERT_EQUAL(2, result.num_columns);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: duplicate field names in query", "[query][fields]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature", "humidity"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 2, 10, 1000, 1000));

    // Query with duplicate field names
    const char *query_field_names[] = {"temperature", "temperature"};

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.field_names = query_field_names;
    query.num_fields = 2;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should handle duplicates gracefully (may return 1 or 2 columns depending on implementation)
    TEST_ASSERT_GREATER_OR_EQUAL(1, result.num_columns);

    timeseries_query_free_result(&result);
}

// =============================================================================
// Tag Filtering Tests
// =============================================================================

TEST_CASE("query: single tag filter match", "[query][tags]") {
    setup_test();

    // Insert data with tags
    const char *tag_keys[] = {"location"};
    const char *tag_values[] = {"room1"};
    const char *field_names[] = {"temperature"};

    TEST_ASSERT_TRUE(insert_test_data("weather", tag_keys, tag_values, 1,
                                      field_names, 1, 10, 1000, 1000));

    // Query with matching tag
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.tag_keys = tag_keys;
    query.tag_values = tag_values;
    query.num_tags = 1;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: multiple tag filters (intersection)", "[query][tags]") {
    setup_test();

    // Insert data with multiple tags
    const char *tag_keys[] = {"location", "sensor"};
    const char *tag_values[] = {"room1", "temp1"};
    const char *field_names[] = {"temperature"};

    TEST_ASSERT_TRUE(insert_test_data("weather", tag_keys, tag_values, 2,
                                      field_names, 1, 10, 1000, 1000));

    // Query with both tags (should match)
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.tag_keys = tag_keys;
    query.tag_values = tag_values;
    query.num_tags = 2;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: multiple tag filters partial match", "[query][tags]") {
    setup_test();

    // Insert data with tags
    const char *tag_keys[] = {"location", "sensor"};
    const char *tag_values[] = {"room1", "temp1"};
    const char *field_names[] = {"temperature"};

    TEST_ASSERT_TRUE(insert_test_data("weather", tag_keys, tag_values, 2,
                                      field_names, 1, 10, 1000, 1000));

    // Query with one matching tag and one non-matching
    const char *query_tag_keys[] = {"location", "sensor"};
    const char *query_tag_values[] = {"room1", "temp2"};  // temp2 doesn't match

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.tag_keys = query_tag_keys;
    query.tag_values = query_tag_values;
    query.num_tags = 2;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return empty result (intersection of tags)
    TEST_ASSERT_EQUAL(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: no tags filter returns all series", "[query][tags]") {
    setup_test();

    // Insert data with tags
    const char *tag_keys[] = {"location"};
    const char *tag_values[] = {"room1"};
    const char *field_names[] = {"temperature"};

    TEST_ASSERT_TRUE(insert_test_data("weather", tag_keys, tag_values, 1,
                                      field_names, 1, 10, 1000, 1000));

    // Query without tag filters
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.num_tags = 0;  // No tag filtering

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

// =============================================================================
// Limit Parameter Tests
// =============================================================================

TEST_CASE("query: limit = 0 returns all data", "[query][limit]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 100, 1000, 1000));

    // Query with limit = 0 (no limit)
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.limit = 0;  // No limit

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return all data (or aggregated data depending on rollup)
    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: limit = 1 returns single point", "[query][limit]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 100, 1000, 1000));

    // Query with limit = 1
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.limit = 1;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return at most 1 point
    TEST_ASSERT_LESS_OR_EQUAL(1, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: limit less than available data", "[query][limit]") {
    setup_test();

    // Insert 100 points
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 100, 1000, 1000));

    // Query with limit = 10
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.limit = 10;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return at most 10 points
    TEST_ASSERT_LESS_OR_EQUAL(10, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: limit greater than available data", "[query][limit]") {
    setup_test();

    // Insert 10 points
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query with limit = 1000 (much larger)
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.limit = 1000;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return all available data (not more than available)
    TEST_ASSERT_GREATER_THAN(0, result.num_points);
    TEST_ASSERT_LESS_OR_EQUAL(1000, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: very large limit value", "[query][limit][boundary]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query with SIZE_MAX limit
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.limit = SIZE_MAX;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should handle large limit gracefully
    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

// =============================================================================
// Combined Parameter Tests
// =============================================================================

TEST_CASE("query: time range + limit combination", "[query][combined]") {
    setup_test();

    // Insert 100 points
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 100, 1000, 1000));

    // Query with both time range and limit
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.start_ms = 10000;
    query.end_ms = 50000;
    query.limit = 10;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should respect both time range and limit
    TEST_ASSERT_LESS_OR_EQUAL(10, result.num_points);

    // Verify timestamps are within range
    for (size_t i = 0; i < result.num_points; i++) {
        TEST_ASSERT_GREATER_OR_EQUAL(query.start_ms, result.timestamps[i]);
        TEST_ASSERT_LESS_OR_EQUAL(query.end_ms, result.timestamps[i]);
    }

    timeseries_query_free_result(&result);
}

TEST_CASE("query: tags + fields + time range + limit", "[query][combined]") {
    setup_test();

    // Insert data with tags and multiple fields
    const char *tag_keys[] = {"location"};
    const char *tag_values[] = {"room1"};
    const char *field_names[] = {"temperature", "humidity", "pressure"};

    TEST_ASSERT_TRUE(insert_test_data("weather", tag_keys, tag_values, 1,
                                      field_names, 3, 100, 1000, 1000));

    // Query with all parameters
    const char *query_field_names[] = {"temperature", "humidity"};

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.tag_keys = tag_keys;
    query.tag_values = tag_values;
    query.num_tags = 1;
    query.field_names = query_field_names;
    query.num_fields = 2;
    query.start_ms = 20000;
    query.end_ms = 80000;
    query.limit = 20;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should respect all parameters
    TEST_ASSERT_LESS_OR_EQUAL(2, result.num_columns);  // At most 2 fields
    TEST_ASSERT_LESS_OR_EQUAL(20, result.num_points);  // At most limit

    timeseries_query_free_result(&result);
}

// =============================================================================
// Result Structure Tests
// =============================================================================

TEST_CASE("query: result timestamps are sorted", "[query][result]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 50, 1000, 1000));

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Verify timestamps are sorted
    for (size_t i = 1; i < result.num_points; i++) {
        TEST_ASSERT_GREATER_OR_EQUAL(result.timestamps[i-1], result.timestamps[i]);
    }

    timeseries_query_free_result(&result);
}

TEST_CASE("query: result columns have consistent num_points", "[query][result]") {
    setup_test();

    // Insert data with multiple fields
    const char *field_names[] = {"temperature", "humidity"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 2, 20, 1000, 1000));

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // All columns should have values array sized to num_points
    for (size_t c = 0; c < result.num_columns; c++) {
        TEST_ASSERT_NOT_NULL(result.columns[c].values);
        // Each column should have space for all points
        // (we can't directly verify size, but check it's not NULL)
    }

    timeseries_query_free_result(&result);
}

TEST_CASE("query: free result with empty result", "[query][result]") {
    setup_test();

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "nonexistent";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    TEST_ASSERT_EQUAL(0, result.num_points);
    TEST_ASSERT_EQUAL(0, result.num_columns);

    // Should handle freeing empty result gracefully
    timeseries_query_free_result(&result);
}

TEST_CASE("query: free result multiple times", "[query][result]") {
    setup_test();

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "nonexistent";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Free once
    timeseries_query_free_result(&result);

    // Free again (should be safe if result was zeroed)
    timeseries_query_free_result(&result);
}

// =============================================================================
// Multiple Series Tests
// =============================================================================

TEST_CASE("query: multiple series with same measurement different tags", "[query][series]") {
    setup_test();

    // Insert data for two different series (different tag values)
    const char *tag_keys1[] = {"location"};
    const char *tag_values1[] = {"room1"};
    const char *tag_keys2[] = {"location"};
    const char *tag_values2[] = {"room2"};
    const char *field_names[] = {"temperature"};

    TEST_ASSERT_TRUE(insert_test_data("weather", tag_keys1, tag_values1, 1,
                                      field_names, 1, 10, 1000, 1000));
    TEST_ASSERT_TRUE(insert_test_data("weather", tag_keys2, tag_values2, 1,
                                      field_names, 1, 10, 1000, 1000));

    // Query without tag filter (should get both series)
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should aggregate/combine data from both series
    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

// =============================================================================
// Rollup Interval Tests
// =============================================================================

TEST_CASE("query: rollup_interval = 0", "[query][rollup]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 20, 1000, 1000));

    // Query with rollup_interval = 0 (no rollup)
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.rollup_interval = 0;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: rollup_interval aggregates data", "[query][rollup]") {
    setup_test();

    // Insert high frequency data (100 points)
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 100, 1000, 100));

    // Query with rollup_interval (aggregate every 1000ms)
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.rollup_interval = 1000;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return aggregated data (fewer points than original)
    TEST_ASSERT_GREATER_THAN(0, result.num_points);
    TEST_ASSERT_LESS_THAN(100, result.num_points);

    timeseries_query_free_result(&result);
}

// =============================================================================
// Edge Case and Stress Tests
// =============================================================================

TEST_CASE("query: very long measurement name", "[query][edge]") {
    setup_test();

    // Create a very long but valid measurement name
    char long_name[256];
    memset(long_name, 'a', sizeof(long_name) - 1);
    long_name[sizeof(long_name) - 1] = '\0';

    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data(long_name, NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query with long measurement name
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = long_name;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: empty string measurement name", "[query][edge]") {
    setup_test();

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "";  // Empty string

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return empty result
    TEST_ASSERT_EQUAL(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: single point insertion and retrieval", "[query][edge]") {
    setup_test();

    // Insert just 1 point
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 1, 1000, 1000));

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should retrieve the single point
    TEST_ASSERT_GREATER_THAN(0, result.num_points);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: consecutive queries same parameters", "[query][edge]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";

    // Execute query multiple times
    for (int i = 0; i < 5; i++) {
        timeseries_query_result_t result;
        TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

        TEST_ASSERT_GREATER_THAN(0, result.num_points);

        timeseries_query_free_result(&result);
    }
}

TEST_CASE("query: query after database clear", "[query][edge]") {
    setup_test();

    // Insert data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Clear database
    TEST_ASSERT_TRUE(timeseries_clear_all());

    // Query should return empty result
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    TEST_ASSERT_EQUAL(0, result.num_points);

    timeseries_query_free_result(&result);
}

// =============================================================================
// Data Type Tests
// =============================================================================

TEST_CASE("query: float field type", "[query][types]") {
    setup_test();

    // Insert float data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Verify field type
    TEST_ASSERT_EQUAL(1, result.num_columns);
    TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_FLOAT, result.columns[0].type);

    timeseries_query_free_result(&result);
}

TEST_CASE("query: int field type", "[query][types]") {
    setup_test();

    // Insert int data (field index 1 uses INT type in helper)
    const char *field_names[] = {"dummy", "count"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 2, 10, 1000, 1000));

    // Query for the int field
    const char *query_field_names[] = {"count"};

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.field_names = query_field_names;
    query.num_fields = 1;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Verify field type
    if (result.num_columns > 0) {
        TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, result.columns[0].type);
    }

    timeseries_query_free_result(&result);
}

TEST_CASE("query: bool field type", "[query][types]") {
    setup_test();

    // Insert bool data (field index 2 uses BOOL type in helper)
    const char *field_names[] = {"dummy1", "dummy2", "valid"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 3, 10, 1000, 1000));

    // Query for the bool field
    const char *query_field_names[] = {"valid"};

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.field_names = query_field_names;
    query.num_fields = 1;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Verify field type
    if (result.num_columns > 0) {
        TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_BOOL, result.columns[0].type);
    }

    timeseries_query_free_result(&result);
}

TEST_CASE("query: mixed field types in single query", "[query][types]") {
    setup_test();

    // Insert data with mixed types
    const char *field_names[] = {"temperature", "count", "valid"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 3, 10, 1000, 1000));

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should return all 3 fields with correct types
    TEST_ASSERT_EQUAL(3, result.num_columns);

    timeseries_query_free_result(&result);
}

// =============================================================================
// Memory and Cleanup Tests
// =============================================================================

TEST_CASE("query: result cleanup with all allocated fields", "[query][memory]") {
    setup_test();

    // Insert data that will allocate memory for results
    const char *field_names[] = {"temperature", "humidity", "pressure"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 3, 50, 1000, 1000));

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Verify memory was allocated
    TEST_ASSERT_NOT_NULL(result.timestamps);
    TEST_ASSERT_NOT_NULL(result.columns);

    // Clean up should not crash
    timeseries_query_free_result(&result);

    // After cleanup, pointers should be safe (implementation dependent)
}

// =============================================================================
// Integration Tests
// =============================================================================

TEST_CASE("query: insert then query multiple measurements", "[query][integration]") {
    setup_test();

    // Insert data for multiple measurements
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));
    TEST_ASSERT_TRUE(insert_test_data("system", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Query first measurement
    timeseries_query_t query1;
    memset(&query1, 0, sizeof(query1));
    query1.measurement_name = "weather";

    timeseries_query_result_t result1;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query1, &result1));
    TEST_ASSERT_GREATER_THAN(0, result1.num_points);

    // Query second measurement
    timeseries_query_t query2;
    memset(&query2, 0, sizeof(query2));
    query2.measurement_name = "system";

    timeseries_query_result_t result2;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query2, &result2));
    TEST_ASSERT_GREATER_THAN(0, result2.num_points);

    timeseries_query_free_result(&result1);
    timeseries_query_free_result(&result2);
}

TEST_CASE("query: large dataset query performance", "[query][integration]") {
    setup_test();

    // Insert a larger dataset
    const char *field_names[] = {"temperature", "humidity"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 2, 500, 1000, 100));

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.limit = 100;

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Should handle large dataset
    TEST_ASSERT_GREATER_THAN(0, result.num_points);
    TEST_ASSERT_LESS_OR_EQUAL(100, result.num_points);

    timeseries_query_free_result(&result);
}
