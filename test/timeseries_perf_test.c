/**
 * @file timeseries_perf_test.c
 * @brief Performance tests for the ESP-IDF Timeseries database
 *
 * These tests are designed to run in QEMU and measure performance characteristics
 * of various database operations to guide optimization efforts.
 */

#include "esp_log.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "unity.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>

// Include gorilla compression headers for codec testing
#include "gorilla/float_stream_decoder.h"
#include "gorilla/float_stream_encoder.h"
#include "gorilla/integer_stream_decoder.h"
#include "gorilla/integer_stream_encoder.h"
#include "gorilla/gorilla_stream_types.h"

static const char *TAG = "perf_test";

// ============================================================================
// Helper Macros and Utilities
// ============================================================================

#define PERF_LOG(test_name, metric, value, unit) \
    ESP_LOGI(TAG, "PERF [%s] %s: %lld %s", test_name, metric, (long long)(value), unit)

#define PERF_LOG_FLOAT(test_name, metric, value, unit) \
    ESP_LOGI(TAG, "PERF [%s] %s: %.3f %s", test_name, metric, (double)(value), unit)

// Helper to measure elapsed time in microseconds
static int64_t get_elapsed_us(int64_t start, int64_t end) {
    return end - start;
}

// Helper to ensure the database is in a known empty state
static void clear_database(void) {
    TEST_ASSERT_TRUE_MESSAGE(timeseries_clear_all(),
                             "Failed to clear database to empty state");
}

// ============================================================================
// Test Setup and Teardown
// ============================================================================

// Initialize the database before running performance tests
TEST_CASE("perf: initialize database", "[perf][setup]") {
    TEST_ASSERT_TRUE_MESSAGE(timeseries_init(),
                             "Failed to initialize timeseries database");
    ESP_LOGI(TAG, "Database initialized for performance testing");
}

// Clear the database before each performance test
TEST_CASE("perf: clear database", "[perf][setup]") {
    clear_database();
    ESP_LOGI(TAG, "Database cleared to empty state");
}

// ============================================================================
// INSERT PERFORMANCE TESTS
// ============================================================================

TEST_CASE("perf: single-point insert performance", "[perf][insert]") {
    clear_database();

    const char *tags_keys[] = {"location"};
    const char *tags_values[] = {"sensor_1"};
    const char *field_names[] = {"temperature"};
    const size_t NUM_INSERTS = 100;

    int64_t total_time = 0;
    int64_t min_time = INT64_MAX;
    int64_t max_time = 0;

    for (size_t i = 0; i < NUM_INSERTS; i++) {
        uint64_t timestamp = 1000 * i;
        timeseries_field_value_t field_value = {
            .type = TIMESERIES_FIELD_TYPE_FLOAT,
            .data.float_val = 20.0 + (double)i * 0.1
        };

        timeseries_insert_data_t insert_data = {
            .measurement_name = "metrics",
            .tag_keys = tags_keys,
            .tag_values = tags_values,
            .num_tags = 1,
            .field_names = field_names,
            .field_values = &field_value,
            .num_fields = 1,
            .timestamps_ms = &timestamp,
            .num_points = 1,
        };

        int64_t start = esp_timer_get_time();
        TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
        int64_t end = esp_timer_get_time();

        int64_t elapsed = get_elapsed_us(start, end);
        total_time += elapsed;
        if (elapsed < min_time) min_time = elapsed;
        if (elapsed > max_time) max_time = elapsed;
    }

    PERF_LOG("single_point_insert", "total_inserts", NUM_INSERTS, "ops");
    PERF_LOG("single_point_insert", "total_time", total_time, "us");
    PERF_LOG("single_point_insert", "avg_time", total_time / NUM_INSERTS, "us/op");
    PERF_LOG("single_point_insert", "min_time", min_time, "us");
    PERF_LOG("single_point_insert", "max_time", max_time, "us");
    PERF_LOG_FLOAT("single_point_insert", "throughput",
                   (double)NUM_INSERTS * 1000000.0 / total_time, "ops/s");
}

TEST_CASE("perf: bulk insert 100 points", "[perf][insert]") {
    clear_database();

    const size_t NUM_POINTS = 100;
    const char *tags_keys[] = {"location", "device"};
    const char *tags_values[] = {"home", "sensor_001"};
    const char *field_names[] = {"value"};

    uint64_t *timestamps = malloc(NUM_POINTS * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(NUM_POINTS * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(field_values);

    for (size_t i = 0; i < NUM_POINTS; i++) {
        timestamps[i] = 1000 * i;
        field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[i].data.float_val = 25.0 + sinf((float)i * 0.1f) * 5.0f;
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "bulk_test",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = 2,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = NUM_POINTS,
    };

    int64_t start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    PERF_LOG("bulk_insert_100", "total_points", NUM_POINTS, "points");
    PERF_LOG("bulk_insert_100", "total_time", elapsed, "us");
    PERF_LOG("bulk_insert_100", "time_per_point", elapsed / NUM_POINTS, "us/point");
    PERF_LOG_FLOAT("bulk_insert_100", "throughput",
                   (double)NUM_POINTS * 1000000.0 / elapsed, "points/s");

    free(timestamps);
    free(field_values);
}

TEST_CASE("perf: bulk insert 1000 points", "[perf][insert]") {
    clear_database();

    const size_t NUM_POINTS = 1000;
    const char *tags_keys[] = {"location"};
    const char *tags_values[] = {"lab"};
    const char *field_names[] = {"measurement"};

    uint64_t *timestamps = malloc(NUM_POINTS * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(NUM_POINTS * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(field_values);

    for (size_t i = 0; i < NUM_POINTS; i++) {
        timestamps[i] = 1000 * i;
        field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[i].data.float_val = (double)i * 0.01;
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "large_bulk_test",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = 1,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = NUM_POINTS,
    };

    int64_t start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    PERF_LOG("bulk_insert_1000", "total_points", NUM_POINTS, "points");
    PERF_LOG("bulk_insert_1000", "total_time", elapsed, "us");
    PERF_LOG("bulk_insert_1000", "time_per_point", elapsed / NUM_POINTS, "us/point");
    PERF_LOG_FLOAT("bulk_insert_1000", "throughput",
                   (double)NUM_POINTS * 1000000.0 / elapsed, "points/s");

    free(timestamps);
    free(field_values);
}

TEST_CASE("perf: multi-field insert (6 fields)", "[perf][insert]") {
    clear_database();

    const size_t NUM_POINTS = 288; // One day at 5-minute intervals
    const size_t NUM_FIELDS = 6;
    const char *tags_keys[] = {"system_id"};
    const char *tags_values[] = {"solar_001"};
    const char *field_names[] = {
        "grid_import_w", "grid_export_w", "solar_w",
        "battery_charge_w", "battery_discharge_w", "house_w"
    };

    uint64_t *timestamps = malloc(NUM_POINTS * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(NUM_FIELDS * NUM_POINTS * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(field_values);

    // Generate solar-like data
    for (size_t i = 0; i < NUM_POINTS; i++) {
        timestamps[i] = 300000 * i; // 5-minute intervals
        float hour = (float)(i % 288) / 12.0f; // Hour of day
        float solar_factor = (hour >= 6.0f && hour <= 18.0f) ?
                             sinf((hour - 6.0f) * 3.14159f / 12.0f) : 0.0f;

        for (size_t f = 0; f < NUM_FIELDS; f++) {
            size_t idx = f * NUM_POINTS + i;
            field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
            field_values[idx].data.float_val = 1000.0f * solar_factor + (float)(rand() % 100);
        }
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "solar",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = 1,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = NUM_FIELDS,
        .timestamps_ms = timestamps,
        .num_points = NUM_POINTS,
    };

    int64_t start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    size_t total_values = NUM_POINTS * NUM_FIELDS;
    PERF_LOG("multi_field_insert", "total_points", NUM_POINTS, "points");
    PERF_LOG("multi_field_insert", "num_fields", NUM_FIELDS, "fields");
    PERF_LOG("multi_field_insert", "total_values", total_values, "values");
    PERF_LOG("multi_field_insert", "total_time", elapsed, "us");
    PERF_LOG("multi_field_insert", "time_per_point", elapsed / NUM_POINTS, "us/point");
    PERF_LOG_FLOAT("multi_field_insert", "throughput",
                   (double)total_values * 1000000.0 / elapsed, "values/s");

    free(timestamps);
    free(field_values);
}

TEST_CASE("perf: integer field insert", "[perf][insert]") {
    clear_database();

    const size_t NUM_POINTS = 500;
    const char *tags_keys[] = {"counter_id"};
    const char *tags_values[] = {"counter_001"};
    const char *field_names[] = {"count"};

    uint64_t *timestamps = malloc(NUM_POINTS * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(NUM_POINTS * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(field_values);

    for (size_t i = 0; i < NUM_POINTS; i++) {
        timestamps[i] = 1000 * i;
        field_values[i].type = TIMESERIES_FIELD_TYPE_INT;
        field_values[i].data.int_val = (int64_t)i * 10;
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "counters",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = 1,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = NUM_POINTS,
    };

    int64_t start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    PERF_LOG("integer_insert", "total_points", NUM_POINTS, "points");
    PERF_LOG("integer_insert", "total_time", elapsed, "us");
    PERF_LOG("integer_insert", "time_per_point", elapsed / NUM_POINTS, "us/point");
    PERF_LOG_FLOAT("integer_insert", "throughput",
                   (double)NUM_POINTS * 1000000.0 / elapsed, "points/s");

    free(timestamps);
    free(field_values);
}

// ============================================================================
// QUERY PERFORMANCE TESTS
// ============================================================================

// Helper to populate database for query tests
static void populate_for_query_tests(size_t num_points, size_t num_fields) {
    clear_database();

    const char *tags_keys[] = {"location"};
    const char *tags_values[] = {"test_site"};
    const char *field_names_1[] = {"temperature"};
    const char *field_names_6[] = {"f1", "f2", "f3", "f4", "f5", "f6"};
    const char **field_names = (num_fields == 1) ? field_names_1 : field_names_6;

    uint64_t *timestamps = malloc(num_points * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(num_fields * num_points * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(field_values);

    for (size_t i = 0; i < num_points; i++) {
        timestamps[i] = 1000 * i;
        for (size_t f = 0; f < num_fields; f++) {
            size_t idx = f * num_points + i;
            field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
            field_values[idx].data.float_val = (double)i + (double)f * 0.1;
        }
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "query_test",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = 1,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = num_fields,
        .timestamps_ms = timestamps,
        .num_points = num_points,
    };

    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
    TEST_ASSERT_TRUE(timeseries_compact());

    free(timestamps);
    free(field_values);
}

TEST_CASE("perf: query 100 points single field", "[perf][query]") {
    populate_for_query_tests(100, 1);

    timeseries_query_t query = {
        .measurement_name = "query_test",
        .tag_keys = NULL,
        .tag_values = NULL,
        .num_tags = 0,
        .field_names = NULL,
        .num_fields = 0,
        .start_ms = 0,
        .end_ms = INT64_MAX,
        .limit = 0,
        .rollup_interval = 0,
    };

    timeseries_query_result_t result;
    memset(&result, 0, sizeof(result));

    int64_t start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_query(&query, &result));
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    PERF_LOG("query_100_1field", "points_returned", result.num_points, "points");
    PERF_LOG("query_100_1field", "columns_returned", result.num_columns, "columns");
    PERF_LOG("query_100_1field", "query_time", elapsed, "us");
    PERF_LOG_FLOAT("query_100_1field", "throughput",
                   (double)result.num_points * 1000000.0 / elapsed, "points/s");

    TEST_ASSERT_EQUAL(100, result.num_points);
    timeseries_query_free_result(&result);
}

TEST_CASE("perf: query 1000 points single field", "[perf][query]") {
    populate_for_query_tests(1000, 1);

    timeseries_query_t query = {
        .measurement_name = "query_test",
        .tag_keys = NULL,
        .tag_values = NULL,
        .num_tags = 0,
        .field_names = NULL,
        .num_fields = 0,
        .start_ms = 0,
        .end_ms = INT64_MAX,
        .limit = 0,
        .rollup_interval = 0,
    };

    timeseries_query_result_t result;
    memset(&result, 0, sizeof(result));

    int64_t start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_query(&query, &result));
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    PERF_LOG("query_1000_1field", "points_returned", result.num_points, "points");
    PERF_LOG("query_1000_1field", "query_time", elapsed, "us");
    PERF_LOG_FLOAT("query_1000_1field", "throughput",
                   (double)result.num_points * 1000000.0 / elapsed, "points/s");

    TEST_ASSERT_EQUAL(1000, result.num_points);
    timeseries_query_free_result(&result);
}

TEST_CASE("perf: query 500 points 6 fields", "[perf][query]") {
    populate_for_query_tests(500, 6);

    timeseries_query_t query = {
        .measurement_name = "query_test",
        .tag_keys = NULL,
        .tag_values = NULL,
        .num_tags = 0,
        .field_names = NULL,
        .num_fields = 0,
        .start_ms = 0,
        .end_ms = INT64_MAX,
        .limit = 0,
        .rollup_interval = 0,
    };

    timeseries_query_result_t result;
    memset(&result, 0, sizeof(result));

    int64_t start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_query(&query, &result));
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    size_t total_values = result.num_points * result.num_columns;
    PERF_LOG("query_500_6fields", "points_returned", result.num_points, "points");
    PERF_LOG("query_500_6fields", "columns_returned", result.num_columns, "columns");
    PERF_LOG("query_500_6fields", "total_values", total_values, "values");
    PERF_LOG("query_500_6fields", "query_time", elapsed, "us");
    PERF_LOG_FLOAT("query_500_6fields", "throughput",
                   (double)total_values * 1000000.0 / elapsed, "values/s");

    TEST_ASSERT_EQUAL(500, result.num_points);
    TEST_ASSERT_EQUAL(6, result.num_columns);
    timeseries_query_free_result(&result);
}

TEST_CASE("perf: query time-range subset", "[perf][query]") {
    populate_for_query_tests(1000, 1);

    // Query middle 50% of data
    timeseries_query_t query = {
        .measurement_name = "query_test",
        .tag_keys = NULL,
        .tag_values = NULL,
        .num_tags = 0,
        .field_names = NULL,
        .num_fields = 0,
        .start_ms = 250000,  // Start at point 250
        .end_ms = 750000,    // End at point 750
        .limit = 0,
        .rollup_interval = 0,
    };

    timeseries_query_result_t result;
    memset(&result, 0, sizeof(result));

    int64_t start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_query(&query, &result));
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    PERF_LOG("query_time_range", "points_returned", result.num_points, "points");
    PERF_LOG("query_time_range", "query_time", elapsed, "us");
    PERF_LOG_FLOAT("query_time_range", "throughput",
                   (double)result.num_points * 1000000.0 / elapsed, "points/s");

    // Should return approximately 501 points (250 to 750 inclusive)
    TEST_ASSERT_TRUE(result.num_points >= 400 && result.num_points <= 600);
    timeseries_query_free_result(&result);
}

TEST_CASE("perf: repeated queries", "[perf][query]") {
    populate_for_query_tests(500, 1);

    const size_t NUM_QUERIES = 10;
    int64_t query_times[NUM_QUERIES];
    int64_t total_time = 0;

    timeseries_query_t query = {
        .measurement_name = "query_test",
        .tag_keys = NULL,
        .tag_values = NULL,
        .num_tags = 0,
        .field_names = NULL,
        .num_fields = 0,
        .start_ms = 0,
        .end_ms = INT64_MAX,
        .limit = 0,
        .rollup_interval = 0,
    };

    for (size_t i = 0; i < NUM_QUERIES; i++) {
        timeseries_query_result_t result;
        memset(&result, 0, sizeof(result));

        int64_t start = esp_timer_get_time();
        TEST_ASSERT_TRUE(timeseries_query(&query, &result));
        int64_t end = esp_timer_get_time();

        query_times[i] = get_elapsed_us(start, end);
        total_time += query_times[i];
        TEST_ASSERT_EQUAL(500, result.num_points);
        timeseries_query_free_result(&result);
    }

    int64_t min_time = query_times[0];
    int64_t max_time = query_times[0];
    for (size_t i = 1; i < NUM_QUERIES; i++) {
        if (query_times[i] < min_time) min_time = query_times[i];
        if (query_times[i] > max_time) max_time = query_times[i];
    }

    PERF_LOG("repeated_queries", "num_queries", NUM_QUERIES, "queries");
    PERF_LOG("repeated_queries", "total_time", total_time, "us");
    PERF_LOG("repeated_queries", "avg_query_time", total_time / NUM_QUERIES, "us");
    PERF_LOG("repeated_queries", "min_query_time", min_time, "us");
    PERF_LOG("repeated_queries", "max_query_time", max_time, "us");
}

// ============================================================================
// COMPACTION PERFORMANCE TESTS
// ============================================================================

TEST_CASE("perf: compaction 500 points", "[perf][compaction]") {
    clear_database();

    // Insert data that will need compaction
    const size_t NUM_POINTS = 500;
    const char *tags_keys[] = {"source"};
    const char *tags_values[] = {"compaction_test"};
    const char *field_names[] = {"value"};

    uint64_t *timestamps = malloc(NUM_POINTS * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(NUM_POINTS * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(field_values);

    for (size_t i = 0; i < NUM_POINTS; i++) {
        timestamps[i] = 1000 * i;
        field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[i].data.float_val = (double)i;
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "compact_test",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = 1,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = NUM_POINTS,
    };

    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

    int64_t start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_compact());
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    PERF_LOG("compaction_500", "points_compacted", NUM_POINTS, "points");
    PERF_LOG("compaction_500", "compaction_time", elapsed, "us");
    PERF_LOG_FLOAT("compaction_500", "throughput",
                   (double)NUM_POINTS * 1000000.0 / elapsed, "points/s");

    free(timestamps);
    free(field_values);
}

TEST_CASE("perf: compaction 2000 points", "[perf][compaction]") {
    clear_database();

    const size_t NUM_POINTS = 2000;
    const char *tags_keys[] = {"source"};
    const char *tags_values[] = {"large_compaction_test"};
    const char *field_names[] = {"measurement"};

    uint64_t *timestamps = malloc(NUM_POINTS * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(NUM_POINTS * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(field_values);

    for (size_t i = 0; i < NUM_POINTS; i++) {
        timestamps[i] = 1000 * i;
        field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[i].data.float_val = sinf((float)i * 0.01f) * 100.0f;
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "large_compact_test",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = 1,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = NUM_POINTS,
    };

    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

    int64_t start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_compact());
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    PERF_LOG("compaction_2000", "points_compacted", NUM_POINTS, "points");
    PERF_LOG("compaction_2000", "compaction_time", elapsed, "us");
    PERF_LOG_FLOAT("compaction_2000", "throughput",
                   (double)NUM_POINTS * 1000000.0 / elapsed, "points/s");

    free(timestamps);
    free(field_values);
}

TEST_CASE("perf: incremental compaction", "[perf][compaction]") {
    clear_database();

    const size_t BATCH_SIZE = 100;
    const size_t NUM_BATCHES = 5;
    const char *tags_keys[] = {"source"};
    const char *tags_values[] = {"incremental_test"};
    const char *field_names[] = {"value"};

    int64_t total_insert_time = 0;
    int64_t total_compact_time = 0;

    for (size_t batch = 0; batch < NUM_BATCHES; batch++) {
        uint64_t *timestamps = malloc(BATCH_SIZE * sizeof(uint64_t));
        timeseries_field_value_t *field_values = malloc(BATCH_SIZE * sizeof(timeseries_field_value_t));
        TEST_ASSERT_NOT_NULL(timestamps);
        TEST_ASSERT_NOT_NULL(field_values);

        for (size_t i = 0; i < BATCH_SIZE; i++) {
            timestamps[i] = (batch * BATCH_SIZE + i) * 1000;
            field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
            field_values[i].data.float_val = (double)(batch * BATCH_SIZE + i);
        }

        timeseries_insert_data_t insert_data = {
            .measurement_name = "incremental_test",
            .tag_keys = tags_keys,
            .tag_values = tags_values,
            .num_tags = 1,
            .field_names = field_names,
            .field_values = field_values,
            .num_fields = 1,
            .timestamps_ms = timestamps,
            .num_points = BATCH_SIZE,
        };

        int64_t insert_start = esp_timer_get_time();
        TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
        int64_t insert_end = esp_timer_get_time();
        total_insert_time += get_elapsed_us(insert_start, insert_end);

        int64_t compact_start = esp_timer_get_time();
        TEST_ASSERT_TRUE(timeseries_compact());
        int64_t compact_end = esp_timer_get_time();
        total_compact_time += get_elapsed_us(compact_start, compact_end);

        free(timestamps);
        free(field_values);
    }

    size_t total_points = BATCH_SIZE * NUM_BATCHES;
    PERF_LOG("incremental_compact", "total_points", total_points, "points");
    PERF_LOG("incremental_compact", "num_batches", NUM_BATCHES, "batches");
    PERF_LOG("incremental_compact", "total_insert_time", total_insert_time, "us");
    PERF_LOG("incremental_compact", "total_compact_time", total_compact_time, "us");
    PERF_LOG("incremental_compact", "avg_insert_time", total_insert_time / NUM_BATCHES, "us/batch");
    PERF_LOG("incremental_compact", "avg_compact_time", total_compact_time / NUM_BATCHES, "us/batch");
}

TEST_CASE("perf: compaction 20 series", "[perf][compaction][multiseries]") {
    clear_database();

    // Multi-series compaction test: 20 unique series, 50 points each = 1000 total points
    const size_t NUM_SERIES = 20;
    const size_t POINTS_PER_SERIES = 50;
    const size_t TOTAL_POINTS = NUM_SERIES * POINTS_PER_SERIES;

    int64_t insert_start = esp_timer_get_time();

    // Insert data for each series
    for (size_t s = 0; s < NUM_SERIES; s++) {
        char tag_value[32];
        snprintf(tag_value, sizeof(tag_value), "device_%zu", s);

        const char *tags_keys[] = {"device_id"};
        const char *tags_values[] = {tag_value};
        const char *field_names[] = {"temperature"};

        uint64_t *timestamps = malloc(POINTS_PER_SERIES * sizeof(uint64_t));
        timeseries_field_value_t *field_values = malloc(POINTS_PER_SERIES * sizeof(timeseries_field_value_t));
        TEST_ASSERT_NOT_NULL(timestamps);
        TEST_ASSERT_NOT_NULL(field_values);

        for (size_t i = 0; i < POINTS_PER_SERIES; i++) {
            // Interleave timestamps across series to simulate real-world patterns
            timestamps[i] = (i * NUM_SERIES + s) * 1000;
            field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
            field_values[i].data.float_val = 20.0 + (double)s + sinf((float)i * 0.1f) * 5.0f;
        }

        timeseries_insert_data_t insert_data = {
            .measurement_name = "multi_series_compact",
            .tag_keys = tags_keys,
            .tag_values = tags_values,
            .num_tags = 1,
            .field_names = field_names,
            .field_values = field_values,
            .num_fields = 1,
            .timestamps_ms = timestamps,
            .num_points = POINTS_PER_SERIES,
        };

        TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

        free(timestamps);
        free(field_values);
    }

    int64_t insert_end = esp_timer_get_time();
    int64_t insert_time = get_elapsed_us(insert_start, insert_end);

    // Now compact
    int64_t compact_start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_compact());
    int64_t compact_end = esp_timer_get_time();

    int64_t compact_time = get_elapsed_us(compact_start, compact_end);

    PERF_LOG("compact_20_series", "num_series", NUM_SERIES, "series");
    PERF_LOG("compact_20_series", "points_per_series", POINTS_PER_SERIES, "points");
    PERF_LOG("compact_20_series", "total_points", TOTAL_POINTS, "points");
    PERF_LOG("compact_20_series", "insert_time", insert_time, "us");
    PERF_LOG("compact_20_series", "compaction_time", compact_time, "us");
    PERF_LOG_FLOAT("compact_20_series", "insert_throughput",
                   (double)TOTAL_POINTS * 1000000.0 / insert_time, "points/s");
    PERF_LOG_FLOAT("compact_20_series", "compact_throughput",
                   (double)TOTAL_POINTS * 1000000.0 / compact_time, "points/s");
}

// ============================================================================
// COMPRESSION CODEC PERFORMANCE TESTS
// ============================================================================

// Buffer for compression testing
typedef struct {
    uint8_t *data;
    size_t size;
    size_t capacity;
} TestBuffer;

static bool test_flush_callback(const uint8_t *data, size_t len, void *ctx) {
    TestBuffer *buf = (TestBuffer *)ctx;
    if (buf->size + len > buf->capacity) {
        size_t new_capacity = (buf->capacity == 0) ? 1024 : buf->capacity * 2;
        while (new_capacity < buf->size + len) {
            new_capacity *= 2;
        }
        uint8_t *new_data = realloc(buf->data, new_capacity);
        if (!new_data) return false;
        buf->data = new_data;
        buf->capacity = new_capacity;
    }
    memcpy(buf->data + buf->size, data, len);
    buf->size += len;
    return true;
}

typedef struct {
    const uint8_t *data;
    size_t size;
    size_t offset;
} TestReadBuffer;

// Fill callback matching FillCallback/gorilla_decoder_fill_cb signature
static bool test_fill_callback(void *ctx, uint8_t *buffer, size_t max_len, size_t *filled) {
    TestReadBuffer *rb = (TestReadBuffer *)ctx;
    size_t remaining = rb->size - rb->offset;
    size_t to_read = (remaining < max_len) ? remaining : max_len;
    if (to_read > 0) {
        memcpy(buffer, rb->data + rb->offset, to_read);
        rb->offset += to_read;
    }
    *filled = to_read;
    return (to_read > 0 || remaining == 0);  // Return true even at EOF if no error
}

TEST_CASE("perf: float compression encode 1000 values", "[perf][codec]") {
    const size_t NUM_VALUES = 1000;
    double *values = malloc(NUM_VALUES * sizeof(double));
    TEST_ASSERT_NOT_NULL(values);

    // Generate realistic temperature-like data
    for (size_t i = 0; i < NUM_VALUES; i++) {
        values[i] = 20.0 + sinf((float)i * 0.01f) * 5.0 + ((float)(rand() % 100) / 100.0f);
    }

    TestBuffer buf = {0};
    FloatStreamEncoder *encoder = float_stream_encoder_create(test_flush_callback, &buf);
    TEST_ASSERT_NOT_NULL(encoder);

    int64_t start = esp_timer_get_time();
    for (size_t i = 0; i < NUM_VALUES; i++) {
        TEST_ASSERT_TRUE(float_stream_encoder_add_value(encoder, values[i]));
    }
    TEST_ASSERT_TRUE(float_stream_encoder_finish(encoder));
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    size_t original_size = NUM_VALUES * sizeof(double);
    float compression_ratio = (float)original_size / (float)buf.size;

    PERF_LOG("float_encode", "num_values", NUM_VALUES, "values");
    PERF_LOG("float_encode", "encode_time", elapsed, "us");
    PERF_LOG("float_encode", "original_size", original_size, "bytes");
    PERF_LOG("float_encode", "compressed_size", buf.size, "bytes");
    PERF_LOG_FLOAT("float_encode", "compression_ratio", compression_ratio, "x");
    PERF_LOG_FLOAT("float_encode", "throughput",
                   (double)NUM_VALUES * 1000000.0 / elapsed, "values/s");

    float_stream_encoder_destroy(encoder);
    free(buf.data);
    free(values);
}

TEST_CASE("perf: float compression decode 1000 values", "[perf][codec]") {
    const size_t NUM_VALUES = 1000;
    double *values = malloc(NUM_VALUES * sizeof(double));
    TEST_ASSERT_NOT_NULL(values);

    // Generate and encode data first
    for (size_t i = 0; i < NUM_VALUES; i++) {
        values[i] = 20.0 + sinf((float)i * 0.01f) * 5.0;
    }

    TestBuffer buf = {0};
    FloatStreamEncoder *encoder = float_stream_encoder_create(test_flush_callback, &buf);
    TEST_ASSERT_NOT_NULL(encoder);
    for (size_t i = 0; i < NUM_VALUES; i++) {
        TEST_ASSERT_TRUE(float_stream_encoder_add_value(encoder, values[i]));
    }
    TEST_ASSERT_TRUE(float_stream_encoder_finish(encoder));
    float_stream_encoder_destroy(encoder);

    // Now decode and measure time
    TestReadBuffer rb = { .data = buf.data, .size = buf.size, .offset = 0 };
    FloatStreamDecoder *decoder = float_stream_decoder_create((FillCallback)test_fill_callback, &rb);
    TEST_ASSERT_NOT_NULL(decoder);

    double *decoded = malloc(NUM_VALUES * sizeof(double));
    TEST_ASSERT_NOT_NULL(decoded);

    int64_t start = esp_timer_get_time();
    for (size_t i = 0; i < NUM_VALUES; i++) {
        TEST_ASSERT_TRUE(float_stream_decoder_get_value(decoder, &decoded[i]));
    }
    int64_t end = esp_timer_get_time();

    // Verify correctness
    for (size_t i = 0; i < NUM_VALUES; i++) {
        TEST_ASSERT_DOUBLE_WITHIN(0.0001, values[i], decoded[i]);
    }

    int64_t elapsed = get_elapsed_us(start, end);
    PERF_LOG("float_decode", "num_values", NUM_VALUES, "values");
    PERF_LOG("float_decode", "decode_time", elapsed, "us");
    PERF_LOG_FLOAT("float_decode", "throughput",
                   (double)NUM_VALUES * 1000000.0 / elapsed, "values/s");

    float_stream_decoder_destroy(decoder);
    free(decoded);
    free(buf.data);
    free(values);
}

TEST_CASE("perf: integer compression encode 1000 values", "[perf][codec]") {
    const size_t NUM_VALUES = 1000;
    uint64_t *values = malloc(NUM_VALUES * sizeof(uint64_t));
    TEST_ASSERT_NOT_NULL(values);

    // Generate monotonic timestamp-like data
    for (size_t i = 0; i < NUM_VALUES; i++) {
        values[i] = 1700000000000ULL + i * 5000; // 5-second intervals
    }

    TestBuffer buf = {0};
    IntegerStreamEncoder *encoder = integer_stream_encoder_create(test_flush_callback, &buf);
    TEST_ASSERT_NOT_NULL(encoder);

    int64_t start = esp_timer_get_time();
    for (size_t i = 0; i < NUM_VALUES; i++) {
        TEST_ASSERT_TRUE(integer_stream_encoder_add_value(encoder, values[i]));
    }
    TEST_ASSERT_TRUE(integer_stream_encoder_finish(encoder));
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    size_t original_size = NUM_VALUES * sizeof(uint64_t);
    float compression_ratio = (float)original_size / (float)buf.size;

    PERF_LOG("int_encode", "num_values", NUM_VALUES, "values");
    PERF_LOG("int_encode", "encode_time", elapsed, "us");
    PERF_LOG("int_encode", "original_size", original_size, "bytes");
    PERF_LOG("int_encode", "compressed_size", buf.size, "bytes");
    PERF_LOG_FLOAT("int_encode", "compression_ratio", compression_ratio, "x");
    PERF_LOG_FLOAT("int_encode", "throughput",
                   (double)NUM_VALUES * 1000000.0 / elapsed, "values/s");

    integer_stream_encoder_destroy(encoder);
    free(buf.data);
    free(values);
}

TEST_CASE("perf: integer compression decode 1000 values", "[perf][codec]") {
    const size_t NUM_VALUES = 1000;
    uint64_t *values = malloc(NUM_VALUES * sizeof(uint64_t));
    TEST_ASSERT_NOT_NULL(values);

    // Generate and encode data first
    for (size_t i = 0; i < NUM_VALUES; i++) {
        values[i] = 1700000000000ULL + i * 5000;
    }

    TestBuffer buf = {0};
    IntegerStreamEncoder *encoder = integer_stream_encoder_create(test_flush_callback, &buf);
    TEST_ASSERT_NOT_NULL(encoder);
    for (size_t i = 0; i < NUM_VALUES; i++) {
        TEST_ASSERT_TRUE(integer_stream_encoder_add_value(encoder, values[i]));
    }
    TEST_ASSERT_TRUE(integer_stream_encoder_finish(encoder));
    integer_stream_encoder_destroy(encoder);

    // Now decode and measure time
    TestReadBuffer rb = { .data = buf.data, .size = buf.size, .offset = 0 };
    IntegerStreamDecoder *decoder = integer_stream_decoder_create((gorilla_decoder_fill_cb)test_fill_callback, &rb);
    TEST_ASSERT_NOT_NULL(decoder);

    uint64_t *decoded = malloc(NUM_VALUES * sizeof(uint64_t));
    TEST_ASSERT_NOT_NULL(decoded);

    int64_t start = esp_timer_get_time();
    for (size_t i = 0; i < NUM_VALUES; i++) {
        TEST_ASSERT_TRUE(integer_stream_decoder_get_value(decoder, &decoded[i]));
    }
    int64_t end = esp_timer_get_time();

    // Verify correctness
    for (size_t i = 0; i < NUM_VALUES; i++) {
        TEST_ASSERT_EQUAL_UINT64(values[i], decoded[i]);
    }

    int64_t elapsed = get_elapsed_us(start, end);
    PERF_LOG("int_decode", "num_values", NUM_VALUES, "values");
    PERF_LOG("int_decode", "decode_time", elapsed, "us");
    PERF_LOG_FLOAT("int_decode", "throughput",
                   (double)NUM_VALUES * 1000000.0 / elapsed, "values/s");

    integer_stream_decoder_destroy(decoder);
    free(decoded);
    free(buf.data);
    free(values);
}

// ============================================================================
// METADATA OPERATIONS PERFORMANCE TESTS
// ============================================================================

TEST_CASE("perf: series creation overhead", "[perf][metadata]") {
    clear_database();

    const size_t NUM_SERIES = 20;
    int64_t creation_times[NUM_SERIES];
    int64_t total_time = 0;

    for (size_t s = 0; s < NUM_SERIES; s++) {
        char tag_value[32];
        snprintf(tag_value, sizeof(tag_value), "device_%zu", s);

        const char *tags_keys[] = {"device_id"};
        const char *tags_values[] = {tag_value};
        const char *field_names[] = {"value"};

        uint64_t timestamp = 1000;
        timeseries_field_value_t field_value = {
            .type = TIMESERIES_FIELD_TYPE_FLOAT,
            .data.float_val = 1.0
        };

        timeseries_insert_data_t insert_data = {
            .measurement_name = "metadata_test",
            .tag_keys = tags_keys,
            .tag_values = tags_values,
            .num_tags = 1,
            .field_names = field_names,
            .field_values = &field_value,
            .num_fields = 1,
            .timestamps_ms = &timestamp,
            .num_points = 1,
        };

        int64_t start = esp_timer_get_time();
        TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
        int64_t end = esp_timer_get_time();

        creation_times[s] = get_elapsed_us(start, end);
        total_time += creation_times[s];
    }

    // Calculate statistics
    int64_t first_5_avg = 0;
    int64_t last_5_avg = 0;
    for (size_t i = 0; i < 5; i++) {
        first_5_avg += creation_times[i];
        last_5_avg += creation_times[NUM_SERIES - 5 + i];
    }
    first_5_avg /= 5;
    last_5_avg /= 5;

    PERF_LOG("series_creation", "num_series", NUM_SERIES, "series");
    PERF_LOG("series_creation", "total_time", total_time, "us");
    PERF_LOG("series_creation", "avg_time", total_time / NUM_SERIES, "us/series");
    PERF_LOG("series_creation", "first_5_avg", first_5_avg, "us");
    PERF_LOG("series_creation", "last_5_avg", last_5_avg, "us");
}

TEST_CASE("perf: multi-tag insert", "[perf][metadata]") {
    clear_database();

    const size_t NUM_TAGS = 5;
    const char *tags_keys[] = {"location", "device", "type", "version", "region"};
    const char *tags_values[] = {"building_a", "sensor_001", "temperature", "v2.0", "north"};
    const char *field_names[] = {"reading"};

    const size_t NUM_POINTS = 100;
    uint64_t *timestamps = malloc(NUM_POINTS * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(NUM_POINTS * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(field_values);

    for (size_t i = 0; i < NUM_POINTS; i++) {
        timestamps[i] = 1000 * i;
        field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[i].data.float_val = (double)i;
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "multi_tag_test",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = NUM_TAGS,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = NUM_POINTS,
    };

    int64_t start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
    int64_t end = esp_timer_get_time();

    int64_t elapsed = get_elapsed_us(start, end);
    PERF_LOG("multi_tag_insert", "num_tags", NUM_TAGS, "tags");
    PERF_LOG("multi_tag_insert", "num_points", NUM_POINTS, "points");
    PERF_LOG("multi_tag_insert", "insert_time", elapsed, "us");
    PERF_LOG("multi_tag_insert", "time_per_point", elapsed / NUM_POINTS, "us/point");

    free(timestamps);
    free(field_values);
}

// ============================================================================
// CACHE PERFORMANCE TESTS
// ============================================================================

TEST_CASE("perf: cache warmup and hit rate", "[perf][cache]") {
    clear_database();

    // Insert data for a single series
    const char *tags_keys[] = {"sensor"};
    const char *tags_values[] = {"cache_test_sensor"};
    const char *field_names[] = {"value"};

    const size_t NUM_POINTS = 100;
    uint64_t *timestamps = malloc(NUM_POINTS * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(NUM_POINTS * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(field_values);

    for (size_t i = 0; i < NUM_POINTS; i++) {
        timestamps[i] = 1000 * i;
        field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[i].data.float_val = (double)i;
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "cache_test",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = 1,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = NUM_POINTS,
    };

    // First insert (cold cache)
    int64_t cold_start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
    int64_t cold_end = esp_timer_get_time();
    int64_t cold_time = get_elapsed_us(cold_start, cold_end);

    // Subsequent inserts to the same series (warm cache)
    const size_t NUM_WARM_INSERTS = 5;
    int64_t warm_times[NUM_WARM_INSERTS];
    int64_t warm_total = 0;

    for (size_t w = 0; w < NUM_WARM_INSERTS; w++) {
        // Update timestamps for new data
        for (size_t i = 0; i < NUM_POINTS; i++) {
            timestamps[i] = (w + 1) * NUM_POINTS * 1000 + 1000 * i;
        }

        int64_t warm_start = esp_timer_get_time();
        TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
        int64_t warm_end = esp_timer_get_time();
        warm_times[w] = get_elapsed_us(warm_start, warm_end);
        warm_total += warm_times[w];
    }

    PERF_LOG("cache_warmup", "cold_insert_time", cold_time, "us");
    PERF_LOG("cache_warmup", "warm_insert_avg", warm_total / NUM_WARM_INSERTS, "us");
    PERF_LOG_FLOAT("cache_warmup", "speedup_ratio", (double)cold_time / (warm_total / NUM_WARM_INSERTS), "x");

    free(timestamps);
    free(field_values);
}

TEST_CASE("perf: cache eviction behavior", "[perf][cache]") {
    clear_database();

    // Create more series than the cache can hold to trigger evictions
    // Default cache size is 64 entries
    const size_t NUM_SERIES = 80;
    int64_t insert_times[NUM_SERIES];
    int64_t total_time = 0;

    for (size_t s = 0; s < NUM_SERIES; s++) {
        char tag_value[32];
        snprintf(tag_value, sizeof(tag_value), "sensor_%zu", s);

        const char *tags_keys[] = {"sensor_id"};
        const char *tags_values[] = {tag_value};
        const char *field_names[] = {"reading"};

        uint64_t timestamp = 1000 * s;
        timeseries_field_value_t field_value = {
            .type = TIMESERIES_FIELD_TYPE_FLOAT,
            .data.float_val = (double)s
        };

        timeseries_insert_data_t insert_data = {
            .measurement_name = "eviction_test",
            .tag_keys = tags_keys,
            .tag_values = tags_values,
            .num_tags = 1,
            .field_names = field_names,
            .field_values = &field_value,
            .num_fields = 1,
            .timestamps_ms = &timestamp,
            .num_points = 1,
        };

        int64_t start = esp_timer_get_time();
        TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
        int64_t end = esp_timer_get_time();

        insert_times[s] = get_elapsed_us(start, end);
        total_time += insert_times[s];
    }

    // Calculate statistics for different cache regions
    int64_t first_64_avg = 0;
    int64_t after_64_avg = 0;

    for (size_t i = 0; i < 64 && i < NUM_SERIES; i++) {
        first_64_avg += insert_times[i];
    }
    first_64_avg /= (NUM_SERIES < 64 ? NUM_SERIES : 64);

    if (NUM_SERIES > 64) {
        for (size_t i = 64; i < NUM_SERIES; i++) {
            after_64_avg += insert_times[i];
        }
        after_64_avg /= (NUM_SERIES - 64);
    }

    PERF_LOG("cache_eviction", "num_series", NUM_SERIES, "series");
    PERF_LOG("cache_eviction", "total_time", total_time, "us");
    PERF_LOG("cache_eviction", "first_64_avg", first_64_avg, "us/series");
    PERF_LOG("cache_eviction", "after_64_avg", after_64_avg, "us/series");
}

// ============================================================================
// END-TO-END PERFORMANCE TESTS
// ============================================================================

TEST_CASE("perf: full workflow - insert, compact, query", "[perf][e2e]") {
    clear_database();

    const size_t NUM_POINTS = 500;
    const size_t NUM_FIELDS = 3;
    const char *tags_keys[] = {"system"};
    const char *tags_values[] = {"e2e_test"};
    const char *field_names[] = {"metric_a", "metric_b", "metric_c"};

    uint64_t *timestamps = malloc(NUM_POINTS * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(NUM_FIELDS * NUM_POINTS * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(field_values);

    for (size_t i = 0; i < NUM_POINTS; i++) {
        timestamps[i] = 1000 * i;
        for (size_t f = 0; f < NUM_FIELDS; f++) {
            size_t idx = f * NUM_POINTS + i;
            field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
            field_values[idx].data.float_val = (double)i + (double)f * 100.0;
        }
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "e2e_test",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = 1,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = NUM_FIELDS,
        .timestamps_ms = timestamps,
        .num_points = NUM_POINTS,
    };

    // Measure insert time
    int64_t insert_start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
    int64_t insert_end = esp_timer_get_time();
    int64_t insert_time = get_elapsed_us(insert_start, insert_end);

    // Measure compaction time
    int64_t compact_start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_compact());
    int64_t compact_end = esp_timer_get_time();
    int64_t compact_time = get_elapsed_us(compact_start, compact_end);

    // Measure query time
    timeseries_query_t query = {
        .measurement_name = "e2e_test",
        .tag_keys = NULL,
        .tag_values = NULL,
        .num_tags = 0,
        .field_names = NULL,
        .num_fields = 0,
        .start_ms = 0,
        .end_ms = INT64_MAX,
        .limit = 0,
        .rollup_interval = 0,
    };

    timeseries_query_result_t result;
    memset(&result, 0, sizeof(result));

    int64_t query_start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_query(&query, &result));
    int64_t query_end = esp_timer_get_time();
    int64_t query_time = get_elapsed_us(query_start, query_end);

    TEST_ASSERT_EQUAL(NUM_POINTS, result.num_points);
    TEST_ASSERT_EQUAL(NUM_FIELDS, result.num_columns);

    int64_t total_time = insert_time + compact_time + query_time;
    size_t total_values = NUM_POINTS * NUM_FIELDS;

    PERF_LOG("e2e_workflow", "num_points", NUM_POINTS, "points");
    PERF_LOG("e2e_workflow", "num_fields", NUM_FIELDS, "fields");
    PERF_LOG("e2e_workflow", "total_values", total_values, "values");
    PERF_LOG("e2e_workflow", "insert_time", insert_time, "us");
    PERF_LOG("e2e_workflow", "compact_time", compact_time, "us");
    PERF_LOG("e2e_workflow", "query_time", query_time, "us");
    PERF_LOG("e2e_workflow", "total_workflow_time", total_time, "us");
    PERF_LOG_FLOAT("e2e_workflow", "insert_throughput",
                   (double)total_values * 1000000.0 / insert_time, "values/s");
    PERF_LOG_FLOAT("e2e_workflow", "query_throughput",
                   (double)total_values * 1000000.0 / query_time, "values/s");

    timeseries_query_free_result(&result);
    free(timestamps);
    free(field_values);
}

TEST_CASE("perf: stress test - large dataset", "[perf][e2e][stress]") {
    clear_database();

    // 14 days of 5-minute interval data (same as example)
    const size_t POINTS_PER_DAY = 288;
    const size_t NUM_DAYS = 3;  // Reduced for QEMU memory constraints
    const size_t NUM_POINTS = POINTS_PER_DAY * NUM_DAYS;
    const size_t NUM_FIELDS = 6;

    const char *tags_keys[] = {"location", "system"};
    const char *tags_values[] = {"home", "solar_001"};
    const char *field_names[] = {
        "grid_import", "grid_export", "solar",
        "battery_charge", "battery_discharge", "house"
    };

    uint64_t *timestamps = malloc(NUM_POINTS * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(NUM_FIELDS * NUM_POINTS * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(timestamps);
    TEST_ASSERT_NOT_NULL(field_values);

    // Generate realistic solar data
    for (size_t i = 0; i < NUM_POINTS; i++) {
        timestamps[i] = 300000 * i; // 5-minute intervals
        float hour = (float)(i % POINTS_PER_DAY) / 12.0f;
        float solar_factor = (hour >= 6.0f && hour <= 18.0f) ?
                             sinf((hour - 6.0f) * 3.14159f / 12.0f) : 0.0f;

        for (size_t f = 0; f < NUM_FIELDS; f++) {
            size_t idx = f * NUM_POINTS + i;
            field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
            field_values[idx].data.float_val =
                1000.0f * solar_factor + (float)(rand() % 500);
        }
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "solar_stress",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = 2,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = NUM_FIELDS,
        .timestamps_ms = timestamps,
        .num_points = NUM_POINTS,
    };

    // Measure insert
    int64_t insert_start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
    int64_t insert_end = esp_timer_get_time();
    int64_t insert_time = get_elapsed_us(insert_start, insert_end);

    // Measure compaction
    int64_t compact_start = esp_timer_get_time();
    TEST_ASSERT_TRUE(timeseries_compact());
    int64_t compact_end = esp_timer_get_time();
    int64_t compact_time = get_elapsed_us(compact_start, compact_end);

    // Query each day and measure performance
    int64_t total_query_time = 0;
    for (size_t day = 0; day < NUM_DAYS; day++) {
        uint64_t day_start_ms = day * POINTS_PER_DAY * 300000;
        uint64_t day_end_ms = (day + 1) * POINTS_PER_DAY * 300000 - 1;

        timeseries_query_t query = {
            .measurement_name = "solar_stress",
            .tag_keys = tags_keys,
            .tag_values = tags_values,
            .num_tags = 2,
            .field_names = NULL,
            .num_fields = 0,
            .start_ms = day_start_ms,
            .end_ms = day_end_ms,
            .limit = 0,
            .rollup_interval = 0,
        };

        timeseries_query_result_t result;
        memset(&result, 0, sizeof(result));

        int64_t q_start = esp_timer_get_time();
        TEST_ASSERT_TRUE(timeseries_query(&query, &result));
        int64_t q_end = esp_timer_get_time();
        total_query_time += get_elapsed_us(q_start, q_end);

        TEST_ASSERT_EQUAL(POINTS_PER_DAY, result.num_points);
        timeseries_query_free_result(&result);
    }

    size_t total_values = NUM_POINTS * NUM_FIELDS;
    PERF_LOG("stress_test", "num_days", NUM_DAYS, "days");
    PERF_LOG("stress_test", "total_points", NUM_POINTS, "points");
    PERF_LOG("stress_test", "total_values", total_values, "values");
    PERF_LOG("stress_test", "insert_time", insert_time / 1000, "ms");
    PERF_LOG("stress_test", "compact_time", compact_time / 1000, "ms");
    PERF_LOG("stress_test", "total_query_time", total_query_time / 1000, "ms");
    PERF_LOG("stress_test", "avg_day_query_time", total_query_time / NUM_DAYS / 1000, "ms");
    PERF_LOG_FLOAT("stress_test", "insert_throughput",
                   (double)total_values * 1000000.0 / insert_time, "values/s");

    free(timestamps);
    free(field_values);
}
