/**
 * @file timeseries_metadata_perf_test.c
 * @brief Performance tests specifically for metadata operations
 *
 * These tests measure the performance of metadata read operations that
 * use esp_partition_read() and could potentially benefit from mmap.
 */

#include "esp_log.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_metadata.h"
#include "timeseries_id_list.h"
#include "timeseries_string_list.h"
#include "mbedtls/md5.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

static const char *TAG = "metadata_perf_test";

#define PERF_LOG(test_name, metric, value, unit) \
    ESP_LOGI(TAG, "PERF [%s] %s: %lld %s", test_name, metric, (long long)(value), unit)

#define PERF_LOG_FLOAT(test_name, metric, value, unit) \
    ESP_LOGI(TAG, "PERF [%s] %s: %.3f %s", test_name, metric, (double)(value), unit)

static int64_t get_elapsed_us(int64_t start, int64_t end) {
    return end - start;
}

static void clear_database(void) {
    TEST_ASSERT_TRUE_MESSAGE(timeseries_clear_all(),
                             "Failed to clear database to empty state");
}

// Get the internal db handle for direct metadata function calls
extern timeseries_db_t* timeseries_get_db_handle(void);

// ============================================================================
// Test Setup
// ============================================================================

TEST_CASE("metadata_perf: initialize database", "[metadata_perf][setup]") {
    TEST_ASSERT_TRUE_MESSAGE(timeseries_init(),
                             "Failed to initialize timeseries database");
    ESP_LOGI(TAG, "Database initialized for metadata performance testing");
}

// ============================================================================
// Setup helper - populate database with test data
// ============================================================================

static void populate_metadata_test_data(size_t num_series, size_t num_tags, size_t points_per_series) {
    clear_database();

    const char *base_tag_keys[] = {"device", "location", "type", "version", "region"};
    const char *field_names[] = {"value"};

    for (size_t s = 0; s < num_series; s++) {
        char tag_values[5][32];
        const char *tag_value_ptrs[5];

        for (size_t t = 0; t < num_tags && t < 5; t++) {
            snprintf(tag_values[t], sizeof(tag_values[t]), "%s_%zu", base_tag_keys[t], s);
            tag_value_ptrs[t] = tag_values[t];
        }

        uint64_t *timestamps = malloc(points_per_series * sizeof(uint64_t));
        timeseries_field_value_t *field_values = malloc(points_per_series * sizeof(timeseries_field_value_t));
        TEST_ASSERT_NOT_NULL(timestamps);
        TEST_ASSERT_NOT_NULL(field_values);

        for (size_t i = 0; i < points_per_series; i++) {
            timestamps[i] = (s * points_per_series + i) * 1000;
            field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
            field_values[i].data.float_val = (double)(s * 100 + i);
        }

        timeseries_insert_data_t insert_data = {
            .measurement_name = "test_measurement",
            .tag_keys = base_tag_keys,
            .tag_values = tag_value_ptrs,
            .num_tags = num_tags,
            .field_names = field_names,
            .field_values = field_values,
            .num_fields = 1,
            .timestamps_ms = timestamps,
            .num_points = points_per_series,
        };

        TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

        free(timestamps);
        free(field_values);
    }

    // Compact to ensure data is properly stored
    TEST_ASSERT_TRUE(timeseries_compact());
}

// ============================================================================
// METADATA LOOKUP PERFORMANCE TESTS
// ============================================================================

TEST_CASE("metadata_perf: measurement_id_lookup_repeated", "[metadata_perf][lookup]") {
    populate_metadata_test_data(10, 2, 50);

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    const size_t NUM_LOOKUPS = 100;
    int64_t total_time = 0;
    int64_t min_time = INT64_MAX;
    int64_t max_time = 0;
    uint32_t measurement_id;

    // Warm up - first lookup
    int64_t warmup_start = esp_timer_get_time();
    TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "test_measurement", &measurement_id));
    int64_t warmup_end = esp_timer_get_time();
    int64_t warmup_time = get_elapsed_us(warmup_start, warmup_end);

    // Repeated lookups
    for (size_t i = 0; i < NUM_LOOKUPS; i++) {
        int64_t start = esp_timer_get_time();
        TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "test_measurement", &measurement_id));
        int64_t end = esp_timer_get_time();

        int64_t elapsed = get_elapsed_us(start, end);
        total_time += elapsed;
        if (elapsed < min_time) min_time = elapsed;
        if (elapsed > max_time) max_time = elapsed;
    }

    PERF_LOG("measurement_id_lookup", "warmup_time", warmup_time, "us");
    PERF_LOG("measurement_id_lookup", "num_lookups", NUM_LOOKUPS, "ops");
    PERF_LOG("measurement_id_lookup", "total_time", total_time, "us");
    PERF_LOG("measurement_id_lookup", "avg_time", total_time / NUM_LOOKUPS, "us/op");
    PERF_LOG("measurement_id_lookup", "min_time", min_time, "us");
    PERF_LOG("measurement_id_lookup", "max_time", max_time, "us");
    PERF_LOG_FLOAT("measurement_id_lookup", "throughput",
                   (double)NUM_LOOKUPS * 1000000.0 / total_time, "ops/s");
}

TEST_CASE("metadata_perf: series_type_lookup_repeated", "[metadata_perf][lookup]") {
    populate_metadata_test_data(20, 2, 10);

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    // First, we need to get a valid series_id by doing an insert
    // Build the series_id the same way the insert code does
    unsigned char series_id[16];
    const char *buffer = "test_measurement:device:device_0:location:location_0:value";
    mbedtls_md5((const unsigned char*)buffer, strlen(buffer), series_id);

    const size_t NUM_LOOKUPS = 100;
    int64_t total_time = 0;
    int64_t min_time = INT64_MAX;
    int64_t max_time = 0;
    timeseries_field_type_e field_type;

    // Warm up
    int64_t warmup_start = esp_timer_get_time();
    bool found = tsdb_lookup_series_type_in_metadata(db, series_id, &field_type);
    int64_t warmup_end = esp_timer_get_time();
    int64_t warmup_time = get_elapsed_us(warmup_start, warmup_end);

    if (!found) {
        ESP_LOGW(TAG, "Series type not found in warmup - may affect results");
    }

    // Repeated lookups
    for (size_t i = 0; i < NUM_LOOKUPS; i++) {
        int64_t start = esp_timer_get_time();
        tsdb_lookup_series_type_in_metadata(db, series_id, &field_type);
        int64_t end = esp_timer_get_time();

        int64_t elapsed = get_elapsed_us(start, end);
        total_time += elapsed;
        if (elapsed < min_time) min_time = elapsed;
        if (elapsed > max_time) max_time = elapsed;
    }

    PERF_LOG("series_type_lookup", "warmup_time", warmup_time, "us");
    PERF_LOG("series_type_lookup", "num_lookups", NUM_LOOKUPS, "ops");
    PERF_LOG("series_type_lookup", "total_time", total_time, "us");
    PERF_LOG("series_type_lookup", "avg_time", total_time / NUM_LOOKUPS, "us/op");
    PERF_LOG("series_type_lookup", "min_time", min_time, "us");
    PERF_LOG("series_type_lookup", "max_time", max_time, "us");
    PERF_LOG_FLOAT("series_type_lookup", "throughput",
                   (double)NUM_LOOKUPS * 1000000.0 / total_time, "ops/s");
}

TEST_CASE("metadata_perf: tag_index_lookup_repeated", "[metadata_perf][lookup]") {
    populate_metadata_test_data(20, 3, 10);

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    // Get measurement ID first
    uint32_t measurement_id;
    TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "test_measurement", &measurement_id));

    const size_t NUM_LOOKUPS = 100;
    int64_t total_time = 0;
    int64_t min_time = INT64_MAX;
    int64_t max_time = 0;

    // Warm up
    timeseries_series_id_list_t warmup_list;
    tsdb_series_id_list_init(&warmup_list);
    int64_t warmup_start = esp_timer_get_time();
    tsdb_find_series_ids_for_tag(db, measurement_id, "device", "device_0", &warmup_list);
    int64_t warmup_end = esp_timer_get_time();
    int64_t warmup_time = get_elapsed_us(warmup_start, warmup_end);
    size_t warmup_count = warmup_list.count;
    tsdb_series_id_list_free(&warmup_list);

    // Repeated lookups
    for (size_t i = 0; i < NUM_LOOKUPS; i++) {
        timeseries_series_id_list_t series_list;
        tsdb_series_id_list_init(&series_list);

        int64_t start = esp_timer_get_time();
        tsdb_find_series_ids_for_tag(db, measurement_id, "device", "device_0", &series_list);
        int64_t end = esp_timer_get_time();

        tsdb_series_id_list_free(&series_list);

        int64_t elapsed = get_elapsed_us(start, end);
        total_time += elapsed;
        if (elapsed < min_time) min_time = elapsed;
        if (elapsed > max_time) max_time = elapsed;
    }

    PERF_LOG("tag_index_lookup", "warmup_time", warmup_time, "us");
    PERF_LOG("tag_index_lookup", "series_found", warmup_count, "series");
    PERF_LOG("tag_index_lookup", "num_lookups", NUM_LOOKUPS, "ops");
    PERF_LOG("tag_index_lookup", "total_time", total_time, "us");
    PERF_LOG("tag_index_lookup", "avg_time", total_time / NUM_LOOKUPS, "us/op");
    PERF_LOG("tag_index_lookup", "min_time", min_time, "us");
    PERF_LOG("tag_index_lookup", "max_time", max_time, "us");
    PERF_LOG_FLOAT("tag_index_lookup", "throughput",
                   (double)NUM_LOOKUPS * 1000000.0 / total_time, "ops/s");
}

TEST_CASE("metadata_perf: field_list_lookup_repeated", "[metadata_perf][lookup]") {
    populate_metadata_test_data(20, 2, 10);

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    // Get measurement ID first
    uint32_t measurement_id;
    TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "test_measurement", &measurement_id));

    const size_t NUM_LOOKUPS = 100;
    int64_t total_time = 0;
    int64_t min_time = INT64_MAX;
    int64_t max_time = 0;

    // Warm up
    timeseries_series_id_list_t warmup_list;
    tsdb_series_id_list_init(&warmup_list);
    int64_t warmup_start = esp_timer_get_time();
    tsdb_find_series_ids_for_field(db, measurement_id, "value", &warmup_list);
    int64_t warmup_end = esp_timer_get_time();
    int64_t warmup_time = get_elapsed_us(warmup_start, warmup_end);
    size_t warmup_count = warmup_list.count;
    tsdb_series_id_list_free(&warmup_list);

    // Repeated lookups
    for (size_t i = 0; i < NUM_LOOKUPS; i++) {
        timeseries_series_id_list_t series_list;
        tsdb_series_id_list_init(&series_list);

        int64_t start = esp_timer_get_time();
        tsdb_find_series_ids_for_field(db, measurement_id, "value", &series_list);
        int64_t end = esp_timer_get_time();

        tsdb_series_id_list_free(&series_list);

        int64_t elapsed = get_elapsed_us(start, end);
        total_time += elapsed;
        if (elapsed < min_time) min_time = elapsed;
        if (elapsed > max_time) max_time = elapsed;
    }

    PERF_LOG("field_list_lookup", "warmup_time", warmup_time, "us");
    PERF_LOG("field_list_lookup", "series_found", warmup_count, "series");
    PERF_LOG("field_list_lookup", "num_lookups", NUM_LOOKUPS, "ops");
    PERF_LOG("field_list_lookup", "total_time", total_time, "us");
    PERF_LOG("field_list_lookup", "avg_time", total_time / NUM_LOOKUPS, "us/op");
    PERF_LOG("field_list_lookup", "min_time", min_time, "us");
    PERF_LOG("field_list_lookup", "max_time", max_time, "us");
    PERF_LOG_FLOAT("field_list_lookup", "throughput",
                   (double)NUM_LOOKUPS * 1000000.0 / total_time, "ops/s");
}

TEST_CASE("metadata_perf: list_all_measurements_repeated", "[metadata_perf][lookup]") {
    // Create multiple measurements
    clear_database();

    const char *field_names[] = {"value"};
    const char *tag_keys[] = {"device"};

    // Insert data for 10 different measurements
    for (int m = 0; m < 10; m++) {
        char measurement_name[32];
        snprintf(measurement_name, sizeof(measurement_name), "measurement_%d", m);

        char tag_value[32];
        snprintf(tag_value, sizeof(tag_value), "device_%d", m);
        const char *tag_values[] = {tag_value};

        uint64_t timestamp = m * 1000;
        timeseries_field_value_t field_value = {
            .type = TIMESERIES_FIELD_TYPE_FLOAT,
            .data.float_val = (double)m
        };

        timeseries_insert_data_t insert_data = {
            .measurement_name = measurement_name,
            .tag_keys = tag_keys,
            .tag_values = tag_values,
            .num_tags = 1,
            .field_names = field_names,
            .field_values = &field_value,
            .num_fields = 1,
            .timestamps_ms = &timestamp,
            .num_points = 1,
        };

        TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
    }

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    const size_t NUM_LOOKUPS = 100;
    int64_t total_time = 0;
    int64_t min_time = INT64_MAX;
    int64_t max_time = 0;

    // Warm up
    timeseries_string_list_t warmup_list;
    tsdb_string_list_init(&warmup_list);
    int64_t warmup_start = esp_timer_get_time();
    tsdb_list_all_measurements(db, &warmup_list);
    int64_t warmup_end = esp_timer_get_time();
    int64_t warmup_time = get_elapsed_us(warmup_start, warmup_end);
    size_t warmup_count = warmup_list.count;
    tsdb_string_list_free(&warmup_list);

    // Repeated lookups
    for (size_t i = 0; i < NUM_LOOKUPS; i++) {
        timeseries_string_list_t measurements;
        tsdb_string_list_init(&measurements);

        int64_t start = esp_timer_get_time();
        tsdb_list_all_measurements(db, &measurements);
        int64_t end = esp_timer_get_time();

        tsdb_string_list_free(&measurements);

        int64_t elapsed = get_elapsed_us(start, end);
        total_time += elapsed;
        if (elapsed < min_time) min_time = elapsed;
        if (elapsed > max_time) max_time = elapsed;
    }

    PERF_LOG("list_measurements", "warmup_time", warmup_time, "us");
    PERF_LOG("list_measurements", "measurements_found", warmup_count, "measurements");
    PERF_LOG("list_measurements", "num_lookups", NUM_LOOKUPS, "ops");
    PERF_LOG("list_measurements", "total_time", total_time, "us");
    PERF_LOG("list_measurements", "avg_time", total_time / NUM_LOOKUPS, "us/op");
    PERF_LOG("list_measurements", "min_time", min_time, "us");
    PERF_LOG("list_measurements", "max_time", max_time, "us");
    PERF_LOG_FLOAT("list_measurements", "throughput",
                   (double)NUM_LOOKUPS * 1000000.0 / total_time, "ops/s");
}

TEST_CASE("metadata_perf: list_fields_for_measurement_repeated", "[metadata_perf][lookup]") {
    // Create measurement with multiple fields
    clear_database();

    const char *field_names[] = {"temp", "humidity", "pressure", "voltage", "current", "power"};
    const size_t NUM_FIELDS = 6;
    const char *tag_keys[] = {"sensor"};
    const char *tag_values[] = {"sensor_1"};

    uint64_t timestamp = 1000;
    timeseries_field_value_t field_values[6];
    for (size_t f = 0; f < NUM_FIELDS; f++) {
        field_values[f].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[f].data.float_val = (double)f;
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "sensor_data",
        .tag_keys = tag_keys,
        .tag_values = tag_values,
        .num_tags = 1,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = NUM_FIELDS,
        .timestamps_ms = &timestamp,
        .num_points = 1,
    };

    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    uint32_t measurement_id;
    TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "sensor_data", &measurement_id));

    const size_t NUM_LOOKUPS = 100;
    int64_t total_time = 0;
    int64_t min_time = INT64_MAX;
    int64_t max_time = 0;

    // Warm up
    timeseries_string_list_t warmup_list;
    tsdb_string_list_init(&warmup_list);
    int64_t warmup_start = esp_timer_get_time();
    tsdb_list_fields_for_measurement(db, measurement_id, &warmup_list);
    int64_t warmup_end = esp_timer_get_time();
    int64_t warmup_time = get_elapsed_us(warmup_start, warmup_end);
    size_t warmup_count = warmup_list.count;
    tsdb_string_list_free(&warmup_list);

    // Repeated lookups
    for (size_t i = 0; i < NUM_LOOKUPS; i++) {
        timeseries_string_list_t fields;
        tsdb_string_list_init(&fields);

        int64_t start = esp_timer_get_time();
        tsdb_list_fields_for_measurement(db, measurement_id, &fields);
        int64_t end = esp_timer_get_time();

        tsdb_string_list_free(&fields);

        int64_t elapsed = get_elapsed_us(start, end);
        total_time += elapsed;
        if (elapsed < min_time) min_time = elapsed;
        if (elapsed > max_time) max_time = elapsed;
    }

    PERF_LOG("list_fields", "warmup_time", warmup_time, "us");
    PERF_LOG("list_fields", "fields_found", warmup_count, "fields");
    PERF_LOG("list_fields", "num_lookups", NUM_LOOKUPS, "ops");
    PERF_LOG("list_fields", "total_time", total_time, "us");
    PERF_LOG("list_fields", "avg_time", total_time / NUM_LOOKUPS, "us/op");
    PERF_LOG("list_fields", "min_time", min_time, "us");
    PERF_LOG("list_fields", "max_time", max_time, "us");
    PERF_LOG_FLOAT("list_fields", "throughput",
                   (double)NUM_LOOKUPS * 1000000.0 / total_time, "ops/s");
}

// ============================================================================
// ENTITY ITERATOR PERFORMANCE (core of metadata scanning)
// ============================================================================

TEST_CASE("metadata_perf: entity_iterator_full_scan", "[metadata_perf][iterator]") {
    // Populate with lots of metadata entries
    populate_metadata_test_data(50, 3, 5);

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    // Find metadata pages
    uint32_t meta_offsets[4];
    size_t meta_count = 0;

    // Use page cache iterator to find metadata pages
    timeseries_page_cache_iterator_t page_iter;
    TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &page_iter));

    timeseries_page_header_t hdr;
    uint32_t page_offset, page_size;
    while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset, &page_size)) {
        if (hdr.page_type == TIMESERIES_PAGE_TYPE_METADATA &&
            hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE &&
            meta_count < 4) {
            meta_offsets[meta_count++] = page_offset;
        }
    }

    TEST_ASSERT_GREATER_THAN(0, meta_count);

    const size_t NUM_SCANS = 50;
    int64_t total_time = 0;
    size_t total_entries = 0;

    for (size_t scan = 0; scan < NUM_SCANS; scan++) {
        size_t entries_this_scan = 0;

        int64_t start = esp_timer_get_time();

        for (size_t p = 0; p < meta_count; p++) {
            timeseries_entity_iterator_t ent_iter;
            if (!timeseries_entity_iterator_init(db, meta_offsets[p], TIMESERIES_METADATA_PAGE_SIZE, &ent_iter)) {
                continue;
            }

            timeseries_entry_header_t e_hdr;
            while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
                entries_this_scan++;
            }

            timeseries_entity_iterator_deinit(&ent_iter);
        }

        int64_t end = esp_timer_get_time();
        total_time += get_elapsed_us(start, end);

        if (scan == 0) {
            total_entries = entries_this_scan;
        }
    }

    PERF_LOG("entity_iterator_scan", "metadata_pages", meta_count, "pages");
    PERF_LOG("entity_iterator_scan", "entries_per_scan", total_entries, "entries");
    PERF_LOG("entity_iterator_scan", "num_scans", NUM_SCANS, "scans");
    PERF_LOG("entity_iterator_scan", "total_time", total_time, "us");
    PERF_LOG("entity_iterator_scan", "avg_scan_time", total_time / NUM_SCANS, "us/scan");
    PERF_LOG_FLOAT("entity_iterator_scan", "entries_per_second",
                   (double)(total_entries * NUM_SCANS) * 1000000.0 / total_time, "entries/s");
}

// ============================================================================
// COMBINED QUERY WORKFLOW (simulates real usage)
// ============================================================================

TEST_CASE("metadata_perf: query_workflow_metadata_overhead", "[metadata_perf][workflow]") {
    populate_metadata_test_data(10, 2, 100);

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    const size_t NUM_QUERIES = 20;
    int64_t total_metadata_time = 0;
    int64_t total_query_time = 0;

    for (size_t q = 0; q < NUM_QUERIES; q++) {
        // Measure metadata lookup time
        int64_t meta_start = esp_timer_get_time();

        uint32_t measurement_id;
        TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "test_measurement", &measurement_id));

        timeseries_series_id_list_t series_list;
        tsdb_series_id_list_init(&series_list);
        tsdb_find_series_ids_for_field(db, measurement_id, "value", &series_list);
        tsdb_series_id_list_free(&series_list);

        int64_t meta_end = esp_timer_get_time();
        total_metadata_time += get_elapsed_us(meta_start, meta_end);

        // Measure full query time
        timeseries_query_t query = {
            .measurement_name = "test_measurement",
            .tag_keys = NULL,
            .tag_values = NULL,
            .num_tags = 0,
            .field_names = NULL,
            .num_fields = 0,
            .start_ms = 0,
            .end_ms = INT64_MAX,
            .limit = 100,
            .rollup_interval = 0,
        };

        timeseries_query_result_t result;
        memset(&result, 0, sizeof(result));

        int64_t query_start = esp_timer_get_time();
        TEST_ASSERT_TRUE(timeseries_query(&query, &result));
        int64_t query_end = esp_timer_get_time();

        total_query_time += get_elapsed_us(query_start, query_end);
        timeseries_query_free_result(&result);
    }

    PERF_LOG("query_workflow", "num_queries", NUM_QUERIES, "queries");
    PERF_LOG("query_workflow", "total_metadata_time", total_metadata_time, "us");
    PERF_LOG("query_workflow", "avg_metadata_time", total_metadata_time / NUM_QUERIES, "us/query");
    PERF_LOG("query_workflow", "total_query_time", total_query_time, "us");
    PERF_LOG("query_workflow", "avg_query_time", total_query_time / NUM_QUERIES, "us/query");
    PERF_LOG_FLOAT("query_workflow", "metadata_percentage",
                   100.0 * total_metadata_time / total_query_time, "%");
}
