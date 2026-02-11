/**
 * @file timeseries_multi_iterator_test.c
 * @brief Unit tests for multi-iterator module
 *
 * Tests the heap-based merging iterator used for combining data from
 * multiple series during compaction and queries.
 */

#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_multi_iterator.h"
#include "timeseries_points_iterator.h"
#include "esp_log.h"
#include <string.h>

static const char *TAG = "multi_iter_test";

static timeseries_db_t *db = NULL;
static bool db_initialized = false;

// Get db handle from timeseries.c (not in public header)
extern timeseries_db_t *timeseries_get_db_handle(void);

static void setup_database(void) {
    if (!db_initialized) {
        TEST_ASSERT_TRUE_MESSAGE(timeseries_init(), "Failed to initialize database");
        db_initialized = true;
    }
    TEST_ASSERT_TRUE(timeseries_clear_all());
    db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL_MESSAGE(db, "Failed to get db handle");
}

static void teardown_database(void) {
    TEST_ASSERT_TRUE(timeseries_clear_all());
}

// Helper to insert float test data (no tags)
static bool insert_float_points(const char *measurement, const char *field,
                                size_t count, uint64_t start_ts, uint64_t ts_step) {
    uint64_t *timestamps = malloc(count * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(count * sizeof(timeseries_field_value_t));
    if (!timestamps || !field_values) {
        free(timestamps);
        free(field_values);
        return false;
    }

    for (size_t i = 0; i < count; i++) {
        timestamps[i] = start_ts + i * ts_step;
        field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[i].data.float_val = (double)(i * 1.5);
    }

    const char *field_names[] = {field};
    timeseries_insert_data_t insert_data = {
        .measurement_name = measurement,
        .tag_keys = NULL,
        .tag_values = NULL,
        .num_tags = 0,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = count,
    };

    bool result = timeseries_insert(&insert_data);

    free(timestamps);
    free(field_values);
    return result;
}

// Helper to insert float test data with tags
static bool insert_float_points_tagged(const char *measurement, const char *field,
                                       const char **tag_keys, const char **tag_values, size_t num_tags,
                                       const float *values, const uint64_t *timestamps, size_t count) {
    timeseries_field_value_t *field_values = malloc(count * sizeof(timeseries_field_value_t));
    if (!field_values) {
        return false;
    }

    for (size_t i = 0; i < count; i++) {
        field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[i].data.float_val = (double)values[i];
    }

    const char *field_names[] = {field};
    timeseries_insert_data_t insert_data = {
        .measurement_name = measurement,
        .tag_keys = tag_keys,
        .tag_values = tag_values,
        .num_tags = num_tags,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = (uint64_t *)timestamps,
        .num_points = count,
    };

    bool result = timeseries_insert(&insert_data);
    free(field_values);
    return result;
}

// Helper to insert int test data (no tags)
static bool insert_int_points(const char *measurement, const char *field,
                              const int64_t *values, const uint64_t *timestamps, size_t count) {
    timeseries_field_value_t *field_values = malloc(count * sizeof(timeseries_field_value_t));
    if (!field_values) {
        return false;
    }

    for (size_t i = 0; i < count; i++) {
        field_values[i].type = TIMESERIES_FIELD_TYPE_INT;
        field_values[i].data.int_val = values[i];
    }

    const char *field_names[] = {field};
    timeseries_insert_data_t insert_data = {
        .measurement_name = measurement,
        .tag_keys = NULL,
        .tag_values = NULL,
        .num_tags = 0,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = (uint64_t *)timestamps,
        .num_points = count,
    };

    bool result = timeseries_insert(&insert_data);
    free(field_values);
    return result;
}

// Helper to insert bool test data (no tags)
static bool insert_bool_points(const char *measurement, const char *field,
                               const bool *values, const uint64_t *timestamps, size_t count) {
    timeseries_field_value_t *field_values = malloc(count * sizeof(timeseries_field_value_t));
    if (!field_values) {
        return false;
    }

    for (size_t i = 0; i < count; i++) {
        field_values[i].type = TIMESERIES_FIELD_TYPE_BOOL;
        field_values[i].data.bool_val = values[i];
    }

    const char *field_names[] = {field};
    timeseries_insert_data_t insert_data = {
        .measurement_name = measurement,
        .tag_keys = NULL,
        .tag_values = NULL,
        .num_tags = 0,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = (uint64_t *)timestamps,
        .num_points = count,
    };

    bool result = timeseries_insert(&insert_data);
    free(field_values);
    return result;
}

// ============================================================================
// NULL Parameter Tests
// ============================================================================

TEST_CASE("multi_iterator: init with zero sub_count", "[multi_iter][null]") {
    timeseries_multi_points_iterator_t iter;
    bool result = timeseries_multi_points_iterator_init(NULL, NULL, 0, &iter);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("multi_iterator: init with NULL iter output", "[multi_iter][null]") {
    timeseries_points_iterator_t sub_iter;
    timeseries_points_iterator_t *subs[] = {&sub_iter};
    uint32_t page_seqs[] = {0};

    bool result = timeseries_multi_points_iterator_init(subs, page_seqs, 1, NULL);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("multi_iterator: next with NULL iter", "[multi_iter][null]") {
    uint64_t ts;
    timeseries_field_value_t val;
    bool result = timeseries_multi_points_iterator_next(NULL, &ts, &val);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("multi_iterator: peek with NULL iter", "[multi_iter][null]") {
    uint64_t ts;
    timeseries_field_value_t val;
    bool result = timeseries_multi_points_iterator_peek(NULL, &ts, &val);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("multi_iterator: deinit with NULL is safe", "[multi_iter][null]") {
    // Should not crash
    timeseries_multi_points_iterator_deinit(NULL);
    TEST_PASS();
}

// ============================================================================
// Integration Tests via High-Level API
// ============================================================================

TEST_CASE("multi_iterator: query single series after compaction", "[multi_iter][integration]") {
    setup_database();

    // Insert data that will create multiple L0 pages
    for (int batch = 0; batch < 5; batch++) {
        TEST_ASSERT_TRUE(insert_float_points("test_series", "value", 50,
                                             batch * 50000, 1000));
    }

    // Compact to create L1 page with multi-iterator merging
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query to verify data integrity
    timeseries_query_result_t result;
    timeseries_query_t query = {
        .measurement_name = "test_series"
    };

    TEST_ASSERT_TRUE(timeseries_query(&query, &result));
    TEST_ASSERT_EQUAL(250, result.num_points);

    // Verify timestamps are in order
    for (size_t i = 1; i < result.num_points; i++) {
        TEST_ASSERT_TRUE_MESSAGE(result.timestamps[i] > result.timestamps[i-1],
                                "Timestamps not in ascending order");
    }

    timeseries_query_free_result(&result);
    teardown_database();
}

TEST_CASE("multi_iterator: query multiple series with overlapping timestamps", "[multi_iter][integration]") {
    setup_database();

    // Insert 3 series with overlapping timestamps
    const char *tag_key = "sensor";

    // Series A: timestamps 0, 1000, 2000, ...
    uint64_t ts_a[10];
    float val_a[10];
    for (int i = 0; i < 10; i++) {
        ts_a[i] = i * 1000;
        val_a[i] = 100.0f + i;
    }

    // Series B: timestamps 500, 1500, 2500, ...
    uint64_t ts_b[10];
    float val_b[10];
    for (int i = 0; i < 10; i++) {
        ts_b[i] = 500 + i * 1000;
        val_b[i] = 200.0f + i;
    }

    // Series C: timestamps 250, 1250, 2250, ...
    uint64_t ts_c[10];
    float val_c[10];
    for (int i = 0; i < 10; i++) {
        ts_c[i] = 250 + i * 1000;
        val_c[i] = 300.0f + i;
    }

    const char *tags1[] = {tag_key};
    const char *vals1[] = {"A"};
    const char *vals2[] = {"B"};
    const char *vals3[] = {"C"};

    TEST_ASSERT_TRUE(insert_float_points_tagged("multi_test", "temp", tags1, vals1, 1, val_a, ts_a, 10));
    TEST_ASSERT_TRUE(insert_float_points_tagged("multi_test", "temp", tags1, vals2, 1, val_b, ts_b, 10));
    TEST_ASSERT_TRUE(insert_float_points_tagged("multi_test", "temp", tags1, vals3, 1, val_c, ts_c, 10));

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query all series - multi-iterator should merge them
    timeseries_query_t query = {
        .measurement_name = "multi_test"
    };
    timeseries_query_result_t result;

    TEST_ASSERT_TRUE(timeseries_query(&query, &result));

    // Should have 30 points total
    TEST_ASSERT_EQUAL(30, result.num_points);

    // Verify ascending order after merge
    for (size_t i = 1; i < result.num_points; i++) {
        TEST_ASSERT_TRUE_MESSAGE(result.timestamps[i] >= result.timestamps[i-1],
                                "Timestamps not in order after merge");
    }

    timeseries_query_free_result(&result);
    teardown_database();
}

TEST_CASE("multi_iterator: deduplication with same timestamps", "[multi_iter][dedup]") {
    setup_database();

    // Insert same timestamp twice with different values
    // The later insert (higher page_seq) should win

    uint64_t ts1[] = {1000, 2000, 3000};
    float val1[] = {1.0f, 2.0f, 3.0f};

    uint64_t ts2[] = {2000, 3000, 4000};  // Overlapping at 2000, 3000
    float val2[] = {20.0f, 30.0f, 40.0f};

    TEST_ASSERT_TRUE(insert_float_points_tagged("dedup_test", "value", NULL, NULL, 0, val1, ts1, 3));
    TEST_ASSERT_TRUE(insert_float_points_tagged("dedup_test", "value", NULL, NULL, 0, val2, ts2, 3));

    // Compact - should deduplicate
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query
    timeseries_query_t query = {
        .measurement_name = "dedup_test"
    };
    timeseries_query_result_t result;

    TEST_ASSERT_TRUE(timeseries_query(&query, &result));

    // Should have 4 unique timestamps: 1000, 2000, 3000, 4000
    TEST_ASSERT_EQUAL(4, result.num_points);

    // Verify unique timestamps
    TEST_ASSERT_EQUAL(1000, result.timestamps[0]);
    TEST_ASSERT_EQUAL(2000, result.timestamps[1]);
    TEST_ASSERT_EQUAL(3000, result.timestamps[2]);
    TEST_ASSERT_EQUAL(4000, result.timestamps[3]);

    timeseries_query_free_result(&result);
    teardown_database();
}

TEST_CASE("multi_iterator: one series exhausts before others", "[multi_iter][exhaust]") {
    setup_database();

    // Short series: 5 points
    uint64_t ts_short[] = {0, 1000, 2000, 3000, 4000};
    float val_short[] = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};

    // Long series: 20 points
    uint64_t ts_long[20];
    float val_long[20];
    for (int i = 0; i < 20; i++) {
        ts_long[i] = i * 500;
        val_long[i] = i * 10.0f;
    }

    const char *tags[] = {"len"};
    const char *vals_short[] = {"short"};
    const char *vals_long[] = {"long"};

    TEST_ASSERT_TRUE(insert_float_points_tagged("exhaust_test", "data", tags, vals_short, 1, val_short, ts_short, 5));
    TEST_ASSERT_TRUE(insert_float_points_tagged("exhaust_test", "data", tags, vals_long, 1, val_long, ts_long, 20));

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query all
    timeseries_query_t query = {
        .measurement_name = "exhaust_test"
    };
    timeseries_query_result_t result;

    TEST_ASSERT_TRUE(timeseries_query(&query, &result));

    // Should merge correctly even after short series exhausts
    // Due to timestamp overlap, some dedup may occur
    TEST_ASSERT_TRUE(result.num_points >= 20);  // At least the long series

    timeseries_query_free_result(&result);
    teardown_database();
}

TEST_CASE("multi_iterator: integer field type", "[multi_iter][types]") {
    setup_database();

    uint64_t ts[] = {1000, 2000, 3000, 4000, 5000};
    int64_t vals[] = {-100, -50, 0, 50, 100};

    TEST_ASSERT_TRUE(insert_int_points("int_test", "count", vals, ts, 5));

    // Insert another batch
    uint64_t ts2[] = {6000, 7000, 8000};
    int64_t vals2[] = {-1000, 0, 1000};

    TEST_ASSERT_TRUE(insert_int_points("int_test", "count", vals2, ts2, 3));

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query
    timeseries_query_t query = {
        .measurement_name = "int_test"
    };
    timeseries_query_result_t result;

    TEST_ASSERT_TRUE(timeseries_query(&query, &result));
    TEST_ASSERT_EQUAL(8, result.num_points);

    timeseries_query_free_result(&result);
    teardown_database();
}

TEST_CASE("multi_iterator: boolean field type", "[multi_iter][types]") {
    setup_database();

    uint64_t ts[] = {1000, 2000, 3000, 4000, 5000};
    bool vals[] = {true, false, true, false, true};

    TEST_ASSERT_TRUE(insert_bool_points("bool_test", "flag", vals, ts, 5));

    uint64_t ts2[] = {6000, 7000};
    bool vals2[] = {false, true};

    TEST_ASSERT_TRUE(insert_bool_points("bool_test", "flag", vals2, ts2, 2));

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query
    timeseries_query_t query = {
        .measurement_name = "bool_test"
    };
    timeseries_query_result_t result;

    TEST_ASSERT_TRUE(timeseries_query(&query, &result));
    TEST_ASSERT_EQUAL(7, result.num_points);

    timeseries_query_free_result(&result);
    teardown_database();
}

// ============================================================================
// Stress Tests
// ============================================================================

TEST_CASE("multi_iterator: many series stress test", "[multi_iter][stress]") {
    setup_database();

    // Create 20 series with 50 points each
    for (int s = 0; s < 20; s++) {
        char tag_val[16];
        snprintf(tag_val, sizeof(tag_val), "sensor%02d", s);

        const char *tags[] = {"id"};
        const char *vals[] = {tag_val};

        uint64_t ts[50];
        float values[50];
        for (int i = 0; i < 50; i++) {
            ts[i] = s * 1000000 + i * 1000;  // Non-overlapping
            values[i] = s * 100.0f + i;
        }

        TEST_ASSERT_TRUE(insert_float_points_tagged("stress_test", "temp", tags, vals, 1, values, ts, 50));
    }

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query all
    timeseries_query_t query = {
        .measurement_name = "stress_test"
    };
    timeseries_query_result_t result;

    TEST_ASSERT_TRUE(timeseries_query(&query, &result));

    // Should have 20 * 50 = 1000 points
    TEST_ASSERT_EQUAL(1000, result.num_points);

    // Verify order
    for (size_t i = 1; i < result.num_points; i++) {
        TEST_ASSERT_TRUE(result.timestamps[i] > result.timestamps[i-1]);
    }

    timeseries_query_free_result(&result);
    ESP_LOGI(TAG, "Many series stress test passed: 1000 points");
    teardown_database();
}

TEST_CASE("multi_iterator: overlapping timestamps stress", "[multi_iter][stress]") {
    setup_database();

    // Create 10 series all with the same timestamps (worst case dedup)
    for (int s = 0; s < 10; s++) {
        char tag_val[16];
        snprintf(tag_val, sizeof(tag_val), "dev%d", s);

        const char *tags[] = {"device"};
        const char *vals[] = {tag_val};

        uint64_t ts[100];
        float values[100];
        for (int i = 0; i < 100; i++) {
            ts[i] = i * 1000;  // All same timestamps!
            values[i] = s * 1000.0f + i;
        }

        TEST_ASSERT_TRUE(insert_float_points_tagged("overlap_test", "reading", tags, vals, 1, values, ts, 100));
    }

    // Compact - heavy deduplication work
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query all
    timeseries_query_t query = {
        .measurement_name = "overlap_test"
    };
    timeseries_query_result_t result;

    TEST_ASSERT_TRUE(timeseries_query(&query, &result));

    // Query result is row-per-timestamp; 10 series share 100 unique timestamps,
    // so the aggregator collapses them into 100 rows.
    TEST_ASSERT_EQUAL(100, result.num_points);

    timeseries_query_free_result(&result);
    ESP_LOGI(TAG, "Overlapping timestamps stress test passed");
    teardown_database();
}
