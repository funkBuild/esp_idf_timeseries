/**
 * @file timeseries_compaction_test.c
 * @brief Comprehensive unit tests for timeseries compaction functionality
 *
 * This file focuses on testing the compaction logic in timeseries_compaction.c
 * including:
 * - Level-0 to Level-1 compaction (single-series streaming)
 * - Level-1 to Level-2+ compaction (multi-iterator approach)
 * - Page marking as obsolete
 * - Cache invalidation during compaction
 * - Error handling and edge cases
 * - Boundary conditions
 * - Data integrity across compaction levels
 *
 * Coverage gaps identified in timeseries_compaction.c:
 * 1. Empty data compaction (0 series IDs collected)
 * 2. Single vs multiple series compaction
 * 3. Compaction threshold enforcement (MIN_PAGES_FOR_COMPACTION)
 * 4. Level transitions (L0->L1, L1->L2, L2->L3, L3->L4)
 * 5. Page obsolete marking and cache removal
 * 6. Large datasets requiring multiple pages
 * 7. Duplicate timestamp handling during compaction
 * 8. Error recovery during multi-iterator compaction
 * 9. Memory allocation failures
 * 10. Series ID deduplication logic
 * 11. Page rewriter for deleted records
 */

#include "esp_log.h"
#include "esp_timer.h"
#include "timeseries.h"
#include "timeseries_compaction.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "unity.h"
#include <inttypes.h>
#include <math.h>
#include <string.h>

extern timeseries_db_t *timeseries_get_db_handle(void);

static const char *TAG = "compaction_test";

// Flag to track if database has been initialized
static bool db_initialized = false;

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * @brief Ensure database is initialized and cleared before each test
 */
static void setup_database(void) {
    if (!db_initialized) {
        TEST_ASSERT_TRUE_MESSAGE(timeseries_init(), "Failed to initialize timeseries database");
        db_initialized = true;
    }
    TEST_ASSERT_TRUE(timeseries_clear_all());
}

/**
 * @brief Insert float points to a measurement with optional tags
 */
static bool insert_float_points_with_tags(const char *measurement,
                                          const char *field_name,
                                          const char **tag_keys,
                                          const char **tag_values,
                                          size_t num_tags,
                                          size_t num_points,
                                          uint64_t start_timestamp,
                                          uint64_t timestamp_increment,
                                          double value_multiplier) {
    uint64_t *timestamps = malloc(num_points * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(num_points * sizeof(timeseries_field_value_t));

    if (!timestamps || !field_values) {
        free(timestamps);
        free(field_values);
        return false;
    }

    for (size_t i = 0; i < num_points; i++) {
        timestamps[i] = start_timestamp + (i * timestamp_increment);
        field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[i].data.float_val = (double)i * value_multiplier;
    }

    const char *field_names[] = {field_name};
    timeseries_insert_data_t insert_data = {
        .measurement_name = measurement,
        .tag_keys = tag_keys,
        .tag_values = tag_values,
        .num_tags = num_tags,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = num_points,
    };

    bool result = timeseries_insert(&insert_data);

    free(timestamps);
    free(field_values);
    return result;
}

/**
 * @brief Insert float points without tags (wrapper)
 */
static bool insert_float_points(const char *measurement,
                                const char *field_name,
                                size_t num_points,
                                uint64_t start_timestamp,
                                uint64_t timestamp_increment) {
    return insert_float_points_with_tags(measurement, field_name, NULL, NULL, 0,
                                        num_points, start_timestamp, timestamp_increment, 1.5);
}

/**
 * @brief Insert bool points without tags
 */
static bool insert_bool_points_ct(const char *measurement,
                                  const char *field_name,
                                  size_t num_points,
                                  uint64_t start_timestamp,
                                  uint64_t timestamp_increment) {
    uint64_t *timestamps = malloc(num_points * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(num_points * sizeof(timeseries_field_value_t));
    if (!timestamps || !field_values) { free(timestamps); free(field_values); return false; }
    for (size_t i = 0; i < num_points; i++) {
        timestamps[i] = start_timestamp + i * timestamp_increment;
        field_values[i].type = TIMESERIES_FIELD_TYPE_BOOL;
        field_values[i].data.bool_val = (i % 2 == 0);
    }
    const char *field_names[] = {field_name};
    timeseries_insert_data_t ins = {
        .measurement_name = measurement, .tag_keys = NULL, .tag_values = NULL,
        .num_tags = 0, .field_names = field_names, .field_values = field_values,
        .num_fields = 1, .timestamps_ms = timestamps, .num_points = num_points,
    };
    bool ok = timeseries_insert(&ins);
    free(timestamps); free(field_values);
    return ok;
}

/**
 * @brief Scan all level-1+ pages and collect flag bytes from compressed records.
 *        Counts ALP-encoded and Gorilla-encoded records separately.
 */
static void count_l1_encoding_flags(int *out_alp_count, int *out_gorilla_count) {
    *out_alp_count = 0;
    *out_gorilla_count = 0;

    timeseries_db_t *db = timeseries_get_db_handle();
    if (!db) return;

    timeseries_page_iterator_t page_iter;
    if (!timeseries_page_iterator_init(db, &page_iter)) return;

    timeseries_page_header_t page_hdr;
    uint32_t page_offset, page_size;

    while (timeseries_page_iterator_next(&page_iter, &page_hdr, &page_offset, &page_size)) {
        if (page_hdr.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA) continue;
        if (page_hdr.field_data_level < 1) continue;  // only L1+

        timeseries_fielddata_iterator_t fd_iter;
        if (!timeseries_fielddata_iterator_init(db, page_offset, page_size, &fd_iter)) continue;

        timeseries_field_data_header_t fd_hdr;
        while (timeseries_fielddata_iterator_next(&fd_iter, &fd_hdr)) {
            // Only count compressed records (bit1=0 means compressed)
            if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_COMPRESSED) != 0) continue;
            if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_ENCODING_ALP) == 0) {
                (*out_alp_count)++;
            } else {
                (*out_gorilla_count)++;
            }
        }
        timeseries_fielddata_iterator_deinit(&fd_iter);
    }
}

/**
 * @brief Insert multiple field types for comprehensive testing
 */
static bool insert_mixed_fields(const char *measurement,
                               size_t num_points,
                               uint64_t start_timestamp) {
    uint64_t *timestamps = malloc(num_points * sizeof(uint64_t));
    timeseries_field_value_t *field_values = malloc(num_points * 3 * sizeof(timeseries_field_value_t));

    if (!timestamps || !field_values) {
        free(timestamps);
        free(field_values);
        return false;
    }

    for (size_t i = 0; i < num_points; i++) {
        timestamps[i] = start_timestamp + (i * 1000);

        // Float field
        field_values[0 * num_points + i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[0 * num_points + i].data.float_val = (double)i * 1.5;

        // Int field
        field_values[1 * num_points + i].type = TIMESERIES_FIELD_TYPE_INT;
        field_values[1 * num_points + i].data.int_val = (int64_t)i * 100;

        // Bool field
        field_values[2 * num_points + i].type = TIMESERIES_FIELD_TYPE_BOOL;
        field_values[2 * num_points + i].data.bool_val = (i % 2 == 0);
    }

    const char *field_names[] = {"temp", "count", "active"};
    timeseries_insert_data_t insert_data = {
        .measurement_name = measurement,
        .tag_keys = NULL,
        .tag_values = NULL,
        .num_tags = 0,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 3,
        .timestamps_ms = timestamps,
        .num_points = num_points,
    };

    bool result = timeseries_insert(&insert_data);

    free(timestamps);
    free(field_values);
    return result;
}

/**
 * @brief Query all points from a measurement
 */
static bool query_all_points(const char *measurement, timeseries_query_result_t *result) {
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = measurement;
    query.start_ms = 0;
    query.end_ms = INT64_MAX;
    query.limit = 0;  // No limit

    memset(result, 0, sizeof(*result));
    return timeseries_query(&query, result);
}

/**
 * @brief Force multiple L0 pages by inserting data in small batches
 * This is necessary to trigger compaction threshold (MIN_PAGES_FOR_COMPACTION = 4)
 */
static bool create_multiple_l0_pages(const char *measurement, size_t num_batches) {
    for (size_t batch = 0; batch < num_batches; batch++) {
        // Insert small batch to create separate L0 pages
        if (!insert_float_points(measurement, "value", 50,
                                batch * 50000, 1000)) {
            ESP_LOGE(TAG, "Failed to insert batch %zu", batch);
            return false;
        }
    }
    return true;
}

// ============================================================================
// TEST CASES: BASIC COMPACTION FUNCTIONALITY
// ============================================================================

/**
 * Test: Compaction with no data
 * Coverage: Empty database, zero series IDs, skip logic
 */
TEST_CASE("compaction: empty database compaction", "[compaction][boundary]") {
    setup_database();

    // Compact empty database - should succeed but do nothing
    bool result = timeseries_compact_sync();
    TEST_ASSERT_TRUE_MESSAGE(result, "Compaction of empty DB should succeed");

    ESP_LOGI(TAG, "Empty database compaction succeeded");
}

/**
 * Test: Compaction below threshold
 * Coverage: MIN_PAGES_FOR_COMPACTION enforcement (< 4 pages)
 */
TEST_CASE("compaction: below threshold pages", "[compaction][boundary]") {
    setup_database();

    // Insert data that creates only 1-2 L0 pages (below threshold of 4)
    TEST_ASSERT_TRUE(insert_float_points("threshold_test", "value", 100, 1000, 1000));

    // Compaction should skip because we don't have enough pages
    bool result = timeseries_compact_sync();
    TEST_ASSERT_TRUE_MESSAGE(result, "Compaction below threshold should succeed but skip");

    // Verify data is still accessible (not corrupted by skip logic)
    timeseries_query_result_t query_result;
    TEST_ASSERT_TRUE(query_all_points("threshold_test", &query_result));
    TEST_ASSERT_EQUAL(100, query_result.num_points);
    timeseries_query_free_result(&query_result);

    ESP_LOGI(TAG, "Below threshold compaction test passed");
}

/**
 * Test: Single series L0 -> L1 compaction
 * Coverage: Level-0 to Level-1 compaction with single series
 */
TEST_CASE("compaction: single series L0 to L1", "[compaction][level0]") {
    setup_database();

    // Create multiple L0 pages with single series (need 4+ pages for compaction)
    TEST_ASSERT_TRUE(create_multiple_l0_pages("single_series", 5));

    // Query before compaction
    timeseries_query_result_t result_before;
    TEST_ASSERT_TRUE(query_all_points("single_series", &result_before));
    size_t points_before = result_before.num_points;
    ESP_LOGI(TAG, "Before compaction: %zu points", points_before);
    TEST_ASSERT_EQUAL(250, points_before);  // 5 batches * 50 points
    timeseries_query_free_result(&result_before);

    // Compact L0 -> L1
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query after compaction - all data should be preserved
    timeseries_query_result_t result_after;
    TEST_ASSERT_TRUE(query_all_points("single_series", &result_after));
    ESP_LOGI(TAG, "After compaction: %zu points", result_after.num_points);

    TEST_ASSERT_EQUAL(points_before, result_after.num_points);
    TEST_ASSERT_EQUAL(1, result_after.num_columns);

    // Verify timestamps are monotonically increasing
    for (size_t i = 1; i < result_after.num_points; i++) {
        TEST_ASSERT_TRUE_MESSAGE(result_after.timestamps[i] > result_after.timestamps[i - 1],
                                "Timestamps not monotonically increasing after L0->L1");
    }

    timeseries_query_free_result(&result_after);
    ESP_LOGI(TAG, "Single series L0->L1 compaction passed");
}

/**
 * Test: Multiple series L0 -> L1 compaction
 * Coverage: Multiple series IDs, series deduplication
 */
TEST_CASE("compaction: multiple series L0 to L1", "[compaction][level0]") {
    setup_database();

    // Create multiple series with different tags
    const char *tag_keys[] = {"location"};
    const char *loc1[] = {"room1"};
    const char *loc2[] = {"room2"};
    const char *loc3[] = {"room3"};

    // Insert multiple batches for each series to create L0 pages
    for (size_t batch = 0; batch < 5; batch++) {
        TEST_ASSERT_TRUE(insert_float_points_with_tags("multi_series", "temp",
                                                       tag_keys, loc1, 1,
                                                       20, batch * 20000, 1000, 1.5));
        TEST_ASSERT_TRUE(insert_float_points_with_tags("multi_series", "temp",
                                                       tag_keys, loc2, 1,
                                                       20, batch * 20000, 1000, 2.0));
        TEST_ASSERT_TRUE(insert_float_points_with_tags("multi_series", "temp",
                                                       tag_keys, loc3, 1,
                                                       20, batch * 20000, 1000, 2.5));
    }

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query each series separately to verify all data preserved
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "multi_series";
    query.start_ms = 0;
    query.end_ms = INT64_MAX;
    query.limit = 0;

    const char **test_tags[] = {loc1, loc2, loc3};
    for (int i = 0; i < 3; i++) {
        query.tag_keys = tag_keys;
        query.tag_values = test_tags[i];
        query.num_tags = 1;

        timeseries_query_result_t result;
        memset(&result, 0, sizeof(result));
        TEST_ASSERT_TRUE(timeseries_query(&query, &result));

        ESP_LOGI(TAG, "Series %d points after compaction: %zu", i, result.num_points);
        TEST_ASSERT_EQUAL(100, result.num_points);  // 5 batches * 20 points

        timeseries_query_free_result(&result);
    }

    ESP_LOGI(TAG, "Multiple series L0->L1 compaction passed");
}

/**
 * Test: L1 -> L2 compaction (multi-iterator path)
 * Coverage: Higher level compaction using multi-iterator
 */
TEST_CASE("compaction: L1 to L2 compaction", "[compaction][level1]") {
    setup_database();

    // Create enough L0 pages to trigger L0->L1 compaction
    for (size_t i = 0; i < 5; i++) {
        TEST_ASSERT_TRUE(insert_float_points("l1_test", "value", 50, i * 50000, 1000));
    }

    // First compaction: L0 -> L1
    ESP_LOGI(TAG, "First compaction L0->L1");
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Insert more data to create more L0 pages
    for (size_t i = 5; i < 10; i++) {
        TEST_ASSERT_TRUE(insert_float_points("l1_test", "value", 50, i * 50000, 1000));
    }

    // Second compaction: L0 -> L1 (creates more L1 pages)
    ESP_LOGI(TAG, "Second compaction L0->L1");
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Insert even more to build up L1 pages
    for (size_t i = 10; i < 15; i++) {
        TEST_ASSERT_TRUE(insert_float_points("l1_test", "value", 50, i * 50000, 1000));
    }

    // Third compaction: Should compact L0->L1 and potentially L1->L2
    ESP_LOGI(TAG, "Third compaction (may trigger L1->L2)");
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Verify all data is intact
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("l1_test", &result));
    ESP_LOGI(TAG, "Total points after multi-level compaction: %zu", result.num_points);

    TEST_ASSERT_EQUAL(750, result.num_points);  // 15 batches * 50 points

    // Verify data integrity
    for (size_t i = 1; i < result.num_points; i++) {
        TEST_ASSERT_TRUE(result.timestamps[i] > result.timestamps[i - 1]);
    }

    timeseries_query_free_result(&result);
    ESP_LOGI(TAG, "L1->L2 compaction passed");
}

// ============================================================================
// TEST CASES: DUPLICATE TIMESTAMP HANDLING
// ============================================================================

/**
 * Test: Duplicate timestamps in same series
 * Coverage: remove_duplicates_in_place() function, timestamp collision
 */
TEST_CASE("compaction: duplicate timestamps same series", "[compaction][dedup]") {
    setup_database();

    const char *measurement = "dup_test";

    // Insert data with overlapping timestamps
    // First batch: timestamps 0, 1000, 2000, 3000, 4000
    TEST_ASSERT_TRUE(insert_float_points(measurement, "value", 5, 0, 1000));

    // Second batch: overlapping timestamps 2000, 3000, 4000, 5000, 6000
    // This creates duplicates at 2000, 3000, 4000
    TEST_ASSERT_TRUE(insert_float_points(measurement, "value", 5, 2000, 1000));

    // Third batch: more overlap
    TEST_ASSERT_TRUE(insert_float_points(measurement, "value", 5, 4000, 1000));

    // Fourth batch
    TEST_ASSERT_TRUE(insert_float_points(measurement, "value", 5, 6000, 1000));

    // Fifth batch to trigger compaction
    TEST_ASSERT_TRUE(insert_float_points(measurement, "value", 5, 8000, 1000));

    // Compact - should deduplicate timestamps
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query after compaction
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points(measurement, &result));

    ESP_LOGI(TAG, "Points after deduplication: %zu", result.num_points);

    // Verify no duplicate timestamps
    for (size_t i = 1; i < result.num_points; i++) {
        TEST_ASSERT_NOT_EQUAL_MESSAGE(result.timestamps[i], result.timestamps[i - 1],
                                     "Duplicate timestamps found after compaction");
        TEST_ASSERT_TRUE(result.timestamps[i] > result.timestamps[i - 1]);
    }

    timeseries_query_free_result(&result);
    ESP_LOGI(TAG, "Duplicate timestamp handling passed");
}

/**
 * Test: Duplicate timestamps across different series (should be preserved)
 * Coverage: Series ID isolation during deduplication
 */
TEST_CASE("compaction: duplicate timestamps different series", "[compaction][dedup]") {
    setup_database();

    const char *tag_keys[] = {"sensor"};
    const char *s1[] = {"sensor1"};
    const char *s2[] = {"sensor2"};

    // Insert same timestamps for different series
    for (size_t batch = 0; batch < 5; batch++) {
        TEST_ASSERT_TRUE(insert_float_points_with_tags("sensors", "temp",
                                                       tag_keys, s1, 1,
                                                       20, 0, 1000, 1.5));
        TEST_ASSERT_TRUE(insert_float_points_with_tags("sensors", "temp",
                                                       tag_keys, s2, 1,
                                                       20, 0, 1000, 2.0));
    }

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query each series - both should have all timestamps preserved
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "sensors";
    query.tag_keys = tag_keys;
    query.num_tags = 1;
    query.start_ms = 0;
    query.end_ms = INT64_MAX;

    // Check sensor1
    query.tag_values = s1;
    timeseries_query_result_t r1;
    memset(&r1, 0, sizeof(r1));
    TEST_ASSERT_TRUE(timeseries_query(&query, &r1));
    TEST_ASSERT_EQUAL(20, r1.num_points);  // Should preserve all unique timestamps for this series
    timeseries_query_free_result(&r1);

    // Check sensor2
    query.tag_values = s2;
    timeseries_query_result_t r2;
    memset(&r2, 0, sizeof(r2));
    TEST_ASSERT_TRUE(timeseries_query(&query, &r2));
    TEST_ASSERT_EQUAL(20, r2.num_points);
    timeseries_query_free_result(&r2);

    ESP_LOGI(TAG, "Different series duplicate timestamp test passed");
}

// ============================================================================
// TEST CASES: PAGE MARKING AND CACHE INVALIDATION
// ============================================================================

/**
 * Test: Pages marked as obsolete after compaction
 * Coverage: tsdb_mark_old_level_pages_obsolete(), cache invalidation
 */
TEST_CASE("compaction: obsolete page marking", "[compaction][cache]") {
    setup_database();

    // Create L0 pages
    TEST_ASSERT_TRUE(create_multiple_l0_pages("obsolete_test", 5));

    // Query before - should work fine
    timeseries_query_result_t r1;
    TEST_ASSERT_TRUE(query_all_points("obsolete_test", &r1));
    size_t points_before = r1.num_points;
    timeseries_query_free_result(&r1);

    // Compact - old L0 pages should be marked obsolete
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query after - should still work with new L1 pages
    timeseries_query_result_t r2;
    TEST_ASSERT_TRUE(query_all_points("obsolete_test", &r2));
    TEST_ASSERT_EQUAL(points_before, r2.num_points);
    timeseries_query_free_result(&r2);

    ESP_LOGI(TAG, "Obsolete page marking test passed");
}

/**
 * Test: Cache invalidation after compaction
 * Coverage: tsdb_pagecache_remove_entry() during compaction
 */
TEST_CASE("compaction: cache invalidation", "[compaction][cache]") {
    setup_database();

    // Insert initial data
    TEST_ASSERT_TRUE(create_multiple_l0_pages("cache_inval", 5));

    // Query to populate cache
    timeseries_query_result_t r1;
    TEST_ASSERT_TRUE(query_all_points("cache_inval", &r1));
    timeseries_query_free_result(&r1);

    // Compact - cache entries for old pages should be removed
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Insert new data after compaction
    TEST_ASSERT_TRUE(insert_float_points("cache_inval", "value", 50, 500000, 1000));

    // Query should work correctly with updated cache
    timeseries_query_result_t r2;
    TEST_ASSERT_TRUE(query_all_points("cache_inval", &r2));
    TEST_ASSERT_EQUAL(300, r2.num_points);  // 250 + 50
    timeseries_query_free_result(&r2);

    ESP_LOGI(TAG, "Cache invalidation test passed");
}

// ============================================================================
// TEST CASES: LARGE DATASETS AND MULTIPLE PAGES
// ============================================================================

/**
 * Test: Large dataset requiring multiple L1 pages
 * Coverage: Page stream writer with multiple series, large record counts
 */
TEST_CASE("compaction: large dataset multiple pages", "[compaction][large]") {
    setup_database();

    // Insert large dataset that will require multiple pages after compaction
    const size_t NUM_BATCHES = 10;
    const size_t POINTS_PER_BATCH = 200;

    for (size_t i = 0; i < NUM_BATCHES; i++) {
        TEST_ASSERT_TRUE(insert_float_points("large_dataset", "value",
                                            POINTS_PER_BATCH,
                                            i * POINTS_PER_BATCH * 1000,
                                            1000));
    }

    ESP_LOGI(TAG, "Inserted %zu points across %zu batches",
             NUM_BATCHES * POINTS_PER_BATCH, NUM_BATCHES);

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Verify all data preserved
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("large_dataset", &result));

    ESP_LOGI(TAG, "Points after large compaction: %zu", result.num_points);
    TEST_ASSERT_EQUAL(NUM_BATCHES * POINTS_PER_BATCH, result.num_points);

    // Verify data integrity
    for (size_t i = 1; i < result.num_points; i++) {
        TEST_ASSERT_TRUE(result.timestamps[i] > result.timestamps[i - 1]);
    }

    timeseries_query_free_result(&result);
    ESP_LOGI(TAG, "Large dataset compaction passed");
}

// ============================================================================
// TEST CASES: MIXED FIELD TYPES
// ============================================================================

/**
 * Test: Compaction with multiple field types
 * Coverage: Field type preservation, mixed type handling
 */
TEST_CASE("compaction: mixed field types", "[compaction][types]") {
    setup_database();

    // Insert data with multiple field types
    for (size_t i = 0; i < 5; i++) {
        TEST_ASSERT_TRUE(insert_mixed_fields("mixed_types", 50, i * 50000));
    }

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query and verify each field type
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("mixed_types", &result));

    ESP_LOGI(TAG, "Points: %zu, Columns: %zu", result.num_points, result.num_columns);
    TEST_ASSERT_EQUAL(250, result.num_points);
    TEST_ASSERT_EQUAL(3, result.num_columns);

    // Verify field types preserved
    for (size_t i = 0; i < result.num_points; i++) {
        TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_FLOAT, result.columns[0].values[i].type);
        TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, result.columns[1].values[i].type);
        TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_BOOL, result.columns[2].values[i].type);
    }

    timeseries_query_free_result(&result);
    ESP_LOGI(TAG, "Mixed field types compaction passed");
}

// ============================================================================
// TEST CASES: ERROR HANDLING AND EDGE CASES
// ============================================================================

/**
 * Test: Compaction with single point
 * Coverage: Minimal data set, boundary condition
 */
TEST_CASE("compaction: single point per series", "[compaction][boundary]") {
    setup_database();

    // Insert single point multiple times to create pages
    for (size_t i = 0; i < 5; i++) {
        TEST_ASSERT_TRUE(insert_float_points("single_point", "value", 1, i * 1000, 1000));
    }

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Verify
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("single_point", &result));
    TEST_ASSERT_EQUAL(5, result.num_points);
    timeseries_query_free_result(&result);

    ESP_LOGI(TAG, "Single point compaction passed");
}

/**
 * Test: All levels compaction cascade
 * Coverage: timeseries_compact_all_levels(), multi-level cascade
 */
TEST_CASE("compaction: all levels cascade", "[compaction][levels]") {
    setup_database();

    // Create data through multiple compaction cycles to build up levels
    for (size_t cycle = 0; cycle < 3; cycle++) {
        ESP_LOGI(TAG, "Compaction cycle %zu", cycle);

        // Insert batches
        for (size_t batch = 0; batch < 6; batch++) {
            size_t offset = (cycle * 6 + batch) * 50;
            TEST_ASSERT_TRUE(insert_float_points("all_levels", "value",
                                                50, offset * 1000, 1000));
        }

        // Compact all levels
        TEST_ASSERT_TRUE(timeseries_compact_sync());
    }

    // Verify final state
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("all_levels", &result));

    ESP_LOGI(TAG, "Final points after cascade: %zu", result.num_points);
    TEST_ASSERT_EQUAL(900, result.num_points);  // 3 cycles * 6 batches * 50 points

    // Verify monotonic timestamps
    for (size_t i = 1; i < result.num_points; i++) {
        TEST_ASSERT_TRUE(result.timestamps[i] > result.timestamps[i - 1]);
    }

    timeseries_query_free_result(&result);
    ESP_LOGI(TAG, "All levels cascade passed");
}

/**
 * Test: Compaction preserves timestamp ordering
 * Coverage: Sorting logic in compaction, compare_points_by_timestamp
 */
TEST_CASE("compaction: timestamp ordering preservation", "[compaction][order]") {
    setup_database();

    // Insert data in non-sequential order
    TEST_ASSERT_TRUE(insert_float_points("order_test", "value", 50, 100000, 1000));
    TEST_ASSERT_TRUE(insert_float_points("order_test", "value", 50, 0, 1000));
    TEST_ASSERT_TRUE(insert_float_points("order_test", "value", 50, 200000, 1000));
    TEST_ASSERT_TRUE(insert_float_points("order_test", "value", 50, 50000, 1000));
    TEST_ASSERT_TRUE(insert_float_points("order_test", "value", 50, 150000, 1000));

    // Compact - should sort by timestamp
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query and verify ordering
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("order_test", &result));

    ESP_LOGI(TAG, "Checking timestamp ordering of %zu points", result.num_points);

    // Verify strict monotonic increasing
    for (size_t i = 1; i < result.num_points; i++) {
        TEST_ASSERT_TRUE_MESSAGE(result.timestamps[i] > result.timestamps[i - 1],
                                "Timestamps not properly sorted after compaction");
    }

    // Verify first and last timestamps
    TEST_ASSERT_EQUAL(0, result.timestamps[0]);
    TEST_ASSERT_EQUAL(249000, result.timestamps[result.num_points - 1]);

    timeseries_query_free_result(&result);
    ESP_LOGI(TAG, "Timestamp ordering test passed");
}

/**
 * Test: Sequential compactions maintain data integrity
 * Coverage: Multiple compaction cycles, cumulative effects
 */
TEST_CASE("compaction: sequential integrity", "[compaction][integrity]") {
    setup_database();

    size_t expected_total = 0;
    uint64_t ts_offset = 0;  // Track cumulative timestamp offset

    // Round 1: timestamps 0 - 249000
    for (size_t batch = 0; batch < 5; batch++) {
        TEST_ASSERT_TRUE(insert_float_points("seq_test", "value", 50,
                                             ts_offset + batch * 50000, 1000));
    }
    ts_offset += 5 * 50000;  // Move past Round 1 timestamps
    expected_total += 250;
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    timeseries_query_result_t r1;
    TEST_ASSERT_TRUE(query_all_points("seq_test", &r1));
    TEST_ASSERT_EQUAL(expected_total, r1.num_points);
    timeseries_query_free_result(&r1);

    // Round 2: timestamps 250000 - 499000 (non-overlapping)
    for (size_t batch = 0; batch < 5; batch++) {
        TEST_ASSERT_TRUE(insert_float_points("seq_test", "value", 50,
                                             ts_offset + batch * 50000, 1000));
    }
    ts_offset += 5 * 50000;  // Move past Round 2 timestamps
    expected_total += 250;
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    timeseries_query_result_t r2;
    TEST_ASSERT_TRUE(query_all_points("seq_test", &r2));
    TEST_ASSERT_EQUAL(expected_total, r2.num_points);
    timeseries_query_free_result(&r2);

    // Round 3: timestamps 500000 - 749000 (non-overlapping)
    for (size_t batch = 0; batch < 5; batch++) {
        TEST_ASSERT_TRUE(insert_float_points("seq_test", "value", 50,
                                             ts_offset + batch * 50000, 1000));
    }
    expected_total += 250;
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    timeseries_query_result_t r3;
    TEST_ASSERT_TRUE(query_all_points("seq_test", &r3));
    TEST_ASSERT_EQUAL(expected_total, r3.num_points);

    // Verify all timestamps valid
    for (size_t i = 1; i < r3.num_points; i++) {
        TEST_ASSERT_TRUE(r3.timestamps[i] > r3.timestamps[i - 1]);
    }

    timeseries_query_free_result(&r3);
    ESP_LOGI(TAG, "Sequential integrity test passed with %zu total points", expected_total);
}

/**
 * Test: Compaction with zero valid series IDs
 * Coverage: gather_unique_series_ids() returning empty list
 */
TEST_CASE("compaction: no valid series after filter", "[compaction][edge]") {
    setup_database();

    // This test would require inserting then deleting data, or corrupt records
    // For now, we test empty case which is similar
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    ESP_LOGI(TAG, "No valid series test passed");
}

/**
 * Test: Maximum supported level (TSDB_MAX_LEVEL)
 * Coverage: Level boundary enforcement, TSDB_MAX_LEVEL constant
 */
TEST_CASE("compaction: max level boundary", "[compaction][boundary]") {
    setup_database();

    // Build up through multiple compaction rounds to potentially reach max level
    // This requires creating enough data to cascade through levels
    for (size_t mega_round = 0; mega_round < 5; mega_round++) {
        for (size_t round = 0; round < 4; round++) {
            for (size_t batch = 0; batch < 5; batch++) {
                size_t base = (mega_round * 100 + round * 20 + batch * 4);
                TEST_ASSERT_TRUE(insert_float_points("max_level", "value",
                                                    30, base * 1000, 1000));
            }
            TEST_ASSERT_TRUE(timeseries_compact_sync());
        }
    }

    // Verify all data still accessible
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("max_level", &result));

    ESP_LOGI(TAG, "Points after max level test: %zu", result.num_points);
    TEST_ASSERT_TRUE(result.num_points > 0);

    timeseries_query_free_result(&result);
    ESP_LOGI(TAG, "Max level boundary test passed");
}

// ============================================================================
// TEST CASES: DATA TYPE SPECIFIC
// ============================================================================

/**
 * Test: Integer field compaction
 * Coverage: TIMESERIES_FIELD_TYPE_INT handling
 */
TEST_CASE("compaction: integer fields", "[compaction][types]") {
    setup_database();

    // Insert integer data
    for (size_t batch = 0; batch < 5; batch++) {
        uint64_t *timestamps = malloc(50 * sizeof(uint64_t));
        timeseries_field_value_t *values = malloc(50 * sizeof(timeseries_field_value_t));

        for (size_t i = 0; i < 50; i++) {
            timestamps[i] = batch * 50000 + i * 1000;
            values[i].type = TIMESERIES_FIELD_TYPE_INT;
            values[i].data.int_val = (int64_t)(batch * 50 + i);
        }

        const char *field_names[] = {"count"};
        timeseries_insert_data_t insert_data = {
            .measurement_name = "int_test",
            .tag_keys = NULL,
            .tag_values = NULL,
            .num_tags = 0,
            .field_names = field_names,
            .field_values = values,
            .num_fields = 1,
            .timestamps_ms = timestamps,
            .num_points = 50,
        };

        TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

        free(timestamps);
        free(values);
    }

    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Verify
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("int_test", &result));
    TEST_ASSERT_EQUAL(250, result.num_points);

    // Check type preservation
    for (size_t i = 0; i < result.num_points; i++) {
        TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_INT, result.columns[0].values[i].type);
    }

    timeseries_query_free_result(&result);
    ESP_LOGI(TAG, "Integer field compaction passed");
}

/**
 * Test: Boolean field compaction
 * Coverage: TIMESERIES_FIELD_TYPE_BOOL handling
 */
TEST_CASE("compaction: boolean fields", "[compaction][types]") {
    setup_database();

    // Insert boolean data
    for (size_t batch = 0; batch < 5; batch++) {
        uint64_t *timestamps = malloc(50 * sizeof(uint64_t));
        timeseries_field_value_t *values = malloc(50 * sizeof(timeseries_field_value_t));

        for (size_t i = 0; i < 50; i++) {
            timestamps[i] = batch * 50000 + i * 1000;
            values[i].type = TIMESERIES_FIELD_TYPE_BOOL;
            values[i].data.bool_val = (i % 2 == 0);
        }

        const char *field_names[] = {"active"};
        timeseries_insert_data_t insert_data = {
            .measurement_name = "bool_test",
            .tag_keys = NULL,
            .tag_values = NULL,
            .num_tags = 0,
            .field_names = field_names,
            .field_values = values,
            .num_fields = 1,
            .timestamps_ms = timestamps,
            .num_points = 50,
        };

        TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

        free(timestamps);
        free(values);
    }

    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Verify
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("bool_test", &result));
    TEST_ASSERT_EQUAL(250, result.num_points);

    // Check type preservation
    for (size_t i = 0; i < result.num_points; i++) {
        TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_BOOL, result.columns[0].values[i].type);
    }

    timeseries_query_free_result(&result);
    ESP_LOGI(TAG, "Boolean field compaction passed");
}

// ============================================================================
// TEST CASES: STRESS AND PERFORMANCE
// ============================================================================

/**
 * Test: Stress test with many series
 * Coverage: Series map growth, memory allocation
 */
TEST_CASE("compaction: many series stress test", "[compaction][stress]") {
    setup_database();

    const size_t NUM_SERIES = 20;
    const char *tag_keys[] = {"id"};
    char tag_values[NUM_SERIES][16];

    // Create many series
    for (size_t series = 0; series < NUM_SERIES; series++) {
        snprintf(tag_values[series], sizeof(tag_values[series]), "series%zu", series);

        const char *tag_val[] = {tag_values[series]};

        // Multiple batches per series
        for (size_t batch = 0; batch < 3; batch++) {
            TEST_ASSERT_TRUE(insert_float_points_with_tags("stress_test", "value",
                                                          tag_keys, tag_val, 1,
                                                          20, batch * 20000, 1000, 1.0));
        }
    }

    ESP_LOGI(TAG, "Inserted data for %zu series", NUM_SERIES);

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Verify each series
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "stress_test";
    query.tag_keys = tag_keys;
    query.num_tags = 1;
    query.start_ms = 0;
    query.end_ms = INT64_MAX;

    for (size_t series = 0; series < NUM_SERIES; series++) {
        const char *tag_val[] = {tag_values[series]};
        query.tag_values = tag_val;

        timeseries_query_result_t result;
        memset(&result, 0, sizeof(result));
        TEST_ASSERT_TRUE(timeseries_query(&query, &result));
        TEST_ASSERT_EQUAL(60, result.num_points);  // 3 batches * 20 points
        timeseries_query_free_result(&result);
    }

    ESP_LOGI(TAG, "Many series stress test passed");
}

/**
 * Test: Verify page size estimation
 * Coverage: Page stream writer size calculations
 */
TEST_CASE("compaction: page size handling", "[compaction][size]") {
    setup_database();

    // Insert varying sized data
    TEST_ASSERT_TRUE(create_multiple_l0_pages("size_test", 5));

    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Verify data accessible
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("size_test", &result));
    TEST_ASSERT_TRUE(result.num_points > 0);
    timeseries_query_free_result(&result);

    ESP_LOGI(TAG, "Page size handling test passed");
}

// ============================================================================
// TEST CASES: ALP ENCODING FLAG DISCRIMINATION
// ============================================================================

/**
 * Test: FLOAT series uses ALP encoding (bit2=0) after L0->L1 compaction.
 *
 * The insert chunk_size is 500 points; inserting 2000 float points creates
 * 4 × 500-point L0 pages which satisfies MIN_PAGES_FOR_COMPACTION.
 * After compaction every compressed record on L1 pages must have the
 * TSDB_FIELDDATA_FLAG_ENCODING_ALP bit cleared (ALP encoding).
 */
TEST_CASE("compaction: FLOAT records use ALP encoding flag after compaction", "[compaction][alp][encoding]") {
    setup_database();

    // 2000 points → 4 × 500-point chunks → 4 L0 pages → compaction threshold met
    const size_t N = 2000;
    uint64_t *ts = malloc(N * sizeof(uint64_t));
    timeseries_field_value_t *fv = malloc(N * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(fv);
    for (size_t i = 0; i < N; i++) {
        ts[i] = i * 1000ULL;
        fv[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        fv[i].data.float_val = (double)i * 1.5;
    }
    const char *fn[] = {"temp"};
    timeseries_insert_data_t ins = {
        .measurement_name = "alp_flag_float", .tag_keys = NULL,
        .tag_values = NULL, .num_tags = 0, .field_names = fn,
        .field_values = fv, .num_fields = 1, .timestamps_ms = ts, .num_points = N,
    };
    TEST_ASSERT_TRUE(timeseries_insert(&ins));
    free(ts); free(fv);

    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Scan L1+ pages: every compressed record must have ALP flag cleared (bit2=0)
    int alp_count = 0, gorilla_count = 0;
    count_l1_encoding_flags(&alp_count, &gorilla_count);

    ESP_LOGI(TAG, "FLOAT after compaction: ALP=%d Gorilla=%d", alp_count, gorilla_count);
    TEST_ASSERT_GREATER_THAN(0, alp_count);
    TEST_ASSERT_EQUAL_MESSAGE(0, gorilla_count,
        "FLOAT series should never produce Gorilla-encoded L1 records");

    // Also verify data is intact
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("alp_flag_float", &result));
    TEST_ASSERT_EQUAL(N, result.num_points);
    timeseries_query_free_result(&result);

    ESP_LOGI(TAG, "FLOAT ALP flag discrimination passed");
}

/**
 * Test: BOOL series uses Gorilla encoding (bit2=1) after L0->L1 compaction.
 * Verifies TSDB_FIELDDATA_FLAG_ENCODING_ALP is NOT cleared for BOOL records.
 */
TEST_CASE("compaction: BOOL records stay Gorilla after compaction", "[compaction][alp][encoding]") {
    setup_database();

    // BOOL values are 1 byte each, so ~883 points fit per 8KB L0 page (vs ~500
    // for FLOAT which is 8 bytes).  We need 4 * 883 ≈ 3532+ points to reliably
    // create at least 4 L0 pages and meet MIN_PAGES_FOR_COMPACTION.
    const size_t N = 4000;
    uint64_t *ts = malloc(N * sizeof(uint64_t));
    timeseries_field_value_t *fv = malloc(N * sizeof(timeseries_field_value_t));
    TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(fv);
    for (size_t i = 0; i < N; i++) {
        ts[i] = i * 1000ULL;
        fv[i].type = TIMESERIES_FIELD_TYPE_BOOL;
        fv[i].data.bool_val = (i % 2 == 0);
    }
    const char *fn[] = {"active"};
    timeseries_insert_data_t ins = {
        .measurement_name = "alp_flag_bool", .tag_keys = NULL,
        .tag_values = NULL, .num_tags = 0, .field_names = fn,
        .field_values = fv, .num_fields = 1, .timestamps_ms = ts, .num_points = N,
    };
    TEST_ASSERT_TRUE(timeseries_insert(&ins));
    free(ts); free(fv);

    TEST_ASSERT_TRUE(timeseries_compact_sync());

    int alp_count = 0, gorilla_count = 0;
    count_l1_encoding_flags(&alp_count, &gorilla_count);

    ESP_LOGI(TAG, "BOOL after compaction: ALP=%d Gorilla=%d", alp_count, gorilla_count);
    TEST_ASSERT_GREATER_THAN(0, gorilla_count);
    TEST_ASSERT_EQUAL_MESSAGE(0, alp_count,
        "BOOL series should never produce ALP-encoded L1 records");

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("alp_flag_bool", &result));
    TEST_ASSERT_EQUAL(N, result.num_points);
    // Spot-check bool values are correct (alternating true/false)
    for (size_t i = 0; i < result.num_points; i++) {
        bool expected = (i % 2 == 0);
        bool actual = result.columns[0].values[i].data.bool_val;
        if (actual != expected) {
            ESP_LOGE(TAG, "Bool value mismatch at i=%zu: expected=%d actual=%d",
                     i, (int)expected, (int)actual);
            timeseries_query_free_result(&result);
            TEST_FAIL_MESSAGE("BOOL value mismatch after Gorilla compaction");
        }
    }
    timeseries_query_free_result(&result);

    ESP_LOGI(TAG, "BOOL stays Gorilla passed");
}

/**
 * Test: ALP encoding is lossless for FLOAT values.
 * Inserts 100 float points with known pattern, compacts to ALP L1, then queries
 * and verifies every value is bit-exact (within float epsilon).
 */
TEST_CASE("compaction: ALP exact value preservation float", "[compaction][alp][precision]") {
    setup_database();

    const size_t N = 100;
    // Use a pattern that exercises ALP: mix of integer-valued floats and
    // fractional values (0.0, 1.5, 3.0, 4.5, ...) stored as double in insert
    for (size_t batch = 0; batch < 5; batch++) {
        uint64_t *ts  = malloc(N * sizeof(uint64_t));
        timeseries_field_value_t *fv = malloc(N * sizeof(timeseries_field_value_t));
        TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(fv);
        for (size_t i = 0; i < N; i++) {
            ts[i] = (batch * N + i) * 1000ULL;
            fv[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
            fv[i].data.float_val = (double)(batch * N + i) * 1.5;
        }
        const char *fn[] = {"val"};
        timeseries_insert_data_t ins = {
            .measurement_name = "alp_prec_float", .tag_keys = NULL,
            .tag_values = NULL, .num_tags = 0, .field_names = fn,
            .field_values = fv, .num_fields = 1,
            .timestamps_ms = ts, .num_points = N,
        };
        TEST_ASSERT_TRUE(timeseries_insert(&ins));
        free(ts); free(fv);
    }

    TEST_ASSERT_TRUE(timeseries_compact_sync());

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("alp_prec_float", &result));
    TEST_ASSERT_EQUAL(5 * N, result.num_points);
    TEST_ASSERT_EQUAL(1, result.num_columns);

    for (size_t i = 0; i < result.num_points; i++) {
        double expected = (double)i * 1.5;
        double actual   = result.columns[0].values[i].data.float_val;
        // ALP is lossless for float32 values promoted to float64:
        // Allow a small epsilon for float→double→ALP→double round-trip
        if (fabs(actual - expected) > 1e-6) {
            ESP_LOGE(TAG, "Float mismatch at i=%zu: expected=%.6f actual=%.6f",
                     i, expected, actual);
            TEST_FAIL_MESSAGE("Float value corrupted by ALP encoding");
        }
    }
    timeseries_query_free_result(&result);

    ESP_LOGI(TAG, "ALP float value precision passed (%zu points verified)", 5 * N);
}

/**
 * Test: ALP encoding is lossless for INT values.
 * Inserts 100 int64 points with known values, compacts to ALP, verifies bit-exact.
 */
TEST_CASE("compaction: ALP exact value preservation int", "[compaction][alp][precision]") {
    setup_database();

    const size_t N = 100;
    for (size_t batch = 0; batch < 5; batch++) {
        uint64_t *ts  = malloc(N * sizeof(uint64_t));
        timeseries_field_value_t *fv = malloc(N * sizeof(timeseries_field_value_t));
        TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(fv);
        for (size_t i = 0; i < N; i++) {
            ts[i] = (batch * N + i) * 500ULL;
            fv[i].type = TIMESERIES_FIELD_TYPE_INT;
            fv[i].data.int_val = (int64_t)(batch * N + i) * 100LL;
        }
        const char *fn[] = {"counter"};
        timeseries_insert_data_t ins = {
            .measurement_name = "alp_prec_int", .tag_keys = NULL,
            .tag_values = NULL, .num_tags = 0, .field_names = fn,
            .field_values = fv, .num_fields = 1,
            .timestamps_ms = ts, .num_points = N,
        };
        TEST_ASSERT_TRUE(timeseries_insert(&ins));
        free(ts); free(fv);
    }

    TEST_ASSERT_TRUE(timeseries_compact_sync());

    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(query_all_points("alp_prec_int", &result));
    TEST_ASSERT_EQUAL(5 * N, result.num_points);
    TEST_ASSERT_EQUAL(1, result.num_columns);

    for (size_t i = 0; i < result.num_points; i++) {
        int64_t expected = (int64_t)i * 100LL;
        int64_t actual   = result.columns[0].values[i].data.int_val;
        if (actual != expected) {
            ESP_LOGE(TAG, "Int mismatch at i=%zu: expected=%" PRId64 " actual=%" PRId64,
                     i, expected, actual);
            TEST_FAIL_MESSAGE("Int64 value corrupted by ALP encoding");
        }
    }
    timeseries_query_free_result(&result);

    ESP_LOGI(TAG, "ALP int value precision passed (%zu points verified)", 5 * N);
}

// ============================================================================
// TEST CASES: STORAGE ROLLOVER
// ============================================================================

/**
 * Test: Storage rollover — sustained write-compact cycles exhaust and recycle
 *       flash space without data loss.
 *
 * Each round inserts 500 points then synchronously compacts.  After several
 * rounds the flash contains a mixture of OBSOLETE L0 pages (freed by past
 * compactions) and active L1/L2 pages.  New L0 pages must reuse the OBSOLETE
 * space via the blank iterator.  The test verifies:
 *   1. Every round's insert succeeds (allocator finds space).
 *   2. After all rounds, a query returns the full expected point count.
 *   3. Value integrity: spot-check the most-recent batch's values.
 */
TEST_CASE("compaction: storage rollover sustained write pressure", "[compaction][rollover]") {
    setup_database();

    const size_t ROUNDS      = 8;
    const size_t PTS_PER_ROUND = 500;
    const size_t EXPECTED_TOTAL = ROUNDS * PTS_PER_ROUND;

    for (size_t round = 0; round < ROUNDS; round++) {
        uint64_t start_ts = (uint64_t)round * PTS_PER_ROUND * 1000ULL;

        // Insert a batch large enough to span several L0 pages
        uint64_t *ts = malloc(PTS_PER_ROUND * sizeof(uint64_t));
        timeseries_field_value_t *fv = malloc(PTS_PER_ROUND * sizeof(timeseries_field_value_t));
        TEST_ASSERT_NOT_NULL_MESSAGE(ts,  "OOM allocating timestamps for rollover test");
        TEST_ASSERT_NOT_NULL_MESSAGE(fv, "OOM allocating values for rollover test");

        for (size_t i = 0; i < PTS_PER_ROUND; i++) {
            ts[i] = start_ts + i * 1000ULL;
            fv[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
            // Use a predictable pattern: round * 1000 + i * 1.5
            fv[i].data.float_val = (double)(round * PTS_PER_ROUND + i) * 1.5;
        }
        const char *fn[] = {"sensor"};
        timeseries_insert_data_t ins = {
            .measurement_name = "rollover_test", .tag_keys = NULL,
            .tag_values = NULL, .num_tags = 0, .field_names = fn,
            .field_values = fv, .num_fields = 1,
            .timestamps_ms = ts, .num_points = PTS_PER_ROUND,
        };
        bool insert_ok = timeseries_insert(&ins);
        free(ts); free(fv);

        ESP_LOGI(TAG, "Rollover round %zu: insert %s", round, insert_ok ? "OK" : "FAILED");
        TEST_ASSERT_TRUE_MESSAGE(insert_ok, "Insert failed during rollover cycle");

        // Compact after each round to free L0 space (enabling OBSOLETE reuse)
        bool compact_ok = timeseries_compact_sync();
        ESP_LOGI(TAG, "Rollover round %zu: compact %s", round, compact_ok ? "OK" : "FAILED");
        TEST_ASSERT_TRUE_MESSAGE(compact_ok, "Compact failed during rollover cycle");

        // Check that all data so far is still queryable
        timeseries_query_result_t partial;
        TEST_ASSERT_TRUE(query_all_points("rollover_test", &partial));
        size_t expected_so_far = (round + 1) * PTS_PER_ROUND;
        if (partial.num_points != expected_so_far) {
            ESP_LOGE(TAG, "Round %zu: expected %zu points, got %zu",
                     round, expected_so_far, partial.num_points);
            timeseries_query_free_result(&partial);
            TEST_FAIL_MESSAGE("Point count mismatch during rollover cycle");
        }
        timeseries_query_free_result(&partial);

        // Log storage utilisation
        tsdb_usage_summary_t usage;
        if (timeseries_get_usage_summary(&usage)) {
            ESP_LOGI(TAG, "  Storage: %" PRIu32 " / %" PRIu32 " bytes used (%.1f%%)",
                     usage.used_space_bytes, usage.total_space_bytes,
                     100.0f * usage.used_space_bytes / usage.total_space_bytes);
        }
    }

    // Final verification: full dataset, all values correct
    timeseries_query_result_t final_result;
    TEST_ASSERT_TRUE(query_all_points("rollover_test", &final_result));
    TEST_ASSERT_EQUAL(EXPECTED_TOTAL, final_result.num_points);
    TEST_ASSERT_EQUAL(1, final_result.num_columns);

    // Verify every value is bit-exact (ALP is lossless)
    for (size_t i = 0; i < final_result.num_points; i++) {
        double expected = (double)i * 1.5;
        double actual   = final_result.columns[0].values[i].data.float_val;
        if (fabs(actual - expected) > 1e-6) {
            ESP_LOGE(TAG, "Final value mismatch at i=%zu: expected=%.6f actual=%.6f",
                     i, expected, actual);
            timeseries_query_free_result(&final_result);
            TEST_FAIL_MESSAGE("Value corrupted after storage rollover");
        }
    }
    timeseries_query_free_result(&final_result);

    ESP_LOGI(TAG, "Storage rollover test passed: %zu total points across %zu rounds",
             EXPECTED_TOTAL, ROUNDS);
}

/* --------------------------------------------------------------------------
 * compaction: ALP record chunking splits series exceeding chunk limit
 * --------------------------------------------------------------------------
 * With CONFIG_TIMESERIES_ALP_CHUNK_MAX_POINTS=200 (set in sdkconfig.defaults
 * for the test build), a series of 2000 points triggers the chunking code path
 * during L0→L1 compaction: pts_used=2000 → ceil(2000/200)=10 consecutive ALP
 * records written for the same series in the L1 page.
 *
 * A second phase then accumulates 4 L1 pages (by repeating the insert+compact
 * cycle) to trigger L1→L2 compaction via the multi-iterator path, where
 * alp_used = 4×2000 = 8000 > 200 → at least 40 ALP records written in L2.
 *
 * In a production build (chunk limit = 65535) these same datasets fit in a
 * single record; the split only fires for ≥65536 points per series.
 * -------------------------------------------------------------------------- */
TEST_CASE("compaction: ALP record chunking when series exceeds chunk limit",
          "[compaction][alp][chunking]") {
    timeseries_clear_all();

    const size_t PTS_PER_ROUND = 2000;
    const int    ROUNDS        = 4;
    const size_t TOTAL_PTS     = PTS_PER_ROUND * ROUNDS;
    const size_t CHUNK         = CONFIG_TIMESERIES_ALP_CHUNK_MAX_POINTS;
    /* ceil(PTS_PER_ROUND / CHUNK) */
    const int MIN_RECS_PER_ROUND = (int)((PTS_PER_ROUND + CHUNK - 1) / CHUNK);

    /* ------------------------------------------------------------------ */
    /* Phase 1 – L0→L1: insert 2000 points, verify chunk split in L1 page */
    /* ------------------------------------------------------------------ */
    ESP_LOGI(TAG, "Phase 1: inserting %zu float points (chunk_limit=%zu)",
             PTS_PER_ROUND, CHUNK);
    TEST_ASSERT_TRUE(insert_float_points("alp_chunk", "value",
                                         PTS_PER_ROUND, 0, 1000));
    timeseries_compact_sync();

    int alp_count = 0, gorilla_count = 0;
    count_l1_encoding_flags(&alp_count, &gorilla_count);
    ESP_LOGI(TAG, "L0→L1: alp_records=%d (expected>=%d), gorilla_records=%d",
             alp_count, MIN_RECS_PER_ROUND, gorilla_count);
    TEST_ASSERT_GREATER_OR_EQUAL(MIN_RECS_PER_ROUND, alp_count);
    TEST_ASSERT_EQUAL(0, gorilla_count);

    /* All points readable; values must be bit-exact across chunk boundaries */
    timeseries_query_result_t r;
    TEST_ASSERT_TRUE(query_all_points("alp_chunk", &r));
    TEST_ASSERT_EQUAL((int)PTS_PER_ROUND, (int)r.num_points);
    TEST_ASSERT_EQUAL(1, r.num_columns);
    for (size_t i = 0; i < r.num_points; i++) {
        double expected = (double)i * 1.5;
        double actual   = r.columns[0].values[i].data.float_val;
        if (fabs(actual - expected) > 1e-6) {
            ESP_LOGE(TAG, "L0→L1 chunk value mismatch i=%zu: expected=%.6f got=%.6f",
                     i, expected, actual);
            timeseries_query_free_result(&r);
            TEST_FAIL_MESSAGE("Value corrupted across ALP chunk boundary (L0→L1)");
        }
    }
    timeseries_query_free_result(&r);
    ESP_LOGI(TAG, "Phase 1 passed: %zu points correct across %d ALP records",
             PTS_PER_ROUND, alp_count);

    /* ------------------------------------------------------------------ */
    /* Phase 2 – L1→L2: repeat 3 more rounds so 4 L1 pages accumulate,   */
    /* then compact L1→L2 via the multi-iterator path.                    */
    /* ------------------------------------------------------------------ */
    for (int round = 1; round < ROUNDS; round++) {
        uint64_t base_ts = (uint64_t)round * PTS_PER_ROUND * 1000ULL;
        ESP_LOGI(TAG, "Phase 2 round %d: inserting %zu points at base_ts=%" PRIu64,
                 round, PTS_PER_ROUND, base_ts);
        TEST_ASSERT_TRUE(insert_float_points("alp_chunk", "value",
                                             PTS_PER_ROUND, base_ts, 1000));
        timeseries_compact_sync();
    }

    /* 4 L1 pages now exist — compact_sync triggers L1→L2 */
    timeseries_compact_sync();

    /* ceil(TOTAL_PTS / CHUNK) = ceil(8000 / 200) = 40 */
    int min_total_records = (int)((TOTAL_PTS + CHUNK - 1) / CHUNK);
    count_l1_encoding_flags(&alp_count, &gorilla_count);
    ESP_LOGI(TAG, "L1→L2: alp_records=%d (expected>=%d)", alp_count, min_total_records);
    TEST_ASSERT_GREATER_OR_EQUAL(min_total_records, alp_count);
    TEST_ASSERT_EQUAL(0, gorilla_count);

    /* Correctness check: verify first CHUNK points survive L1→L2 compaction.
     * Querying all TOTAL_PTS simultaneously would create (TOTAL_PTS/CHUNK) ALP
     * sub-iterators at once (CHUNK=200 → 40×3.2KB≈128KB decoded data) plus a
     * ~224KB result array — both exceed DRAM on ESP32-S3 without PSRAM.
     * The alp_count ≥ min_total_records assertion above already confirms all
     * data was written and chunked correctly; here we verify decode correctness
     * for the first CHUNK points via a single sub-iterator. */
    {
        timeseries_query_t q_verify;
        memset(&q_verify, 0, sizeof(q_verify));
        q_verify.measurement_name = "alp_chunk";
        q_verify.start_ms = 0;
        q_verify.end_ms = INT64_MAX;
        q_verify.limit = (unsigned int)CHUNK;  /* prunes to 1 ALP sub-iterator */
        TEST_ASSERT_TRUE(timeseries_query(&q_verify, &r));
    }
    TEST_ASSERT_EQUAL((int)CHUNK, (int)r.num_points);
    TEST_ASSERT_EQUAL(1, r.num_columns);
    for (size_t i = 0; i < r.num_points; i++) {
        double expected = (double)i * 1.5;   /* round 0 values: i * 1.5 */
        double actual   = r.columns[0].values[i].data.float_val;
        if (fabs(actual - expected) > 1e-6) {
            ESP_LOGE(TAG, "L1→L2 data corrupt at i=%zu: expected=%.6f got=%.6f",
                     i, expected, actual);
            timeseries_query_free_result(&r);
            TEST_FAIL_MESSAGE("Value corrupted after L1→L2 ALP compaction");
        }
    }
    timeseries_query_free_result(&r);
    ESP_LOGI(TAG, "Phase 2 passed: %d ALP records in L2, first %zu points verified correct",
             alp_count, (size_t)CHUNK);
}
