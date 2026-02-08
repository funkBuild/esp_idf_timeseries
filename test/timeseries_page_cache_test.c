/**
 * @file timeseries_page_cache_test.c
 * @brief Unit tests for page cache module
 *
 * Tests the in-memory page cache that tracks active pages for fast lookups.
 * NOTE: This is different from the series ID cache tested in timeseries_cache_test.c
 */

#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_page_cache.h"
#include "esp_log.h"
#include <string.h>

static const char *TAG = "page_cache_test";

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

// Helper to insert test data
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

// ============================================================================
// NULL Parameter Tests
// ============================================================================

TEST_CASE("page_cache: build with NULL db", "[page_cache][null]") {
    bool result = tsdb_build_page_cache(NULL);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("page_cache: add entry with NULL db", "[page_cache][null]") {
    timeseries_page_header_t hdr = {
        .magic_number = TIMESERIES_MAGIC_NUM,
        .page_size = 4096,
        .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
        .page_state = TIMESERIES_PAGE_STATE_ACTIVE,
        .sequence_num = 1,
        .field_data_level = 0
    };
    // Should not crash
    tsdb_pagecache_add_entry(NULL, 0x10000, &hdr);
    TEST_PASS();
}

TEST_CASE("page_cache: add entry with NULL header", "[page_cache][null]") {
    setup_database();
    // Should not crash
    tsdb_pagecache_add_entry(db, 0x10000, NULL);
    teardown_database();
    TEST_PASS();
}

TEST_CASE("page_cache: remove entry with NULL db", "[page_cache][null]") {
    bool result = tsdb_pagecache_remove_entry(NULL, 0x10000);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("page_cache: get total size with NULL db", "[page_cache][null]") {
    uint32_t size = tsdb_pagecache_get_total_active_size(NULL);
    TEST_ASSERT_EQUAL(0, size);
}

TEST_CASE("page_cache: get page size with NULL db", "[page_cache][null]") {
    uint32_t size = tsdb_pagecache_get_page_size(NULL, 0x10000);
    TEST_ASSERT_EQUAL(0, size);
}

TEST_CASE("page_cache: clear with NULL db", "[page_cache][null]") {
    // Should not crash
    tsdb_pagecache_clear(NULL);
    TEST_PASS();
}

// ============================================================================
// Empty Database Tests
// ============================================================================

TEST_CASE("page_cache: build cache on empty partition", "[page_cache][empty]") {
    setup_database();

    // Cache should have been built during init
    // Query cache - should be empty (no field data pages)
    uint32_t total_size = tsdb_pagecache_get_total_active_size(db);
    ESP_LOGI(TAG, "Total active size after init: %" PRIu32, total_size);

    teardown_database();
    TEST_PASS();
}

TEST_CASE("page_cache: clear empty cache", "[page_cache][empty]") {
    setup_database();

    tsdb_pagecache_clear(db);

    // Rebuild should work
    TEST_ASSERT_TRUE(tsdb_build_page_cache(db));

    teardown_database();
}

// ============================================================================
// Basic Operations Tests
// ============================================================================

TEST_CASE("page_cache: cache populated after insert", "[page_cache][basic]") {
    setup_database();

    // Insert some data to create pages
    TEST_ASSERT_TRUE(insert_float_points("cache_test", "value", 100, 1000, 1000));

    // Check cache has entries
    uint32_t total_size = tsdb_pagecache_get_total_active_size(db);
    ESP_LOGI(TAG, "Total active size after insert: %" PRIu32, total_size);
    TEST_ASSERT_TRUE(total_size > 0);

    teardown_database();
}

TEST_CASE("page_cache: add and remove entry", "[page_cache][basic]") {
    setup_database();

    // Add a fake entry
    timeseries_page_header_t hdr = {
        .magic_number = TIMESERIES_MAGIC_NUM,
        .page_size = 8192,
        .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
        .page_state = TIMESERIES_PAGE_STATE_ACTIVE,
        .sequence_num = 999,
        .field_data_level = 0
    };

    uint32_t test_offset = 0x50000;
    tsdb_pagecache_add_entry(db, test_offset, &hdr);

    // Verify it's in cache
    uint32_t size = tsdb_pagecache_get_page_size(db, test_offset);
    TEST_ASSERT_EQUAL(8192, size);

    // Remove it
    TEST_ASSERT_TRUE(tsdb_pagecache_remove_entry(db, test_offset));

    // Verify it's gone
    size = tsdb_pagecache_get_page_size(db, test_offset);
    TEST_ASSERT_EQUAL(0, size);  // Not found returns 0

    teardown_database();
}

TEST_CASE("page_cache: remove non-existent entry", "[page_cache][basic]") {
    setup_database();

    // Try to remove an entry that doesn't exist
    bool result = tsdb_pagecache_remove_entry(db, 0xDEADBEEF);
    TEST_ASSERT_FALSE(result);

    teardown_database();
}

TEST_CASE("page_cache: get size of non-existent page", "[page_cache][basic]") {
    setup_database();

    uint32_t size = tsdb_pagecache_get_page_size(db, 0xBADC0DE);
    TEST_ASSERT_EQUAL(0, size);  // Not found returns 0

    teardown_database();
}

// ============================================================================
// Cache Update Tests
// ============================================================================

TEST_CASE("page_cache: add duplicate offset updates entry", "[page_cache][update]") {
    setup_database();

    uint32_t test_offset = 0x60000;

    // Add first entry
    timeseries_page_header_t hdr1 = {
        .magic_number = TIMESERIES_MAGIC_NUM,
        .page_size = 4096,
        .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
        .page_state = TIMESERIES_PAGE_STATE_ACTIVE,
        .sequence_num = 1,
        .field_data_level = 0
    };
    tsdb_pagecache_add_entry(db, test_offset, &hdr1);

    // Verify first size
    TEST_ASSERT_EQUAL(4096, tsdb_pagecache_get_page_size(db, test_offset));

    // Add same offset with different size
    timeseries_page_header_t hdr2 = {
        .magic_number = TIMESERIES_MAGIC_NUM,
        .page_size = 8192,  // Different size
        .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
        .page_state = TIMESERIES_PAGE_STATE_ACTIVE,
        .sequence_num = 2,
        .field_data_level = 0
    };
    tsdb_pagecache_add_entry(db, test_offset, &hdr2);

    // Should have updated size
    TEST_ASSERT_EQUAL(8192, tsdb_pagecache_get_page_size(db, test_offset));

    teardown_database();
}

TEST_CASE("page_cache: cache remains sorted", "[page_cache][order]") {
    setup_database();

    // Add entries in random order
    uint32_t offsets[] = {0x30000, 0x10000, 0x50000, 0x20000, 0x40000};
    size_t count = sizeof(offsets)/sizeof(offsets[0]);

    for (size_t i = 0; i < count; i++) {
        timeseries_page_header_t hdr = {
            .magic_number = TIMESERIES_MAGIC_NUM,
            .page_size = 4096,
            .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
            .page_state = TIMESERIES_PAGE_STATE_ACTIVE,
            .sequence_num = i,
            .field_data_level = 0
        };
        tsdb_pagecache_add_entry(db, offsets[i], &hdr);
    }

    // All entries should be findable
    for (size_t i = 0; i < count; i++) {
        uint32_t size = tsdb_pagecache_get_page_size(db, offsets[i]);
        TEST_ASSERT_EQUAL(4096, size);
    }

    teardown_database();
}

// ============================================================================
// Total Size Calculation Tests
// ============================================================================

TEST_CASE("page_cache: total size calculation", "[page_cache][size]") {
    setup_database();

    // Clear existing cache
    tsdb_pagecache_clear(db);

    // Add some entries with known sizes
    uint32_t expected_total = 0;

    timeseries_page_header_t hdr1 = {
        .magic_number = TIMESERIES_MAGIC_NUM,
        .page_size = 4096,
        .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
        .page_state = TIMESERIES_PAGE_STATE_ACTIVE,  // Active
        .sequence_num = 1,
        .field_data_level = 0
    };
    tsdb_pagecache_add_entry(db, 0x10000, &hdr1);
    expected_total += 4096;

    timeseries_page_header_t hdr2 = {
        .magic_number = TIMESERIES_MAGIC_NUM,
        .page_size = 8192,
        .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
        .page_state = TIMESERIES_PAGE_STATE_ACTIVE,  // Active
        .sequence_num = 2,
        .field_data_level = 0
    };
    tsdb_pagecache_add_entry(db, 0x20000, &hdr2);
    expected_total += 8192;

    // Add an obsolete page - should NOT count
    timeseries_page_header_t hdr3 = {
        .magic_number = TIMESERIES_MAGIC_NUM,
        .page_size = 16384,
        .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
        .page_state = TIMESERIES_PAGE_STATE_OBSOLETE,  // Obsolete
        .sequence_num = 3,
        .field_data_level = 0
    };
    tsdb_pagecache_add_entry(db, 0x30000, &hdr3);
    // NOT added to expected_total

    uint32_t actual_total = tsdb_pagecache_get_total_active_size(db);
    TEST_ASSERT_EQUAL(expected_total, actual_total);

    teardown_database();
}

TEST_CASE("page_cache: total size with all obsolete", "[page_cache][size]") {
    setup_database();

    tsdb_pagecache_clear(db);

    // Add only obsolete pages
    for (int i = 0; i < 5; i++) {
        timeseries_page_header_t hdr = {
            .magic_number = TIMESERIES_MAGIC_NUM,
            .page_size = 4096,
            .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
            .page_state = TIMESERIES_PAGE_STATE_OBSOLETE,
            .sequence_num = i,
            .field_data_level = 0
        };
        tsdb_pagecache_add_entry(db, 0x10000 + i * 0x10000, &hdr);
    }

    uint32_t total = tsdb_pagecache_get_total_active_size(db);
    TEST_ASSERT_EQUAL(0, total);

    teardown_database();
}

// ============================================================================
// Clear and Rebuild Tests
// ============================================================================

TEST_CASE("page_cache: clear then add works", "[page_cache][clear]") {
    setup_database();

    // Add an entry
    timeseries_page_header_t hdr = {
        .magic_number = TIMESERIES_MAGIC_NUM,
        .page_size = 4096,
        .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
        .page_state = TIMESERIES_PAGE_STATE_ACTIVE,
        .sequence_num = 1,
        .field_data_level = 0
    };
    tsdb_pagecache_add_entry(db, 0x10000, &hdr);

    // Clear
    tsdb_pagecache_clear(db);

    // Entry should be gone
    uint32_t size = tsdb_pagecache_get_page_size(db, 0x10000);
    TEST_ASSERT_EQUAL(0, size);

    // Add new entry after clear
    tsdb_pagecache_add_entry(db, 0x20000, &hdr);

    // New entry should be there
    size = tsdb_pagecache_get_page_size(db, 0x20000);
    TEST_ASSERT_EQUAL(4096, size);

    teardown_database();
}

TEST_CASE("page_cache: double clear is safe", "[page_cache][clear]") {
    setup_database();

    tsdb_pagecache_clear(db);
    tsdb_pagecache_clear(db);  // Second clear should be safe

    teardown_database();
    TEST_PASS();
}

TEST_CASE("page_cache: rebuild after clear", "[page_cache][clear]") {
    setup_database();

    // Insert data to create real pages
    TEST_ASSERT_TRUE(insert_float_points("rebuild_test", "value", 100, 1000, 1000));

    uint32_t size_before = tsdb_pagecache_get_total_active_size(db);

    // Clear cache
    tsdb_pagecache_clear(db);

    // Size should now be 0
    uint32_t size_cleared = tsdb_pagecache_get_total_active_size(db);
    TEST_ASSERT_EQUAL(0, size_cleared);

    // Rebuild
    TEST_ASSERT_TRUE(tsdb_build_page_cache(db));

    // Size should be restored
    uint32_t size_rebuilt = tsdb_pagecache_get_total_active_size(db);
    TEST_ASSERT_EQUAL(size_before, size_rebuilt);

    teardown_database();
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST_CASE("page_cache: cache consistent after compaction", "[page_cache][integration]") {
    setup_database();

    // Insert enough data to trigger compaction
    for (int i = 0; i < 5; i++) {
        TEST_ASSERT_TRUE(insert_float_points("compact_test", "value", 100,
                                             i * 100000, 1000));
    }

    uint32_t size_before = tsdb_pagecache_get_total_active_size(db);
    ESP_LOGI(TAG, "Size before compaction: %" PRIu32, size_before);

    // Compact
    TEST_ASSERT_TRUE(timeseries_compact());

    uint32_t size_after = tsdb_pagecache_get_total_active_size(db);
    ESP_LOGI(TAG, "Size after compaction: %" PRIu32, size_after);

    // Size after compaction should be less due to compression
    // But cache should still be consistent
    TEST_ASSERT_TRUE(size_after <= size_before);

    // Rebuild cache and verify consistency
    tsdb_pagecache_clear(db);
    TEST_ASSERT_TRUE(tsdb_build_page_cache(db));

    uint32_t size_rebuilt = tsdb_pagecache_get_total_active_size(db);
    TEST_ASSERT_EQUAL(size_after, size_rebuilt);

    teardown_database();
}

TEST_CASE("page_cache: add multiple entries and verify", "[page_cache][basic]") {
    setup_database();

    // Add some known entries
    tsdb_pagecache_clear(db);

    uint32_t offsets[] = {0x10000, 0x20000, 0x30000};
    uint32_t sizes[] = {4096, 8192, 12288};
    for (size_t i = 0; i < 3; i++) {
        timeseries_page_header_t hdr = {
            .magic_number = TIMESERIES_MAGIC_NUM,
            .page_size = sizes[i],
            .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
            .page_state = TIMESERIES_PAGE_STATE_ACTIVE,
            .sequence_num = i,
            .field_data_level = 0
        };
        tsdb_pagecache_add_entry(db, offsets[i], &hdr);
    }

    // Verify all entries are findable with correct sizes
    for (size_t i = 0; i < 3; i++) {
        uint32_t size = tsdb_pagecache_get_page_size(db, offsets[i]);
        TEST_ASSERT_EQUAL(sizes[i], size);
    }

    // Verify total size
    uint32_t total = tsdb_pagecache_get_total_active_size(db);
    TEST_ASSERT_EQUAL(4096 + 8192 + 12288, total);

    teardown_database();
}

// ============================================================================
// Stress Tests
// ============================================================================

TEST_CASE("page_cache: many entries", "[page_cache][stress]") {
    setup_database();

    tsdb_pagecache_clear(db);

    // Add 100 entries
    for (int i = 0; i < 100; i++) {
        timeseries_page_header_t hdr = {
            .magic_number = TIMESERIES_MAGIC_NUM,
            .page_size = 4096,
            .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
            .page_state = TIMESERIES_PAGE_STATE_ACTIVE,
            .sequence_num = i,
            .field_data_level = 0
        };
        tsdb_pagecache_add_entry(db, i * 0x1000, &hdr);
    }

    // All should be findable
    for (int i = 0; i < 100; i++) {
        uint32_t size = tsdb_pagecache_get_page_size(db, i * 0x1000);
        TEST_ASSERT_EQUAL(4096, size);
    }

    // Total size should be correct
    uint32_t total = tsdb_pagecache_get_total_active_size(db);
    TEST_ASSERT_EQUAL(100 * 4096, total);

    ESP_LOGI(TAG, "100 entry stress test passed");
    teardown_database();
}

TEST_CASE("page_cache: add remove cycles", "[page_cache][stress]") {
    setup_database();

    tsdb_pagecache_clear(db);

    // Add and remove in cycles
    for (int cycle = 0; cycle < 10; cycle++) {
        // Add 10 entries
        for (int i = 0; i < 10; i++) {
            timeseries_page_header_t hdr = {
                .magic_number = TIMESERIES_MAGIC_NUM,
                .page_size = 4096,
                .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
                .page_state = TIMESERIES_PAGE_STATE_ACTIVE,
                .sequence_num = cycle * 10 + i,
                .field_data_level = 0
            };
            tsdb_pagecache_add_entry(db, (cycle * 10 + i) * 0x1000, &hdr);
        }

        // Remove half (first 5 of each cycle)
        for (int i = 0; i < 5; i++) {
            tsdb_pagecache_remove_entry(db, (cycle * 10 + i) * 0x1000);
        }
    }

    // Should have 50 entries remaining (10 cycles * 5 remaining per cycle)
    // Verify total size: 50 entries * 4096 bytes each
    uint32_t total = tsdb_pagecache_get_total_active_size(db);
    TEST_ASSERT_EQUAL(50 * 4096, total);

    // Verify the remaining entries (indices 5-9 of each cycle) are still there
    for (int cycle = 0; cycle < 10; cycle++) {
        for (int i = 5; i < 10; i++) {
            uint32_t size = tsdb_pagecache_get_page_size(db, (cycle * 10 + i) * 0x1000);
            TEST_ASSERT_EQUAL(4096, size);
        }
    }

    // Verify removed entries are gone
    for (int cycle = 0; cycle < 10; cycle++) {
        for (int i = 0; i < 5; i++) {
            uint32_t size = tsdb_pagecache_get_page_size(db, (cycle * 10 + i) * 0x1000);
            TEST_ASSERT_EQUAL(0, size);  // Not found returns 0
        }
    }

    ESP_LOGI(TAG, "Add/remove cycle test passed: 50 entries");
    teardown_database();
}
