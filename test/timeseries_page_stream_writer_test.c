/**
 * @file timeseries_page_stream_writer_test.c
 * @brief Unit tests for page stream writer module
 *
 * This module handles streaming writes to L1/L2 pages during compaction.
 * These tests cover edge cases and error paths not tested through
 * indirect compaction tests.
 */

#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_page_stream_writer.h"
#include "esp_log.h"
#include <string.h>

static const char *TAG = "psw_test";

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

// ============================================================================
// NULL Parameter Tests
// ============================================================================

TEST_CASE("page_stream_writer: init with NULL db", "[psw][null]") {
    timeseries_page_stream_writer_t writer = {0};
    bool result = timeseries_page_stream_writer_init(NULL, &writer, 1, 4096);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("page_stream_writer: init with NULL writer", "[psw][null]") {
    setup_database();
    bool result = timeseries_page_stream_writer_init(db, NULL, 1, 4096);
    TEST_ASSERT_FALSE(result);
    teardown_database();
}

TEST_CASE("page_stream_writer: begin_series with NULL writer", "[psw][null]") {
    uint8_t series_id[16] = {0};
    bool result = timeseries_page_stream_writer_begin_series(
        NULL, series_id, TIMESERIES_FIELD_TYPE_FLOAT);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("page_stream_writer: write_timestamp with NULL writer", "[psw][null]") {
    bool result = timeseries_page_stream_writer_write_timestamp(NULL, 1000);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("page_stream_writer: finalize_timestamp with NULL writer", "[psw][null]") {
    bool result = timeseries_page_stream_writer_finalize_timestamp(NULL);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("page_stream_writer: write_value with NULL writer", "[psw][null]") {
    timeseries_field_value_t val = { .type = TIMESERIES_FIELD_TYPE_FLOAT };
    val.data.float_val = 1.0f;
    bool result = timeseries_page_stream_writer_write_value(NULL, &val);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("page_stream_writer: write_value with NULL value", "[psw][null]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    // Initialize writer
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 4096));

    // Begin series
    uint8_t series_id[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(
        &writer, series_id, TIMESERIES_FIELD_TYPE_FLOAT));

    // Write timestamp
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&writer, 1000));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));

    // Try to write NULL value
    bool result = timeseries_page_stream_writer_write_value(&writer, NULL);
    TEST_ASSERT_FALSE(result);

    teardown_database();
}

TEST_CASE("page_stream_writer: end_series with NULL writer", "[psw][null]") {
    bool result = timeseries_page_stream_writer_end_series(NULL);
    TEST_ASSERT_FALSE(result);
}

TEST_CASE("page_stream_writer: finalize with NULL writer", "[psw][null]") {
    bool result = timeseries_page_stream_writer_finalize(NULL);
    TEST_ASSERT_FALSE(result);
}

// ============================================================================
// Invalid State Transition Tests
// ============================================================================

TEST_CASE("page_stream_writer: write after finalize", "[psw][state]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    // Initialize and finalize immediately (empty page)
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 4096));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

    // Try to begin series after finalize
    uint8_t series_id[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
    bool result = timeseries_page_stream_writer_begin_series(
        &writer, series_id, TIMESERIES_FIELD_TYPE_FLOAT);
    TEST_ASSERT_FALSE(result);

    teardown_database();
}

TEST_CASE("page_stream_writer: double finalize is safe", "[psw][state]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    // Initialize and finalize twice
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 4096));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

    // Second finalize should return false but not crash
    bool result = timeseries_page_stream_writer_finalize(&writer);
    TEST_ASSERT_FALSE(result);

    teardown_database();
}

// ============================================================================
// Basic Write Tests
// ============================================================================

TEST_CASE("page_stream_writer: single point write", "[psw][basic]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    // Initialize writer
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 4096));

    // Begin series
    uint8_t series_id[16] = {0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89,
                             0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78};
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(
        &writer, series_id, TIMESERIES_FIELD_TYPE_FLOAT));

    // Write single timestamp
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&writer, 1000));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));

    // Write single value
    timeseries_field_value_t val = { .type = TIMESERIES_FIELD_TYPE_FLOAT };
    val.data.float_val = 42.5f;
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&writer, &val));

    // End series and finalize
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

    ESP_LOGI(TAG, "Single point write successful");
    teardown_database();
}

TEST_CASE("page_stream_writer: multiple points same series", "[psw][basic]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    // Initialize writer
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 4096));

    // Begin series
    uint8_t series_id[16] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                             0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10};
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(
        &writer, series_id, TIMESERIES_FIELD_TYPE_INT));

    // Write 100 timestamps
    for (int i = 0; i < 100; i++) {
        TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&writer, i * 1000));
    }
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));

    // Write 100 values
    for (int i = 0; i < 100; i++) {
        timeseries_field_value_t val = { .type = TIMESERIES_FIELD_TYPE_INT };
        val.data.int_val = i * 10;
        TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&writer, &val));
    }

    // End series and finalize
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

    ESP_LOGI(TAG, "100 points written successfully");
    teardown_database();
}

// ============================================================================
// Field Type Tests
// ============================================================================

TEST_CASE("page_stream_writer: all field types", "[psw][types]") {
    setup_database();

    // Test each field type
    timeseries_field_type_e types[] = {
        TIMESERIES_FIELD_TYPE_FLOAT,
        TIMESERIES_FIELD_TYPE_INT,
        TIMESERIES_FIELD_TYPE_BOOL,
        TIMESERIES_FIELD_TYPE_STRING
    };

    for (size_t t = 0; t < sizeof(types)/sizeof(types[0]); t++) {
        timeseries_page_stream_writer_t writer = {0};

        // Initialize writer
        TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 4096));

        // Begin series with this type
        uint8_t series_id[16] = {0};
        series_id[0] = t;
        TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(
            &writer, series_id, types[t]));

        // Write timestamps
        for (int i = 0; i < 10; i++) {
            TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&writer, i * 1000));
        }
        TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));

        // Write values of appropriate type
        for (int i = 0; i < 10; i++) {
            timeseries_field_value_t val = { .type = types[t] };
            switch (types[t]) {
                case TIMESERIES_FIELD_TYPE_FLOAT:
                    val.data.float_val = i * 1.5f;
                    break;
                case TIMESERIES_FIELD_TYPE_INT:
                    val.data.int_val = i * 100;
                    break;
                case TIMESERIES_FIELD_TYPE_BOOL:
                    val.data.bool_val = (i % 2 == 0);
                    break;
                case TIMESERIES_FIELD_TYPE_STRING:
                    val.data.string_val.str = "test";
                    val.data.string_val.length = 4;
                    break;
                default:
                    break;
            }
            TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&writer, &val));
        }

        TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
        TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

        ESP_LOGI(TAG, "Field type %d test passed", types[t]);
    }

    teardown_database();
}

// ============================================================================
// Boundary Condition Tests
// ============================================================================

TEST_CASE("page_stream_writer: zero points in series", "[psw][boundary]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    // Initialize writer
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 4096));

    // Begin series but don't write any points
    uint8_t series_id[16] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                             0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(
        &writer, series_id, TIMESERIES_FIELD_TYPE_FLOAT));

    // Finalize timestamps immediately (no timestamps written)
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));

    // End series (with 0 points)
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

    ESP_LOGI(TAG, "Zero points test passed");
    teardown_database();
}

TEST_CASE("page_stream_writer: very small initial size", "[psw][boundary]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    // Initialize with very small prev_data_size (should round up to 4KB)
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 100));

    // Begin series
    uint8_t series_id[16] = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
                             0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00};
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(
        &writer, series_id, TIMESERIES_FIELD_TYPE_FLOAT));

    // Write some data
    for (int i = 0; i < 50; i++) {
        TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&writer, i * 1000));
    }
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));

    for (int i = 0; i < 50; i++) {
        timeseries_field_value_t val = { .type = TIMESERIES_FIELD_TYPE_FLOAT };
        val.data.float_val = i * 0.1f;
        TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&writer, &val));
    }

    TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

    ESP_LOGI(TAG, "Small initial size test passed");
    teardown_database();
}

// ============================================================================
// Relocation Tests (Force page growth)
// ============================================================================

TEST_CASE("page_stream_writer: force relocation during writes", "[psw][relocation]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    // Initialize with minimal size to force relocations
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 100));

    // Begin series
    uint8_t series_id[16] = {0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
                             0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0};
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(
        &writer, series_id, TIMESERIES_FIELD_TYPE_FLOAT));

    // Write many timestamps to force relocation
    for (int i = 0; i < 500; i++) {
        bool result = timeseries_page_stream_writer_write_timestamp(&writer, i * 1000);
        TEST_ASSERT_TRUE_MESSAGE(result, "Failed to write timestamp during relocation test");
    }
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));

    // Write many values (may cause additional relocation)
    for (int i = 0; i < 500; i++) {
        timeseries_field_value_t val = { .type = TIMESERIES_FIELD_TYPE_FLOAT };
        val.data.float_val = i * 0.5f;
        bool result = timeseries_page_stream_writer_write_value(&writer, &val);
        TEST_ASSERT_TRUE_MESSAGE(result, "Failed to write value during relocation test");
    }

    TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

    ESP_LOGI(TAG, "Relocation test with 500 points passed");
    teardown_database();
}

TEST_CASE("page_stream_writer: multiple series with relocations", "[psw][relocation]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    // Initialize with minimal size
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 100));

    // Write 5 series, each with 100 points
    for (int s = 0; s < 5; s++) {
        uint8_t series_id[16] = {0};
        series_id[0] = s;

        TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(
            &writer, series_id, TIMESERIES_FIELD_TYPE_INT));

        for (int i = 0; i < 100; i++) {
            TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&writer, s * 100000 + i * 1000));
        }
        TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));

        for (int i = 0; i < 100; i++) {
            timeseries_field_value_t val = { .type = TIMESERIES_FIELD_TYPE_INT };
            val.data.int_val = s * 1000 + i;
            TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&writer, &val));
        }

        TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
    }

    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

    ESP_LOGI(TAG, "Multiple series relocation test passed");
    teardown_database();
}

// ============================================================================
// String Value Tests
// ============================================================================

TEST_CASE("page_stream_writer: string values", "[psw][string]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 4096));

    uint8_t series_id[16] = {0xAA, 0xBB, 0xCC, 0xDD};
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(
        &writer, series_id, TIMESERIES_FIELD_TYPE_STRING));

    const char *strings[] = {"hello", "world", "test", "string", "values"};

    for (int i = 0; i < 5; i++) {
        TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&writer, i * 1000));
    }
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));

    for (int i = 0; i < 5; i++) {
        timeseries_field_value_t val = { .type = TIMESERIES_FIELD_TYPE_STRING };
        val.data.string_val.str = strings[i];
        val.data.string_val.length = strlen(strings[i]);
        TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&writer, &val));
    }

    TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

    ESP_LOGI(TAG, "String values test passed");
    teardown_database();
}

TEST_CASE("page_stream_writer: empty string values", "[psw][string]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 4096));

    uint8_t series_id[16] = {0x00, 0x11, 0x22, 0x33};
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(
        &writer, series_id, TIMESERIES_FIELD_TYPE_STRING));

    TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&writer, 1000));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));

    // Write empty string
    timeseries_field_value_t val = { .type = TIMESERIES_FIELD_TYPE_STRING };
    val.data.string_val.str = "";
    val.data.string_val.length = 0;
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&writer, &val));

    TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

    ESP_LOGI(TAG, "Empty string test passed");
    teardown_database();
}

// ============================================================================
// Large Dataset Tests
// ============================================================================

TEST_CASE("page_stream_writer: large dataset (2000 points)", "[psw][large]") {
    setup_database();
    timeseries_page_stream_writer_t writer = {0};

    // Start with small size to test growth
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 1000));

    uint8_t series_id[16] = {0xAB, 0xCD, 0xEF, 0x01};
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(
        &writer, series_id, TIMESERIES_FIELD_TYPE_FLOAT));

    // Write 2000 points
    for (int i = 0; i < 2000; i++) {
        bool result = timeseries_page_stream_writer_write_timestamp(&writer, i * 1000);
        if (!result) {
            ESP_LOGE(TAG, "Failed to write timestamp %d", i);
            TEST_FAIL_MESSAGE("Timestamp write failed");
        }
    }
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));

    for (int i = 0; i < 2000; i++) {
        timeseries_field_value_t val = { .type = TIMESERIES_FIELD_TYPE_FLOAT };
        val.data.float_val = i * 0.001f;
        bool result = timeseries_page_stream_writer_write_value(&writer, &val);
        if (!result) {
            ESP_LOGE(TAG, "Failed to write value %d", i);
            TEST_FAIL_MESSAGE("Value write failed");
        }
    }

    TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));

    ESP_LOGI(TAG, "Large dataset (2000 points) test passed");
    teardown_database();
}
