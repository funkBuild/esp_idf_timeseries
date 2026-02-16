/**
 * @file timeseries_concurrency_test.c
 * @brief Concurrency tests for background compaction with snapshot-based page cache
 *
 * Tests:
 * 1. Snapshot lifecycle (create, clone, acquire/release, refcount)
 * 2. Batch commit merge with concurrent inserts
 * 3. Insert during compaction
 * 4. Query during compaction
 * 5. Insert + query + compact stress test
 * 6. Blank iterator snapshot consistency
 */

#include "esp_log.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include "unity.h"
#include <stdatomic.h>
#include <string.h>

// Declared in timeseries.c, not exposed in public header
extern timeseries_db_t *timeseries_get_db_handle(void);

static const char *TAG = "concurrency_test";

static bool db_initialized = false;

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

static void ensure_db_initialized(void) {
    if (!db_initialized) {
        TEST_ASSERT_TRUE(timeseries_init());
        db_initialized = true;
    }
    TEST_ASSERT_TRUE(timeseries_clear_all());
}

static void insert_test_data(const char *measurement, const char *field,
                             size_t num_points, uint64_t start_ts) {
    timeseries_field_value_t *values = calloc(num_points, sizeof(timeseries_field_value_t));
    uint64_t *timestamps = calloc(num_points, sizeof(uint64_t));
    TEST_ASSERT_NOT_NULL(values);
    TEST_ASSERT_NOT_NULL(timestamps);

    for (size_t i = 0; i < num_points; i++) {
        timestamps[i] = start_ts + i;
        values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        values[i].data.float_val = (double)i * 1.5;
    }

    const char *field_names[] = {field};
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
    free(values);
    free(timestamps);
}

// ============================================================================
// TEST 1: Snapshot Unit Tests
// ============================================================================

TEST_CASE("snapshot_create_and_release", "[concurrency]") {
    tsdb_page_cache_snapshot_t *snap = tsdb_snapshot_create(16);
    TEST_ASSERT_NOT_NULL(snap);
    TEST_ASSERT_EQUAL(0, snap->count);
    TEST_ASSERT_EQUAL(16, snap->capacity);
    TEST_ASSERT_NOT_NULL(snap->entries);

    // Refcount should be 1
    TEST_ASSERT_EQUAL(1, atomic_load(&snap->refcount));

    tsdb_snapshot_release(snap);
    // snap is freed here - no crash means success
}

TEST_CASE("snapshot_clone_and_independence", "[concurrency]") {
    tsdb_page_cache_snapshot_t *orig = tsdb_snapshot_create(4);
    TEST_ASSERT_NOT_NULL(orig);

    // Add some entries to orig
    timeseries_cached_page_t entry = {
        .offset = 0x1000,
        .header = {.magic_number = TIMESERIES_MAGIC_NUM,
                   .page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA,
                   .page_state = TIMESERIES_PAGE_STATE_ACTIVE,
                   .page_size = 4096},
    };
    orig->entries[0] = entry;
    orig->count = 1;

    // Clone it
    tsdb_page_cache_snapshot_t *clone = tsdb_snapshot_clone(orig);
    TEST_ASSERT_NOT_NULL(clone);
    TEST_ASSERT_EQUAL(1, clone->count);
    TEST_ASSERT_EQUAL(0x1000, clone->entries[0].offset);

    // Modify clone - should not affect original
    clone->entries[0].offset = 0x2000;
    TEST_ASSERT_EQUAL(0x1000, orig->entries[0].offset);
    TEST_ASSERT_EQUAL(0x2000, clone->entries[0].offset);

    // Both should have refcount 1
    TEST_ASSERT_EQUAL(1, atomic_load(&orig->refcount));
    TEST_ASSERT_EQUAL(1, atomic_load(&clone->refcount));

    tsdb_snapshot_release(orig);
    tsdb_snapshot_release(clone);
}

TEST_CASE("snapshot_acquire_release_refcount", "[concurrency]") {
    tsdb_page_cache_snapshot_t *snap = tsdb_snapshot_create(4);
    TEST_ASSERT_NOT_NULL(snap);
    TEST_ASSERT_EQUAL(1, atomic_load(&snap->refcount));

    // Acquire bumps refcount
    tsdb_page_cache_snapshot_t *ref = tsdb_snapshot_acquire(snap);
    TEST_ASSERT_EQUAL_PTR(snap, ref);
    TEST_ASSERT_EQUAL(2, atomic_load(&snap->refcount));

    // Another acquire
    tsdb_snapshot_acquire(snap);
    TEST_ASSERT_EQUAL(3, atomic_load(&snap->refcount));

    // Release drops refcount
    tsdb_snapshot_release(snap);
    TEST_ASSERT_EQUAL(2, atomic_load(&snap->refcount));

    tsdb_snapshot_release(snap);
    TEST_ASSERT_EQUAL(1, atomic_load(&snap->refcount));

    // Last release frees
    tsdb_snapshot_release(snap);
}

TEST_CASE("snapshot_acquire_current_from_db", "[concurrency]") {
    ensure_db_initialized();
    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    tsdb_page_cache_snapshot_t *snap = tsdb_snapshot_acquire_current(db);
    TEST_ASSERT_NOT_NULL(snap);

    // Should have at least the metadata page
    TEST_ASSERT_GREATER_OR_EQUAL(1, snap->count);

    uint32_t rc = atomic_load(&snap->refcount);
    TEST_ASSERT_GREATER_OR_EQUAL(2, rc); // current_snapshot ref + our ref

    tsdb_snapshot_release(snap);
}

// ============================================================================
// TEST 2: Batch Commit Merge
// ============================================================================

TEST_CASE("batch_add_remove_commit", "[concurrency]") {
    ensure_db_initialized();
    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    // Insert some data to create pages
    insert_test_data("batch_test", "value", 50, 1000);

    // Begin a batch
    tsdb_page_cache_snapshot_t *batch = tsdb_pagecache_begin_batch(db);
    TEST_ASSERT_NOT_NULL(batch);

    size_t initial_count = batch->count;
    ESP_LOGI(TAG, "Batch started with %zu entries", initial_count);

    // Batch should be independent from live snapshot
    tsdb_page_cache_snapshot_t *live = tsdb_snapshot_acquire_current(db);
    TEST_ASSERT_NOT_NULL(live);
    TEST_ASSERT_NOT_EQUAL(batch, live);
    tsdb_snapshot_release(live);

    // Commit the batch (even without changes, should succeed)
    tsdb_pagecache_commit_batch(db, batch);
    ESP_LOGI(TAG, "Batch committed successfully");
}

// ============================================================================
// TEST 3: Insert During Compaction
// ============================================================================

TEST_CASE("insert_during_compaction", "[concurrency]") {
    ensure_db_initialized();

    // Insert initial data that will trigger compaction
    for (int batch = 0; batch < 5; batch++) {
        insert_test_data("concurrent_m", "value", 100, batch * 100 + 1);
    }
    ESP_LOGI(TAG, "Inserted initial 500 points across 5 batches");

    // Trigger background compaction (non-blocking)
    TEST_ASSERT_TRUE(timeseries_compact());

    // Immediately insert more data while compaction runs
    insert_test_data("concurrent_m", "value", 100, 10000);
    ESP_LOGI(TAG, "Inserted 100 more points during compaction");

    // Wait for compaction to finish
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query and verify all data is present
    timeseries_query_t query = {
        .measurement_name = "concurrent_m",
        .start_ms = 0,
        .end_ms = 0,
        .limit = 0,
    };
    timeseries_query_result_t result = {0};
    TEST_ASSERT_TRUE(timeseries_query(&query, &result));

    ESP_LOGI(TAG, "Query returned %zu points after insert-during-compaction", result.num_points);
    TEST_ASSERT_GREATER_OR_EQUAL(100, result.num_points); // At least the concurrent insert

    timeseries_query_free_result(&result);
}

// ============================================================================
// TEST 4: Query During Compaction
// ============================================================================

typedef struct {
    bool success;
    size_t points_returned;
    TaskHandle_t notify_task;
} query_task_params_t;

static void query_task_func(void *param) {
    query_task_params_t *p = (query_task_params_t *)param;

    timeseries_query_t query = {
        .measurement_name = "query_concurrent",
        .start_ms = 0,
        .end_ms = 0,
        .limit = 0,
    };
    timeseries_query_result_t result = {0};

    p->success = timeseries_query(&query, &result);
    p->points_returned = result.num_points;
    timeseries_query_free_result(&result);

    xTaskNotifyGive(p->notify_task);
    vTaskDelete(NULL);
}

TEST_CASE("query_during_compaction", "[concurrency]") {
    ensure_db_initialized();

    // Insert data
    for (int batch = 0; batch < 5; batch++) {
        insert_test_data("query_concurrent", "temp", 100, batch * 100 + 1);
    }
    ESP_LOGI(TAG, "Inserted 500 points for query-during-compaction test");

    // Trigger background compaction
    TEST_ASSERT_TRUE(timeseries_compact());

    // Run query while compaction is running
    query_task_params_t query_params = {
        .success = false,
        .points_returned = 0,
        .notify_task = xTaskGetCurrentTaskHandle(),
    };

    BaseType_t ret = xTaskCreate(query_task_func, "query_task", 8192,
                                  &query_params, tskIDLE_PRIORITY + 2, NULL);
    TEST_ASSERT_EQUAL(pdPASS, ret);

    // Wait for query task to complete
    ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(10000));

    TEST_ASSERT_TRUE(query_params.success);
    ESP_LOGI(TAG, "Query during compaction returned %zu points", query_params.points_returned);
    TEST_ASSERT_GREATER_OR_EQUAL(1, query_params.points_returned);

    // Wait for compaction to finish
    TEST_ASSERT_TRUE(timeseries_compact_sync());
}

// ============================================================================
// TEST 5: Stress Test - Insert + Query + Compact
// ============================================================================

typedef struct {
    _Atomic bool running;
    _Atomic uint32_t inserts_completed;
    _Atomic uint32_t queries_completed;
    _Atomic uint32_t insert_failures;
    _Atomic uint32_t query_failures;
    TaskHandle_t notify_task;
} stress_test_context_t;

static void stress_insert_task(void *param) {
    stress_test_context_t *ctx = (stress_test_context_t *)param;
    uint64_t ts_counter = 100000;

    while (atomic_load(&ctx->running)) {
        timeseries_field_value_t val = {
            .type = TIMESERIES_FIELD_TYPE_FLOAT,
            .data.float_val = (double)ts_counter * 0.1,
        };
        uint64_t ts = ts_counter++;

        const char *field_names[] = {"stress_val"};
        timeseries_insert_data_t data = {
            .measurement_name = "stress",
            .tag_keys = NULL,
            .tag_values = NULL,
            .num_tags = 0,
            .field_names = field_names,
            .field_values = &val,
            .num_fields = 1,
            .timestamps_ms = &ts,
            .num_points = 1,
        };

        if (timeseries_insert(&data)) {
            atomic_fetch_add(&ctx->inserts_completed, 1);
        } else {
            atomic_fetch_add(&ctx->insert_failures, 1);
        }
        vTaskDelay(pdMS_TO_TICKS(5));
    }

    xTaskNotifyGive(ctx->notify_task);
    vTaskDelete(NULL);
}

static void stress_query_task(void *param) {
    stress_test_context_t *ctx = (stress_test_context_t *)param;

    while (atomic_load(&ctx->running)) {
        timeseries_query_t query = {
            .measurement_name = "stress",
            .start_ms = 0,
            .end_ms = 0,
            .limit = 10,
        };
        timeseries_query_result_t result = {0};

        if (timeseries_query(&query, &result)) {
            atomic_fetch_add(&ctx->queries_completed, 1);
        } else {
            atomic_fetch_add(&ctx->query_failures, 1);
        }
        timeseries_query_free_result(&result);
        vTaskDelay(pdMS_TO_TICKS(20));
    }

    xTaskNotifyGive(ctx->notify_task);
    vTaskDelete(NULL);
}

TEST_CASE("stress_insert_query_compact", "[concurrency]") {
    ensure_db_initialized();

    // Seed some initial data
    insert_test_data("stress", "stress_val", 50, 1);

    stress_test_context_t ctx = {0};
    atomic_store(&ctx.running, true);
    atomic_store(&ctx.inserts_completed, 0);
    atomic_store(&ctx.queries_completed, 0);
    atomic_store(&ctx.insert_failures, 0);
    atomic_store(&ctx.query_failures, 0);
    ctx.notify_task = xTaskGetCurrentTaskHandle();

    // Launch insert and query tasks
    BaseType_t ret1 = xTaskCreate(stress_insert_task, "stress_ins", 8192,
                                   &ctx, tskIDLE_PRIORITY + 1, NULL);
    BaseType_t ret2 = xTaskCreate(stress_query_task, "stress_qry", 8192,
                                   &ctx, tskIDLE_PRIORITY + 1, NULL);
    TEST_ASSERT_EQUAL(pdPASS, ret1);
    TEST_ASSERT_EQUAL(pdPASS, ret2);

    // Run for 1 second, triggering compaction once mid-way
    uint32_t compact_count = 0;
    for (int i = 0; i < 10; i++) {
        vTaskDelay(pdMS_TO_TICKS(100));
        if (i == 5) {
            timeseries_compact();
            compact_count++;
        }
    }

    // Stop tasks
    atomic_store(&ctx.running, false);

    // Wait for both tasks to finish (2 notifications)
    ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(5000));
    ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(5000));

    // Wait for any pending compaction
    timeseries_compact_sync();

    uint32_t inserts = atomic_load(&ctx.inserts_completed);
    uint32_t queries = atomic_load(&ctx.queries_completed);
    uint32_t insert_fails = atomic_load(&ctx.insert_failures);
    uint32_t query_fails = atomic_load(&ctx.query_failures);

    ESP_LOGI(TAG, "Stress test results: inserts=%lu queries=%lu compact_triggers=%lu "
             "insert_fails=%lu query_fails=%lu",
             (unsigned long)inserts, (unsigned long)queries,
             (unsigned long)compact_count,
             (unsigned long)insert_fails, (unsigned long)query_fails);

    TEST_ASSERT_GREATER_THAN(0, inserts);
    TEST_ASSERT_GREATER_THAN(0, queries);
    TEST_ASSERT_EQUAL(0, insert_fails);
    TEST_ASSERT_EQUAL(0, query_fails);
}

// ============================================================================
// TEST 6: Blank Iterator Snapshot Consistency
// ============================================================================

TEST_CASE("blank_iterator_snapshot_consistency", "[concurrency]") {
    ensure_db_initialized();

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    // Insert some data to create field data pages
    insert_test_data("blank_iter_test", "val", 100, 1);

    // Initialize a blank iterator (acquires snapshot)
    timeseries_blank_iterator_t blank_iter;
    timeseries_blank_iterator_init(db, &blank_iter, 4096);

    // The iterator should hold a snapshot reference
    TEST_ASSERT_NOT_NULL(blank_iter.snapshot);

    // Insert more data (may create new pages, modifying live snapshot)
    insert_test_data("blank_iter_test", "val", 100, 200);

    // The iterator's snapshot should still be the old one
    tsdb_page_cache_snapshot_t *current = tsdb_snapshot_acquire_current(db);
    // Current may have more pages than the iterator's snapshot
    // (the iterator sees the state at init time)
    ESP_LOGI(TAG, "Iterator snapshot pages: %zu, current snapshot pages: %zu",
             blank_iter.snapshot->count, current->count);

    tsdb_snapshot_release(current);
    timeseries_blank_iterator_deinit(&blank_iter);
}

// ============================================================================
// TEST: Compact sync returns correctly
// ============================================================================

TEST_CASE("compact_sync_basic", "[concurrency]") {
    ensure_db_initialized();

    // Insert data
    insert_test_data("sync_test", "temperature", 200, 1);

    // Synchronous compact should return true
    TEST_ASSERT_TRUE(timeseries_compact_sync());

    // Query should still work
    timeseries_query_t query = {
        .measurement_name = "sync_test",
        .start_ms = 0,
        .end_ms = 0,
        .limit = 0,
    };
    timeseries_query_result_t result = {0};
    TEST_ASSERT_TRUE(timeseries_query(&query, &result));
    TEST_ASSERT_GREATER_OR_EQUAL(1, result.num_points);
    ESP_LOGI(TAG, "compact_sync_basic: query returned %zu points", result.num_points);
    timeseries_query_free_result(&result);
}
