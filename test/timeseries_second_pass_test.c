#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_data.h"
#include "timeseries_compaction.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include "esp_timer.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

extern timeseries_db_t *timeseries_get_db_handle(void);
static void reset_second(void) {
    TEST_ASSERT_TRUE(timeseries_init());
    TEST_ASSERT_TRUE(timeseries_clear_all());
}
static bool insert_second(uint64_t *ts, timeseries_field_value_t *vals, size_t count) {
    const char *field = "v";
    timeseries_insert_data_t data = {.measurement_name="second", .field_names=&field,
        .num_fields=1, .timestamps_ms=ts, .field_values=vals, .num_points=count};
    return timeseries_insert(&data);
}

TEST_CASE("second: stale compaction snapshot cannot allocate over an insert", "[second][second_reserve]") {
    reset_second();
    timeseries_db_t *db = timeseries_get_db_handle();
    tsdb_page_cache_snapshot_t *batch = tsdb_pagecache_begin_batch(db);
    TEST_ASSERT_NOT_NULL(batch);
    uint32_t inserted, compacted, size;
    TEST_ASSERT_TRUE(tsdb_reserve_blank_region(db, 8192, NULL, &inserted, &size));
    TEST_ASSERT_TRUE(tsdb_reserve_blank_region(db, 8192, batch, &compacted, &size));
    tsdb_snapshot_release(batch);
    timeseries_deinit();
    TEST_ASSERT_NOT_EQUAL(inserted, compacted);
}

TEST_CASE("second: insert leaves the compactor's private snapshot unchanged", "[second][second_private]") {
    reset_second();
    timeseries_db_t *db = timeseries_get_db_handle();
    tsdb_page_cache_snapshot_t *batch = tsdb_pagecache_begin_batch(db);
    TEST_ASSERT_NOT_NULL(batch);
    /* Model an insert while the compaction worker holds a pointer into entries. */
    db->active_batch = batch;
    size_t count = batch->count;
    uint64_t ts = 1;
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_INT, .data.int_val=42};
    bool ok = insert_second(&ts, &val, 1);
    bool unchanged = count == batch->count;
    db->active_batch = NULL;
    tsdb_snapshot_release(batch);
    timeseries_deinit();
    TEST_ASSERT_TRUE(ok);
    TEST_ASSERT_TRUE(unchanged);
}

TEST_CASE("second: query handles record headers crossing the scan window", "[second][second_window]") {
    reset_second();
    uint64_t *ts = malloc(252 * sizeof(*ts));
    timeseries_field_value_t *vals = calloc(252, sizeof(*vals));
    TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(vals);
    for (int i=0; i<252; i++) {
        ts[i] = i+1;
        vals[i].type = TIMESERIES_FIELD_TYPE_INT;
        vals[i].data.int_val = i+1;
    }
    TEST_ASSERT_TRUE(insert_second(ts, vals, 252));
    ts[0]=253; vals[0].data.int_val=253;
    TEST_ASSERT_TRUE(insert_second(ts, vals, 1));
    ts[0]=254; vals[0].data.int_val=254;
    TEST_ASSERT_TRUE(insert_second(ts, vals, 1));
    free(ts); free(vals);
    // A cold reopen also verifies scanning without an occupancy-cache hint.
    timeseries_deinit(); TEST_ASSERT_TRUE(timeseries_init());
    timeseries_query_t q = {.measurement_name="second"};
    timeseries_query_result_t r = {0};
    TEST_ASSERT_TRUE(timeseries_query(&q, &r));
    TEST_ASSERT_EQUAL(254, r.num_points);
    for (int i=0; i<254; i++) {
        TEST_ASSERT_EQUAL_UINT64(i+1, r.timestamps[i]);
        TEST_ASSERT_EQUAL_INT64(i+1, r.columns[0].values[i].data.int_val);
    }
    timeseries_query_free_result(&r);
    timeseries_deinit();
}

TEST_CASE("second: occupancy survives compaction reuse and cold reopen", "[second][second_reuse]") {
    reset_second();
    timeseries_db_t *db = timeseries_get_db_handle();
    atomic_store(&db->compaction_in_progress, true); // use only the explicit compactor below
    uint64_t *ts = malloc(500 * sizeof(*ts));
    timeseries_field_value_t *vals = calloc(500, sizeof(*vals));
    TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(vals);
    for (int page=0; page<5; page++) {
        for (int i=0; i<500; i++) {
            ts[i] = page*500+i+1;
            vals[i].type = TIMESERIES_FIELD_TYPE_INT;
            vals[i].data.int_val = ts[i];
        }
        TEST_ASSERT_TRUE(insert_second(ts, vals, 500));
        if (page==3) TEST_ASSERT_TRUE(timeseries_compact_level0_pages(db));
    }
    free(ts); free(vals);
    atomic_store(&db->compaction_in_progress, false);
    timeseries_query_t q = {.measurement_name="second", .start_ms=1999, .end_ms=2002};
    for (int cold=0; cold<2; cold++) {
        timeseries_query_result_t r = {0};
        TEST_ASSERT_TRUE(timeseries_query(&q, &r));
        TEST_ASSERT_EQUAL(4, r.num_points);
        for (int i=0; i<4; i++) {
            TEST_ASSERT_EQUAL_UINT64(1999+i, r.timestamps[i]);
            TEST_ASSERT_EQUAL_INT64(1999+i, r.columns[0].values[i].data.int_val);
        }
        timeseries_query_free_result(&r);
        timeseries_deinit(); TEST_ASSERT_TRUE(timeseries_init());
    }
    TEST_ASSERT_TRUE(timeseries_clear_all());
    uint64_t fresh_ts=1;
    timeseries_field_value_t fresh_val = {.type=TIMESERIES_FIELD_TYPE_INT, .data.int_val=99};
    TEST_ASSERT_TRUE(insert_second(&fresh_ts, &fresh_val, 1));
    timeseries_query_result_t r = {0};
    q.start_ms=q.end_ms=0;
    TEST_ASSERT_TRUE(timeseries_query(&q, &r));
    TEST_ASSERT_EQUAL(1, r.num_points);
    TEST_ASSERT_EQUAL_INT64(99, r.columns[0].values[0].data.int_val);
    timeseries_query_free_result(&r);
    timeseries_deinit();
}

TEST_CASE("second: cached page bounds expand for earlier and later inserts", "[second][second_bounds]") {
    reset_second();
    uint64_t ts[] = {100, 200, 50};
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_INT};
    for (int i=0; i<3; i++) {
        val.data.int_val=ts[i];
        TEST_ASSERT_TRUE(insert_second(&ts[i], &val, 1));
        timeseries_query_t q = {.measurement_name="second", .start_ms=ts[i], .end_ms=ts[i]};
        timeseries_query_result_t r = {0};
        TEST_ASSERT_TRUE(timeseries_query(&q, &r));
        TEST_ASSERT_EQUAL(1, r.num_points);
        TEST_ASSERT_EQUAL_UINT64(ts[i], r.timestamps[0]);
        TEST_ASSERT_EQUAL_INT64(ts[i], r.columns[0].values[0].data.int_val);
        timeseries_query_free_result(&r);
    }
    timeseries_deinit();
}

#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_reset_reads(void);
size_t tsdb_audit_read_bytes(void);
size_t tsdb_audit_read_calls(void);
void tsdb_audit_on_decode(TaskHandle_t task, SemaphoreHandle_t done);
bool tsdb_audit_decode_progress(void);

void tsdb_audit_fail_malloc(size_t size);
void tsdb_audit_fail_realloc(size_t size);
// The batch is a journal replayed onto the live cache at commit; building the
// published snapshot (live + journal) is the commit's one allocation. When it
// fails, the live cache, including the insert made while the batch was open,
// must stay exactly as it was, with no journal entry half-applied.
static void check_commit_oom(bool with_journal_entry) {
    reset_second();
    timeseries_db_t *db = timeseries_get_db_handle();
    tsdb_page_cache_snapshot_t *batch = tsdb_pagecache_begin_batch(db);
    TEST_ASSERT_NOT_NULL(batch);
    uint32_t journal_offset = db->partition->size - 4096;
    if (with_journal_entry) {
        timeseries_page_header_t h; memset(&h, 0xFF, sizeof h);
        h.magic_number = TIMESERIES_MAGIC_NUM; h.page_state = TIMESERIES_PAGE_STATE_ACTIVE; h.page_size = 4096;
        TEST_ASSERT_TRUE(tsdb_pagecache_batch_add(batch, journal_offset, &h));
    }
    uint64_t ts=1;
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_INT, .data.int_val=42};
    TEST_ASSERT_TRUE(insert_second(&ts, &val, 1));
    tsdb_page_cache_snapshot_t *live = tsdb_snapshot_acquire_current(db);
    size_t live_count = live->count;
    tsdb_snapshot_release(live);
    tsdb_audit_fail_malloc((live_count + batch->count) * sizeof(timeseries_cached_page_t));
    bool committed = tsdb_pagecache_commit_batch(db, batch);
    tsdb_audit_fail_malloc(0); tsdb_audit_fail_realloc(0);
    live = tsdb_snapshot_acquire_current(db);
    bool unchanged = live->count == live_count;
    tsdb_snapshot_release(live);
    TEST_ASSERT_FALSE(committed); TEST_ASSERT_TRUE(unchanged);
    (void)journal_offset;
    timeseries_query_t q = {.measurement_name="second"};
    timeseries_query_result_t r = {0};
    bool ok = timeseries_query(&q, &r);
    size_t points = r.num_points;
    int64_t value = points ? r.columns[0].values[0].data.int_val : 0;
    timeseries_query_free_result(&r);
    timeseries_deinit();
    TEST_ASSERT_TRUE(ok); TEST_ASSERT_EQUAL(1, points); TEST_ASSERT_EQUAL_INT64(42, value);
}
// A compaction that gives a region back (released reservation) records that
// in its journal. If another writer is then given the same region and
// publishes a page there before the compaction publishes, publication must
// leave that page alone: the journal's removal was of its own placeholder.
TEST_CASE("second: a region the compaction gave up and another writer reused survives publication",
          "[second][second_reused_region]") {
    reset_second();
    timeseries_db_t *db = timeseries_get_db_handle();
    tsdb_page_cache_snapshot_t *batch = tsdb_pagecache_begin_batch(db);
    TEST_ASSERT_NOT_NULL(batch);
    uint32_t given_up, reused, size;
    TEST_ASSERT_TRUE(tsdb_reserve_blank_region(db, 4096, batch, &given_up, &size));
    tsdb_release_reserved_region(db, given_up, batch);
    TEST_ASSERT_TRUE(tsdb_reserve_blank_region(db, 4096, NULL, &reused, &size));
    timeseries_page_header_t h; memset(&h, 0xFF, sizeof h);
    h.magic_number = TIMESERIES_MAGIC_NUM; h.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
    h.page_state = TIMESERIES_PAGE_STATE_ACTIVE; h.field_data_level = 0; h.sequence_num = 7; h.page_size = 4096;
    TEST_ASSERT_TRUE(tsdb_finalize_reserved_region(db, reused, &h, NULL));
    TEST_ASSERT_TRUE(tsdb_pagecache_commit_batch(db, batch));
    tsdb_page_cache_snapshot_t *live = tsdb_snapshot_acquire_current(db);
    bool kept = false;
    for (size_t i = 0; i < live->count; i++)
        kept |= live->entries[i].offset == reused && live->entries[i].header.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA;
    tsdb_snapshot_release(live);
    tsdb_release_reserved_region(db, reused, NULL);
    timeseries_deinit();
    TEST_ASSERT_EQUAL_HEX32(given_up, reused);  // the scenario needs the same region
    TEST_ASSERT_TRUE(kept);
}

TEST_CASE("second: compaction commit backup OOM preserves concurrent inserts", "[second][second_commit_backup]") {
    check_commit_oom(false);
}
TEST_CASE("second: compaction commit merge OOM preserves concurrent inserts", "[second][second_commit_merge]") {
    check_commit_oom(true);
}

typedef struct { SemaphoreHandle_t done; bool ok; bool compact; } progress_t;
static void progress_worker(void *arg) {
    progress_t *p = arg;
    ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
    uint64_t ts = 10000;
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_INT, .data.int_val=99};
    p->ok = p->compact ? timeseries_compact_level0_pages(timeseries_get_db_handle())
                       : insert_second(&ts, &val, 1);
    xSemaphoreGive(p->done);
    vTaskDelete(NULL);
}
void tsdb_audit_on_publish(TaskHandle_t task, SemaphoreHandle_t done);
bool tsdb_audit_publish_progress(void);
TEST_CASE("second: compaction keeps source pages claimed through publication", "[second][second_publish]") {
    reset_second();
    timeseries_db_t *db = timeseries_get_db_handle();
    atomic_store(&db->compaction_in_progress, true);
    uint64_t *ts = malloc(500*sizeof(*ts));
    timeseries_field_value_t *vals = calloc(500,sizeof(*vals));
    TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(vals);
    for (int page=0; page<4; page++) {
        for (int i=0; i<500; i++) {
            ts[i] = page*500+i+1;
            vals[i].type = TIMESERIES_FIELD_TYPE_INT;
            vals[i].data.int_val = ts[i];
        }
        TEST_ASSERT_TRUE(insert_second(ts, vals, 500));
    }
    free(ts); free(vals);
    progress_t p = {.done=xSemaphoreCreateBinary()};
    TEST_ASSERT_NOT_NULL(p.done);
    TaskHandle_t task;
    TEST_ASSERT_EQUAL(pdPASS, xTaskCreate(progress_worker, "second_publish", 8192,
                                         &p, tskIDLE_PRIORITY+1, &task));
    tsdb_audit_on_publish(task, p.done);
    bool compacted = timeseries_compact_level0_pages(db);
    bool progress = tsdb_audit_publish_progress();
    tsdb_audit_on_publish(NULL, NULL);
    bool done = progress || xSemaphoreTake(p.done, pdMS_TO_TICKS(2000)) == pdTRUE;
    vSemaphoreDelete(p.done);
    atomic_store(&db->compaction_in_progress, false);
    timeseries_query_t q = {.measurement_name="second", .start_ms=10000};
    timeseries_query_result_t r = {0};
    bool ok = timeseries_query(&q, &r);
    size_t points = r.num_points;
    int64_t value = points ? r.columns[0].values[0].data.int_val : 0;
    timeseries_query_free_result(&r);
    timeseries_deinit();
    TEST_ASSERT_TRUE(done); TEST_ASSERT_TRUE(p.ok); TEST_ASSERT_TRUE(compacted);
    TEST_ASSERT_TRUE(ok); TEST_ASSERT_EQUAL(1, points); TEST_ASSERT_EQUAL_INT64(99, value);
}

TEST_CASE("second: compaction allocation failure leaves sources writable", "[second][second_preflight]") {
    reset_second();
    timeseries_db_t *db = timeseries_get_db_handle();
    atomic_store(&db->compaction_in_progress, true);
    uint64_t *ts = malloc(500*sizeof(*ts));
    timeseries_field_value_t *vals = calloc(500,sizeof(*vals));
    TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(vals);
    for (int page=0; page<4; page++) {
        for (int i=0; i<500; i++) {
            ts[i] = page*500+i+1;
            vals[i].type = TIMESERIES_FIELD_TYPE_INT;
            vals[i].data.int_val = ts[i];
        }
        TEST_ASSERT_TRUE(insert_second(ts, vals, 500));
    }
    free(ts); free(vals);
    // Publication builds the next snapshot (live + the journal's one output +
    // room to put back all four sources) in one allocation, before any source
    // changes on flash. Live then holds the pages above plus the output's
    // placeholder.
    tsdb_page_cache_snapshot_t *live = tsdb_snapshot_acquire_current(db);
    size_t live_pages = live->count;
    tsdb_snapshot_release(live);
    tsdb_audit_fail_malloc((live_pages + 1 + 1 + 4) * sizeof(timeseries_cached_page_t));
    bool compacted = timeseries_compact_level0_pages(db);
    tsdb_audit_fail_malloc(0);
    atomic_store(&db->compaction_in_progress, false);
    uint64_t next=10000;
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_INT, .data.int_val=99};
    bool inserted = insert_second(&next, &val, 1);
    timeseries_deinit(); TEST_ASSERT_TRUE(timeseries_init());
    timeseries_query_t q = {.measurement_name="second", .start_ms=1999};
    timeseries_query_result_t r = {0};
    bool ok = timeseries_query(&q, &r);
    size_t points = r.num_points;
    timeseries_query_free_result(&r);
    timeseries_deinit();
    TEST_ASSERT_FALSE(compacted); TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE(ok);
    TEST_ASSERT_EQUAL(3, points);
}

static void check_decode_progress(bool compact, bool rollup) {
    reset_second();
    timeseries_db_t *db = timeseries_get_db_handle();
    atomic_store(&db->compaction_in_progress, true); // explicit worker below
    uint64_t *ts = malloc(500 * sizeof(*ts));
    timeseries_field_value_t *vals = calloc(500, sizeof(*vals));
    TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(vals);
    for (int page=0; page<4; page++) {
        for (int i=0; i<500; i++) {
            ts[i] = page*500+i+1;
            vals[i].type = TIMESERIES_FIELD_TYPE_INT;
            vals[i].data.int_val = ts[i];
        }
        TEST_ASSERT_TRUE(insert_second(ts, vals, 500));
    }
    free(ts); free(vals);
    progress_t p = {.done=xSemaphoreCreateBinary(), .compact=compact};
    TEST_ASSERT_NOT_NULL(p.done);
    TaskHandle_t task;
    TEST_ASSERT_EQUAL(pdPASS, xTaskCreate(progress_worker, "second_progress", 16384,
                                         &p, tskIDLE_PRIORITY+1, &task));
    tsdb_audit_on_decode(task, p.done);
    timeseries_query_t q = {.measurement_name="second", .rollup_interval=rollup ? 100 : 0,
                            .aggregate_method=TSDB_AGGREGATION_SUM};
    timeseries_query_result_t r = {0};
    bool ok = timeseries_query(&q, &r);
    bool progress = tsdb_audit_decode_progress();
    tsdb_audit_on_decode(NULL, NULL);
    bool done = progress || xSemaphoreTake(p.done, pdMS_TO_TICKS(2000)) == pdTRUE;
    size_t points = r.num_points;
    bool values_ok = true;
    if (!rollup && points == 2000) {
        for (size_t i=0; i<points; i++)
            if (r.timestamps[i] != i+1 || r.columns[0].values[i].data.int_val != (int64_t)i+1)
                values_ok = false;
    }
    timeseries_query_free_result(&r);
    vSemaphoreDelete(p.done);
    atomic_store(&db->compaction_in_progress, false);
    timeseries_deinit();
    TEST_ASSERT_TRUE(done); TEST_ASSERT_TRUE(p.ok); TEST_ASSERT_TRUE(ok);
    TEST_ASSERT_EQUAL(rollup ? 20 : 2000, points);
    TEST_ASSERT_TRUE(values_ok);
    TEST_ASSERT_TRUE_MESSAGE(progress, "writer blocked while query only needs RAM");
}
TEST_CASE("second: insert progresses during query result decoding", "[second][second_insert_lock]") {
    check_decode_progress(false, false);
}
TEST_CASE("second: compaction progresses during query result decoding", "[second][second_compact_lock]") {
    check_decode_progress(true, false);
}
TEST_CASE("second: compaction progresses during rollup decoding", "[second][second_rollup_lock]") {
    check_decode_progress(true, true);
}

TEST_CASE("second: repeated queries warm page bounds after a cold reopen", "[second][second_cold_cache]") {
    reset_second();
    timeseries_db_t *db = timeseries_get_db_handle();
    atomic_store(&db->compaction_in_progress, true);
    uint64_t *ts = malloc(500*sizeof(*ts));
    timeseries_field_value_t *vals = calloc(500,sizeof(*vals));
    TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(vals);
    for (int page=0; page<16; page++) {
        for (int i=0; i<500; i++) {
            ts[i] = page*500+i+1;
            vals[i].type = TIMESERIES_FIELD_TYPE_INT;
            vals[i].data.int_val = ts[i];
        }
        TEST_ASSERT_TRUE(insert_second(ts, vals, 500));
    }
    free(ts); free(vals);
    atomic_store(&db->compaction_in_progress, false);
    timeseries_deinit(); TEST_ASSERT_TRUE(timeseries_init());
    size_t bytes[2];
    for (int rep=0; rep<2; rep++) {
        tsdb_audit_reset_reads();
        timeseries_query_t q = {.measurement_name="second", .start_ms=7991, .end_ms=8000};
        timeseries_query_result_t r = {0};
        TEST_ASSERT_TRUE(timeseries_query(&q, &r));
        bytes[rep] = tsdb_audit_read_bytes();
        TEST_ASSERT_EQUAL(10, r.num_points);
        for (int i=0; i<10; i++) TEST_ASSERT_EQUAL_UINT64(7991+i, r.timestamps[i]);
        timeseries_query_free_result(&r);
    }
    timeseries_deinit();
    TEST_ASSERT_LESS_THAN(bytes[0]/2, bytes[1]);
}

TEST_CASE("second: narrow query skips page payload during record discovery", "[second][second_scan]") {
    reset_second();
    timeseries_db_t *db = timeseries_get_db_handle();
    atomic_store(&db->compaction_in_progress, true);
    uint64_t *ts = malloc(500*sizeof(*ts));
    timeseries_field_value_t *vals = calloc(500,sizeof(*vals));
    TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(vals);
    tsdb_audit_reset_reads();
    int64_t start = esp_timer_get_time();
    for (int page=0; page<32; page++) {
        for (int i=0; i<500; i++) {
            ts[i] = page*500+i+1;
            vals[i].type = TIMESERIES_FIELD_TYPE_INT;
            vals[i].data.int_val = ts[i];
        }
        TEST_ASSERT_TRUE(insert_second(ts, vals, 500));
    }
    int64_t insert_us = esp_timer_get_time()-start;
    size_t insert_bytes = tsdb_audit_read_bytes();
    free(ts); free(vals);
    timeseries_query_t q = {.measurement_name="second", .start_ms=15991, .end_ms=16000, .limit=10};
    size_t bytes=0, calls=0;
    start = esp_timer_get_time();
    for (int rep=0; rep<10; rep++) {
        tsdb_audit_reset_reads();
        timeseries_query_result_t r = {0};
        TEST_ASSERT_TRUE(timeseries_query(&q, &r));
        bytes = tsdb_audit_read_bytes(); calls = tsdb_audit_read_calls();
        TEST_ASSERT_EQUAL(10, r.num_points);
        for (int i=0; i<10; i++) {
            TEST_ASSERT_EQUAL_UINT64(15991+i, r.timestamps[i]);
            TEST_ASSERT_EQUAL_INT64(15991+i, r.columns[0].values[i].data.int_val);
        }
        timeseries_query_free_result(&r);
    }
    printf("SECOND_BENCH insert_16000_us=%lld narrow_query_avg_us=%lld bytes=%zu reads=%zu insert_bytes=%zu\n",
           (long long)insert_us, (long long)((esp_timer_get_time()-start)/10), bytes, calls, insert_bytes);
    atomic_store(&db->compaction_in_progress, false);
    timeseries_deinit();
    TEST_ASSERT_LESS_THAN(150000, bytes);
    TEST_ASSERT_LESS_THAN(100000, insert_bytes);
}
#endif
