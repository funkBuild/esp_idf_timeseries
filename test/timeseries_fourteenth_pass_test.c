#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_data.h"
#include "timeseries_query.h"
#include "timeseries_compaction.h"
#include "timeseries_expiration.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include <stdatomic.h>
#include <string.h>

extern timeseries_db_t *timeseries_get_db_handle(void);
enum operation { QUERY, INSERT, COMPACT, FORCE, EXPIRE, CLEAR, BACKGROUND };
static enum operation selected;
static TaskHandle_t operation_owner, shutdown_owner;
static SemaphoreHandle_t entered, resume, operation_done, shutdown_done;
static timeseries_db_t *fixture_db;
static bool fast_timeout, operation_ok;
static unsigned timeout_delays;
static TaskHandle_t fail_create_owner;
static bool create_failed;
static TaskHandle_t slow_create_owner;
static SemaphoreHandle_t worker_finished;
BaseType_t __real_xTaskCreatePinnedToCore(TaskFunction_t, const char *, configSTACK_DEPTH_TYPE,
                                         void *, UBaseType_t, TaskHandle_t *, BaseType_t);
BaseType_t __wrap_xTaskCreatePinnedToCore(TaskFunction_t task, const char *name, configSTACK_DEPTH_TYPE depth,
                                         void *arg, UBaseType_t priority, TaskHandle_t *handle, BaseType_t core) {
    if (fail_create_owner == xTaskGetCurrentTaskHandle() && !strcmp(name, "tsdb_compact")) {
        create_failed = true;
        return errCOULD_NOT_ALLOCATE_REQUIRED_MEMORY;
    }
    BaseType_t result = __real_xTaskCreatePinnedToCore(task, name, depth, arg, priority, handle, core);
    if (result == pdPASS && slow_create_owner == xTaskGetCurrentTaskHandle() && !strcmp(name, "tsdb_compact")) {
        // Give the new worker a chance to finish before the caller publishes
        // its handle. The fixed worker waits for the launcher's notification.
        xSemaphoreTake(worker_finished, pdMS_TO_TICKS(50));
        vTaskDelay(1);
    }
    return result;
}

bool tsdb_audit_fourteenth_skip_delay(void) {
    if (fast_timeout && shutdown_owner == xTaskGetCurrentTaskHandle()) {
        ++timeout_delays;
        return true;
    }
    return false;
}

static bool gate(enum operation op) {
    if (operation_owner != xTaskGetCurrentTaskHandle() || selected != op) return true;
    operation_owner = NULL; // pause only once, even if the operation erases again
    xSemaphoreGive(entered);
    if (xSemaphoreTake(resume, pdMS_TO_TICKS(5000)) != pdTRUE) return false;
    // Let the baseline report premature teardown without dereferencing freed
    // mutexes/buffers when the deliberately paused operation resumes.
    return fixture_db->snapshot_mutex && fixture_db->flash_write_mutex && fixture_db->current_snapshot;
}

bool tsdb_audit_fourteenth_erase_gate(void) { return gate(CLEAR); }
bool __real_timeseries_query_execute(timeseries_db_t *, const timeseries_query_t *, timeseries_query_result_t *);
bool __wrap_timeseries_query_execute(timeseries_db_t *db, const timeseries_query_t *q, timeseries_query_result_t *r) {
    return gate(QUERY) && __real_timeseries_query_execute(db, q, r);
}
bool __real_tsdb_point_order_init(const uint64_t *, size_t, tsdb_point_order_t *);
bool __wrap_tsdb_point_order_init(const uint64_t *ts, size_t n, tsdb_point_order_t *order) {
    return gate(INSERT) && __real_tsdb_point_order_init(ts, n, order);
}
bool __real_timeseries_compact_all_levels(timeseries_db_t *);
bool __wrap_timeseries_compact_all_levels(timeseries_db_t *db) {
    return gate(COMPACT) && __real_timeseries_compact_all_levels(db);
}
bool __real_timeseries_compact_all_levels_force(timeseries_db_t *);
bool __wrap_timeseries_compact_all_levels_force(timeseries_db_t *db) {
    return gate(FORCE) && __real_timeseries_compact_all_levels_force(db);
}
bool __real_timeseries_expiration_run(timeseries_db_t *, float, float);
bool __wrap_timeseries_expiration_run(timeseries_db_t *db, float threshold, float fraction) {
    return gate(EXPIRE) && __real_timeseries_expiration_run(db, threshold, fraction);
}

static bool insert_one(uint64_t timestamp) {
    const char *field = "value";
    timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_INT, .data.int_val = timestamp};
    timeseries_insert_data_t in = {.measurement_name = "fourteenth", .field_names = &field,
        .num_fields = 1, .num_points = 1, .timestamps_ms = &timestamp, .field_values = &value};
    return timeseries_insert(&in);
}

static void background_post(void) {
    operation_owner = xTaskGetCurrentTaskHandle();
    operation_ok = gate(BACKGROUND);
}

static void run_operation(void *unused) {
    (void)unused;
    operation_owner = xTaskGetCurrentTaskHandle();
    timeseries_query_t q = {.measurement_name = "fourteenth"};
    timeseries_query_result_t r = {0};
    switch (selected) {
    case QUERY: operation_ok = timeseries_query(&q, &r) && r.num_points == 1; break;
    case INSERT: operation_ok = insert_one(2); break;
    case COMPACT: operation_ok = timeseries_compact_sync(); break;
    case FORCE: operation_ok = timeseries_compact_force_sync(); break;
    case EXPIRE: operation_ok = timeseries_expire(); break;
    case CLEAR: operation_ok = timeseries_clear_all(); break;
    case BACKGROUND: timeseries_compact(); break;
    }
    timeseries_query_free_result(&r);
    xSemaphoreGive(operation_done);
    vTaskDelete(NULL);
}

static void run_shutdown(void *unused) {
    (void)unused;
    shutdown_owner = xTaskGetCurrentTaskHandle();
    timeseries_deinit();
    xSemaphoreGive(shutdown_done);
    vTaskDelete(NULL);
}

static void lifecycle_test(enum operation op, bool timeout) {
    TEST_ASSERT_TRUE(timeseries_init());
    TEST_ASSERT_TRUE(timeseries_clear_all());
    TEST_ASSERT_TRUE(insert_one(1));
    fixture_db = timeseries_get_db_handle();
    selected = op;
    fast_timeout = timeout;
    timeout_delays = 0;
    operation_owner = shutdown_owner = NULL;
    operation_ok = false;
    entered = xSemaphoreCreateBinary(); resume = xSemaphoreCreateBinary();
    operation_done = xSemaphoreCreateBinary(); shutdown_done = xSemaphoreCreateBinary();
    TEST_ASSERT_NOT_NULL(entered); TEST_ASSERT_NOT_NULL(resume);
    TEST_ASSERT_NOT_NULL(operation_done); TEST_ASSERT_NOT_NULL(shutdown_done);
    if (op == BACKGROUND) timeseries_set_compaction_hooks(NULL, background_post);
    TEST_ASSERT_EQUAL(pdPASS, xTaskCreate(run_operation, "lifetime_op", 8192, NULL, tskIDLE_PRIORITY + 1, NULL));
    TEST_ASSERT_EQUAL(pdTRUE, xSemaphoreTake(entered, pdMS_TO_TICKS(2000)));
    // A completion from an older worker is not evidence that the currently
    // paused synchronous operation is finished. Background case uses its real
    // completion signal and parks in the post hook instead.
    if (op != BACKGROUND) xSemaphoreGive(fixture_db->compaction_done_sem);
    TEST_ASSERT_EQUAL(pdPASS, xTaskCreate(run_shutdown, "lifetime_close", 8192, NULL, tskIDLE_PRIORITY + 1, NULL));
    bool returned = xSemaphoreTake(shutdown_done, pdMS_TO_TICKS(100)) == pdTRUE;
    bool preserved = fixture_db->snapshot_mutex && fixture_db->flash_write_mutex && fixture_db->current_snapshot;
    bool closed = !atomic_load(&fixture_db->initialized);
    // Avoid replacing the baseline's destroyed resources beneath its paused op.
    bool init_rejected = preserved && !timeseries_init();
    timeseries_query_t q = {.measurement_name = "fourteenth"};
    timeseries_query_result_t r = {0};
    bool new_work_rejected = !timeseries_query(&q, &r) && !insert_one(3);
    timeseries_query_free_result(&r);
    xSemaphoreGive(resume);
    bool op_finished = xSemaphoreTake(operation_done, pdMS_TO_TICKS(2000)) == pdTRUE;
    bool close_finished = returned || xSemaphoreTake(shutdown_done, pdMS_TO_TICKS(2000)) == pdTRUE;
    if (op == BACKGROUND) timeseries_set_compaction_hooks(NULL, NULL);
    fast_timeout = false;
    operation_owner = shutdown_owner = NULL;
    // A timed-out close remains recoverable once its admitted work finishes.
    timeseries_deinit();
    bool reclaimed = !fixture_db->snapshot_mutex && !fixture_db->current_snapshot && !fixture_db->write_buffer;
    bool reopened = timeseries_init();
    bool data_ok = reopened && timeseries_query(&q, &r) &&
        r.num_points == (op == INSERT ? 2 : op == CLEAR ? 0 : 1);
    timeseries_query_free_result(&r);
    timeseries_deinit();
    vSemaphoreDelete(entered); vSemaphoreDelete(resume);
    vSemaphoreDelete(operation_done); vSemaphoreDelete(shutdown_done);
    fixture_db = NULL;
    TEST_ASSERT_EQUAL(timeout, returned);
    TEST_ASSERT_TRUE(preserved); TEST_ASSERT_TRUE(closed); TEST_ASSERT_TRUE(init_rejected);
    TEST_ASSERT_TRUE(new_work_rejected); TEST_ASSERT_TRUE(op_finished); TEST_ASSERT_TRUE(close_finished);
    TEST_ASSERT_TRUE(operation_ok); TEST_ASSERT_TRUE(reclaimed); TEST_ASSERT_TRUE(reopened); TEST_ASSERT_TRUE(data_ok);
    if (timeout) TEST_ASSERT_GREATER_THAN(0, timeout_delays);
}

TEST_CASE("fourteenth: teardown waits for query", "[fourteenth][fourteenth_query]") { lifecycle_test(QUERY, false); }
TEST_CASE("fourteenth: teardown waits for insert", "[fourteenth][fourteenth_insert]") { lifecycle_test(INSERT, false); }
TEST_CASE("fourteenth: stale completion cannot bypass synchronous compaction", "[fourteenth][fourteenth_sync]") { lifecycle_test(COMPACT, false); }
TEST_CASE("fourteenth: stale completion cannot bypass forced compaction", "[fourteenth][fourteenth_force]") { lifecycle_test(FORCE, false); }
TEST_CASE("fourteenth: teardown waits for expiration", "[fourteenth][fourteenth_expire]") { lifecycle_test(EXPIRE, false); }
TEST_CASE("fourteenth: stale completion cannot bypass clear", "[fourteenth][fourteenth_clear]") { lifecycle_test(CLEAR, false); }
TEST_CASE("fourteenth: teardown waits through background post hook", "[fourteenth][fourteenth_background]") { lifecycle_test(BACKGROUND, false); }
TEST_CASE("fourteenth: timed out teardown can reclaim and reopen later", "[fourteenth][fourteenth_timeout]") { lifecycle_test(QUERY, true); }

static void assert_no_outstanding_operations(timeseries_db_t *db) {
    shutdown_owner = xTaskGetCurrentTaskHandle(); fast_timeout = true; timeout_delays = 0;
    timeseries_deinit();
    fast_timeout = false; shutdown_owner = NULL;
    bool reclaimed = !db->snapshot_mutex && !db->current_snapshot && !db->write_buffer;
    bool reopened = timeseries_init();
    timeseries_deinit();
    TEST_ASSERT_EQUAL(0, timeout_delays);
    TEST_ASSERT_TRUE(reclaimed); TEST_ASSERT_TRUE(reopened);
}

TEST_CASE("fourteenth: failed worker creation releases both lifetime references", "[fourteenth][fourteenth_create_failure]") {
    TEST_ASSERT_TRUE(timeseries_init());
    timeseries_db_t *db = timeseries_get_db_handle();
    fail_create_owner = xTaskGetCurrentTaskHandle(); create_failed = false;
    bool rejected = !tsdb_launch_compaction(db);
    fail_create_owner = NULL;
    bool released = !atomic_load(&db->compaction_in_progress) && !atomic_load(&db->compaction_task_handle);
    assert_no_outstanding_operations(db);
    TEST_ASSERT_TRUE(create_failed); TEST_ASSERT_TRUE(rejected); TEST_ASSERT_TRUE(released);
}

TEST_CASE("fourteenth: public argument failures release operation references", "[fourteenth][fourteenth_arguments]") {
    TEST_ASSERT_TRUE(timeseries_init());
    timeseries_db_t *db = timeseries_get_db_handle();
    bool rejected = !timeseries_insert(NULL) && !timeseries_query(NULL, NULL) &&
        !timeseries_get_measurements(NULL, NULL) && !timeseries_get_fields_for_measurement(NULL, NULL, NULL) &&
        !timeseries_get_tags_for_measurement(NULL, NULL, NULL) && !timeseries_get_usage_summary(NULL) &&
        !timeseries_delete_measurement(NULL) && !timeseries_delete_measurement_and_field(NULL, NULL) &&
        !timeseries_get_timestamp_range(NULL, NULL, NULL, NULL);
    timeseries_set_chunk_size(0);
    assert_no_outstanding_operations(db);
    TEST_ASSERT_TRUE(rejected);
}

static void worker_post_signal(void) { xSemaphoreGive(worker_finished); }
TEST_CASE("fourteenth: fast worker cannot leave a stale published task handle", "[fourteenth][fourteenth_handle]") {
    TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
    timeseries_db_t *db = timeseries_get_db_handle();
    worker_finished = xSemaphoreCreateBinary(); TEST_ASSERT_NOT_NULL(worker_finished);
    timeseries_set_compaction_hooks(NULL, worker_post_signal);
    slow_create_owner = xTaskGetCurrentTaskHandle();
    bool launched = tsdb_launch_compaction(db);
    slow_create_owner = NULL;
    for (unsigned i = 0; i < 200 && atomic_load(&db->compaction_in_progress); ++i) vTaskDelay(1);
    bool completed = !atomic_load(&db->compaction_in_progress);
    bool cleared = !atomic_load(&db->compaction_task_handle);
    timeseries_deinit();
    timeseries_set_compaction_hooks(NULL, NULL);
    vSemaphoreDelete(worker_finished); worker_finished = NULL;
    TEST_ASSERT_TRUE(launched); TEST_ASSERT_TRUE(completed); TEST_ASSERT_TRUE(cleared);
}
