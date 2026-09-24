#include "sdkconfig.h"
#if CONFIG_IDF_TARGET_LINUX
#include <stddef.h>
#include <stdbool.h>
#include "esp_partition.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "timeseries_points_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_stream_writer.h"

/* Optional allocation/read probes for compaction regressions. */
static _Thread_local size_t sixth_fail_nth, sixth_alloc_index, sixth_largest, sixth_bytes;
static _Thread_local bool sixth_hit, sixth_measure;
static _Thread_local size_t sixth_read_offset;
static _Thread_local bool sixth_read_armed;
static _Thread_local bool sixth_read_persistent;
static _Thread_local void (*allocation_callback)(void);
void tsdb_audit_on_allocation(void (*callback)(void)) { allocation_callback = callback; }
void tsdb_audit_allocation_probe(size_t fail_nth, bool measure) {
    sixth_fail_nth = fail_nth; sixth_alloc_index = sixth_largest = sixth_bytes = 0;
    sixth_hit = false; sixth_measure = measure;
}
bool tsdb_audit_allocation_hit(void) { return sixth_hit; }
size_t tsdb_audit_largest_allocation(void) { return sixth_largest; }
size_t tsdb_audit_allocation_count(void) { return sixth_alloc_index; }
/* Sum of requested sizes, including failed requests; not live or peak heap. */
size_t tsdb_audit_allocation_bytes(void) { return sixth_bytes; }
static bool allocation_probe(size_t size) {
    if (allocation_callback) {
        void (*callback)(void) = allocation_callback;
        allocation_callback = NULL;
        callback();
    }
    if (sixth_measure && size > sixth_largest) sixth_largest = size;
    if (sixth_measure) sixth_bytes = size > SIZE_MAX - sixth_bytes ? SIZE_MAX : sixth_bytes + size;
    if (sixth_measure || sixth_fail_nth) ++sixth_alloc_index;
    if (sixth_fail_nth && sixth_alloc_index == sixth_fail_nth) {
        sixth_hit = true; return true;
    }
    return false;
}
void tsdb_audit_fail_read_at(size_t offset, bool armed) {
    sixth_read_offset = offset; sixth_read_armed = armed; sixth_read_persistent = false;
}
void tsdb_audit_fail_reads_at(size_t offset) {
    sixth_read_offset = offset; sixth_read_armed = sixth_read_persistent = true;
}

static _Thread_local bool fail_timestamp_finalize, timestamp_finalize_hit;
void tsdb_audit_fail_timestamp_finalize(bool fail) {
    fail_timestamp_finalize = fail; timestamp_finalize_hit = false;
}
bool tsdb_audit_timestamp_finalize_hit(void) { return timestamp_finalize_hit; }
bool __real_timeseries_page_stream_writer_finalize_timestamp(timeseries_page_stream_writer_t *);
bool __wrap_timeseries_page_stream_writer_finalize_timestamp(timeseries_page_stream_writer_t *writer) {
    bool result = __real_timeseries_page_stream_writer_finalize_timestamp(writer);
    if (fail_timestamp_finalize) {
        fail_timestamp_finalize = false; timestamp_finalize_hit = true; return false;
    }
    return result;
}

/* Fail one matching allocation in the calling test task only. */
static _Thread_local size_t fail_count, fail_size;
static _Thread_local bool did_fail;
void tsdb_audit_fail_calloc(size_t count, size_t size) {
    fail_count = count;
    fail_size = size;
    did_fail = false;
}
bool tsdb_audit_calloc_failed(void) { return did_fail; }
// Allocations run with all signals blocked (timeseries_host_sim_guards.c).
#include <signal.h>
void tsdb_host_sim_block_signals(sigset_t *old);
void *__real_calloc(size_t, size_t);
void *__wrap_calloc(size_t count, size_t size) {
    if (size && count <= SIZE_MAX / size && allocation_probe(count * size)) return NULL;
    if (fail_size && count == fail_count && size == fail_size) {
        fail_size = 0;
        did_fail = true;
        return NULL;
    }
    sigset_t old;
    tsdb_host_sim_block_signals(&old);
    void *p = __real_calloc(count, size);
    pthread_sigmask(SIG_SETMASK, &old, NULL);
    return p;
}

static _Thread_local size_t fail_malloc_size, fail_realloc_size, fail_malloc_always_size;
void tsdb_audit_fail_malloc(size_t size) { fail_malloc_size = size; }
void tsdb_audit_fail_realloc(size_t size) { fail_realloc_size = size; }
/* Every malloc of exactly this size fails until reset with 0. */
void tsdb_audit_fail_malloc_always(size_t size) { fail_malloc_always_size = size; }
void *__real_malloc(size_t);
void *__wrap_malloc(size_t size) {
    if (allocation_probe(size)) return NULL;
    if (size && size == fail_malloc_size) { fail_malloc_size = 0; return NULL; }
    if (size && size == fail_malloc_always_size) return NULL;
    sigset_t old;
    tsdb_host_sim_block_signals(&old);
    void *p = __real_malloc(size);
    pthread_sigmask(SIG_SETMASK, &old, NULL);
    return p;
}
void *__real_realloc(void *, size_t);
void *__wrap_realloc(void *ptr, size_t size) {
    if (allocation_probe(size)) return NULL;
    if (size && size == fail_realloc_size) { fail_realloc_size = 0; return NULL; }
    sigset_t old;
    tsdb_host_sim_block_signals(&old);
    void *p = __real_realloc(ptr, size);
    pthread_sigmask(SIG_SETMASK, &old, NULL);
    return p;
}

static _Thread_local size_t read_bytes, read_calls;
void tsdb_audit_reset_reads(void) { read_bytes = read_calls = 0; }
size_t tsdb_audit_read_bytes(void) { return read_bytes; }
size_t tsdb_audit_read_calls(void) { return read_calls; }
static _Thread_local TaskHandle_t decode_task;
static _Thread_local SemaphoreHandle_t decode_done;
static _Thread_local bool decode_progress;
void tsdb_audit_on_decode(TaskHandle_t task, SemaphoreHandle_t done) {
    decode_task = task; decode_done = done; decode_progress = false;
}
bool tsdb_audit_decode_progress(void) { return decode_progress; }
bool __real_timeseries_points_iterator_next_value(timeseries_points_iterator_t *, timeseries_field_value_t *);
bool __wrap_timeseries_points_iterator_next_value(timeseries_points_iterator_t *iter, timeseries_field_value_t *value) {
    if (decode_task) {
        TaskHandle_t task = decode_task;
        decode_task = NULL;
        xTaskNotifyGive(task);
        decode_progress = xSemaphoreTake(decode_done, pdMS_TO_TICKS(200)) == pdTRUE;
    }
    return __real_timeseries_points_iterator_next_value(iter, value);
}
extern timeseries_db_t *timeseries_get_db_handle(void);
static _Thread_local TaskHandle_t publish_task;
static _Thread_local SemaphoreHandle_t publish_done;
static _Thread_local bool publish_progress;
void tsdb_audit_on_publish(TaskHandle_t task, SemaphoreHandle_t done) {
    publish_task = task; publish_done = done; publish_progress = false;
}
bool tsdb_audit_publish_progress(void) { return publish_progress; }
void __real_tsdb_pagecache_batch_sort(tsdb_page_cache_snapshot_t *);
void __wrap_tsdb_pagecache_batch_sort(tsdb_page_cache_snapshot_t *batch) {
    timeseries_db_t *db;
    if (publish_task && (db = timeseries_get_db_handle()) && !db->active_batch) {
        TaskHandle_t task = publish_task;
        publish_task = NULL;
        xTaskNotifyGive(task);
        publish_progress = xSemaphoreTake(publish_done, pdMS_TO_TICKS(200)) == pdTRUE;
    }
    __real_tsdb_pagecache_batch_sort(batch);
}

static _Thread_local TaskHandle_t recycle_task;
static _Thread_local size_t recycle_offset;
void tsdb_audit_recycle_on_read(TaskHandle_t task, size_t offset) {
    recycle_task = task;
    recycle_offset = offset;
}
esp_err_t __real_esp_partition_read(const esp_partition_t *, size_t, void *, size_t);
bool tsdb_test_read_fault(size_t offset, size_t size);  // timeseries_recovery_test.c
esp_err_t __wrap_esp_partition_read(const esp_partition_t *part, size_t offset,
                                  void *buffer, size_t size) {
    if (sixth_read_armed && offset == sixth_read_offset) {
        sixth_read_armed = sixth_read_persistent; return ESP_FAIL;
    }
    if (tsdb_test_read_fault(offset, size)) return ESP_FAIL;
    read_bytes += size;
    read_calls++;
    if (recycle_task && offset == recycle_offset && size == 8) {
        TaskHandle_t task = recycle_task;
        recycle_task = NULL;
        xTaskNotifyGive(task);
        // Allow the writer to attempt reuse after query record discovery.
        vTaskDelay(pdMS_TO_TICKS(20));
    }
    return __real_esp_partition_read(part, offset, buffer, size);
}
#endif
