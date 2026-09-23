#include "timeseries_page_cache_snapshot.h"
#include "timeseries_epoch.h"

#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

#include <stdlib.h>
#include <string.h>

static const char *TAG = "PageCacheSnapshot";

tsdb_page_cache_snapshot_t *tsdb_snapshot_create(size_t initial_capacity) {
    tsdb_page_cache_snapshot_t *snap = calloc(1, sizeof(tsdb_page_cache_snapshot_t));
    if (!snap) {
        ESP_LOGE(TAG, "OOM creating snapshot");
        return NULL;
    }

    if (initial_capacity > 0) {
        snap->entries = calloc(initial_capacity, sizeof(timeseries_cached_page_t));
        if (!snap->entries) {
            ESP_LOGE(TAG, "OOM allocating snapshot entries");
            free(snap);
            return NULL;
        }
    }

    snap->count = 0;
    snap->capacity = initial_capacity;
    atomic_store(&snap->refcount, 1);
    snap->removed_offsets = NULL;
    snap->removed_count = 0;
    snap->removed_capacity = 0;

    return snap;
}

tsdb_page_cache_snapshot_t *tsdb_snapshot_clone(const tsdb_page_cache_snapshot_t *src) {
    return tsdb_snapshot_clone_with_room(src, 0);
}

tsdb_page_cache_snapshot_t *tsdb_snapshot_clone_with_room(const tsdb_page_cache_snapshot_t *src,
                                                        size_t additional) {
    size_t count = src ? src->count : 0;
    size_t maximum = SIZE_MAX / sizeof(timeseries_cached_page_t);
    if (count > maximum || additional > maximum - count) return NULL;
    size_t capacity = count + additional;
    if (!count && capacity < 8) capacity = 8;

    tsdb_page_cache_snapshot_t *snap = calloc(1, sizeof(tsdb_page_cache_snapshot_t));
    if (!snap) {
        ESP_LOGE(TAG, "OOM cloning snapshot");
        return NULL;
    }

    snap->capacity = capacity;
    // Every live entry is copied below; unused headroom is initialized by add.
    snap->entries = malloc(snap->capacity * sizeof(timeseries_cached_page_t));
    if (!snap->entries) {
        ESP_LOGE(TAG, "OOM allocating clone entries");
        free(snap);
        return NULL;
    }

    if (count > 0) {
        memcpy(snap->entries, src->entries, count * sizeof(timeseries_cached_page_t));
    }
    snap->count = count;
    atomic_store(&snap->refcount, 1);
    snap->removed_offsets = NULL;
    snap->removed_count = 0;
    snap->removed_capacity = 0;

    return snap;
}

tsdb_page_cache_snapshot_t *tsdb_snapshot_acquire(tsdb_page_cache_snapshot_t *snap) {
    if (!snap) {
        return NULL;
    }
    atomic_fetch_add(&snap->refcount, 1);
    return snap;
}

void tsdb_snapshot_release(tsdb_page_cache_snapshot_t *snap) {
    if (!snap) {
        return;
    }
    uint32_t prev = atomic_fetch_sub(&snap->refcount, 1);
    if (prev == 0) {
        // Underflow: refcount was already 0 before decrement (double release)
        ESP_LOGE(TAG, "BUG: refcount underflow on snapshot %p - double release?", (void *)snap);
        atomic_fetch_add(&snap->refcount, 1);  // restore to 0
        return;
    }
    if (prev == 1) {
        // Refcount dropped to 0 -- free everything
        free(snap->entries);
        free(snap->removed_offsets);
        free(snap);
    }
}

tsdb_page_cache_snapshot_t *tsdb_snapshot_acquire_current(timeseries_db_t *db) {
    if (!db || !db->snapshot_mutex) {
        return NULL;
    }
    xSemaphoreTake(db->snapshot_mutex, portMAX_DELAY);
    tsdb_page_cache_snapshot_t *snap = tsdb_snapshot_acquire(db->current_snapshot);
    xSemaphoreGive(db->snapshot_mutex);
    return snap;
}

void tsdb_snapshot_swap(timeseries_db_t *db, tsdb_page_cache_snapshot_t *new_snap) {
    if (!db || !db->snapshot_mutex) {
        return;
    }
    xSemaphoreTake(db->snapshot_mutex, portMAX_DELAY);
    tsdb_page_cache_snapshot_t *old = db->current_snapshot;
    tsdb_retire_diff_locked(db, old, new_snap);
    db->current_snapshot = new_snap;
    xSemaphoreGive(db->snapshot_mutex);
    tsdb_snapshot_release(old);
}

bool tsdb_snapshot_compare_and_swap(timeseries_db_t *db,
                                     tsdb_page_cache_snapshot_t *expected,
                                     tsdb_page_cache_snapshot_t *new_snap) {
    if (!db || !db->snapshot_mutex) return false;
    xSemaphoreTake(db->snapshot_mutex, portMAX_DELAY);
    if (db->current_snapshot != expected) {
        xSemaphoreGive(db->snapshot_mutex);
        return false;  // Someone else swapped - caller should retry
    }
    tsdb_page_cache_snapshot_t *old = db->current_snapshot;
    tsdb_retire_diff_locked(db, old, new_snap);
    db->current_snapshot = new_snap;
    xSemaphoreGive(db->snapshot_mutex);
    tsdb_snapshot_release(old);
    return true;
}
