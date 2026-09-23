#include "timeseries_epoch.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "timeseries_page_cache.h"

static const char *TAG = "TimeseriesEpoch";

#define TSDB_REGION_GRANULE 4096u

bool tsdb_epoch_init(timeseries_db_t *db) {
  size_t cap = db->partition ? db->partition->size / TSDB_REGION_GRANULE : 0;
  db->retired_count = 0;
  db->retired_cap = 0;
  if (!cap) return true;
  // Plain malloc: PSRAM on the device (6 KiB for a 2 MiB partition).
  db->retired = malloc(cap * sizeof(*db->retired));
  if (!db->retired) return false;
  db->retired_cap = cap;
  return true;
}

void tsdb_epoch_deinit(timeseries_db_t *db) {
  free(db->retired);
  db->retired = NULL;
  db->retired_count = db->retired_cap = 0;
}

int tsdb_epoch_enter(timeseries_db_t *db) {
  TaskHandle_t me = xTaskGetCurrentTaskHandle();
  for (;;) {
    for (int i = 0; i < TSDB_EPOCH_SLOTS; i++) {
      TaskHandle_t expected = NULL;
      if (atomic_compare_exchange_strong(&db->epoch_slots[i].owner, &expected, me)) {
        // A reclaimer that looks at this slot before the stores below sees an
        // older epoch, which only delays reclamation.
        atomic_store(&db->epoch_slots[i].idle, false);
        atomic_store(&db->epoch_slots[i].epoch, atomic_load(&db->reclaim_epoch));
        return i;
      }
    }
    vTaskDelay(1);  // every slot busy: wait for an operation to finish
  }
}

void tsdb_epoch_exit(timeseries_db_t *db, int slot) {
  if (slot >= 0 && slot < TSDB_EPOCH_SLOTS) atomic_store(&db->epoch_slots[slot].owner, (TaskHandle_t)NULL);
}

void tsdb_epoch_quiesce(timeseries_db_t *db) {
  TaskHandle_t me = xTaskGetCurrentTaskHandle();
  uint32_t now = atomic_load(&db->reclaim_epoch);
  for (int i = 0; i < TSDB_EPOCH_SLOTS; i++)
    if (atomic_load(&db->epoch_slots[i].owner) == me) atomic_store(&db->epoch_slots[i].epoch, now);
}

void tsdb_epoch_idle(timeseries_db_t *db, bool idle) {
  TaskHandle_t me = xTaskGetCurrentTaskHandle();
  uint32_t now = atomic_load(&db->reclaim_epoch);
  for (int i = 0; i < TSDB_EPOCH_SLOTS; i++) {
    if (atomic_load(&db->epoch_slots[i].owner) != me) continue;
    // Leaving idle: move to the current epoch before becoming visible again.
    if (!idle) atomic_store(&db->epoch_slots[i].epoch, now);
    atomic_store(&db->epoch_slots[i].idle, idle);
  }
}

void tsdb_write_lock(timeseries_db_t *db) {
  if (!db->flash_write_mutex) return;
  if (xSemaphoreTake(db->flash_write_mutex, 0) == pdTRUE) return;
  tsdb_epoch_idle(db, true);
  xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY);
  tsdb_epoch_idle(db, false);
}

// True when an active, non-idle slot (other than the caller's, if skip_self)
// entered at or before `epoch`, so it may read a region retired at `epoch`.
// Serial arithmetic relative to the current epoch survives 32-bit wrap.
static bool epoch_pinned(timeseries_db_t *db, uint32_t epoch, bool skip_self) {
  TaskHandle_t me = skip_self ? xTaskGetCurrentTaskHandle() : NULL;
  uint32_t now = atomic_load(&db->reclaim_epoch);
  for (int i = 0; i < TSDB_EPOCH_SLOTS; i++) {
    TaskHandle_t owner = atomic_load(&db->epoch_slots[i].owner);
    if (!owner || (skip_self && owner == me) || atomic_load(&db->epoch_slots[i].idle)) continue;
    uint32_t e = atomic_load(&db->epoch_slots[i].epoch);
    if ((uint32_t)(now - e) >= (uint32_t)(now - epoch)) return true;  // e <= epoch
  }
  return false;
}

bool tsdb_epoch_synchronize(timeseries_db_t *db, uint32_t timeout_ms) {
  uint32_t e = atomic_fetch_add(&db->reclaim_epoch, 1);
  TickType_t start = xTaskGetTickCount();
  while (epoch_pinned(db, e, true)) {
    if ((TickType_t)(xTaskGetTickCount() - start) > pdMS_TO_TICKS(timeout_ms)) return false;
    vTaskDelay(1);
  }
  return true;
}

// snapshot_mutex held. Drop retired regions no active operation can read.
static void prune_locked(timeseries_db_t *db) {
  size_t kept = 0;
  for (size_t i = 0; i < db->retired_count; i++)
    if (epoch_pinned(db, db->retired[i].epoch, false)) db->retired[kept++] = db->retired[i];
  db->retired_count = kept;
}

void tsdb_retire_region_locked(timeseries_db_t *db, uint32_t offset, uint32_t size) {
  if (!db->retired || !size) return;
  if (db->retired_count == db->retired_cap) prune_locked(db);
  if (db->retired_count == db->retired_cap) {
    // Unreachable: retired regions start on distinct 4 KiB boundaries.
    ESP_LOGE(TAG, "Retired-region table full at @0x%08" PRIx32 "; region reusable at once", offset);
    return;
  }
  // The live snapshot without this region is published before the epoch
  // advances, so an operation that enters at the new epoch cannot see it.
  uint32_t e = atomic_fetch_add(&db->reclaim_epoch, 1);
  db->retired[db->retired_count++] = (struct tsdb_retired_region){offset, size, e};
}

void tsdb_retire_diff_locked(timeseries_db_t *db, const tsdb_page_cache_snapshot_t *o,
                             const tsdb_page_cache_snapshot_t *n) {
  if (!db->retired || !o) return;
  size_t j = 0, nc = n ? n->count : 0;
  for (size_t i = 0; i < o->count; i++) {  // both sorted by offset
    uint32_t off = o->entries[i].offset;
    while (j < nc && n->entries[j].offset < off) j++;
    if (j >= nc || n->entries[j].offset != off) tsdb_retire_region_locked(db, off, o->entries[i].header.page_size);
  }
}

void tsdb_retired_forget_live(timeseries_db_t *db) {
  xSemaphoreTake(db->snapshot_mutex, portMAX_DELAY);
  const tsdb_page_cache_snapshot_t *live = db->current_snapshot;
  size_t kept = 0;
  for (size_t i = 0; i < db->retired_count; i++) {
    bool is_live = false;
    for (size_t k = 0; live && k < live->count && !is_live; k++) is_live = live->entries[k].offset == db->retired[i].offset;
    if (!is_live) db->retired[kept++] = db->retired[i];
  }
  db->retired_count = kept;
  xSemaphoreGive(db->snapshot_mutex);
}

void tsdb_retired_clear(timeseries_db_t *db) {
  xSemaphoreTake(db->snapshot_mutex, portMAX_DELAY);
  db->retired_count = 0;
  xSemaphoreGive(db->snapshot_mutex);
}

bool tsdb_retired_pinned_by_others(timeseries_db_t *db) {
  bool pinned = false;
  xSemaphoreTake(db->snapshot_mutex, portMAX_DELAY);
  for (size_t i = 0; i < db->retired_count && !pinned; i++) pinned = epoch_pinned(db, db->retired[i].epoch, true);
  xSemaphoreGive(db->snapshot_mutex);
  return pinned;
}

tsdb_page_cache_snapshot_t *tsdb_occupied_snapshot(timeseries_db_t *db) {
  xSemaphoreTake(db->snapshot_mutex, portMAX_DELAY);
  prune_locked(db);
  size_t pinned = db->retired_count;
  if (!pinned) {
    // Common case: nothing to protect; the live snapshot is the occupied set.
    tsdb_page_cache_snapshot_t *live = tsdb_snapshot_acquire(db->current_snapshot);
    xSemaphoreGive(db->snapshot_mutex);
    return live;
  }
  tsdb_page_cache_snapshot_t *occupied = tsdb_snapshot_clone_with_room(db->current_snapshot, pinned);
  for (size_t i = 0; occupied && i < pinned; i++) {
    timeseries_page_header_t h;
    memset(&h, 0xFF, sizeof h);
    h.magic_number = TIMESERIES_MAGIC_NUM;
    h.page_state = TIMESERIES_PAGE_STATE_ACTIVE;  // the blank iterator skips ACTIVE extents
    h.page_size = db->retired[i].size;
    // Room was reserved; retired offsets are never live, so this inserts.
    tsdb_pagecache_batch_add(occupied, db->retired[i].offset, &h);
  }
  xSemaphoreGive(db->snapshot_mutex);
  return occupied;
}
