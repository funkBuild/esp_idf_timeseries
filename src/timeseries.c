#include "timeseries_epoch.h"
#include "timeseries.h"
#include "timeseries_revision.h"
#include "timeseries_repair.h"
#include "esp_attr.h"
#include "esp_log.h"
#include "esp_partition.h"
#include "timeseries_cache.h"
#include "timeseries_compaction.h"
#include "timeseries_data.h"
#include "timeseries_delete.h"
#include "timeseries_expiration.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_metadata.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include "timeseries_points_iterator.h"
#include "timeseries_query.h"
#include "timeseries_string_list.h"

#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

#include "mbedtls/md.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static const char* TAG = "TimeseriesDB";

// Global/Static DB Context (single instance by design). Zero-initialised and
// in PSRAM (~300 B off the internal .data segment); init_locked() sets the
// only non-zero default, next_measurement_id, before anything reads it.
static EXT_RAM_BSS_ATTR timeseries_db_t s_tsdb;
static _Atomic bool s_lifecycle_busy = false;

// Admission and active-operation count share one atomic word, so teardown
// cannot miss an operation between checking readiness and acquiring a reference.
#define TSDB_CLOSED (UINT32_C(1) << 31)
static _Atomic uint32_t s_operations = TSDB_CLOSED;
// Protected by s_lifecycle_busy. A timed-out close can be retried, but init
// cannot replace resources that still belong to an admitted operation.
static bool s_cleanup_pending;

static bool tsdb_operation_acquire(void) {
    uint32_t state = atomic_load(&s_operations);
    do {
        if ((state & TSDB_CLOSED) || state == TSDB_CLOSED - 1) return false;
    } while (!atomic_compare_exchange_weak(&s_operations, &state, state + 1));
    return true;
}

static void tsdb_operation_release(bool *held) {
    if (*held) atomic_fetch_sub(&s_operations, 1);
}

// ESP-IDF's C compiler runs cleanup on every return, including error paths.
// This reference protects resource lifetime; it does not serialize operations.
// It also owns an epoch slot for the operation (timeseries_epoch.h): no flash
// region the operation could have seen is erased or reused before it ends.
typedef struct {
    bool held;
    int slot;
} tsdb_operation_t;

static tsdb_operation_t tsdb_operation_begin(void) {
    tsdb_operation_t op = {tsdb_operation_acquire(), -1};
    if (op.held) op.slot = tsdb_epoch_enter(&s_tsdb);
    return op;
}

static void tsdb_operation_end(tsdb_operation_t *op) {
    if (op->slot >= 0) tsdb_epoch_exit(&s_tsdb, op->slot);
    tsdb_operation_release(&op->held);
}

#define TSDB_OPERATION_GUARD(failure) \
    tsdb_operation_t operation __attribute__((cleanup(tsdb_operation_end))) = tsdb_operation_begin(); \
    if (!operation.held) return failure

// clear_all waits this long for operations that may still read a page.
#define TSDB_CLEAR_READER_WAIT_MS 60000

void timeseries_set_compaction_hooks(timeseries_compaction_hook_t pre_compact,
                                     timeseries_compaction_hook_t post_compact) {
    s_tsdb.pre_compact_hook = pre_compact;
    s_tsdb.post_compact_hook = post_compact;
}

void timeseries_set_log_hook(timeseries_log_hook_t hook) {
    s_tsdb.log_hook = hook;
}

// Compaction task: loops while work remains, then self-deletes.
// Using a loop instead of re-triggering via tsdb_launch_compaction avoids
// creating a new task (4 KiB stack) before the current task's stack is freed
// by the IDLE task, which caused progressive heap exhaustion.
static _Atomic uint32_t s_compaction_consecutive_failures = 0;
#define COMPACTION_MAX_CONSECUTIVE_FAILURES 3

static void tsdb_compaction_oneshot_task(void *param) {
    timeseries_db_t *db = (timeseries_db_t *)param;

    // The launcher publishes the handle before allowing this task to finish.
    ulTaskNotifyTake(pdTRUE, portMAX_DELAY);

    // Fire the pre-compact hook once, before any work (e.g. hold a sleep lock
    // for the whole compaction, including any re-runs in the loop below).
    if (db->pre_compact_hook) {
        db->pre_compact_hook();
    }

    for (;;) {
        ESP_LOGI(TAG, "Compaction task started");
        uint32_t l0_before = atomic_load(&db->l0_page_count);
        int slot = tsdb_epoch_enter(db);
        // Clear the oldest data first when space is low (db->space): a
        // compaction writes its outputs before it frees its sources.
        (void)tsdb_clear_space_held(db);
        bool success = timeseries_compact_all_levels(db);
        tsdb_usable_free(db);
        tsdb_epoch_exit(db, slot);
        atomic_fetch_add(&db->compaction_generation, 1);
        ESP_LOGI(TAG, "Compaction task finished");

        if (!success) {
            uint32_t failures = atomic_fetch_add(&s_compaction_consecutive_failures, 1) + 1;
            if (failures >= COMPACTION_MAX_CONSECUTIVE_FAILURES) {
                ESP_LOGW(TAG, "Compaction failed %" PRIu32 " consecutive times, backing off", failures);
                break;
            }
        } else {
            atomic_store(&s_compaction_consecutive_failures, 0);
        }

        // Re-check L0 count — inserts during compaction may have accumulated
        // new pages. A run that reduced none (no room: timeseries_compaction.c)
        // is not repeated; the next insert that needs space launches one.
        uint32_t l0_count = atomic_load(&db->l0_page_count);
        if (!atomic_load(&db->initialized) || l0_count < MIN_PAGES_FOR_COMPACTION || l0_count >= l0_before) {
            break;
        }
        ESP_LOGI(TAG, "Re-running compaction: %" PRIu32 " L0 pages accumulated", l0_count);
    }

    UBaseType_t hwm = uxTaskGetStackHighWaterMark(NULL);
    ESP_LOGI(TAG, "Compaction task stack high water mark: %u bytes free", (unsigned int)(hwm * sizeof(StackType_t)));

    atomic_store(&db->compaction_task_handle, (TaskHandle_t)NULL);
    // Fire the post-compact hook once, after all compaction work is done
    // (paired with pre_compact_hook above — e.g. release the sleep lock).
    if (db->post_compact_hook) {
        db->post_compact_hook();
    }
    xSemaphoreGive(db->compaction_done_sem);
    atomic_store(&db->compaction_in_progress, false);
    // Last access to database resources, including the post hook and semaphore.
    bool held = true;
    tsdb_operation_release(&held);
    vTaskDelete(NULL);
}

bool tsdb_launch_compaction(timeseries_db_t *db) {
    if (db != &s_tsdb) return false; // the public database is a singleton
    TSDB_OPERATION_GUARD(false);
    bool expected = false;
    if (!atomic_compare_exchange_strong(&db->compaction_in_progress, &expected, true)) {
        return false;  // already running
    }
    // Transfer a separate lifetime reference to the worker before creation.
    bool worker_held = tsdb_operation_acquire();
    if (!worker_held) {
        atomic_store(&db->compaction_in_progress, false);
        return false;
    }
    TaskHandle_t handle = NULL;
    BaseType_t ret = xTaskCreate(tsdb_compaction_oneshot_task, "tsdb_compact",
                                  TSDB_COMPACTION_TASK_STACK_SIZE, db, tskIDLE_PRIORITY + 1,
                                  &handle);
    if (ret != pdPASS) {
        tsdb_operation_release(&worker_held);
        ESP_LOGW(TAG, "Failed to create compaction task");
        atomic_store(&db->compaction_in_progress, false);
        return false;
    }
    atomic_store(&db->compaction_task_handle, handle);
    xTaskNotifyGive(handle);
    return true;
}

// Init and teardown share s_lifecycle_busy, including teardown's wait
// interval. False when another lifecycle call holds it or a timed-out
// teardown is still pending.
static bool lifecycle_enter(void) {
  bool expected = false;
  if (!atomic_compare_exchange_strong(&s_lifecycle_busy, &expected, true)) return false;
  if (s_cleanup_pending) {
    ESP_LOGW(TAG, "Teardown is pending; call timeseries_deinit again after active work finishes");
    atomic_store(&s_lifecycle_busy, false);
    return false;
  }
  return true;
}

static const esp_partition_t *find_partition(void) {
  return esp_partition_find_first(0x40, ESP_PARTITION_SUBTYPE_ANY, "timeseries");
}

typedef enum { TSDB_INIT_OPENED, TSDB_INIT_NO_PARTITION, TSDB_INIT_FAILED } tsdb_init_outcome_t;

// One init attempt; the caller holds s_lifecycle_busy. On TSDB_INIT_FAILED
// everything the attempt set up is released and s_tsdb.init_faults says why
// (see timeseries_repair.h).
static tsdb_init_outcome_t init_locked(void) {
  if (s_tsdb.initialized) return TSDB_INIT_OPENED;
  tsdb_init_faults_reset(&s_tsdb);
  s_tsdb.next_measurement_id = 1;  // restored from metadata below

  ESP_LOGV(TAG, "Initializing Timeseries DB...");

  const esp_partition_t* part = find_partition();
  if (!part) {
    ESP_LOGE(TAG, "Failed to find 'timeseries' partition in partition table.");
    return TSDB_INIT_NO_PARTITION;
  }
  s_tsdb.partition = part;

  // Create synchronization primitives
  s_tsdb.snapshot_mutex = xSemaphoreCreateMutex();
  s_tsdb.flash_write_mutex = xSemaphoreCreateMutex();
  s_tsdb.region_alloc_mutex = xSemaphoreCreateMutex();
  if (!s_tsdb.snapshot_mutex || !s_tsdb.flash_write_mutex || !s_tsdb.region_alloc_mutex) {
    ESP_LOGE(TAG, "Failed to create synchronization primitives");
    goto init_fail;
  }
  s_tsdb.active_batch = NULL;
  s_tsdb.compaction_claimed_count = 0;
  if (!tsdb_epoch_init(&s_tsdb)) {
    ESP_LOGE(TAG, "Failed to allocate the retired-region table");
    goto init_fail;
  }

  // Create initial empty snapshot (tsdb_load_pages_into_memory will swap in the real one)
  s_tsdb.current_snapshot = tsdb_snapshot_create(8);
  if (!s_tsdb.current_snapshot) {
    ESP_LOGE(TAG, "Failed to create initial page cache snapshot");
    goto init_fail;
  }

  if (!tsdb_load_pages_into_memory(&s_tsdb)) {
    ESP_LOGE(TAG, "Failed to load existing pages from flash.");
    goto init_fail;
  }

  // Readable stored content never fails startup. Neutralise records torn by
  // an interrupted write first, so the revision scan, inserts, queries and
  // compaction all see well-formed pages. Repair fails only on OOM; the
  // revision scan also fails on a page it cannot read, which it records in
  // init_faults for timeseries_init_with_recovery to erase.
  if (!tsdb_repair_torn_records(&s_tsdb)) {
    ESP_LOGW(TAG, "Torn-record repair skipped (out of memory)");
  }
  if (!tsdb_revision_recover(&s_tsdb)) goto init_fail;

  s_tsdb.l0_usage_cache = calloc(TSDB_L0_USAGE_CACHE_SIZE, sizeof(*s_tsdb.l0_usage_cache));
  s_tsdb.last_l0_cache_valid = false;
  s_tsdb.last_l0_page_offset = 0;
  s_tsdb.last_l0_used_offset = 0;

  // Initialize L0 page count from loaded snapshot
  atomic_store(&s_tsdb.l0_page_count, (uint32_t)tsdb_count_l0_pages(&s_tsdb));

  // Restore next_measurement_id from persisted metadata. No measurements is
  // true with max 0. False means the metadata pages could not be listed (OOM
  // growing the page list, or no page-cache snapshot) or read (the page is
  // recorded in init_faults); the highest ID is then
  // unknown, and any guessed next ID could hand new measurements IDs that
  // existing ones already use, merging their data. Fail init instead: the
  // database stays closed (inserts refused) and the caller can retry.
  uint32_t max_id = 0;
  if (!tsdb_find_max_measurement_id(&s_tsdb, &max_id)) {
    ESP_LOGE(TAG, "Cannot determine the highest measurement ID (metadata page lookup failed); "
                  "refusing to open the database");
    goto init_fail;
  }
  s_tsdb.next_measurement_id = max_id + 1;
  ESP_LOGI(TAG, "Restored next_measurement_id to %" PRIu32, s_tsdb.next_measurement_id);

  // Initialize series ID cache
  if (!tsdb_cache_init(&s_tsdb)) {
    ESP_LOGW(TAG, "Failed to initialize series ID cache (continuing without cache)");
  }

  // Allocate measurement ID cache
  s_tsdb.meas_cache = (meas_cache_entry_t *)calloc(MEAS_ID_CACHE_SIZE, sizeof(meas_cache_entry_t));
  s_tsdb.meas_cache_next = 0;

  // Pre-allocate write buffer
#ifdef CONFIG_TIMESERIES_CHUNK_BUFFER_SIZE
  size_t initial_buffer_size = CONFIG_TIMESERIES_CHUNK_BUFFER_SIZE * 1024;
#else
  size_t initial_buffer_size = 16 * 1024; // Default 16KB
#endif
  s_tsdb.write_buffer = (uint8_t *)malloc(initial_buffer_size);
  if (s_tsdb.write_buffer) {
    s_tsdb.write_buffer_capacity = initial_buffer_size;
    ESP_LOGI(TAG, "Allocated %zu KB write buffer", initial_buffer_size / 1024);
  } else {
    s_tsdb.write_buffer_capacity = 0;
    ESP_LOGW(TAG, "Failed to pre-allocate write buffer (will use malloc per write)");
  }

  // Initialize chunk size (default 500 points - optimal from performance testing)
  s_tsdb.chunk_size = 500;

  tsdb_space_config_default(&s_tsdb);

  // Initialize series type cache (Phase 2 optimization)
  s_tsdb.type_cache_size = 128;  // Simple fixed-size cache
  s_tsdb.type_cache = (series_type_cache_entry_t*)calloc(s_tsdb.type_cache_size,
                                                          sizeof(series_type_cache_entry_t));
  if (s_tsdb.type_cache) {
    ESP_LOGI(TAG, "Initialized series type cache with %zu entries", s_tsdb.type_cache_size);
  } else {
    ESP_LOGW(TAG, "Failed to allocate type cache (will use direct metadata lookups)");
    s_tsdb.type_cache_size = 0;
  }
  // Guards the type cache: read by query tasks, written by insert tasks.
  s_tsdb.type_cache_mutex = xSemaphoreCreateMutex();
  if (!s_tsdb.type_cache_mutex) {
    ESP_LOGW(TAG, "Failed to create type cache mutex (continuing unlocked)");
  }

  // Initialize on-demand compaction state
  atomic_store(&s_tsdb.compaction_in_progress, false);
  atomic_store(&s_tsdb.compaction_generation, 0);
  atomic_store(&s_tsdb.compaction_task_handle, (TaskHandle_t)NULL);
  s_tsdb.compaction_done_sem = xSemaphoreCreateBinary();
  if (!s_tsdb.compaction_done_sem) {
    ESP_LOGE(TAG, "Failed to create compaction_done semaphore");
    goto init_fail;
  }

  atomic_store(&s_tsdb.reserve_exempt_task, (TaskHandle_t)NULL);
  tsdb_usable_free(&s_tsdb);  // as of the loaded page cache

  atomic_store(&s_tsdb.initialized, true);
  atomic_store(&s_operations, 0);

  ESP_LOGI(TAG, "Timeseries DB initialized successfully.");
  return TSDB_INIT_OPENED;

init_fail:
  free(s_tsdb.l0_usage_cache);
  s_tsdb.l0_usage_cache = NULL;
  tsdb_epoch_deinit(&s_tsdb);
  // Clean up any resources allocated during partial init
  if (s_tsdb.write_buffer) {
    free(s_tsdb.write_buffer);
    s_tsdb.write_buffer = NULL;
    s_tsdb.write_buffer_capacity = 0;
  }
  if (s_tsdb.meas_cache) {
    free(s_tsdb.meas_cache);
    s_tsdb.meas_cache = NULL;
  }
  if (s_tsdb.type_cache) {
    free(s_tsdb.type_cache);
    s_tsdb.type_cache = NULL;
    s_tsdb.type_cache_size = 0;
  }
  if (s_tsdb.type_cache_mutex) {
    vSemaphoreDelete(s_tsdb.type_cache_mutex);
    s_tsdb.type_cache_mutex = NULL;
  }
  tsdb_cache_free(&s_tsdb);
  if (s_tsdb.current_snapshot) {
    tsdb_snapshot_release(s_tsdb.current_snapshot);
    s_tsdb.current_snapshot = NULL;
  }
  if (s_tsdb.compaction_done_sem) {
    vSemaphoreDelete(s_tsdb.compaction_done_sem);
    s_tsdb.compaction_done_sem = NULL;
  }
  if (s_tsdb.region_alloc_mutex) {
    vSemaphoreDelete(s_tsdb.region_alloc_mutex);
    s_tsdb.region_alloc_mutex = NULL;
  }
  if (s_tsdb.flash_write_mutex) {
    vSemaphoreDelete(s_tsdb.flash_write_mutex);
    s_tsdb.flash_write_mutex = NULL;
  }
  if (s_tsdb.snapshot_mutex) {
    vSemaphoreDelete(s_tsdb.snapshot_mutex);
    s_tsdb.snapshot_mutex = NULL;
  }
  s_tsdb.partition = NULL;
  return TSDB_INIT_FAILED;
}

bool timeseries_init(void) {
  if (!lifecycle_enter()) return false;
  bool opened = init_locked() == TSDB_INIT_OPENED;
  atomic_store(&s_lifecycle_busy, false);
  return opened;
}

// ---------------------------------------------------------------------------
// Recovery ladder (timeseries_init_with_recovery)
//
// Each init attempt records why it failed (db->init_faults, see
// timeseries_repair.h): stored content that could not be read names its page;
// a failure that says nothing about stored content (allocation, no snapshot)
// is transient. An attempt that failed without naming a page is transient
// too: erasing needs positive evidence of bad content.
//
//   transient    -> retry a few times after a short pause; never erase.
//   bad pages    -> erase exactly those pages and retry (a data page holds
//                   records keyed by series ID, so no metadata points into
//                   it; a metadata page's loss is covered by the measurement
//                   ID floor in tsdb_find_max_measurement_id). Bounded rounds.
//   still bad    -> a page that is bad again after its erase, a page erase
//                   that fails, or too many rounds: erase the partition.
//   still bad    -> give up (flash fault); the database stays closed.
// ---------------------------------------------------------------------------
#define TSDB_RECOVERY_TRANSIENT_DELAY_MS 100
#define TSDB_RECOVERY_PAGE_ROUNDS 3

static bool erased_before(const uint32_t *erased, size_t count, uint32_t offset) {
  for (size_t i = 0; i < count; i++)
    if (erased[i] == offset) return true;
  return false;
}

bool timeseries_init_with_recovery(timeseries_recovery_report_t *out) {
  timeseries_recovery_report_t report = {.status = TIMESERIES_INIT_ERR_BUSY};
  if (!lifecycle_enter()) {
    if (out) *out = report;
    return false;
  }
  uint32_t erased[TSDB_RECOVERY_PAGE_ROUNDS * TSDB_INIT_MAX_BAD_PAGES];
  size_t erased_count = 0;
  unsigned transient_retries = TIMESERIES_RECOVERY_TRANSIENT_ATTEMPTS - 1;
  unsigned rounds = 0;
  for (;;) {
    report.attempts++;
    tsdb_init_outcome_t outcome = init_locked();
    if (outcome == TSDB_INIT_OPENED) {
      report.status = TIMESERIES_INIT_OK;
      break;
    }
    if (outcome == TSDB_INIT_NO_PARTITION) {
      report.status = TIMESERIES_INIT_ERR_NO_PARTITION;
      break;
    }
    const tsdb_init_faults_t *faults = &s_tsdb.init_faults;
    if (faults->transient || faults->count == 0) {
      if (!transient_retries) {
        ESP_LOGE(TAG, "Recovery: init failed %" PRIu32 " time(s) for a transient reason (out of memory?); "
                      "nothing erased", report.attempts);
        report.status = TIMESERIES_INIT_ERR_TRANSIENT;
        break;
      }
      transient_retries--;
      ESP_LOGW(TAG, "Recovery: init attempt %" PRIu32 " failed for a transient reason; retrying", report.attempts);
      vTaskDelay(pdMS_TO_TICKS(TSDB_RECOVERY_TRANSIENT_DELAY_MS));
      continue;
    }
    // Stored content could not be read.
    if (report.action == TIMESERIES_RECOVERY_PARTITION_ERASED) {
      ESP_LOGE(TAG, "Recovery: a freshly erased partition still cannot be read (flash fault?); giving up");
      report.status = TIMESERIES_INIT_ERR_CORRUPT;
      break;
    }
    const esp_partition_t *part = find_partition();
    if (!part) {
      report.status = TIMESERIES_INIT_ERR_NO_PARTITION;
      break;
    }
    bool full = rounds >= TSDB_RECOVERY_PAGE_ROUNDS;
    for (size_t i = 0; !full && i < faults->count; i++)
      full = erased_before(erased, erased_count, faults->pages[i].offset);
    if (!full) {
      rounds++;
      ESP_LOGE(TAG, "Recovery round %u: erasing %u unreadable page(s)%s", rounds, (unsigned)faults->count,
               faults->overflow ? " (more were found; a later round takes them)" : "");
      for (size_t i = 0; !full && i < faults->count; i++) {
        uint32_t offset = faults->pages[i].offset, size = faults->pages[i].size;
        esp_err_t err = esp_partition_erase_range(part, offset, size);
        if (err != ESP_OK) {
          ESP_LOGE(TAG, "Recovery: erasing page @0x%08" PRIx32 " (+0x%" PRIx32 ") failed: %s", offset, size,
                   esp_err_to_name(err));
          full = true;
          break;
        }
        ESP_LOGW(TAG, "Recovery: erased page @0x%08" PRIx32 " (+0x%" PRIx32 ")", offset, size);
        erased[erased_count++] = offset;
        report.pages_erased++;
        report.action = TIMESERIES_RECOVERY_PAGES_ERASED;
      }
      if (!full) continue;
    }
    ESP_LOGE(TAG, "Recovery: erasing the whole partition (%" PRIu32 " page(s) erased before)", report.pages_erased);
    esp_err_t wipe = esp_partition_erase_range(part, 0, part->size);
    if (wipe != ESP_OK) {
      ESP_LOGE(TAG, "Recovery: partition erase failed: %s; database stays closed", esp_err_to_name(wipe));
      report.status = TIMESERIES_INIT_ERR_FLASH;
      break;
    }
    report.action = TIMESERIES_RECOVERY_PARTITION_ERASED;
  }
  if (report.status == TIMESERIES_INIT_OK && report.action != TIMESERIES_RECOVERY_NONE) {
    ESP_LOGW(TAG, "Recovery: database opened after %s (%" PRIu32 " page(s) erased)",
             report.action == TIMESERIES_RECOVERY_PARTITION_ERASED ? "a partition erase" : "erasing pages",
             report.pages_erased);
  }
  atomic_store(&s_lifecycle_busy, false);
  if (out) *out = report;
  return report.status == TIMESERIES_INIT_OK;
}

bool timeseries_erase_partition(void) {
  if (!lifecycle_enter()) return false;
  bool ok = false;
  if (s_tsdb.initialized) {
    ESP_LOGW(TAG, "erase_partition: the database is open; use timeseries_clear_all");
  } else {
    const esp_partition_t *part = find_partition();
    esp_err_t err = part ? esp_partition_erase_range(part, 0, part->size) : ESP_ERR_NOT_FOUND;
    ok = err == ESP_OK;
    if (!ok) ESP_LOGE(TAG, "erase_partition failed: %s", esp_err_to_name(err));
  }
  atomic_store(&s_lifecycle_busy, false);
  return ok;
}

static bool insert_ordered(const timeseries_insert_data_t *data, const tsdb_point_order_t *order);

bool timeseries_insert(const timeseries_insert_data_t* data) {
  TSDB_OPERATION_GUARD(false);
  if (!data) {
    ESP_LOGE(TAG, "timeseries_insert data is NULL.");
    return false;
  }
  if (!data->measurement_name || !data->field_names || !data->timestamps_ms || !data->field_values) {
    ESP_LOGE(TAG, "timeseries_insert: required field is NULL.");
    return false;
  }
  // Series ids are "measurement:tagkey:tagvalue:...:field"; a ':' in the
  // measurement name would let two different measurements share a series.
  if (strchr(data->measurement_name, ':') != NULL) {
    ESP_LOGE(TAG, "timeseries_insert: measurement name '%s' contains ':'", data->measurement_name);
    return false;
  }
  if (data->num_points == 0) {
    ESP_LOGW(TAG, "No data points provided, nothing to store.");
    return true;
  }

  if (data->num_fields > SIZE_MAX / data->num_points ||
      data->num_points > SIZE_MAX / sizeof(*data->timestamps_ms) ||
      (data->num_tags && (!data->tag_keys || !data->tag_values))) return false;
  for (size_t t = 0; t < data->num_tags; t++) {
    if (!data->tag_keys[t] || !data->tag_values[t]) return false;
  }
  for (size_t f = 0; f < data->num_fields; f++) {
    if (!data->field_names[f]) return false;
  }
  if (!tsdb_validate_point_values(data->field_values,
                                  data->num_fields * data->num_points)) return false;

  tsdb_point_order_t order;
  if (!tsdb_point_order_init(data->timestamps_ms, data->num_points, &order)) return false;
  bool ok = insert_ordered(data, &order);
  tsdb_point_order_deinit(&order);
  return ok;
}

static bool insert_ordered_once(const timeseries_insert_data_t *data, const tsdb_point_order_t *order,
                                size_t *resume, bool *any_field_failed);

// The measurement id, series ids and types used below come partly from caches
// read without flash_write_mutex. A delete that completes in between removes
// that metadata; the append then reports the schema change and the whole
// insert is redone, so it is ordered entirely after the delete (it re-creates
// the metadata it needs) instead of writing records no metadata describes.
// Fields handled before the restart point are written again only if the
// delete removed them (their series has no type metadata any more); the hot
// path (no race) does no extra work and no allocation.
static bool insert_ordered(const timeseries_insert_data_t *data, const tsdb_point_order_t *order) {
  size_t resume = 0;  // fields [0, resume) were handled by an earlier attempt
  bool any_field_failed = false;
  for (int attempt = 0; attempt < 4; attempt++) {
    if (insert_ordered_once(data, order, &resume, &any_field_failed)) return !any_field_failed;
    if (resume == SIZE_MAX) return false;  // failed, not stale
  }
  ESP_LOGW(TAG, "Insert into '%s' kept racing deletes; giving up", data->measurement_name);
  return false;
}

// Returns true when the insert ran to the end (per-field failures are
// reported through *any_field_failed). Returns false with *resume set to the
// field to restart from after a racing delete, or to SIZE_MAX on failure.
static bool insert_ordered_once(const timeseries_insert_data_t *data, const tsdb_point_order_t *order,
                                size_t *resume, bool *any_field_failed_out) {
  const size_t handled = *resume;
  *resume = SIZE_MAX;
  const uint32_t schema_gen = atomic_load(&s_tsdb.schema_gen);
  // Pre-compute the common prefix (measurement + tags) using memcpy.
  char prefix_buffer[256];
  size_t meas_len = strlen(data->measurement_name);
  if (meas_len >= sizeof(prefix_buffer)) {
    ESP_LOGE(TAG, "Measurement name too long for prefix buffer");
    return false;
  }
  memcpy(prefix_buffer, data->measurement_name, meas_len);
  size_t prefix_len = meas_len;
  for (size_t t = 0; t < data->num_tags; t++) {
    size_t tk_len = strlen(data->tag_keys[t]);
    size_t tv_len = strlen(data->tag_values[t]);
    size_t need = 1 + tk_len + 1 + tv_len; // ":key:val"
    if (prefix_len + need >= sizeof(prefix_buffer)) {
      ESP_LOGE(TAG, "Prefix buffer overflow");
      return false;
    }
    prefix_buffer[prefix_len++] = ':';
    memcpy(prefix_buffer + prefix_len, data->tag_keys[t], tk_len);
    prefix_len += tk_len;
    prefix_buffer[prefix_len++] = ':';
    memcpy(prefix_buffer + prefix_len, data->tag_values[t], tv_len);
    prefix_len += tv_len;
  }

  uint64_t start_time = esp_timer_get_time();

  // 1) Find or create measurement ID.
  //    The meas_cache (and meas_cache_next round-robin index) is shared across
  //    concurrent insert tasks, so all access is serialized under flash_write_mutex
  //    to prevent torn reads/writes.  On a cache hit the mutex is held only for
  //    the brief 16-entry scan; on a miss the mutex is already needed for the
  //    flash lookup / create that follows.
  uint32_t measurement_id = 0;
  bool meas_cached = false;

  if (s_tsdb.flash_write_mutex) {
    tsdb_write_lock(&s_tsdb);
  }

  meas_cached = tsdb_meas_cache_lookup(&s_tsdb, data->measurement_name, &measurement_id);
  if (!meas_cached) {
    if (!tsdb_find_measurement_id(&s_tsdb, data->measurement_name, &measurement_id)) {
      if (!tsdb_create_measurement_id(&s_tsdb, data->measurement_name, &measurement_id)) {
        if (s_tsdb.flash_write_mutex) {
          xSemaphoreGive(s_tsdb.flash_write_mutex);
        }
        return false;
      }
    }
    tsdb_meas_cache_store(&s_tsdb, data->measurement_name, measurement_id);
  }

  if (s_tsdb.flash_write_mutex) {
    xSemaphoreGive(s_tsdb.flash_write_mutex);
  }

  // Log if this is a large insert
  if (data->num_points > 500) {
    ESP_LOGI(TAG, "Starting large insert: %zu points x %zu fields = %zu total values",
             data->num_points, data->num_fields, data->num_points * data->num_fields);
  }

  // For each field i => build the series_id => store all points in one shot
  bool any_field_failed = *any_field_failed_out;
  for (size_t i = 0; i < data->num_fields; i++) {
    // Build the cache key string using memcpy
    char cache_key[256];
    size_t fn_len = strlen(data->field_names[i]);
    size_t key_len = prefix_len + 1 + fn_len; // "prefix:field"
    if (key_len >= sizeof(cache_key)) {
      ESP_LOGE(TAG, "Cache key buffer overflow for field '%s'", data->field_names[i]);
      return false;
    }
    memcpy(cache_key, prefix_buffer, prefix_len);
    cache_key[prefix_len] = ':';
    memcpy(cache_key + prefix_len + 1, data->field_names[i], fn_len);
    cache_key[key_len] = '\0';

    unsigned char series_id[16];
    bool found_in_cache = false;

    // Try to get series_id from cache first
    if (tsdb_cache_lookup_series_id(&s_tsdb, cache_key, series_id)) {
      found_in_cache = true;
      ESP_LOGD(TAG, "Cache HIT for field '%s' - skipping metadata operations", data->field_names[i]);
    } else {
      // Cache miss - compute MD5
      mbedtls_md(mbedtls_md_info_from_type(MBEDTLS_MD_MD5), (const unsigned char*)cache_key, key_len, series_id);
      ESP_LOGD(TAG, "Cache MISS for field '%s' - performing metadata operations", data->field_names[i]);
    }

    if (i < handled) {
      // Handled by an earlier attempt. Still described by metadata means the
      // delete that restarted this insert did not remove it.
      timeseries_field_type_e kept;
      if (tsdb_lookup_series_type_cached(&s_tsdb, series_id, &kept)) continue;
      found_in_cache = false;
    }

    // Only do metadata operations if not found in cache
    if (!found_in_cache) {
      // Protect metadata flash writes from concurrent compaction
      if (s_tsdb.flash_write_mutex) {
        tsdb_write_lock(&s_tsdb);
      }
      // measurement_id was resolved before; a delete since then removed it.
      if (atomic_load(&s_tsdb.schema_gen) != schema_gen) {
        if (s_tsdb.flash_write_mutex) xSemaphoreGive(s_tsdb.flash_write_mutex);
        *resume = i;
        *any_field_failed_out = any_field_failed;
        return false;
      }

      // Ensure the field name to series_id mapping is in metadata
      if (!tsdb_index_field_for_series(&s_tsdb, measurement_id, data->field_names[i], series_id)) {
        ESP_LOGE(TAG, "Failed to index field '%s'", data->field_names[i]);
        if (s_tsdb.flash_write_mutex) {
          xSemaphoreGive(s_tsdb.flash_write_mutex);
        }
        return false;
      }

      // Ensure field type in metadata
      const timeseries_field_value_t* first_val = &data->field_values[i * data->num_points + 0];
      if (!tsdb_ensure_series_type_in_metadata(&s_tsdb, series_id, first_val->type)) {
        if (s_tsdb.flash_write_mutex) {
          xSemaphoreGive(s_tsdb.flash_write_mutex);
        }
        return false;
      }

      // Index tags
      if (!tsdb_index_tags_for_series(&s_tsdb, measurement_id, data->tag_keys, data->tag_values, data->num_tags,
                                      series_id)) {
        if (s_tsdb.flash_write_mutex) {
          xSemaphoreGive(s_tsdb.flash_write_mutex);
        }
        return false;
      }

      if (s_tsdb.flash_write_mutex) {
        xSemaphoreGive(s_tsdb.flash_write_mutex);
      }

      // Successfully indexed - add to cache for next time
      tsdb_cache_insert_series_id(&s_tsdb, cache_key, series_id);
    }

    // Gather the array of values for this field
    const timeseries_field_value_t* field_array = &data->field_values[i * data->num_points];

    // A series' type is immutable, but the metadata check above only runs on
    // a series-id cache miss and only inspects the first value. Re-validate on
    // every insert and check every point, so a type change or a mixed-type
    // batch can't write values that disagree with the metadata type (readers
    // size each value from the metadata type, so a mismatched record is
    // misparsed on read/compaction). Usually a RAM type-cache hit, but a cache
    // eviction makes it a metadata scan.
    //
    // A mismatch fails the whole field rather than dropping the offending
    // points: partial acceptance while still returning success would hide lost
    // writes from the caller, and the cache-miss path above already hard-fails
    // a type conflict.
    timeseries_field_type_e expected_type;
    if (tsdb_lookup_series_type_cached(&s_tsdb, series_id, &expected_type)) {
      size_t mismatched = 0;
      for (size_t p = 0; p < data->num_points; p++) {
        if (field_array[p].type != expected_type) {
          mismatched++;
        }
      }
      if (mismatched > 0) {
        // Tick count, not esp_timer_get_time(): inserts arrive from more than
        // one task and 64-bit loads/stores are not atomic on Xtensa.
        static uint32_t s_last_type_warn_tick = 0;
        uint32_t now_tick = (uint32_t)xTaskGetTickCount();
        if (now_tick - s_last_type_warn_tick > pdMS_TO_TICKS(10000)) {
          s_last_type_warn_tick = now_tick;
          ESP_LOGW(TAG,
                   "Field '%s': %zu of %zu point(s) do not match series type=%d, "
                   "skipping field (a series' type is immutable)",
                   data->field_names[i], mismatched, data->num_points, (int)expected_type);
        }
        any_field_failed = true;
        continue;
      }
    }

    // 2) Insert multi data points in one entry
    ESP_LOGD(TAG, "Inserting field %zu/%zu: '%s' (%zu points)",
             i + 1, data->num_fields, data->field_names[i], data->num_points);
    bool field_stale = false;
    bool field_ok = tsdb_append_ordered_points_checked(&s_tsdb, series_id, data->timestamps_ms, field_array, order,
                                                       &schema_gen, &field_stale);
    if (field_stale) {
      *resume = i;
      *any_field_failed_out = any_field_failed;
      return false;
    }
    if (!field_ok) {
      ESP_LOGE(TAG, "Failed to insert multi points for field '%s'", data->field_names[i]);
      any_field_failed = true;
      continue;  // best-effort: try remaining fields to minimize inconsistency
    }
    ESP_LOGV(TAG, "Successfully inserted field='%s' with %zu points for measurement=%s",
             data->field_names[i], data->num_points, data->measurement_name);
  }

  uint64_t end_time = esp_timer_get_time();
  ESP_LOGD(TAG, "Inserted %zu points in %zu fields for measurement '%s' in %.3f ms", data->num_points, data->num_fields,
           data->measurement_name, (end_time - start_time) / 1000.0);

  // Auto-trigger background maintenance when L0 page count is high enough,
  // or to clear the oldest data when free space runs low.
  if (!any_field_failed && !atomic_load(&s_tsdb.compaction_in_progress)) {
    uint32_t l0_count = atomic_load(&s_tsdb.l0_page_count);
    if (l0_count >= MIN_PAGES_FOR_COMPACTION) {
      ESP_LOGI(TAG, "Auto-triggering compaction: %" PRIu32 " L0 pages", l0_count);
      tsdb_launch_compaction(&s_tsdb);
    } else if (tsdb_clear_space_needed(&s_tsdb)) {
      ESP_LOGI(TAG, "Auto-triggering maintenance: %" PRIu32 " KiB usable free",
               atomic_load(&s_tsdb.usable_free) / 1024);
      tsdb_launch_compaction(&s_tsdb);
    }
  }

  *any_field_failed_out = any_field_failed;
  return true;
}

bool timeseries_compact(void) {
  TSDB_OPERATION_GUARD(false);
  tsdb_launch_compaction(&s_tsdb);
  return true;
}

bool timeseries_compact_sync(void) {
  TSDB_OPERATION_GUARD(false);
  // Wait for any in-progress compaction to finish
  int timeout_ms = 60000;
  tsdb_epoch_idle(&s_tsdb, true);  // waits holding no page offsets
  while (atomic_load(&s_tsdb.compaction_in_progress) && timeout_ms > 0) {
    vTaskDelay(pdMS_TO_TICKS(10));
    timeout_ms -= 10;
  }
  tsdb_epoch_idle(&s_tsdb, false);
  if (timeout_ms <= 0) {
    return false;
  }
  // Claim the slot and run synchronously
  bool expected = false;
  if (!atomic_compare_exchange_strong(&s_tsdb.compaction_in_progress, &expected, true)) {
    // Race: someone else claimed it. Wait for them.
    tsdb_epoch_idle(&s_tsdb, true);
    while (atomic_load(&s_tsdb.compaction_in_progress) && timeout_ms > 0) {
      vTaskDelay(pdMS_TO_TICKS(10));
      timeout_ms -= 10;
    }
    tsdb_epoch_idle(&s_tsdb, false);
    return timeout_ms > 0;
  }
  bool ok = timeseries_compact_all_levels(&s_tsdb);
  atomic_fetch_add(&s_tsdb.compaction_generation, 1);
  atomic_store(&s_tsdb.compaction_in_progress, false);
  return ok;
}

bool timeseries_compact_force_sync(void) {
  TSDB_OPERATION_GUARD(false);
  int timeout_ms = 60000;
  tsdb_epoch_idle(&s_tsdb, true);  // waits holding no page offsets
  while (atomic_load(&s_tsdb.compaction_in_progress) && timeout_ms > 0) {
    vTaskDelay(pdMS_TO_TICKS(10));
    timeout_ms -= 10;
  }
  tsdb_epoch_idle(&s_tsdb, false);
  if (timeout_ms <= 0) {
    return false;
  }
  bool expected = false;
  if (!atomic_compare_exchange_strong(&s_tsdb.compaction_in_progress, &expected, true)) {
    tsdb_epoch_idle(&s_tsdb, true);
    while (atomic_load(&s_tsdb.compaction_in_progress) && timeout_ms > 0) {
      vTaskDelay(pdMS_TO_TICKS(10));
      timeout_ms -= 10;
    }
    tsdb_epoch_idle(&s_tsdb, false);
    return timeout_ms > 0;
  }
  bool ok = timeseries_compact_all_levels_force(&s_tsdb);
  atomic_fetch_add(&s_tsdb.compaction_generation, 1);
  atomic_store(&s_tsdb.compaction_in_progress, false);
  return ok;
}

void timeseries_deinit(void) {
  bool expected = false;
  if (!atomic_compare_exchange_strong(&s_lifecycle_busy, &expected, true)) return;
  if (!atomic_load(&s_tsdb.initialized) && !s_cleanup_pending) {
    atomic_store(&s_lifecycle_busy, false);
    return;
  }

  atomic_fetch_or(&s_operations, TSDB_CLOSED);
  atomic_store(&s_tsdb.initialized, false);
  s_cleanup_pending = true;
  // Completion semaphores may contain a token from an older worker and do not
  // cover synchronous operations. Only the admission count proves quiescence.
  TickType_t poll_ticks = pdMS_TO_TICKS(10);
  if (!poll_ticks) poll_ticks = 1;
  TickType_t remaining = pdMS_TO_TICKS(30000);
  while (atomic_load(&s_operations) != TSDB_CLOSED) {
    if (!remaining) {
      ESP_LOGW(TAG, "Timed out waiting for active operations; teardown remains pending");
      atomic_store(&s_lifecycle_busy, false);
      return;
    }
    TickType_t pause = remaining < poll_ticks ? remaining : poll_ticks;
    vTaskDelay(pause);
    remaining -= pause;
  }

  // Delete compaction semaphore
  if (s_tsdb.compaction_done_sem) {
    vSemaphoreDelete(s_tsdb.compaction_done_sem);
    s_tsdb.compaction_done_sem = NULL;
  }

  // Delete synchronization primitives
  if (s_tsdb.flash_write_mutex) {
    vSemaphoreDelete(s_tsdb.flash_write_mutex);
    s_tsdb.flash_write_mutex = NULL;
  }
  if (s_tsdb.region_alloc_mutex) {
    vSemaphoreDelete(s_tsdb.region_alloc_mutex);
    s_tsdb.region_alloc_mutex = NULL;
  }

  // Release current snapshot
  if (s_tsdb.current_snapshot) {
    tsdb_snapshot_release(s_tsdb.current_snapshot);
    s_tsdb.current_snapshot = NULL;
  }

  if (s_tsdb.snapshot_mutex) {
    vSemaphoreDelete(s_tsdb.snapshot_mutex);
    s_tsdb.snapshot_mutex = NULL;
  }

  // Free write buffer
  if (s_tsdb.write_buffer) {
    free(s_tsdb.write_buffer);
    s_tsdb.write_buffer = NULL;
    s_tsdb.write_buffer_capacity = 0;
  }

  // Free measurement ID cache
  if (s_tsdb.meas_cache) {
    free(s_tsdb.meas_cache);
    s_tsdb.meas_cache = NULL;
  }

  free(s_tsdb.l0_usage_cache);
  s_tsdb.l0_usage_cache = NULL;
  tsdb_epoch_deinit(&s_tsdb);

  // Free type cache
  if (s_tsdb.type_cache) {
    free(s_tsdb.type_cache);
    s_tsdb.type_cache = NULL;
    s_tsdb.type_cache_size = 0;
  }
  if (s_tsdb.type_cache_mutex) {
    vSemaphoreDelete(s_tsdb.type_cache_mutex);
    s_tsdb.type_cache_mutex = NULL;
  }

  tsdb_cache_free(&s_tsdb);

  atomic_store(&s_compaction_consecutive_failures, 0);
  atomic_store(&s_tsdb.compaction_in_progress, false);
  s_tsdb.partition = NULL;
  s_cleanup_pending = false;
  atomic_store(&s_lifecycle_busy, false);
  ESP_LOGI(TAG, "Timeseries DB deinitialized.");
}

bool timeseries_expire(void) {
  TSDB_OPERATION_GUARD(false);

  ESP_LOGV(TAG, "Starting expiration...");

  if (!timeseries_expiration_run(&s_tsdb, 0.5, 0.05)) {
    ESP_LOGE(TAG, "Expiration failed.");
    return false;
  }

  ESP_LOGV(TAG, "Expiration complete.");
  return true;
}

bool timeseries_query(const timeseries_query_t* query, timeseries_query_result_t* result) {
  TSDB_OPERATION_GUARD(false);
  return timeseries_query_execute(&s_tsdb, query, result);
}

void timeseries_query_free_result(timeseries_query_result_t* result) {
  if (!result) {
    return;  // nothing to do
  }

  // 1) Free the timestamps array
  if (result->timestamps) {
    free(result->timestamps);
    result->timestamps = NULL;
  }

  // 2) Free each column
  if (result->columns) {
    for (size_t col = 0; col < result->num_columns; col++) {
      timeseries_query_result_column_t* c = &result->columns[col];

      // Free the column name
      if (c->name) {
        free(c->name);
        c->name = NULL;
      }

      // Free each value in this column (including string data, if any)
      if (c->values) {
        for (size_t row = 0; row < result->num_points; row++) {
          // If it's a string, free the string buffer
          if (c->values[row].type == TIMESERIES_FIELD_TYPE_STRING) {
            // Only free if you know it was dynamically allocated
            if (c->values[row].data.string_val.str) {
              free((void*)c->values[row].data.string_val.str);
            }
          }
        }
        free(c->values);
        c->values = NULL;
      }
    }

    // 3) Finally, free the columns array itself
    free(result->columns);
    result->columns = NULL;
  }

  // 4) Reset the numeric fields
  result->num_points = 0;
  result->num_columns = 0;
}

bool timeseries_clear_all(void) {
  TSDB_OPERATION_GUARD(false);

  // Claim the slot before erase so compaction/expiration cannot start between
  // the wait and the flash lock. Never erase under an owner that timed out.
  int timeout_ms = 60000;
  tsdb_epoch_idle(&s_tsdb, true);  // waits holding no page offsets
  for (;;) {
    bool expected = false;
    if (atomic_compare_exchange_strong(&s_tsdb.compaction_in_progress, &expected, true)) break;
    if (timeout_ms <= 0) {
      tsdb_epoch_idle(&s_tsdb, false);
      ESP_LOGW(TAG, "Timed out waiting for compaction before clear_all");
      return false;
    }
    vTaskDelay(pdMS_TO_TICKS(10));
    timeout_ms -= 10;
  }
  tsdb_epoch_idle(&s_tsdb, false);

  // Hold flash_write_mutex to prevent concurrent inserts during erase
  if (s_tsdb.flash_write_mutex) {
    tsdb_write_lock(&s_tsdb);
  }

  atomic_fetch_add(&s_tsdb.schema_gen, 1);
  // Retire every page, then wait until no operation that could still read
  // one of them is running (operations blocked on flash_write_mutex or on the
  // maintenance slot hold no page offsets and do not count), then erase.
  // Readers without a lock are short, so a timeout means something is wrong:
  // fail without erasing rather than erase under a reader.
  tsdb_pagecache_clear(&s_tsdb);
  esp_err_t err = ESP_ERR_TIMEOUT;
  if (tsdb_epoch_synchronize(&s_tsdb, TSDB_CLEAR_READER_WAIT_MS)) {
    err = esp_partition_erase_range(s_tsdb.partition, 0, s_tsdb.partition->size);
  } else {
    ESP_LOGW(TAG, "clear_all: readers still active after %d ms; not erasing", TSDB_CLEAR_READER_WAIT_MS);
  }

  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to erase partition (err=0x%x)", err);
    // Describe flash as it is (untouched, or partly erased) again.
    if (!tsdb_build_page_cache(&s_tsdb)) ESP_LOGE(TAG, "clear_all: page cache rebuild failed");
    tsdb_retired_forget_live(&s_tsdb);
    if (s_tsdb.flash_write_mutex) {
      xSemaphoreGive(s_tsdb.flash_write_mutex);
    }
    atomic_store(&s_tsdb.compaction_in_progress, false);
    return false;
  }
  // Nothing retired before the erase can be read any more.
  tsdb_retired_clear(&s_tsdb);

  // The erased database has no revisions to order against.
  s_tsdb.write_revision = 0;

  // Clear out any cached data
  tsdb_cache_clear(&s_tsdb);      // Clear series ID cache
  tsdb_clear_type_cache(&s_tsdb); // Clear series type cache

  if (s_tsdb.l0_usage_cache)
    memset(s_tsdb.l0_usage_cache, 0, TSDB_L0_USAGE_CACHE_SIZE * sizeof(*s_tsdb.l0_usage_cache));

  // Reset the last L0 cache
  s_tsdb.last_l0_cache_valid = false;
  s_tsdb.last_l0_page_offset = 0;
  s_tsdb.last_l0_used_offset = 0;
  s_tsdb.compaction_claimed_count = 0;
  atomic_store(&s_tsdb.l0_page_count, 0);

  // Reset measurement ID cache
  if (s_tsdb.meas_cache) {
    for (int mc = 0; mc < MEAS_ID_CACHE_SIZE; mc++) {
      s_tsdb.meas_cache[mc].valid = false;
    }
  }

  // Add back the metadata page
  bool metadata_ok = timeseries_metadata_create_page(&s_tsdb);

  if (s_tsdb.flash_write_mutex) {
    xSemaphoreGive(s_tsdb.flash_write_mutex);
  }

  atomic_store(&s_tsdb.compaction_in_progress, false);
  if (!metadata_ok) {
    ESP_LOGE(TAG, "Partition cleared but metadata page recreation failed");
    return false;
  }
  return true;
}

void timeseries_set_chunk_size(size_t chunk_size) {
  TSDB_OPERATION_GUARD();
  if (chunk_size == 0) {
    ESP_LOGW(TAG, "Invalid chunk size (0), keeping current value");
    return;
  }
  if (chunk_size > UINT16_MAX) {
    ESP_LOGW(TAG, "Chunk size %zu exceeds max (65535), clamping", chunk_size);
    chunk_size = UINT16_MAX;
  }
  s_tsdb.chunk_size = chunk_size;
  ESP_LOGI(TAG, "Chunk size set to %zu points", chunk_size);
}

timeseries_db_t* timeseries_get_db_handle(void) {
  if (!s_tsdb.initialized) {
    return NULL;
  }
  return &s_tsdb;
}

// =============================================================================
// Public API: Listing measurements, fields, tags
// =============================================================================

bool timeseries_get_measurements(char*** measurements, size_t* num_measurements) {
  TSDB_OPERATION_GUARD(false);
  if (!measurements || !num_measurements) {
    return false;
  }
  *measurements = NULL;
  *num_measurements = 0;

  timeseries_string_list_t list;
  tsdb_string_list_init(&list);

  if (!tsdb_list_all_measurements(&s_tsdb, &list)) {
    tsdb_string_list_free(&list);
    return false;
  }

  // Transfer ownership of the items array to the caller
  *measurements = list.items;
  *num_measurements = list.count;
  // Don't call tsdb_string_list_free — caller owns the strings now
  return true;
}

bool timeseries_get_fields_for_measurement(const char* measurement_name, char*** fields, size_t* num_fields) {
  TSDB_OPERATION_GUARD(false);
  if (!measurement_name || !fields || !num_fields) {
    return false;
  }
  *fields = NULL;
  *num_fields = 0;

  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(&s_tsdb, measurement_name, &measurement_id)) {
    ESP_LOGW(TAG, "Measurement '%s' not found", measurement_name);
    return false;
  }

  timeseries_string_list_t list;
  tsdb_string_list_init(&list);

  if (!tsdb_list_fields_for_measurement(&s_tsdb, measurement_id, &list)) {
    tsdb_string_list_free(&list);
    return false;
  }

  *fields = list.items;
  *num_fields = list.count;
  return true;
}

bool timeseries_get_tags_for_measurement(const char* measurement_name, tsdb_tag_pair_t** tags, size_t* num_tags) {
  TSDB_OPERATION_GUARD(false);
  if (!measurement_name || !tags || !num_tags) {
    return false;
  }
  *tags = NULL;
  *num_tags = 0;

  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(&s_tsdb, measurement_name, &measurement_id)) {
    ESP_LOGW(TAG, "Measurement '%s' not found", measurement_name);
    return false;
  }

  return timeseries_metadata_get_tags_for_measurement(&s_tsdb, measurement_id, tags, num_tags);
}

// =============================================================================
// Public API: Usage summary
// =============================================================================

bool timeseries_get_usage_summary(tsdb_usage_summary_t* summary) {
  TSDB_OPERATION_GUARD(false);
  if (!summary) {
    return false;
  }
  memset(summary, 0, sizeof(*summary));

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(&s_tsdb, &page_iter)) {
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0;
  uint32_t page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset, &page_size)) {
    if (hdr.page_state != TIMESERIES_PAGE_STATE_ACTIVE) {
      continue;
    }

    if (hdr.page_type == TIMESERIES_PAGE_TYPE_METADATA) {
      summary->metadata_summary.num_pages++;
      summary->metadata_summary.size_bytes += page_size;
    } else if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA) {
      uint8_t level = hdr.field_data_level;
      if (level < 5) {
        summary->page_summaries[level].num_pages++;
        summary->page_summaries[level].size_bytes += page_size;
      }
    }

    summary->used_space_bytes += page_size;
  }
  timeseries_page_cache_iterator_deinit(&page_iter);

  summary->total_space_bytes = s_tsdb.partition->size;
  return true;
}

// =============================================================================
// Public API: Delete
// =============================================================================

// Deletes own the maintenance slot for their whole run, like expiration and
// clear_all. A level compaction reads its source records before it writes
// them to the next level without holding flash_write_mutex; a delete in
// between would be undone by the compaction's output (TSDB_REVIEW R1).
static bool claim_maintenance_slot(void) {
  tsdb_epoch_idle(&s_tsdb, true);  // waits holding no page offsets
  bool claimed = false;
  for (int timeout_ms = 60000;; timeout_ms -= 10) {
    bool expected = false;
    if (atomic_compare_exchange_strong(&s_tsdb.compaction_in_progress, &expected, true)) {
      claimed = true;
      break;
    }
    if (timeout_ms <= 0) {
      ESP_LOGW(TAG, "Timed out waiting for compaction before delete");
      break;
    }
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  tsdb_epoch_idle(&s_tsdb, false);
  return claimed;
}

static bool delete_measurement_owned(const char* measurement_name);
bool timeseries_delete_measurement(const char* measurement_name) {
  TSDB_OPERATION_GUARD(false);
  if (!measurement_name) {
    return false;
  }
  if (!claim_maintenance_slot()) return false;
  bool ok = delete_measurement_owned(measurement_name);
  atomic_store(&s_tsdb.compaction_in_progress, false);
  return ok;
}

static bool delete_measurement_owned(const char* measurement_name) {
  // Hold flash_write_mutex for the entire delete operation to prevent
  // concurrent inserts from writing to flash while we soft-delete records.
  if (s_tsdb.flash_write_mutex) {
    tsdb_write_lock(&s_tsdb);
  }
  // Invalidate measurement ID cache entry
  if (s_tsdb.meas_cache) {
    for (int mc = 0; mc < MEAS_ID_CACHE_SIZE; mc++) {
      if (s_tsdb.meas_cache[mc].valid &&
          strcmp(measurement_name, s_tsdb.meas_cache[mc].name) == 0) {
        s_tsdb.meas_cache[mc].valid = false;
        break;
      }
    }
  }
  atomic_fetch_add(&s_tsdb.schema_gen, 1);
  bool result = timeseries_delete_by_measurement(&s_tsdb, measurement_name);
  if (result) {
    // Invalidate all caches that may reference the deleted measurement's data
    tsdb_cache_clear(&s_tsdb);        // Series ID cache
    tsdb_clear_type_cache(&s_tsdb);   // Series type cache
  }
  if (s_tsdb.flash_write_mutex) {
    xSemaphoreGive(s_tsdb.flash_write_mutex);
  }
  return result;
}

static bool delete_field_owned(const char* measurement_name, const char* field_name);
bool timeseries_delete_measurement_and_field(const char* measurement_name, const char* field_name) {
  TSDB_OPERATION_GUARD(false);
  if (!measurement_name || !field_name) {
    return false;
  }
  if (!claim_maintenance_slot()) return false;
  bool ok = delete_field_owned(measurement_name, field_name);
  atomic_store(&s_tsdb.compaction_in_progress, false);
  return ok;
}

static bool delete_field_owned(const char* measurement_name, const char* field_name) {
  // Hold flash_write_mutex for the entire delete operation to prevent
  // concurrent inserts from writing to flash while we soft-delete records.
  if (s_tsdb.flash_write_mutex) {
    tsdb_write_lock(&s_tsdb);
  }
  atomic_fetch_add(&s_tsdb.schema_gen, 1);
  bool result = timeseries_delete_by_measurement_and_field(&s_tsdb, measurement_name, field_name);
  if (result) {
    // Invalidate all caches that may reference the deleted field's data
    tsdb_cache_clear(&s_tsdb);        // Series ID cache
    tsdb_clear_type_cache(&s_tsdb);   // Series type cache
    // Invalidate measurement ID cache for this measurement
    if (s_tsdb.meas_cache) {
      for (int mc = 0; mc < MEAS_ID_CACHE_SIZE; mc++) {
        if (s_tsdb.meas_cache[mc].valid &&
            strcmp(measurement_name, s_tsdb.meas_cache[mc].name) == 0) {
          s_tsdb.meas_cache[mc].valid = false;
          break;
        }
      }
    }
  }
  if (s_tsdb.flash_write_mutex) {
    xSemaphoreGive(s_tsdb.flash_write_mutex);
  }
  return result;
}

bool timeseries_get_timestamp_range(const char *measurement_name,
                                    uint64_t *out_min_ms,
                                    uint64_t *out_max_ms,
                                    uint32_t *out_point_count) {
  TSDB_OPERATION_GUARD(false);
  return timeseries_query_get_timestamp_range(&s_tsdb, measurement_name,
                                              out_min_ms, out_max_ms,
                                              out_point_count);
}
