#include "timeseries.h"
#include "esp_log.h"
#include "esp_partition.h"
#include "timeseries_cache.h"
#include "timeseries_compaction.h"
#include "timeseries_data.h"
#include "timeseries_expiration.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_metadata.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include "timeseries_points_iterator.h"
#include "timeseries_query.h"
#include "timeseries_delete.h"
#include "timeseries_series_id.h"

#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

#include "mbedtls/md5.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static const char* TAG = "TimeseriesDB";

// Global/Static DB Context (single instance by design)
static timeseries_db_t s_tsdb = {
    .initialized = false,
    .next_measurement_id = 1,
    .partition = NULL,
};
static _Atomic bool s_init_in_progress = false;

// Background compaction task
static void tsdb_compaction_task(void *param) {
    timeseries_db_t *db = (timeseries_db_t *)param;
    ESP_LOGI(TAG, "Background compaction task started");
    while (db->compaction_running) {
        xSemaphoreTake(db->compaction_trigger, pdMS_TO_TICKS(30000));
        if (!db->compaction_running) break;
        atomic_store(&db->compaction_in_progress, true);
        ESP_LOGI(TAG, "Background compaction triggered");
        timeseries_compact_all_levels(db);
        atomic_fetch_add(&db->compaction_generation, 1);
        atomic_store(&db->compaction_in_progress, false);
        ESP_LOGI(TAG, "Background compaction finished");
    }
    ESP_LOGI(TAG, "Background compaction task exiting");
    vTaskDelete(NULL);
}

static SemaphoreHandle_t s_tsdb_mutex = NULL;

#define TSDB_MUTEX_CREATE()                            \
  do {                                                 \
    if (s_tsdb_mutex == NULL) {                        \
      s_tsdb_mutex = xSemaphoreCreateRecursiveMutex(); \
      if (s_tsdb_mutex == NULL) {                      \
        ESP_LOGE(TAG, "Failed to create TSDB mutex");  \
      }                                                \
    }                                                  \
  } while (0)

#define TSDB_MUTEX_LOCK()                                   \
  do {                                                      \
    if (s_tsdb_mutex) {                                     \
      xSemaphoreTakeRecursive(s_tsdb_mutex, portMAX_DELAY); \
    }                                                       \
  } while (0)

#define TSDB_MUTEX_UNLOCK()                  \
  do {                                       \
    if (s_tsdb_mutex) {                      \
      xSemaphoreGiveRecursive(s_tsdb_mutex); \
    }                                        \
  } while (0)

bool timeseries_init(void) {
  /* Ensure mutex exists before any other work */
  TSDB_MUTEX_CREATE();
  TSDB_MUTEX_LOCK();

  if (s_tsdb.initialized) {
    TSDB_MUTEX_UNLOCK();
    return true;
  }

  // Guard against concurrent init calls
  bool expected = false;
  if (!atomic_compare_exchange_strong(&s_init_in_progress, &expected, true)) {
    ESP_LOGW(TAG, "timeseries_init already in progress from another task");
    return false;
  }

  // Double-check after acquiring the guard
  if (s_tsdb.initialized) {
    atomic_store(&s_init_in_progress, false);
    return true;
  }

  ESP_LOGV(TAG, "Initializing Timeseries DB...");

  const esp_partition_t* part = esp_partition_find_first(0x40, ESP_PARTITION_SUBTYPE_ANY, "timeseries");
  if (!part) {
    ESP_LOGE(TAG, "Failed to find 'timeseries' partition in partition table.");
    atomic_store(&s_init_in_progress, false);
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  s_tsdb.partition = part;

  // Create synchronization primitives
  s_tsdb.snapshot_mutex = xSemaphoreCreateMutex();
  s_tsdb.flash_write_mutex = xSemaphoreCreateMutex();
  s_tsdb.region_alloc_mutex = xSemaphoreCreateMutex();
  s_tsdb.compaction_trigger = xSemaphoreCreateCounting(2, 0);
  if (!s_tsdb.snapshot_mutex || !s_tsdb.flash_write_mutex || !s_tsdb.compaction_trigger || !s_tsdb.region_alloc_mutex) {
    ESP_LOGE(TAG, "Failed to create synchronization primitives");
    goto init_fail;
  }
  s_tsdb.active_batch = NULL;
  s_tsdb.compaction_claimed_count = 0;

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

  s_tsdb.last_l0_cache_valid = false;
  s_tsdb.last_l0_page_offset = 0;
  s_tsdb.last_l0_used_offset = 0;

  // Restore next_measurement_id from persisted metadata
  uint32_t max_id = 0;
  if (tsdb_find_max_measurement_id(&s_tsdb, &max_id)) {
    s_tsdb.next_measurement_id = max_id + 1;
    ESP_LOGI(TAG, "Restored next_measurement_id to %" PRIu32, s_tsdb.next_measurement_id);
  }

  // Initialize series ID cache
  if (!tsdb_cache_init(&s_tsdb)) {
    ESP_LOGW(TAG, "Failed to initialize series ID cache (continuing without cache)");
  }

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

  // Start background compaction task
  s_tsdb.compaction_running = true;
  atomic_store(&s_tsdb.compaction_in_progress, false);
  atomic_store(&s_tsdb.compaction_generation, 0);
  BaseType_t task_ret = xTaskCreate(tsdb_compaction_task, "tsdb_compact",
                                     12288, &s_tsdb, tskIDLE_PRIORITY + 1,
                                     &s_tsdb.compaction_task_handle);
  if (task_ret != pdPASS) {
    ESP_LOGW(TAG, "Failed to create background compaction task (will use synchronous compaction)");
    s_tsdb.compaction_running = false;
    s_tsdb.compaction_task_handle = NULL;
  } else {
    ESP_LOGI(TAG, "Background compaction task created");
  }

  s_tsdb.initialized = true;
  atomic_store(&s_init_in_progress, false);

  /* Get current measurement ID from metadata */
  if (!tsdb_get_next_available_measurement_id(&s_tsdb, &s_tsdb.next_measurement_id)) {
    ESP_LOGE(TAG, "Failed to get next available measurement ID.");
    s_tsdb.next_measurement_id = 1;
  }

  /* Initialize metadata cache */
  s_tsdb.meta_cache = ts_cache_create(1024, 256 * 1024, 300000);
  if (!s_tsdb.meta_cache) {
    ESP_LOGW(TAG, "Failed to initialize metadata cache, continuing without it");
  }

  ESP_LOGI(TAG, "Timeseries DB initialized successfully.");
  TSDB_MUTEX_UNLOCK();
  return true;

init_fail:
  // Clean up any resources allocated during partial init
  if (s_tsdb.current_snapshot) {
    tsdb_snapshot_release(s_tsdb.current_snapshot);
    s_tsdb.current_snapshot = NULL;
  }
  if (s_tsdb.snapshot_mutex) {
    vSemaphoreDelete(s_tsdb.snapshot_mutex);
    s_tsdb.snapshot_mutex = NULL;
  }
  if (s_tsdb.flash_write_mutex) {
    vSemaphoreDelete(s_tsdb.flash_write_mutex);
    s_tsdb.flash_write_mutex = NULL;
  }
  if (s_tsdb.region_alloc_mutex) {
    vSemaphoreDelete(s_tsdb.region_alloc_mutex);
    s_tsdb.region_alloc_mutex = NULL;
  }
  if (s_tsdb.compaction_trigger) {
    vSemaphoreDelete(s_tsdb.compaction_trigger);
    s_tsdb.compaction_trigger = NULL;
  }
  s_tsdb.partition = NULL;
  atomic_store(&s_init_in_progress, false);
  TSDB_MUTEX_UNLOCK();
  return false;
}

bool timeseries_insert(const timeseries_insert_data_t* data) {
  TSDB_MUTEX_LOCK();

  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  if (!data) {
    ESP_LOGE(TAG, "timeseries_insert data is NULL.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  if (!data->measurement_name || !data->field_names || !data->timestamps_ms || !data->field_values) {
    ESP_LOGE(TAG, "timeseries_insert: required field is NULL.");
    return false;
  }
  if (data->num_points == 0) {
    ESP_LOGW(TAG, "No data points provided, nothing to store.");
    TSDB_MUTEX_UNLOCK();
    return true;
  }

  // Check if expiry is required
  timeseries_expire();

  uint64_t start_time = esp_timer_get_time();

  // 1) Find or create measurement ID (under mutex to prevent duplicate creation)
  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(&s_tsdb, data->measurement_name, &measurement_id)) {
    if (s_tsdb.flash_write_mutex) {
      xSemaphoreTake(s_tsdb.flash_write_mutex, portMAX_DELAY);
    }
    // Double-check after acquiring mutex (another task may have created it)
    if (!tsdb_find_measurement_id(&s_tsdb, data->measurement_name, &measurement_id)) {
      if (!tsdb_create_measurement_id(&s_tsdb, data->measurement_name, &measurement_id)) {
        if (s_tsdb.flash_write_mutex) {
          xSemaphoreGive(s_tsdb.flash_write_mutex);
        }
        TSDB_MUTEX_UNLOCK();
        return false;
      }
    }
    if (s_tsdb.flash_write_mutex) {
      xSemaphoreGive(s_tsdb.flash_write_mutex);
    }
  }

  // Log if this is a large insert
  if (data->num_points > 500) {
    ESP_LOGI(TAG, "Starting large insert: %zu points x %zu fields = %zu total values",
             data->num_points, data->num_fields, data->num_points * data->num_fields);
  }

  // Pre-compute the common prefix (measurement + tags) for cache key optimization
  char prefix_buffer[192];
  size_t prefix_len = snprintf(prefix_buffer, sizeof(prefix_buffer), "%s", data->measurement_name);
  for (size_t t = 0; t < data->num_tags; t++) {
    prefix_len += snprintf(prefix_buffer + prefix_len, sizeof(prefix_buffer) - prefix_len,
                          ":%s:%s", data->tag_keys[t], data->tag_values[t]);
    if (prefix_len >= sizeof(prefix_buffer)) {
      ESP_LOGE(TAG, "Prefix buffer overflow");
      TSDB_MUTEX_UNLOCK();
      return false;
    }
  }

  /* For each field i => build the series_id => store all points in one shot */
  for (size_t i = 0; i < data->num_fields; i++) {
    // Build the cache key string
    char cache_key[256];
    size_t key_len = snprintf(cache_key, sizeof(cache_key), "%s:%s",
                              prefix_buffer, data->field_names[i]);
    if (key_len >= sizeof(cache_key)) {
      ESP_LOGE(TAG, "Cache key buffer overflow for field '%s'", data->field_names[i]);
      TSDB_MUTEX_UNLOCK();
      return false;
    }

    unsigned char series_id[16];
    bool found_in_cache = false;

    // Try to get series_id from cache first
    if (tsdb_cache_lookup_series_id(&s_tsdb, cache_key, series_id)) {
      found_in_cache = true;
      ESP_LOGV(TAG, "Cache HIT for field '%s' - skipping metadata operations", data->field_names[i]);
    } else {
      // Cache miss - compute series ID using the canonical sorted-tag calculation
      if (!calculate_series_id(data->measurement_name, data->tag_keys, data->tag_values, data->num_tags,
                               data->field_names[i], series_id)) {
        TSDB_MUTEX_UNLOCK();
        return false;
      }
      ESP_LOGV(TAG, "Cache MISS for field '%s' - performing metadata operations", data->field_names[i]);
    }

    // Only do metadata operations if not found in cache
    if (!found_in_cache) {
      // Protect metadata flash writes from concurrent compaction
      if (s_tsdb.flash_write_mutex) {
        xSemaphoreTake(s_tsdb.flash_write_mutex, portMAX_DELAY);
      }

      /* Ensure the field name to series_id mapping is in metadata */
      if (!tsdb_index_field_for_series(&s_tsdb, measurement_id, data->field_names[i], series_id)) {
        ESP_LOGE(TAG, "Failed to index field '%s'", data->field_names[i]);
        if (s_tsdb.flash_write_mutex) {
          xSemaphoreGive(s_tsdb.flash_write_mutex);
        }
        TSDB_MUTEX_UNLOCK();
        return false;
      }

      /* Ensure field type in metadata */
      const timeseries_field_value_t* first_val = &data->field_values[i * data->num_points + 0];
      if (!tsdb_ensure_series_type_in_metadata(&s_tsdb, series_id, first_val->type)) {
        if (s_tsdb.flash_write_mutex) {
          xSemaphoreGive(s_tsdb.flash_write_mutex);
        }
        TSDB_MUTEX_UNLOCK();
        return false;
      }

      /* Index tags */
      if (!tsdb_index_tags_for_series(&s_tsdb, measurement_id, data->tag_keys, data->tag_values, data->num_tags,
                                      series_id)) {
        if (s_tsdb.flash_write_mutex) {
          xSemaphoreGive(s_tsdb.flash_write_mutex);
        }
        TSDB_MUTEX_UNLOCK();
        return false;
      }

      if (s_tsdb.flash_write_mutex) {
        xSemaphoreGive(s_tsdb.flash_write_mutex);
      }

      // Successfully indexed - add to cache for next time
      tsdb_cache_insert_series_id(&s_tsdb, cache_key, series_id);
    }

    /* Gather the array of values for this field */
    const timeseries_field_value_t* field_array = &data->field_values[i * data->num_points];

    /* 2) Insert multi data points in one entry */
    ESP_LOGV(TAG, "Inserting field %zu/%zu: '%s' (%zu points)",
             i + 1, data->num_fields, data->field_names[i], data->num_points);
    if (!tsdb_append_multiple_points(&s_tsdb, series_id, data->timestamps_ms, field_array, data->num_points)) {
      ESP_LOGE(TAG, "Failed to insert multi points for field '%s'", data->field_names[i]);
      TSDB_MUTEX_UNLOCK();
      return false;
    }
    ESP_LOGV(TAG, "Successfully inserted field='%s' with %zu points for measurement=%s",
             data->field_names[i], data->num_points, data->measurement_name);
  }

  uint64_t end_time = esp_timer_get_time();
  ESP_LOGV(TAG, "Inserted %zu points in %zu fields for measurement '%s' in %.3f ms", data->num_points, data->num_fields,
           data->measurement_name, (end_time - start_time) / 1000.0);

  // Suggest compaction for large inserts
  if (data->num_points > 500) {
    ESP_LOGI(TAG, "Large insert complete. Consider calling timeseries_compact() to optimize storage.");
  }

  TSDB_MUTEX_UNLOCK();
  return true;
}

bool timeseries_compact(void) {
  TSDB_MUTEX_LOCK();

  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  if (s_tsdb.compaction_running && s_tsdb.compaction_task_handle) {
    // Trigger background compaction
    ESP_LOGV(TAG, "Triggering background compaction...");
    xSemaphoreGive(s_tsdb.compaction_trigger);
    TSDB_MUTEX_UNLOCK();
    return true;
  }

  // Fallback to synchronous compaction if background task isn't running
  ESP_LOGV(TAG, "Starting synchronous compaction...");
  if (!timeseries_compact_all_levels(&s_tsdb)) {
    ESP_LOGE(TAG, "Compaction failed.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  ESP_LOGV(TAG, "Compaction complete.");
  TSDB_MUTEX_UNLOCK();
  return true;
}

bool timeseries_compact_sync(void) {
  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    return false;
  }

  if (s_tsdb.compaction_running && s_tsdb.compaction_task_handle) {
    // If compaction is already in-progress, we need to wait for TWO
    // generation advances: the current in-flight run + our triggered run.
    uint32_t gen_before = atomic_load(&s_tsdb.compaction_generation);
    uint32_t wait_target = gen_before + 1;
    if (atomic_load(&s_tsdb.compaction_in_progress)) {
      wait_target = gen_before + 2;  // Wait for current + our run
    }

    // Trigger and wait for background compaction to complete
    ESP_LOGV(TAG, "Triggering synchronous compaction via background task (gen=%u, target=%u)...",
             gen_before, wait_target);
    xSemaphoreGive(s_tsdb.compaction_trigger);

    // Wait until the generation counter reaches the target
    int timeout_ms = 60000;  // 60 second timeout
    while (atomic_load(&s_tsdb.compaction_generation) < wait_target && timeout_ms > 0) {
      vTaskDelay(pdMS_TO_TICKS(10));
      timeout_ms -= 10;
    }
    if (timeout_ms <= 0) {
      ESP_LOGW(TAG, "compact_sync timed out waiting for compaction to complete");
      return false;
    }
    ESP_LOGV(TAG, "Background compaction complete (sync wait done, gen=%u).",
             atomic_load(&s_tsdb.compaction_generation));
    return true;
  }

  // Fallback to direct synchronous compaction
  ESP_LOGV(TAG, "Starting synchronous compaction (no background task)...");
  if (!timeseries_compact_all_levels(&s_tsdb)) {
    ESP_LOGE(TAG, "Compaction failed.");
    return false;
  }
  ESP_LOGV(TAG, "Compaction complete.");
  return true;
}

void timeseries_deinit(void) {
  if (!s_tsdb.initialized) {
    return;
  }

  // Stop background compaction task
  if (s_tsdb.compaction_running && s_tsdb.compaction_task_handle) {
    s_tsdb.compaction_running = false;
    xSemaphoreGive(s_tsdb.compaction_trigger);  // Wake the task so it exits
    // Wait for any in-progress compaction to finish, then for the task to exit
    int wait_ms = 30000;
    while (atomic_load(&s_tsdb.compaction_in_progress) && wait_ms > 0) {
      vTaskDelay(pdMS_TO_TICKS(10));
      wait_ms -= 10;
    }
    // Give the task a moment to call vTaskDelete after the loop exits
    vTaskDelay(pdMS_TO_TICKS(50));
    s_tsdb.compaction_task_handle = NULL;
  }

  // Delete synchronization primitives
  if (s_tsdb.compaction_trigger) {
    vSemaphoreDelete(s_tsdb.compaction_trigger);
    s_tsdb.compaction_trigger = NULL;
  }
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

  // Free type cache
  if (s_tsdb.type_cache) {
    free(s_tsdb.type_cache);
    s_tsdb.type_cache = NULL;
    s_tsdb.type_cache_size = 0;
  }

  s_tsdb.initialized = false;
  ESP_LOGI(TAG, "Timeseries DB deinitialized.");
}

bool timeseries_expire(void) {
  TSDB_MUTEX_LOCK();

  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  ESP_LOGV(TAG, "Starting expiration...");

  if (!timeseries_expiration_run(&s_tsdb, 0.95, 0.05)) {
    ESP_LOGE(TAG, "Expiration failed.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  ESP_LOGV(TAG, "Expiration complete.");
  TSDB_MUTEX_UNLOCK();
  return true;
}

bool timeseries_query(const timeseries_query_t* query, timeseries_query_result_t* result) {
  TSDB_MUTEX_LOCK();
  bool ok = timeseries_query_execute(&s_tsdb, query, result);
  TSDB_MUTEX_UNLOCK();
  return ok;
}

void timeseries_query_free_result(timeseries_query_result_t* result) {
  TSDB_MUTEX_LOCK();

  if (!result) {
    TSDB_MUTEX_UNLOCK();
    return; /* nothing to do */
  }

  /* 1) Free the timestamps array */
  if (result->timestamps) {
    free(result->timestamps);
    result->timestamps = NULL;
  }

  /* 2) Free each column */
  if (result->columns) {
    for (size_t col = 0; col < result->num_columns; col++) {
      timeseries_query_result_column_t* c = &result->columns[col];

      /* Free the column name */
      if (c->name) {
        free(c->name);
        c->name = NULL;
      }

      /* Free each value in this column (including string data, if any) */
      if (c->values) {
        for (size_t row = 0; row < result->num_points; row++) {
          if (c->values[row].type == TIMESERIES_FIELD_TYPE_STRING) {
            if (c->values[row].data.string_val.str) {
              free((void*)c->values[row].data.string_val.str);
            }
          }
        }
        free(c->values);
        c->values = NULL;
      }
    }

    free(result->columns);
    result->columns = NULL;
  }

  result->num_points = 0;
  result->num_columns = 0;
  TSDB_MUTEX_UNLOCK();
}

bool timeseries_clear_all() {
  TSDB_MUTEX_LOCK();

  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  // Wait for any in-progress compaction to finish before erasing
  if (atomic_load(&s_tsdb.compaction_in_progress)) {
    ESP_LOGI(TAG, "Waiting for in-progress compaction before clear_all...");
    while (atomic_load(&s_tsdb.compaction_in_progress)) {
      vTaskDelay(pdMS_TO_TICKS(10));
    }
  }

  // Hold flash_write_mutex to prevent concurrent inserts during erase
  if (s_tsdb.flash_write_mutex) {
    xSemaphoreTake(s_tsdb.flash_write_mutex, portMAX_DELAY);
  }

  /* Erase the entire partition */
  esp_err_t err = esp_partition_erase_range(s_tsdb.partition, 0, s_tsdb.partition->size);

  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to erase partition (err=0x%x)", err);
    if (s_tsdb.flash_write_mutex) {
      xSemaphoreGive(s_tsdb.flash_write_mutex);
    }
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  /* Clear out any cached data */
  tsdb_pagecache_clear(&s_tsdb);
  tsdb_cache_clear(&s_tsdb);      // Clear series ID cache
  tsdb_clear_type_cache(&s_tsdb); // Clear series type cache

  /* Reset the last L0 cache */
  s_tsdb.last_l0_cache_valid = false;
  s_tsdb.last_l0_page_offset = 0;
  s_tsdb.last_l0_used_offset = 0;
  s_tsdb.compaction_claimed_count = 0;

  // Reset metadata page cache
  s_tsdb.cached_metadata_valid = false;
  s_tsdb.cached_metadata_count = 0;

  /* Add back the metadata page */
  timeseries_metadata_create_page(&s_tsdb);

  if (s_tsdb.flash_write_mutex) {
    xSemaphoreGive(s_tsdb.flash_write_mutex);
  }

  TSDB_MUTEX_UNLOCK();
  return true;
}

bool timeseries_get_measurements(char*** measurements, size_t* num_measurements) {
  TSDB_MUTEX_LOCK();

  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  if (!measurements || !num_measurements) {
    ESP_LOGE(TAG, "Invalid output parameters for measurements.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  *measurements = NULL;
  *num_measurements = 0;

  timeseries_string_list_t list;
  tsdb_string_list_init(&list);

  if (!tsdb_list_all_measurements(&s_tsdb, &list)) {
    ESP_LOGE(TAG, "Failed to retrieve measurements from metadata.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  *measurements = list.items;
  *num_measurements = list.count;
  TSDB_MUTEX_UNLOCK();
  return true;
}

bool timeseries_get_fields_for_measurement(const char* measurement_name, char*** fields, size_t* num_fields) {
  TSDB_MUTEX_LOCK();

  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  if (!measurement_name || !fields || !num_fields) {
    ESP_LOGE(TAG, "Invalid parameters for getting fields.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(&s_tsdb, measurement_name, &measurement_id)) {
    ESP_LOGE(TAG, "Measurement '%s' not found.", measurement_name);
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  *fields = NULL;
  *num_fields = 0;

  timeseries_string_list_t list;
  tsdb_string_list_init(&list);

  if (!tsdb_list_fields_for_measurement(&s_tsdb, measurement_id, &list)) {
    ESP_LOGE(TAG, "Failed to retrieve fields for measurement '%s'.", measurement_name);
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  *fields = list.items;
  *num_fields = list.count;
  TSDB_MUTEX_UNLOCK();
  return true;
}

bool timeseries_get_tags_for_measurement(const char* measurement_name, tsdb_tag_pair_t** tags, size_t* num_tags) {
  TSDB_MUTEX_LOCK();

  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  if (!measurement_name || !tags || !num_tags) {
    ESP_LOGE(TAG, "Invalid parameters for getting tags.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(&s_tsdb, measurement_name, &measurement_id)) {
    ESP_LOGE(TAG, "Measurement '%s' not found.", measurement_name);
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  bool ok = timeseries_metadata_get_tags_for_measurement(&s_tsdb, measurement_id, tags, num_tags);
  TSDB_MUTEX_UNLOCK();
  return ok;
}

bool timeseries_get_usage_summary(tsdb_usage_summary_t* summary) {
  TSDB_MUTEX_LOCK();

  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  if (!summary) {
    ESP_LOGE(TAG, "Invalid output parameter for usage summary.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  memset(summary, 0, sizeof(tsdb_usage_summary_t));
  summary->total_space_bytes = s_tsdb.partition->size;

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(&s_tsdb, &page_iter)) {
    ESP_LOGE(TAG, "Failed to initialize page iterator");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0;
  uint32_t page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset, &page_size)) {
    if (hdr.field_data_level > 4) {
      ESP_LOGW(TAG, "Skipping page with invalid level %d", (unsigned int)hdr.field_data_level);
      continue;
    }

    if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA) {
      summary->page_summaries[hdr.field_data_level].num_pages++;
      summary->page_summaries[hdr.field_data_level].size_bytes += page_size;
    } else {
      summary->metadata_summary.num_pages++;
      summary->metadata_summary.size_bytes += page_size;
    }

    summary->used_space_bytes += page_size;
  }

  TSDB_MUTEX_UNLOCK();
  return true;
}

bool timeseries_delete_measurement(const char* measurement_name) {
  TSDB_MUTEX_LOCK();

  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  if (!measurement_name) {
    ESP_LOGE(TAG, "Invalid parameters for delete by measurement.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  bool result = timeseries_delete_by_measurement(&s_tsdb, measurement_name);

  TSDB_MUTEX_UNLOCK();

  return result;
}

bool timeseries_delete_measurement_and_field(const char* measurement_name, const char* field_name) {
  TSDB_MUTEX_LOCK();

  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  if (!measurement_name || !field_name) {
    ESP_LOGE(TAG, "Invalid parameters for delete by measurement and field.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  bool ok = timeseries_delete_by_measurement_and_field(&s_tsdb, measurement_name, field_name);

  TSDB_MUTEX_UNLOCK();
  return ok;
}

void timeseries_set_chunk_size(size_t chunk_size) {
  if (!s_tsdb.initialized) {
    ESP_LOGW(TAG, "Cannot set chunk size before initialization");
    return;
  }
  if (chunk_size == 0) {
    ESP_LOGW(TAG, "Invalid chunk size (0), keeping current value");
    return;
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
