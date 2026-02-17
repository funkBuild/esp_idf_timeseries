#include "timeseries.h"
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

// Global/Static DB Context (single instance by design)
static timeseries_db_t s_tsdb = {
    .initialized = false,
    .next_measurement_id = 1,
    .partition = NULL,
};
static _Atomic bool s_init_in_progress = false;

// One-shot compaction task: runs compaction once, then exits
static void tsdb_compaction_oneshot_task(void *param) {
    timeseries_db_t *db = (timeseries_db_t *)param;
    ESP_LOGI(TAG, "Compaction task started");
    timeseries_compact_all_levels(db);
    atomic_fetch_add(&db->compaction_generation, 1);
    db->compaction_task_handle = NULL;
    atomic_store(&db->compaction_in_progress, false);
    ESP_LOGI(TAG, "Compaction task finished");

    // Re-check L0 count - inserts during compaction may have accumulated new pages
    size_t l0_count = tsdb_count_l0_pages(db);
    if (l0_count >= MIN_PAGES_FOR_COMPACTION) {
        ESP_LOGI(TAG, "Re-triggering compaction: %zu L0 pages accumulated", l0_count);
        tsdb_launch_compaction(db);
    }

    vTaskDelete(NULL);
}

bool tsdb_launch_compaction(timeseries_db_t *db) {
    bool expected = false;
    if (!atomic_compare_exchange_strong(&db->compaction_in_progress, &expected, true)) {
        return false;  // already running
    }
    BaseType_t ret = xTaskCreate(tsdb_compaction_oneshot_task, "tsdb_compact",
                                  12288, db, tskIDLE_PRIORITY + 1,
                                  &db->compaction_task_handle);
    if (ret != pdPASS) {
        ESP_LOGW(TAG, "Failed to create compaction task");
        atomic_store(&db->compaction_in_progress, false);
        return false;
    }
    return true;
}

bool timeseries_init(void) {
  if (s_tsdb.initialized) {
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
    return false;
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

  // Initialize on-demand compaction state
  atomic_store(&s_tsdb.compaction_in_progress, false);
  atomic_store(&s_tsdb.compaction_generation, 0);
  s_tsdb.compaction_task_handle = NULL;

  s_tsdb.initialized = true;
  atomic_store(&s_init_in_progress, false);

  ESP_LOGI(TAG, "Timeseries DB initialized successfully.");
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
  s_tsdb.partition = NULL;
  atomic_store(&s_init_in_progress, false);
  return false;
}

bool timeseries_insert(const timeseries_insert_data_t* data) {
  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    return false;
  }
  if (!data) {
    ESP_LOGE(TAG, "timeseries_insert data is NULL.");
    return false;
  }
  if (!data->measurement_name || !data->field_names || !data->timestamps_ms || !data->field_values) {
    ESP_LOGE(TAG, "timeseries_insert: required field is NULL.");
    return false;
  }
  if (data->num_points == 0) {
    ESP_LOGW(TAG, "No data points provided, nothing to store.");
    return true;
  }

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

  // Pre-compute the common prefix (measurement + tags) for MD5 optimization.
  // Size matches cache_key to avoid truncation for valid inputs.
  char prefix_buffer[256];
  size_t prefix_len = snprintf(prefix_buffer, sizeof(prefix_buffer), "%s", data->measurement_name);
  if (prefix_len >= sizeof(prefix_buffer)) {
    ESP_LOGE(TAG, "Measurement name too long for prefix buffer");
    return false;
  }
  for (size_t t = 0; t < data->num_tags; t++) {
    size_t remaining = sizeof(prefix_buffer) - prefix_len;
    size_t written = snprintf(prefix_buffer + prefix_len, remaining,
                          ":%s:%s", data->tag_keys[t], data->tag_values[t]);
    prefix_len += written;
    if (prefix_len >= sizeof(prefix_buffer)) {
      ESP_LOGE(TAG, "Prefix buffer overflow");
      return false;
    }
  }

  // For each field i => build the series_id => store all points in one shot
  bool any_field_failed = false;
  for (size_t i = 0; i < data->num_fields; i++) {
    // Build the cache key string
    char cache_key[256];
    size_t key_len = snprintf(cache_key, sizeof(cache_key), "%s:%s",
                              prefix_buffer, data->field_names[i]);
    if (key_len >= sizeof(cache_key)) {
      ESP_LOGE(TAG, "Cache key buffer overflow for field '%s'", data->field_names[i]);
      return false;
    }

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

    // Only do metadata operations if not found in cache
    if (!found_in_cache) {
      // Protect metadata flash writes from concurrent compaction
      if (s_tsdb.flash_write_mutex) {
        xSemaphoreTake(s_tsdb.flash_write_mutex, portMAX_DELAY);
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

    // 2) Insert multi data points in one entry
    ESP_LOGD(TAG, "Inserting field %zu/%zu: '%s' (%zu points)",
             i + 1, data->num_fields, data->field_names[i], data->num_points);
    if (!tsdb_append_multiple_points(&s_tsdb, series_id, data->timestamps_ms, field_array, data->num_points)) {
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

  // Auto-trigger background compaction when L0 page count is high enough
  if (!any_field_failed && !atomic_load(&s_tsdb.compaction_in_progress)) {
    size_t l0_count = tsdb_count_l0_pages(&s_tsdb);
    if (l0_count >= MIN_PAGES_FOR_COMPACTION) {
      ESP_LOGI(TAG, "Auto-triggering compaction: %zu L0 pages", l0_count);
      tsdb_launch_compaction(&s_tsdb);
    }
  }

  return !any_field_failed;
}

bool timeseries_compact(void) {
  if (!s_tsdb.initialized) {
    return false;
  }
  tsdb_launch_compaction(&s_tsdb);
  return true;
}

bool timeseries_compact_sync(void) {
  if (!s_tsdb.initialized) {
    return false;
  }
  // Wait for any in-progress compaction to finish
  int timeout_ms = 60000;
  while (atomic_load(&s_tsdb.compaction_in_progress) && timeout_ms > 0) {
    vTaskDelay(pdMS_TO_TICKS(10));
    timeout_ms -= 10;
  }
  if (timeout_ms <= 0) {
    return false;
  }
  // Claim the slot and run synchronously
  bool expected = false;
  if (!atomic_compare_exchange_strong(&s_tsdb.compaction_in_progress, &expected, true)) {
    // Race: someone else claimed it. Wait for them.
    while (atomic_load(&s_tsdb.compaction_in_progress) && timeout_ms > 0) {
      vTaskDelay(pdMS_TO_TICKS(10));
      timeout_ms -= 10;
    }
    return timeout_ms > 0;
  }
  bool ok = timeseries_compact_all_levels(&s_tsdb);
  atomic_fetch_add(&s_tsdb.compaction_generation, 1);
  atomic_store(&s_tsdb.compaction_in_progress, false);
  return ok;
}

void timeseries_deinit(void) {
  if (!s_tsdb.initialized) {
    return;
  }

  // Wait for any in-progress compaction to finish
  int wait_ms = 30000;
  while (atomic_load(&s_tsdb.compaction_in_progress) && wait_ms > 0) {
    vTaskDelay(pdMS_TO_TICKS(10));
    wait_ms -= 10;
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
  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    return false;
  }

  ESP_LOGV(TAG, "Starting expiration...");

  if (!timeseries_expiration_run(&s_tsdb, 0.5, 0.05)) {
    ESP_LOGE(TAG, "Expiration failed.");
    return false;
  }

  ESP_LOGV(TAG, "Expiration complete.");
  return true;
}

bool timeseries_query(const timeseries_query_t* query, timeseries_query_result_t* result) {
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

bool timeseries_clear_all() {
  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
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

  // Erase the entire partition
  esp_err_t err = esp_partition_erase_range(s_tsdb.partition, 0, s_tsdb.partition->size);

  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to erase partition (err=0x%x)", err);
    if (s_tsdb.flash_write_mutex) {
      xSemaphoreGive(s_tsdb.flash_write_mutex);
    }
    return false;
  }

  // Clear out any cached data
  tsdb_pagecache_clear(&s_tsdb);
  tsdb_cache_clear(&s_tsdb);      // Clear series ID cache
  tsdb_clear_type_cache(&s_tsdb); // Clear series type cache

  // Reset the last L0 cache
  s_tsdb.last_l0_cache_valid = false;
  s_tsdb.last_l0_page_offset = 0;
  s_tsdb.last_l0_used_offset = 0;
  s_tsdb.compaction_claimed_count = 0;

  // Reset metadata page cache
  s_tsdb.cached_metadata_valid = false;
  s_tsdb.cached_metadata_count = 0;

  // Add back the metadata page
  timeseries_metadata_create_page(&s_tsdb);

  if (s_tsdb.flash_write_mutex) {
    xSemaphoreGive(s_tsdb.flash_write_mutex);
  }

  return true;
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
  if (!s_tsdb.initialized || !measurements || !num_measurements) {
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
  if (!s_tsdb.initialized || !measurement_name || !fields || !num_fields) {
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
  if (!s_tsdb.initialized || !measurement_name || !tags || !num_tags) {
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
  if (!s_tsdb.initialized || !summary) {
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

bool timeseries_delete_measurement(const char* measurement_name) {
  if (!s_tsdb.initialized || !measurement_name) {
    return false;
  }
  return timeseries_delete_by_measurement(&s_tsdb, measurement_name);
}

bool timeseries_delete_measurement_and_field(const char* measurement_name, const char* field_name) {
  if (!s_tsdb.initialized || !measurement_name || !field_name) {
    return false;
  }
  return timeseries_delete_by_measurement_and_field(&s_tsdb, measurement_name, field_name);
}