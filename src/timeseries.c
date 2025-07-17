#include "timeseries.h"
#include "esp_log.h"
#include "esp_partition.h"
#include "timeseries_compaction.h"
#include "timeseries_data.h"
#include "timeseries_expiration.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_metadata.h"
#include "timeseries_page_cache.h"
#include "timeseries_query.h"
#include "timeseries_delete.h"
#include "timeseries_series_id.h"

#include "esp_timer.h"

#include "mbedtls/md5.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

#include <stdlib.h>
#include <string.h>

static const char* TAG = "TimeseriesDB";

static timeseries_db_t s_tsdb = {
    .initialized = false,
    .next_measurement_id = 1,
    .partition = NULL,
};

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

  ESP_LOGV(TAG, "Initializing Timeseries DB...");

  const esp_partition_t* part = esp_partition_find_first(0x40, ESP_PARTITION_SUBTYPE_ANY, "timeseries");
  if (!part) {
    ESP_LOGE(TAG, "Failed to find 'storage' partition in partition table.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }
  s_tsdb.partition = part;

  if (!tsdb_load_pages_into_memory(&s_tsdb)) {
    ESP_LOGE(TAG, "Failed to load existing pages from flash.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  s_tsdb.last_l0_cache_valid = false;
  s_tsdb.last_l0_page_offset = 0;
  s_tsdb.last_l0_used_offset = 0;
  s_tsdb.initialized = true;

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
  if (data->num_points == 0) {
    ESP_LOGW(TAG, "No data points provided, nothing to store.");
    TSDB_MUTEX_UNLOCK();
    return true;
  }

  // Check if expiry is required
  timeseries_expire();

  uint64_t start_time = esp_timer_get_time();

  /* 1) Find or create measurement ID */
  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(&s_tsdb, data->measurement_name, &measurement_id)) {
    if (!tsdb_create_measurement_id(&s_tsdb, data->measurement_name, &measurement_id)) {
      TSDB_MUTEX_UNLOCK();
      return false;
    }
  }

  /* For each field i => build the series_id => store all points in one shot */
  for (size_t i = 0; i < data->num_fields; i++) {
    /* Calculate series_id using sorted tags */
    unsigned char series_id[16];
    if (!calculate_series_id(data->measurement_name, data->tag_keys, data->tag_values, data->num_tags,
                             data->field_names[i], series_id)) {
      TSDB_MUTEX_UNLOCK();
      return false;
    }

    /* Ensure the field name to series_id mapping is in metadata */
    if (!tsdb_index_field_for_series(&s_tsdb, measurement_id, data->field_names[i], series_id)) {
      ESP_LOGE(TAG, "Failed to index field '%s'", data->field_names[i]);
      TSDB_MUTEX_UNLOCK();
      return false;
    }

    /* Ensure field type in metadata */
    const timeseries_field_value_t* first_val = &data->field_values[i * data->num_points + 0];
    if (!tsdb_ensure_series_type_in_metadata(&s_tsdb, series_id, first_val->type)) {
      TSDB_MUTEX_UNLOCK();
      return false;
    }

    /* Index tags */
    if (!tsdb_index_tags_for_series(&s_tsdb, measurement_id, data->tag_keys, data->tag_values, data->num_tags,
                                    series_id)) {
      TSDB_MUTEX_UNLOCK();
      return false;
    }

    /* Gather the array of values for this field */
    const timeseries_field_value_t* field_array = &data->field_values[i * data->num_points];

    /* 2) Insert multi data points in one entry */
    if (!tsdb_append_multiple_points(&s_tsdb, series_id, data->timestamps_ms, field_array, data->num_points)) {
      ESP_LOGE(TAG, "Failed to insert multi points for field '%s'", data->field_names[i]);
      TSDB_MUTEX_UNLOCK();
      return false;
    }
    ESP_LOGV(TAG, "Inserted field='%s' with %zu points for measurement=%s", data->field_names[i], data->num_points,
             data->measurement_name);
  }

  uint64_t end_time = esp_timer_get_time();
  ESP_LOGV(TAG, "Inserted %zu points in %zu fields for measurement '%s' in %.3f ms", data->num_points, data->num_fields,
           data->measurement_name, (end_time - start_time) / 1000.0);

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

  ESP_LOGV(TAG, "Starting compaction...");

  if (!timeseries_compact_all_levels(&s_tsdb)) {
    ESP_LOGE(TAG, "Level-0 compaction failed.");
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  ESP_LOGV(TAG, "Compaction complete.");
  TSDB_MUTEX_UNLOCK();
  return true;
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

  /* Erase the entire partition */
  esp_err_t err = esp_partition_erase_range(s_tsdb.partition, 0, s_tsdb.partition->size);

  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to erase partition (err=0x%x)", err);
    TSDB_MUTEX_UNLOCK();
    return false;
  }

  /* Clear out any cached data */
  tsdb_pagecache_clear(&s_tsdb);

  /* Reset the last L0 cache */
  s_tsdb.last_l0_cache_valid = false;
  s_tsdb.last_l0_page_offset = 0;
  s_tsdb.last_l0_used_offset = 0;

  /* Add back the metadata page */
  timeseries_metadata_create_page(&s_tsdb);

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
