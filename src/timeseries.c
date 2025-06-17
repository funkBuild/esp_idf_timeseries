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

#include "esp_timer.h"

#include "mbedtls/md5.h"

#include <stdlib.h>
#include <string.h>

static const char *TAG = "TimeseriesDB";

// Global/Static DB Context
static timeseries_db_t s_tsdb = {
    .initialized = false,
    .next_measurement_id = 1,
    .partition = NULL,
};

bool timeseries_init(void) {
  if (s_tsdb.initialized) {
    return true;
  }

  ESP_LOGV(TAG, "Initializing Timeseries DB...");

  const esp_partition_t *part = esp_partition_find_first(
      ESP_PARTITION_TYPE_DATA, ESP_PARTITION_SUBTYPE_ANY, "storage");
  if (!part) {
    ESP_LOGE(TAG, "Failed to find 'storage' partition in partition table.");
    return false;
  }
  s_tsdb.partition = part;

  if (!tsdb_load_pages_into_memory(&s_tsdb)) {
    ESP_LOGE(TAG, "Failed to load existing pages from flash.");
    return false;
  }

  s_tsdb.last_l0_cache_valid = false;
  s_tsdb.last_l0_page_offset = 0;
  s_tsdb.last_l0_used_offset = 0;
  s_tsdb.initialized = true;

  ESP_LOGV(TAG, "Timeseries DB initialized successfully.");
  return true;
}

bool timeseries_insert(const timeseries_insert_data_t *data) {
  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    return false;
  }
  if (!data) {
    ESP_LOGE(TAG, "timeseries_insert data is NULL.");
    return false;
  }
  if (data->num_points == 0) {
    ESP_LOGW(TAG, "No data points provided, nothing to store.");
    return true;
  }

  // 1) Find or create measurement ID
  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(&s_tsdb, data->measurement_name,
                                &measurement_id)) {
    if (!tsdb_create_measurement_id(&s_tsdb, data->measurement_name,
                                    &measurement_id)) {
      return false;
    }
  }

  // For each field i => build the series_id => store all points in one shot
  for (size_t i = 0; i < data->num_fields; i++) {
    // Build MD5 input =>
    // "<measurementName>:<tagKey1>:<tagValue1>:...:<fieldName>" same approach
    // as before
    char buffer[256];
    memset(buffer, 0, sizeof(buffer));
    size_t offset = 0;
    offset += snprintf(buffer + offset, sizeof(buffer) - offset, "%s",
                       data->measurement_name);
    for (size_t t = 0; t < data->num_tags; t++) {
      offset += snprintf(buffer + offset, sizeof(buffer) - offset, ":%s:%s",
                         data->tag_keys[t], data->tag_values[t]);
      if (offset >= sizeof(buffer)) {
        ESP_LOGE(TAG, "MD5 input buffer overflow for field '%s'",
                 data->field_names[i]);
        return false;
      }
    }
    offset += snprintf(buffer + offset, sizeof(buffer) - offset, ":%s",
                       data->field_names[i]);
    if (offset >= sizeof(buffer)) {
      ESP_LOGE(TAG, "MD5 buffer overflow (field_name too long?)");
      return false;
    }

    // MD5 => series_id
    unsigned char series_id[16];
    mbedtls_md5((const unsigned char *)buffer, strlen(buffer), series_id);

    // Ensure the field name to series_id mapping is in metadata
    if (!tsdb_index_field_for_series(&s_tsdb, measurement_id,
                                     data->field_names[i], series_id)) {
      ESP_LOGE(TAG, "Failed to index field '%s'", data->field_names[i]);
      return false;
    }

    // Ensure field type in metadata
    // We'll assume all data points in this field are the same type,
    // so we check the first one
    const timeseries_field_value_t *first_val =
        &data->field_values[i * data->num_points + 0];
    if (!tsdb_ensure_series_type_in_metadata(&s_tsdb, series_id,
                                             first_val->type)) {
      return false;
    }

    // Index tags
    if (!tsdb_index_tags_for_series(&s_tsdb, measurement_id, data->tag_keys,
                                    data->tag_values, data->num_tags,
                                    series_id)) {
      return false;
    }

    // Gather the array of values for this field
    const timeseries_field_value_t *field_array =
        &data->field_values[i * data->num_points];

    // 2) Insert multi data points in one entry
    if (!tsdb_append_multiple_points(&s_tsdb, series_id, data->timestamps_ms,
                                     field_array, data->num_points)) {
      ESP_LOGE(TAG, "Failed to insert multi points for field '%s'",
               data->field_names[i]);
      return false;
    }
    ESP_LOGV(TAG, "Inserted field='%s' with %zu points for measurement=%s",
             data->field_names[i], data->num_points, data->measurement_name);
  }

  return true;
}

bool timeseries_compact(void) {
  if (!s_tsdb.initialized) {
    ESP_LOGE(TAG, "Timeseries DB not initialized yet.");
    return false;
  }

  ESP_LOGV(TAG, "Starting compaction...");

  if (!timeseries_compact_all_levels(&s_tsdb)) {
    ESP_LOGE(TAG, "Level-0 compaction failed.");
    return false;
  }

  ESP_LOGV(TAG, "Compaction complete.");
  return true;
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

bool timeseries_query(const timeseries_query_t *query,
                      timeseries_query_result_t *result) {
  return timeseries_query_execute(&s_tsdb, query, result);
}

void timeseries_query_free_result(timeseries_query_result_t *result) {
  if (!result) {
    return; // nothing to do
  }

  // 1) Free the timestamps array
  if (result->timestamps) {
    free(result->timestamps);
    result->timestamps = NULL;
  }

  // 2) Free each column
  if (result->columns) {
    for (size_t col = 0; col < result->num_columns; col++) {
      timeseries_query_result_column_t *c = &result->columns[col];

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
              free((void *)c->values[row].data.string_val.str);
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

  // Erase the entire partition
  esp_err_t err =
      esp_partition_erase_range(s_tsdb.partition, 0, s_tsdb.partition->size);

  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to erase partition (err=0x%x)", err);
    return false;
  }

  // Clear out any cached data
  tsdb_pagecache_clear(&s_tsdb);

  // Reset the last L0 cache
  s_tsdb.last_l0_cache_valid = false;
  s_tsdb.last_l0_page_offset = 0;
  s_tsdb.last_l0_used_offset = 0;

  // Add back the metadata page
  timeseries_metadata_create_page(&s_tsdb);

  return true;
}