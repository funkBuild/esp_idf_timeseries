#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "timeseries.h"
#include "timeseries_query.h"
#include <stdio.h>

#include "esp_timer.h"

static const char* TAG = "example";

void example_query_weather_data(void) {
  // Build a timeseries_query_t for "weather"
  timeseries_query_t query;
  memset(&query, 0, sizeof(query));

  // 2) Set the measurement name
  query.measurement_name = "weather";

  // 3) Tag filtering: same tags as inserted by app_main()
  static const char* tag_keys[] = {"suburb", "city"};
  static const char* tag_values[] = {"beldon", "perth"};
  query.tag_keys = tag_keys;
  query.tag_values = tag_values;
  query.num_tags = 2;

  // 4) Field filtering. If you set num_fields = 0, it means "ALL fields".
  //    But here, let's explicitly request the 3 known fields.
  static const char* fields[] = {"temperature", "status", "device_id"};
  query.field_names = fields;  // fields;
  query.num_fields = 1;

  // 5) Limit: maximum total data points across all returned columns.
  //    e.g. 50 means we won't read more than 50 total points among all fields.
  query.limit = 250;

  query.start_ms = 0;
  query.end_ms = 2737381864000ULL;

  // Prepare the result structure
  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));

  // Execute the query
  int64_t start_time = esp_timer_get_time();
  bool success = timeseries_query(&query, &result);
  int64_t end_time = esp_timer_get_time();

  ESP_LOGI(TAG, "Query took %lld ms", (end_time - start_time) / 1000);

  if (!success) {
    ESP_LOGE(TAG, "Query execution failed!");
    return;
  }

  if (result.num_points == 0 || result.num_columns == 0) {
    ESP_LOGI(TAG, "Query returned no points.");
    // Free the result and return
    timeseries_query_free_result(&result);
    return;
  }

  // Print the results
  ESP_LOGI(TAG, "Query returned %u points and %u columns", (unsigned)result.num_points, (unsigned)result.num_columns);

  // Print timestamps first
  for (size_t r = 0; r < result.num_points; r++) {
    ESP_LOGI(TAG, "Row %u: timestamp = %llu", (unsigned)r, (unsigned long long)result.timestamps[r]);
    // For each column, print the value
    for (size_t c = 0; c < result.num_columns; c++) {
      timeseries_query_result_column_t* col = &result.columns[c];
      timeseries_field_value_t* val = &col->values[r];

      // Print the column name + value
      switch (val->type) {
        case TIMESERIES_FIELD_TYPE_FLOAT:
          ESP_LOGI(TAG, "  Column '%s': float=%.3f", col->name, val->data.float_val);
          break;
        case TIMESERIES_FIELD_TYPE_INT:
          ESP_LOGI(TAG, "  Column '%s': int=%lld", col->name, val->data.int_val);
          break;
        case TIMESERIES_FIELD_TYPE_BOOL:
          ESP_LOGI(TAG, "  Column '%s': bool=%s", col->name, val->data.bool_val ? "true" : "false");
          break;
        case TIMESERIES_FIELD_TYPE_STRING:
          ESP_LOGI(TAG, "  Column '%s': string=%s", col->name, val->data.string_val.str);
          break;
        default:
          ESP_LOGI(TAG, "  Column '%s': unknown/unsupported type %d", col->name, (int)val->type);
          break;
      }
    }
  }

  // Finally, free the result memory
  timeseries_query_free_result(&result);
}

void app_main(void) {
  // 1) Initialize TSDB
  if (!timeseries_init()) {
    ESP_LOGE(TAG, "Timeseries DB init failed!");
    return;
  }

  // ------------------------------------------------------------------
  // Constants shared by every write
  // ------------------------------------------------------------------
  const char* tag_keys[] = {"suburb", "city"};
  const char* tag_values[] = {"beldon", "perth"};
  const size_t NUM_TAGS = 2;

  const char* field_names[] = {"temperature", "status", "device_id"};
  const size_t NUM_FIELDS = 3;

  // ------------------------------------------------------------------
  // Continuous writer – 10 points every cycle, forever
  // ------------------------------------------------------------------
  static const size_t BATCH_N = 10;
  static const uint32_t WRITE_PERIOD_MS = 10;  // 30 s

  uint64_t last_ts = 1737381865010ULL;  // strictly > last point above
  uint64_t inserted_values = 0;         // for logging
  size_t batch_no = 0;
  float prev_value = 0.0f;  // for temperature field

  for (;;) {
    vTaskDelay(pdMS_TO_TICKS(WRITE_PERIOD_MS));

    /* --- build one batch of 10 points --- */
    uint64_t timestamps[BATCH_N];
    timeseries_field_value_t field_values[NUM_FIELDS * BATCH_N];

    for (size_t i = 0; i < BATCH_N; i++) {
      /*  Guarantee monotonic, non-overlapping timestamps:
          - leave a small random gap in front of each batch,
          - then step 1 ms per point inside the batch.
      */
      if (i == 0) {
        uint32_t gap = (rand() % 100) + 1;  // 1-100 ms
        last_ts += gap;
      } else {
        last_ts += 1;  // 1 ms apart in-batch
      }
      timestamps[i] = last_ts;

      /* Field 0: temperature (float) */
      size_t idx_temp = 0 * BATCH_N + i;
      field_values[idx_temp].type = TIMESERIES_FIELD_TYPE_FLOAT;
      // add random small amount to prev_value
      prev_value += ((rand() % 100) / 100.0f) - 0.5f;  // random float in [-0.5, +0.5]
      field_values[idx_temp].data.float_val = prev_value;

      /* Field 1: status (bool) */
      size_t idx_status = 1 * BATCH_N + i;
      field_values[idx_status].type = TIMESERIES_FIELD_TYPE_FLOAT;
      field_values[idx_status].data.bool_val = prev_value;

      /* Field 2: device_id (int) */
      size_t idx_dev = 2 * BATCH_N + i;
      field_values[idx_dev].type = TIMESERIES_FIELD_TYPE_FLOAT;
      field_values[idx_dev].data.int_val = prev_value;
    }

    timeseries_insert_data_t insert = {
        .measurement_name = "weather",
        .tag_keys = tag_keys,
        .tag_values = tag_values,
        .num_tags = NUM_TAGS,

        .field_names = field_names,
        .field_values = field_values,
        .num_fields = NUM_FIELDS,

        .timestamps_ms = timestamps,
        .num_points = BATCH_N,
    };

    if (!timeseries_insert(&insert)) {
      ESP_LOGE(TAG, "Failed to insert %u-point batch #%u!", (unsigned)BATCH_N, (unsigned)batch_no);
    } else {
      inserted_values += 3 * BATCH_N;

      ESP_LOGI(TAG, "Batch #%u inserted, total points %llu", (unsigned)batch_no, inserted_values);
    }
    batch_no++;
  }
}
