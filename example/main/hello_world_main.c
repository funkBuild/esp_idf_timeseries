#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "timeseries.h"
#include "timeseries_query.h"
#include <stdio.h>

#include "esp_timer.h"

static const char *TAG = "example";

void example_query_weather_data(void) {
  // Build a timeseries_query_t for "weather"
  timeseries_query_t query;
  memset(&query, 0, sizeof(query));

  // 2) Set the measurement name
  query.measurement_name = "weather";

  // 3) Tag filtering: same tags as inserted by app_main()
  static const char *tag_keys[] = {"suburb", "city"};
  static const char *tag_values[] = {"beldon", "perth"};
  query.tag_keys = tag_keys;
  query.tag_values = tag_values;
  query.num_tags = 2;

  // 4) Field filtering. If you set num_fields = 0, it means "ALL fields".
  //    But here, let's explicitly request the 3 known fields.
  static const char *fields[] = {"temperature", "status", "device_id"};
  query.field_names = fields; // fields;
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
  ESP_LOGI(TAG, "Query returned %u points and %u columns",
           (unsigned)result.num_points, (unsigned)result.num_columns);

  // Print timestamps first
  for (size_t r = 0; r < result.num_points; r++) {
    ESP_LOGI(TAG, "Row %u: timestamp = %llu", (unsigned)r,
             (unsigned long long)result.timestamps[r]);
    // For each column, print the value
    for (size_t c = 0; c < result.num_columns; c++) {
      timeseries_query_result_column_t *col = &result.columns[c];
      timeseries_field_value_t *val = &col->values[r];

      // Print the column name + value
      switch (val->type) {
      case TIMESERIES_FIELD_TYPE_FLOAT:
        ESP_LOGI(TAG, "  Column '%s': float=%.3f", col->name,
                 val->data.float_val);
        break;
      case TIMESERIES_FIELD_TYPE_INT:
        ESP_LOGI(TAG, "  Column '%s': int=%lld", col->name, val->data.int_val);
        break;
      case TIMESERIES_FIELD_TYPE_BOOL:
        ESP_LOGI(TAG, "  Column '%s': bool=%s", col->name,
                 val->data.bool_val ? "true" : "false");
        break;
      case TIMESERIES_FIELD_TYPE_STRING:
        ESP_LOGI(TAG, "  Column '%s': string=%s", col->name,
                 val->data.string_val.str);
        break;
      default:
        ESP_LOGI(TAG, "  Column '%s': unknown/unsupported type %d", col->name,
                 (int)val->type);
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

  example_query_weather_data();
  return;

  // Common tags & field definitions
  const char *tags_keys[] = {"suburb", "city"};
  const char *tags_values[] = {"beldon", "perth"};
  size_t num_tags = 2;

  const char *field_names[] = {"temperature", "status", "device_id"};
  size_t num_fields = 3;

  // ---------------------------------------------------------
  // First batch of data (10 points) inserted just once
  // ---------------------------------------------------------
  {
    const size_t NUM_POINTS_1 = 10;
    uint64_t timestamps1[NUM_POINTS_1];
    // We need 'num_fields * NUM_POINTS_1' field values, in row-major form:
    // field_values[ field_index * num_points + point_index ]
    timeseries_field_value_t field_values1[num_fields * NUM_POINTS_1];

    // Fill the arrays for i in [0..9]
    for (size_t i = 0; i < NUM_POINTS_1; i++) {
      timestamps1[i] = 1737381864000ULL + i; // some fake epoch ms

      // Field 0: "temperature" (float)
      size_t idx_temp = 0 * NUM_POINTS_1 + i;
      field_values1[idx_temp].type = TIMESERIES_FIELD_TYPE_FLOAT;
      field_values1[idx_temp].data.float_val = 1.23f * (float)i;

      // Field 1: "status" (bool)
      size_t idx_status = 1 * NUM_POINTS_1 + i;
      field_values1[idx_status].type = TIMESERIES_FIELD_TYPE_BOOL;
      field_values1[idx_status].data.bool_val = ((int)i % 2 == 0);

      // Field 2: "device_id" (int)
      size_t idx_device = 2 * NUM_POINTS_1 + i;
      field_values1[idx_device].type = TIMESERIES_FIELD_TYPE_INT;
      field_values1[idx_device].data.int_val = (int)i * 4;
    }

    // Prepare the insert descriptor
    timeseries_insert_data_t insert_data1 = {
        .measurement_name = "weather",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = num_tags,

        .field_names = field_names,
        .field_values = field_values1,
        .num_fields = num_fields,

        .timestamps_ms = timestamps1,
        .num_points = NUM_POINTS_1,
    };

    if (!timeseries_insert(&insert_data1)) {
      ESP_LOGE(TAG, "First multi-point insert failed!");
      return;
    }
    ESP_LOGI(TAG, "First batch of 10 data points inserted successfully.");

    // Optionally do a compaction pass here
    ESP_LOGI(TAG, "Starting initial compaction...");
    if (!timeseries_compact()) {
      ESP_LOGE(TAG, "Timeseries compaction failed!");
    } else {
      ESP_LOGI(TAG, "Initial compaction finished successfully.");
    }
  }

  // ---------------------------------------------------------
  // 2) Continuously write new data points every 30 seconds
  // ---------------------------------------------------------
  ESP_LOGI(TAG, "Now continuously writing new points every 30 seconds...");

  // We'll keep an incrementing index to vary the data
  uint64_t base_ts = 1737381865000ULL; // arbitrary start
  size_t i = 0;

  for (;;) {
    // Wait 30 seconds between each write
    vTaskDelay(pdMS_TO_TICKS(1));

    // Build a single point (timestamp + 3 fields)
    unsigned int random_increment = (rand() % 10000) + 50000;
    base_ts += random_increment;

    timeseries_field_value_t single_point_fields[3];

    // Field 0: temperature
    single_point_fields[0].type = TIMESERIES_FIELD_TYPE_FLOAT;
    single_point_fields[0].data.float_val = 0.5f * i;

    // Field 1: status (bool)
    single_point_fields[1].type = TIMESERIES_FIELD_TYPE_BOOL;
    single_point_fields[1].data.bool_val = (i % 2 == 0);

    // Field 2: device_id (int)
    single_point_fields[2].type = TIMESERIES_FIELD_TYPE_INT;
    single_point_fields[2].data.int_val = (int)(100 + random_increment);

    // Prepare insert descriptor for 1 point
    timeseries_insert_data_t insert_data = {
        .measurement_name = "weather",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = num_tags,

        .field_names = field_names,
        .field_values = single_point_fields,
        .num_fields = num_fields,

        .timestamps_ms = &base_ts,
        .num_points = 1, // single point
    };

    if (!timeseries_insert(&insert_data)) {
      ESP_LOGE(TAG, "Failed to insert single-point data (i=%u)!", (unsigned)i);

      while (1) {
        vTaskDelay(pdMS_TO_TICKS(1000));
      }
    }

    if (i % 100 == 0) {
      ESP_LOGE(TAG, "Inserted point #%u at time=%llu ms", (unsigned)i,
               (unsigned long long)base_ts);

      timeseries_expire();
    }

    i++;
  }
}
