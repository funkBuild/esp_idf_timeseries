#include "esp_log.h"
#include "timeseries.h"
#include "unity.h"
#include <string.h>

#include "esp_heap_trace.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#define NUM_RECORDS 100
static heap_trace_record_t
    trace_record[NUM_RECORDS]; // This buffer must be in internal RAM

// Final test case to tear down
TEST_CASE("initializes the timeseries db", "[esp_idf_timeseries]") {
  ESP_ERROR_CHECK(heap_trace_init_standalone(trace_record, NUM_RECORDS));
  TEST_ASSERT_TRUE(timeseries_init());
}

TEST_CASE("clears the existing data", "[esp_idf_timeseries]") {
  TEST_ASSERT_TRUE(timeseries_clear_all());
}

TEST_CASE("query returns nothing with an empty db", "[esp_idf_timeseries]") {
  timeseries_query_t query;
  memset(&query, 0, sizeof(query));

  // 2) Set the measurement name
  query.measurement_name = "weather";
  query.tag_keys = NULL;
  query.tag_values = NULL;
  query.num_tags = 0;
  query.num_fields = 0;
  query.limit = 250;
  query.start_ms = 0;
  query.end_ms = 2737381864000ULL;

  // Prepare the result structure
  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));

  // Execute the query
  bool success = timeseries_query(&query, &result);
  TEST_ASSERT_TRUE(success);
  TEST_ASSERT_TRUE(result.num_points == 0);
  TEST_ASSERT_TRUE(result.num_columns == 0);

  // Finally, free the result memory
  timeseries_query_free_result(&result);
}

TEST_CASE("Inserts a small amount of data", "[esp_idf_timeseries]") {
  const char *tags_keys[] = {"suburb", "city"};
  const char *tags_values[] = {"beldon", "perth"};
  size_t num_tags = 2;

  const char *field_names[] = {"temperature", "valid", "status"};
  size_t num_fields = 3;

  // ---------------------------------------------------------
  // First batch of data (10 points) inserted just once
  // ---------------------------------------------------------

  const size_t NUM_POINTS_1 = 10;
  uint64_t timestamps[NUM_POINTS_1];
  // We need 'num_fields * NUM_POINTS_1' field values, in row-major form:
  // field_values[ field_index * num_points + point_index ]
  timeseries_field_value_t field_values[num_fields * NUM_POINTS_1];

  // Fill the arrays for i in [0..9]
  for (size_t i = 0; i < NUM_POINTS_1; i++) {
    timestamps[i] = 1000 * i;

    size_t idx_temp = 0 * NUM_POINTS_1 + i;
    field_values[idx_temp].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[idx_temp].data.float_val = 1.23f * (float)i;

    size_t idx_status = 1 * NUM_POINTS_1 + i;
    field_values[idx_status].type = TIMESERIES_FIELD_TYPE_BOOL;
    field_values[idx_status].data.bool_val = ((int)i % 2 == 0);

    size_t idx_device = 2 * NUM_POINTS_1 + i;
    field_values[idx_device].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[idx_device].data.int_val = (int)i * 4;
  }

  // Prepare the insert descriptor
  timeseries_insert_data_t insert_data1 = {
      .measurement_name = "weather",
      .tag_keys = tags_keys,
      .tag_values = tags_values,
      .num_tags = num_tags,

      .field_names = field_names,
      .field_values = field_values,
      .num_fields = num_fields,

      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS_1,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data1));
}

TEST_CASE("Can query the data with no aggregation", "[esp_idf_timeseries]") {
  timeseries_query_t query;
  memset(&query, 0, sizeof(query));

  // 2) Set the measurement name
  query.measurement_name = "weather";
  query.tag_keys = NULL;
  query.tag_values = NULL;
  query.num_tags = 0;
  query.num_fields = 0;
  query.limit = 250;
  query.start_ms = 0;
  query.end_ms = 2737381864000ULL;
  query.rollup_interval = 0;

  // Prepare the result structure
  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));

  // Execute the query
  bool success = timeseries_query(&query, &result);
  TEST_ASSERT_TRUE(success);

  ESP_LOGI("TAG", "Num points: %d", result.num_points);
  ESP_LOGI("TAG", "Num columns: %d", result.num_columns);

  TEST_ASSERT_TRUE(result.num_points == 10);
  TEST_ASSERT_TRUE(result.num_columns == 3);

  // Ensure the data is correct
  for (size_t i = 0; i < 10; i++) {
    TEST_ASSERT_TRUE(result.timestamps[i] == 1000 * i);
    TEST_ASSERT_TRUE(result.columns[0].values[i].data.float_val == 1.23f * i);
    TEST_ASSERT_TRUE(result.columns[1].values[i].data.bool_val ==
                     ((int)i % 2 == 0));
    TEST_ASSERT_TRUE(result.columns[2].values[i].data.int_val == (int)i * 4);
  }

  // Finally, free the result memory
  timeseries_query_free_result(&result);
}
/*
TEST_CASE("Can query the data with aggregation", "[esp_idf_timeseries]") {
  timeseries_query_t query;
  memset(&query, 0, sizeof(query));

  // 2) Set the measurement name
  query.measurement_name = "weather";
  query.tag_keys = NULL;
  query.tag_values = NULL;
  query.num_tags = 0;
  query.num_fields = 0;
  query.limit = 250;
  query.start_ms = 0;
  query.end_ms = 2737381864000ULL;
  query.rollup_interval = 2000;

  // Prepare the result structure
  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));

  // Execute the query
  bool success = timeseries_query(&query, &result);
  TEST_ASSERT_TRUE(success);

  ESP_LOGI("TAG", "Num points: %d", result.num_points);
  ESP_LOGI("TAG", "Num columns: %d", result.num_columns);

  TEST_ASSERT_TRUE(result.num_points == 5);
  TEST_ASSERT_TRUE(result.num_columns == 3);

  // Ensure the data is correct
  for (size_t i = 0; i < 5; i++) {
    TEST_ASSERT_TRUE(result.timestamps[i] == 2000 * i);

    int start_index = i * 2;
    double expected_value = (1.23f * start_index) + (1.23f * (start_index + 1));
    expected_value /= 2;

    ESP_LOGI("TAG", "Expected value: %f, actual value: %f", expected_value,
             result.columns[0].values[i].data.float_val);

    TEST_ASSERT_TRUE(result.columns[0].values[i].type ==
                     TIMESERIES_FIELD_TYPE_FLOAT);
    TEST_ASSERT_TRUE(result.columns[1].values[i].type ==
                     TIMESERIES_FIELD_TYPE_FLOAT);
    TEST_ASSERT_TRUE(result.columns[2].values[i].type ==
                     TIMESERIES_FIELD_TYPE_FLOAT);

    TEST_ASSERT_TRUE(fabs(result.columns[0].values[i].data.float_val -
                          expected_value) < 0.0001);

    TEST_ASSERT_TRUE(result.columns[1].values[i].data.float_val == 0.5f);

    // Test for the average integer value
    double expected_value_int =
        ((float)(2 * i) * 4 + (float)(2 * i + 1) * 4) / 2;

    TEST_ASSERT_TRUE(fabs(result.columns[2].values[i].data.float_val -
                          expected_value_int) < 0.0001);
  }

  // Finally, free the result memory
  timeseries_query_free_result(&result);
}

TEST_CASE("Can query the data with strings", "[esp_idf_timeseries]") {
  const char *measurement_name = "logs";
  const char *tags_keys[] = {"severity", "deviceId"};
  const char *tags_values[] = {"info", "abcdefgh"};
  size_t num_tags = 2;

  const char *field_names[] = {"message"};
  size_t num_fields = 1;

  // ---------------------------------------------------------
  // First batch of data (10 points) inserted just once
  // ---------------------------------------------------------

  const size_t NUM_POINTS_1 = 10;
  uint64_t timestamps[NUM_POINTS_1];
  // We need 'num_fields * NUM_POINTS_1' field values, in row-major form:
  // field_values[ field_index * num_points + point_index ]
  timeseries_field_value_t field_values[num_fields * NUM_POINTS_1];

  // Fill the arrays for i in [0..9]
  for (size_t i = 0; i < NUM_POINTS_1; i++) {
    timestamps[i] = 1000 * i;

    size_t idx_message = i;

    char message[100];
    snprintf(message, sizeof(message), "Hello, world! %d", i);

    field_values[idx_message].type = TIMESERIES_FIELD_TYPE_STRING;
    field_values[idx_message].data.string_val.str = strdup(message);
    field_values[idx_message].data.string_val.length = strlen(message);
  }

  // Prepare the insert descriptor
  timeseries_insert_data_t insert_data1 = {
      .measurement_name = measurement_name,
      .tag_keys = tags_keys,
      .tag_values = tags_values,
      .num_tags = num_tags,

      .field_names = field_names,
      .field_values = field_values,
      .num_fields = num_fields,

      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS_1,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data1));

  // Clean up the string values
  for (size_t i = 0; i < NUM_POINTS_1; i++) {
    free(field_values[i].data.string_val.str);
  }

  timeseries_compact_sync();

  // ---------------------------------------------------------

  timeseries_query_t query;
  memset(&query, 0, sizeof(query));

  // Set the measurement name
  query.measurement_name = "logs";
  query.tag_keys = NULL;
  query.tag_values = NULL;
  query.num_tags = 0;
  query.num_fields = 0;
  query.limit = 250;
  query.start_ms = 0;
  query.end_ms = 2737381864000ULL;
  query.rollup_interval = 0; // No aggregation

  // Prepare the result structure
  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));

  // Execute the query
  bool success = timeseries_query(&query, &result);
  TEST_ASSERT_TRUE(success);

  ESP_LOGI("TAG", "Num points: %d", result.num_points);
  ESP_LOGI("TAG", "Num columns: %d", result.num_columns);

  TEST_ASSERT_TRUE(result.num_points == 10);
  TEST_ASSERT_TRUE(result.num_columns == 1);

  // Ensure the string data is correct
  for (size_t i = 0; i < 10; i++) {
    char message[100];
    snprintf(message, sizeof(message), "Hello, world! %d", i);

    ESP_LOGI("TAG", "String value: %.*s",
             (int)result.columns[0].values[i].data.string_val.length,
             result.columns[0].values[i].data.string_val.str);

    ESP_LOGI("TAG", "Timestamp: %llu", result.timestamps[i]);

    TEST_ASSERT_TRUE(result.timestamps[i] == 1000 * i);
    TEST_ASSERT_TRUE(result.columns[0].values[i].type ==
                     TIMESERIES_FIELD_TYPE_STRING);
    TEST_ASSERT_EQUAL_STRING_LEN(
        message, result.columns[0].values[i].data.string_val.str,
        result.columns[0].values[i].data.string_val.length);
  }

  // Finally, free the result memory
  timeseries_query_free_result(&result);
}

TEST_CASE("Can store a large amount of unique strings",
          "[esp_idf_timeseries]") {
  // Insert 1000 unique strings into the database that will trigger a compaction
  const size_t NUM_POINTS = 10000;

  for (size_t i = 0; i < NUM_POINTS; i++) {
    const char *tags_keys[] = {"severity", "deviceId"};
    const char *tags_values[] = {"info", "abcdefgh"};
    size_t num_tags = 2;

    const char *field_names[] = {"message"};
    size_t num_fields = 1;

    uint64_t timestamps[1];
    timestamps[0] = 1000 * i;

    timeseries_field_value_t field_values[1];
    field_values[0].type = TIMESERIES_FIELD_TYPE_STRING;

    // Generate a unique string using the loop index
    char unique_message[100];
    snprintf(unique_message, sizeof(unique_message), "Unique test string %zu",
             i);

    field_values[0].data.string_val.str = strdup(unique_message);
    field_values[0].data.string_val.length = strlen(unique_message);

    // Prepare the insert descriptor
    timeseries_insert_data_t insert_data = {
        .measurement_name = "logs",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = num_tags,

        .field_names = field_names,
        .field_values = field_values,
        .num_fields = num_fields,

        .timestamps_ms = timestamps,
        .num_points = 1,
    };

    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

    // Clean up the string values
    free(field_values[0].data.string_val.str);
  }

  // Test query to ensure the data is correct
  timeseries_query_t query;
  memset(&query, 0, sizeof(query));

  // Set the measurement name
  query.measurement_name = "logs";
  query.tag_keys = NULL;
  query.tag_values = NULL;
  query.num_tags = 0;
  query.num_fields = 0;
  query.limit = 10;

  query.start_ms = 0;
  query.end_ms = 2737381864000ULL;

  // Prepare the result structure
  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));

  // Execute the query
  bool success = timeseries_query(&query, &result);

  TEST_ASSERT_TRUE(success);

  ESP_LOGI("TAG", "Num points: %d", result.num_points);

  TEST_ASSERT_TRUE(result.num_points == 10);

  // Ensure the string data is correct

  for (size_t i = 0; i < 10; i++) {
    char unique_message[100];
    snprintf(unique_message, sizeof(unique_message), "Unique test string %zu",
             i);

    ESP_LOGI("TAG", "String value: %.*s",
             (int)result.columns[0].values[i].data.string_val.length,
             result.columns[0].values[i].data.string_val.str);

    ESP_LOGI("TAG", "Timestamp: %llu", result.timestamps[i]);

    TEST_ASSERT_TRUE(result.columns[0].values[i].type ==
                     TIMESERIES_FIELD_TYPE_STRING);
    TEST_ASSERT_EQUAL_STRING_LEN(
        unique_message, result.columns[0].values[i].data.string_val.str,
        result.columns[0].values[i].data.string_val.length);
  }
}

TEST_CASE("Can store a large amount of unique floats", "[esp_idf_timeseries]") {
  // Insert 10000 unique floats into the database that will trigger a compaction
  const size_t NUM_POINTS = 2000;

  ESP_ERROR_CHECK(heap_trace_start(HEAP_TRACE_LEAKS));

  for (size_t i = 0; i < NUM_POINTS; i++) {
    const char *tags_keys[] = {"severity", "deviceId"};
    const char *tags_values[] = {"info", "abcdefgh"};
    size_t num_tags = 2;

    const char *field_names[] = {"temperature"};
    size_t num_fields = 1;

    uint64_t timestamps[1];
    timestamps[0] = 1000 * i;

    timeseries_field_value_t field_values[1];
    field_values[0].type = TIMESERIES_FIELD_TYPE_FLOAT;

    field_values[0].data.float_val = (float)i;

    // Prepare the insert descriptor
    timeseries_insert_data_t insert_data = {
        .measurement_name = "logs",
        .tag_keys = tags_keys,
        .tag_values = tags_values,
        .num_tags = num_tags,

        .field_names = field_names,
        .field_values = field_values,
        .num_fields = num_fields,

        .timestamps_ms = timestamps,
        .num_points = 1,
    };

    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
  }

  vTaskDelay(1000 / portTICK_PERIOD_MS);

  ESP_ERROR_CHECK(heap_trace_stop());
  heap_trace_dump();
}

*/