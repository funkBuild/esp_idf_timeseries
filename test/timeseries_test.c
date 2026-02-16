#include "esp_log.h"
#include "unity.h"
#include <string.h>
#include "float.h"

#include "timeseries.h"
#include "timeseries_query_parser.h"

#include "esp_heap_trace.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_timer.h"

#define NUM_RECORDS 100
static heap_trace_record_t trace_record[NUM_RECORDS];  // This buffer must be in internal RAM

static const char* TAG = "timeseries_test";

// Final test case to tear down
TEST_CASE("initializes the timeseries db", "[esp_idf_timeseries]") {
  ESP_ERROR_CHECK(heap_trace_init_standalone(trace_record, NUM_RECORDS));
  TEST_ASSERT_TRUE(timeseries_init());
}

TEST_CASE("clears the existing data", "[esp_idf_timeseries]") { TEST_ASSERT_TRUE(timeseries_clear_all()); }

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

  // Check again after compaction
  timeseries_compact();

  // Query again, should still return nothing
  success = timeseries_query(&query, &result);
  TEST_ASSERT_TRUE(success);
  TEST_ASSERT_TRUE(result.num_points == 0);
  TEST_ASSERT_TRUE(result.num_columns == 0);
}

TEST_CASE("Inserts a small amount of data", "[esp_idf_timeseries]") {
  const char* tags_keys[] = {"suburb", "city"};
  const char* tags_values[] = {"beldon", "perth"};
  size_t num_tags = 2;

  const char* field_names[] = {"temperature", "valid", "status"};
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
  TEST_ASSERT_TRUE(result.num_points == 10);
  TEST_ASSERT_TRUE(result.num_columns == 3);

  // Ensure the data is correct
  for (size_t i = 0; i < 10; i++) {
    TEST_ASSERT_TRUE(result.timestamps[i] == 1000 * i);
    TEST_ASSERT_TRUE(result.columns[0].values[i].data.float_val == 1.23f * i);
    TEST_ASSERT_TRUE(result.columns[1].values[i].data.bool_val == ((int)i % 2 == 0));
    TEST_ASSERT_TRUE(result.columns[2].values[i].data.int_val == (int)i * 4);
  }

  // Finally, free the result memory
  timeseries_query_free_result(&result);

  // Check again after compaction
  timeseries_compact();

  // Query again, should still return the same data
  success = timeseries_query(&query, &result);
  TEST_ASSERT_TRUE(success);
  TEST_ASSERT_TRUE(result.num_points == 10);
  TEST_ASSERT_TRUE(result.num_columns == 3);
}

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

    ESP_LOGI("TAG", "Expected value: %f, actual value: %f", expected_value, result.columns[0].values[i].data.float_val);

    TEST_ASSERT_TRUE(result.columns[0].values[i].type == TIMESERIES_FIELD_TYPE_FLOAT);
    TEST_ASSERT_TRUE(result.columns[1].values[i].type == TIMESERIES_FIELD_TYPE_FLOAT);
    TEST_ASSERT_TRUE(result.columns[2].values[i].type == TIMESERIES_FIELD_TYPE_FLOAT);

    TEST_ASSERT_TRUE(fabs(result.columns[0].values[i].data.float_val - expected_value) < 0.0001);

    TEST_ASSERT_TRUE(result.columns[1].values[i].data.float_val == 0.5f);

    // Test for the average integer value
    double expected_value_int = ((float)(2 * i) * 4 + (float)(2 * i + 1) * 4) / 2;

    TEST_ASSERT_TRUE(fabs(result.columns[2].values[i].data.float_val - expected_value_int) < 0.0001);
  }

  // Finally, free the result memory
  timeseries_query_free_result(&result);

  // Check again after compaction
  timeseries_compact();

  // Query again, should still return the same data
  success = timeseries_query(&query, &result);
  TEST_ASSERT_TRUE(success);
  TEST_ASSERT_TRUE(result.num_points == 5);
  TEST_ASSERT_TRUE(result.num_columns == 3);

  // Ensure the data is still correct
  for (size_t i = 0; i < 5; i++) {
    TEST_ASSERT_TRUE(result.timestamps[i] == 2000 * i);

    int start_index = i * 2;
    double expected_value = (1.23f * start_index) + (1.23f * (start_index + 1));
    expected_value /= 2;

    ESP_LOGI("TAG", "Expected value: %f, actual value: %f", expected_value, result.columns[0].values[i].data.float_val);

    TEST_ASSERT_TRUE(result.columns[0].values[i].type == TIMESERIES_FIELD_TYPE_FLOAT);
    TEST_ASSERT_TRUE(result.columns[1].values[i].type == TIMESERIES_FIELD_TYPE_FLOAT);
    TEST_ASSERT_TRUE(result.columns[2].values[i].type == TIMESERIES_FIELD_TYPE_FLOAT);

    TEST_ASSERT_TRUE(fabs(result.columns[0].values[i].data.float_val - expected_value) < 0.0001);

    TEST_ASSERT_TRUE(result.columns[1].values[i].data.float_val == 0.5f);

    // Test for the average integer value
    double expected_value_int = ((float)(2 * i) * 4 + (float)(2 * i + 1) * 4) / 2;

    TEST_ASSERT_TRUE(fabs(result.columns[2].values[i].data.float_val - expected_value_int) < 0.0001);
  }

  // Finally, free the result memory
  timeseries_query_free_result(&result);
}

TEST_CASE("Can handle mixed field types in single measurement", "[esp_idf_timeseries]") {
  const char* tags_keys[] = {"sensor_type", "location"};
  const char* tags_values[] = {"multi_sensor", "lab_a"};
  size_t num_tags = 2;

  const char* field_names[] = {"temperature", "pressure", "is_active", "status_message"};
  size_t num_fields = 4;

  const size_t NUM_POINTS = 5;
  uint64_t timestamps[NUM_POINTS];
  timeseries_field_value_t field_values[num_fields * NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 1000 * i;

    // Float field
    size_t idx_temp = 0 * NUM_POINTS + i;
    field_values[idx_temp].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[idx_temp].data.float_val = 20.5f + (float)i * 0.1f;

    // Int field
    size_t idx_pressure = 1 * NUM_POINTS + i;
    field_values[idx_pressure].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[idx_pressure].data.int_val = 1013 + (int)i;

    // Bool field
    size_t idx_active = 2 * NUM_POINTS + i;
    field_values[idx_active].type = TIMESERIES_FIELD_TYPE_BOOL;
    field_values[idx_active].data.bool_val = (i != 2);  // false at index 2

    // String field
    size_t idx_status = 3 * NUM_POINTS + i;
    char status[50];
    snprintf(status, sizeof(status), "Status_%zu", i);
    field_values[idx_status].type = TIMESERIES_FIELD_TYPE_STRING;
    field_values[idx_status].data.string_val.str = strdup(status);
    field_values[idx_status].data.string_val.length = strlen(status);
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = "sensor_data",
      .tag_keys = tags_keys,
      .tag_values = tags_values,
      .num_tags = num_tags,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = num_fields,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  // Clean up strings
  for (size_t i = 0; i < NUM_POINTS; i++) {
    free(field_values[3 * NUM_POINTS + i].data.string_val.str);
  }

  // Query and verify
  timeseries_query_t query = {
      .measurement_name = "sensor_data",
      .tag_keys = tags_keys,
      .tag_values = tags_values,
      .num_tags = num_tags,
      .limit = 10,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));
  TEST_ASSERT_EQUAL(NUM_POINTS, result.num_points);
  TEST_ASSERT_EQUAL(num_fields, result.num_columns);

  timeseries_query_free_result(&result);
}

TEST_CASE("Can query with time range filters", "[esp_idf_timeseries]") {
  const char* tags_keys[] = {"device"};
  const char* tags_values[] = {"sensor1"};
  const char* field_names[] = {"value"};

  const size_t NUM_POINTS = 20;
  uint64_t timestamps[NUM_POINTS];
  timeseries_field_value_t field_values[NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 1000 * i;
    field_values[i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[i].data.int_val = (int)i;
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = "time_test",
      .tag_keys = tags_keys,
      .tag_values = tags_values,
      .num_tags = 1,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  // Test 1: Query middle range [5000, 15000)
  timeseries_query_t query = {
      .measurement_name = "time_test",
      .start_ms = 5000,
      .end_ms = 15000,
      .limit = 250,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  // Debug print points
  ESP_LOGI("TAG", "Query result: %d points", result.num_points);
  for (int i = 0; i < result.num_points; i++) {
    ESP_LOGI("TAG", "Point %d: timestamp=%llu, value=%lld", i, result.timestamps[i],
             result.columns[0].values[i].data.int_val);
  }

  // Should get points 5 through 14 (10 points)
  TEST_ASSERT_EQUAL(10, result.num_points);
  TEST_ASSERT_EQUAL(5000, result.timestamps[0]);
  TEST_ASSERT_EQUAL(14000, result.timestamps[9]);

  timeseries_query_free_result(&result);

  // Test 2: Query with only start time
  query.start_ms = 15000;
  query.end_ms = 2737381864000ULL;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  // Should get points 15 through 19 (5 points)
  TEST_ASSERT_EQUAL(5, result.num_points);
  TEST_ASSERT_EQUAL(15000, result.timestamps[0]);

  timeseries_query_free_result(&result);
}

TEST_CASE("Can query with field name filtering", "[esp_idf_timeseries]") {
  const char* tags_keys[] = {"type"};
  const char* tags_values[] = {"multi"};
  const char* field_names[] = {"field_a", "field_b", "field_c"};

  const size_t NUM_POINTS = 3;
  uint64_t timestamps[NUM_POINTS];
  timeseries_field_value_t field_values[3 * NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 1000 * i;

    field_values[0 * NUM_POINTS + i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[0 * NUM_POINTS + i].data.int_val = (int)i * 10;

    field_values[1 * NUM_POINTS + i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[1 * NUM_POINTS + i].data.float_val = (float)i * 1.5f;

    field_values[2 * NUM_POINTS + i].type = TIMESERIES_FIELD_TYPE_BOOL;
    field_values[2 * NUM_POINTS + i].data.bool_val = (i % 2 == 0);
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = "field_test",
      .tag_keys = tags_keys,
      .tag_values = tags_values,
      .num_tags = 1,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 3,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  // Query only specific fields
  const char* query_fields[] = {"field_a", "field_c"};
  timeseries_query_t query = {
      .measurement_name = "field_test",
      .field_names = query_fields,
      .num_fields = 2,
      .limit = 10,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(NUM_POINTS, result.num_points);
  TEST_ASSERT_EQUAL(2, result.num_columns);  // Should only get requested fields

  timeseries_query_free_result(&result);
}

TEST_CASE("Can handle empty strings", "[esp_idf_timeseries]") {
  const char* tags_keys[] = {"source"};
  const char* tags_values[] = {"test"};
  const char* field_names[] = {"message"};

  timeseries_field_value_t field_values[3];
  uint64_t timestamps[3] = {1000, 2000, 3000};

  // Normal string
  field_values[0].type = TIMESERIES_FIELD_TYPE_STRING;
  field_values[0].data.string_val.str = strdup("Hello");
  field_values[0].data.string_val.length = 5;

  // Empty string
  field_values[1].type = TIMESERIES_FIELD_TYPE_STRING;
  field_values[1].data.string_val.str = strdup("");
  field_values[1].data.string_val.length = 0;

  // Another normal string
  field_values[2].type = TIMESERIES_FIELD_TYPE_STRING;
  field_values[2].data.string_val.str = strdup("World");
  field_values[2].data.string_val.length = 5;

  timeseries_insert_data_t insert_data = {
      .measurement_name = "string_test",
      .tag_keys = tags_keys,
      .tag_values = tags_values,
      .num_tags = 1,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = 3,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  for (int i = 0; i < 3; i++) {
    free(field_values[i].data.string_val.str);
  }

  // Query and verify
  timeseries_query_t query = {
      .measurement_name = "string_test",
      .limit = 10,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(3, result.num_points);
  TEST_ASSERT_EQUAL(0, result.columns[0].values[1].data.string_val.length);

  timeseries_query_free_result(&result);
}

TEST_CASE("Can handle multiple measurements", "[esp_idf_timeseries]") {
  // Insert data into different measurements
  const char* measurements[] = {"cpu_metrics", "memory_metrics", "disk_metrics"};

  for (int m = 0; m < 3; m++) {
    const char* field_names[] = {"usage"};
    timeseries_field_value_t field_values[5];
    uint64_t timestamps[5];

    for (int i = 0; i < 5; i++) {
      timestamps[i] = 1000 * i;
      field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
      field_values[i].data.float_val = (float)(m * 10 + i);
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = measurements[m],
        .tag_keys = NULL,
        .tag_values = NULL,
        .num_tags = 0,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = 5,
    };

    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
  }

  // Query each measurement separately
  for (int m = 0; m < 3; m++) {
    timeseries_query_t query = {
        .measurement_name = measurements[m],
        .limit = 10,
        .start_ms = 0,
        .end_ms = 2737381864000ULL,
    };

    timeseries_query_result_t result;
    memset(&result, 0, sizeof(result));
    TEST_ASSERT_TRUE(timeseries_query(&query, &result));

    TEST_ASSERT_EQUAL(5, result.num_points);
    // Verify first value is correct for each measurement
    TEST_ASSERT_EQUAL_FLOAT((float)(m * 10), result.columns[0].values[0].data.float_val);

    timeseries_query_free_result(&result);
  }
}

TEST_CASE("Can query with complex tag filtering", "[esp_idf_timeseries]") {
  // Insert data with different tag combinations
  const char* field_names[] = {"value"};

  // Dataset 1: region=us, env=prod
  const char* tags1_keys[] = {"region", "env"};
  const char* tags1_values[] = {"us", "prod"};

  // Dataset 2: region=eu, env=prod
  const char* tags2_keys[] = {"region", "env"};
  const char* tags2_values[] = {"eu", "prod"};

  // Dataset 3: region=us, env=dev
  const char* tags3_keys[] = {"region", "env"};
  const char* tags3_values[] = {"us", "dev"};

  // Insert 3 points for each tag combination
  for (int dataset = 0; dataset < 3; dataset++) {
    const char** tag_values = (dataset == 0) ? tags1_values : (dataset == 1) ? tags2_values : tags3_values;

    timeseries_field_value_t field_values[3];
    uint64_t timestamps[3];

    for (int i = 0; i < 3; i++) {
      timestamps[i] = 1000 * (dataset * 3 + i);
      field_values[i].type = TIMESERIES_FIELD_TYPE_INT;
      field_values[i].data.int_val = dataset * 100 + i;
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "tag_test",
        .tag_keys = tags1_keys,
        .tag_values = tag_values,
        .num_tags = 2,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = 3,
    };

    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
  }

  // Query 1: All data from region=us (should get 6 points)
  const char* query1_keys[] = {"region"};
  const char* query1_values[] = {"us"};
  timeseries_query_t query1 = {
      .measurement_name = "tag_test",
      .tag_keys = query1_keys,
      .tag_values = query1_values,
      .num_tags = 1,
      .limit = 20,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query1, &result));
  TEST_ASSERT_EQUAL(6, result.num_points);  // 3 from us/prod + 3 from us/dev
  timeseries_query_free_result(&result);

  // Query 2: Specific combination region=eu, env=prod (should get 3 points)
  timeseries_query_t query2 = {
      .measurement_name = "tag_test",
      .tag_keys = tags2_keys,
      .tag_values = tags2_values,
      .num_tags = 2,
      .limit = 20,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
  };

  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query2, &result));
  TEST_ASSERT_EQUAL(3, result.num_points);
  TEST_ASSERT_EQUAL(100, result.columns[0].values[0].data.int_val);  // First value from dataset 2
  timeseries_query_free_result(&result);
}

TEST_CASE("Can handle out-of-order timestamp insertion", "[esp_idf_timeseries]") {
  const char* field_names[] = {"counter"};

  // Insert points in non-chronological order
  uint64_t timestamps[] = {5000, 1000, 3000, 7000, 2000};
  int expected_order[] = {1, 4, 2, 0, 3};  // Expected indices after sorting

  timeseries_field_value_t field_values[5];
  for (int i = 0; i < 5; i++) {
    field_values[i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[i].data.int_val = i;  // Store original index as value
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = "order_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = 5,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  // Query and verify chronological order
  timeseries_query_t query = {
      .measurement_name = "order_test",
      .limit = 10,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(5, result.num_points);

  // Print to console
  for (int i = 0; i < result.num_points; i++) {
    ESP_LOGI("Order Test", "Point %d: Timestamp: %llu, Value: %lld", i, result.timestamps[i],
             result.columns[0].values[i].data.int_val);
  }

  // Verify timestamps are in order
  for (int i = 0; i < 4; i++) {
    TEST_ASSERT_TRUE(result.timestamps[i] < result.timestamps[i + 1]);
  }

  // Verify values match expected order
  for (int i = 0; i < 5; i++) {
    TEST_ASSERT_EQUAL(expected_order[i], result.columns[0].values[i].data.int_val);
  }

  timeseries_query_free_result(&result);
}

TEST_CASE("Can test limit functionality", "[esp_idf_timeseries]") {
  const char* field_names[] = {"value"};

  // Insert 100 points
  const size_t NUM_POINTS = 100;
  timeseries_field_value_t field_values[NUM_POINTS];
  uint64_t timestamps[NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 1000 * i;
    field_values[i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[i].data.int_val = (int)i;
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = "limit_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  // Test different limit values
  size_t test_limits[] = {1, 10, 25, 50, 200};

  for (int i = 0; i < 5; i++) {
    timeseries_query_t query = {
        .measurement_name = "limit_test",
        .limit = test_limits[i],
        .start_ms = 0,
        .end_ms = 2737381864000ULL,
    };

    timeseries_query_result_t result;
    memset(&result, 0, sizeof(result));
    TEST_ASSERT_TRUE(timeseries_query(&query, &result));

    size_t expected = (test_limits[i] < NUM_POINTS) ? test_limits[i] : NUM_POINTS;
    TEST_ASSERT_EQUAL(expected, result.num_points);

    // Verify we get the first N points
    if (result.num_points > 0) {
      TEST_ASSERT_EQUAL(0, result.columns[0].values[0].data.int_val);
      if (result.num_points > 1) {
        TEST_ASSERT_EQUAL(result.num_points - 1, result.columns[0].values[result.num_points - 1].data.int_val);
      }
    }

    timeseries_query_free_result(&result);
  }
}

TEST_CASE("Can handle boolean aggregation", "[esp_idf_timeseries]") {
  const char* field_names[] = {"sensor_active", "alarm_triggered"};

  // Create pattern: T,T,F,F,T,T,F,F for sensor_active
  // Create pattern: F,T,T,F,F,T,T,F for alarm_triggered
  const size_t NUM_POINTS = 8;
  timeseries_field_value_t field_values[2 * NUM_POINTS];
  uint64_t timestamps[NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 1000 * i;

    // sensor_active: alternating pairs
    field_values[0 * NUM_POINTS + i].type = TIMESERIES_FIELD_TYPE_BOOL;
    field_values[0 * NUM_POINTS + i].data.bool_val = ((i / 2) % 2) == 0;

    // alarm_triggered: offset pattern
    field_values[1 * NUM_POINTS + i].type = TIMESERIES_FIELD_TYPE_BOOL;
    field_values[1 * NUM_POINTS + i].data.bool_val = ((i + 1) / 2) % 2 == 1;
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = "bool_agg_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 2,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  // Query with rollup of 2 seconds (aggregate pairs)
  timeseries_query_t query = {
      .measurement_name = "bool_agg_test",
      .limit = 10,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
      .rollup_interval = 2000,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(4, result.num_points);  // 8 points aggregated to 4

  // Verify aggregation results (boolean -> float average)
  // First pair (T,T) -> 1.0, (F,T) -> 0.5
  TEST_ASSERT_EQUAL_FLOAT(1.0f, result.columns[0].values[0].data.float_val);
  TEST_ASSERT_EQUAL_FLOAT(0.5f, result.columns[1].values[0].data.float_val);

  // Second pair (F,F) -> 0.0, (T,F) -> 0.5
  TEST_ASSERT_EQUAL_FLOAT(0.0f, result.columns[0].values[1].data.float_val);
  TEST_ASSERT_EQUAL_FLOAT(0.5f, result.columns[1].values[1].data.float_val);

  timeseries_query_free_result(&result);
}

TEST_CASE("Can handle duplicate timestamps", "[esp_idf_timeseries]") {
  const char* field_names[] = {"reading"};

  // Insert multiple values with same timestamp
  uint64_t timestamps[] = {1000, 1000, 2000, 2000, 2000, 3000};
  timeseries_field_value_t field_values[6];

  for (int i = 0; i < 6; i++) {
    field_values[i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[i].data.int_val = i;
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = "dup_time_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = 6,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  // Query without aggregation - should get all points
  timeseries_query_t query = {
      .measurement_name = "dup_time_test",
      .limit = 10,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  // Should only return 3 points, removing duplicates
  TEST_ASSERT_EQUAL(3, result.num_points);

  timeseries_query_free_result(&result);
}

TEST_CASE("Can handle integer aggregation", "[esp_idf_timeseries]") {
  const char* field_names[] = {"integer_positive", "integer_negative"};

  const size_t NUM_POINTS = 8;
  timeseries_field_value_t field_values[2 * NUM_POINTS];
  uint64_t timestamps[NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 1000 * i;

    // sensor_active: alternating pairs
    field_values[0 * NUM_POINTS + i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[0 * NUM_POINTS + i].data.int_val = i;

    // alarm_triggered: offset pattern
    field_values[1 * NUM_POINTS + i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[1 * NUM_POINTS + i].data.int_val = -1 * (int64_t)(i);
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = "int_agg_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 2,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  // Query with rollup of 2 seconds (aggregate pairs)
  timeseries_query_t query = {
      .measurement_name = "int_agg_test",
      .limit = 10,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
      .rollup_interval = 2000,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(4, result.num_points);  // 8 points aggregated to 4

  // Print points
  for (int i = 0; i < result.num_points; i++) {
    ESP_LOGI("Int Agg Test", "Point %d: Timestamp: %llu, Positive: %f, Negative: %f", i, result.timestamps[i],
             result.columns[0].values[i].data.float_val, result.columns[1].values[i].data.float_val);
  }

  // Verify aggregation results (integer -> float average)
  // First pair (0,1) -> 0.5, (2,3) -> 2.5 etc
  for (int i = 0; i < 4; i++) {
    /* integer → float */
    TEST_ASSERT_TRUE(result.columns[0].values[i].type == TIMESERIES_FIELD_TYPE_FLOAT);
    TEST_ASSERT_TRUE(result.columns[1].values[i].type == TIMESERIES_FIELD_TYPE_FLOAT);

    /* (2i   + (2i+1)) / 2  ->  (4i+1)/2  */
    double expected_positive = (4 * i + 1) / 2.0;

    /* –(2i) + –(2i+1)  -> –(4i+1)  then /2 */
    double expected_negative = -1 * (4 * i + 1) / 2.0;

    TEST_ASSERT_EQUAL_FLOAT(expected_positive, result.columns[0].values[i].data.float_val);
    TEST_ASSERT_EQUAL_FLOAT(expected_negative, result.columns[1].values[i].data.float_val);
  }

  timeseries_query_free_result(&result);
}

TEST_CASE("Can handle negative integers", "[esp_idf_timeseries]") {
  const char* field_names[] = {"negative_value", "negative_value2"};

  // Insert negative integers for two fields
  const size_t NUM_POINTS = 5;
  timeseries_field_value_t field_values[2 * NUM_POINTS];
  uint64_t timestamps[NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 1000 * i;
    // First field: -1 * (i + 10)
    field_values[0 * NUM_POINTS + i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[0 * NUM_POINTS + i].data.int_val = -1 * (int)(i + 10);
    // Second field: -2 * (i + 20)
    field_values[1 * NUM_POINTS + i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[1 * NUM_POINTS + i].data.int_val = -2 * (int)(i + 20);
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = "negative_int_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 2,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  // Query and verify
  timeseries_query_t query = {
      .measurement_name = "negative_int_test",
      .limit = 10,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(NUM_POINTS, result.num_points);
  TEST_ASSERT_EQUAL(2, result.num_columns);

  for (size_t i = 0; i < NUM_POINTS; i++) {
    TEST_ASSERT_EQUAL(-1 * (int)(i + 10), result.columns[0].values[i].data.int_val);
    TEST_ASSERT_EQUAL(-2 * (int)(i + 20), result.columns[1].values[i].data.int_val);
    TEST_ASSERT_EQUAL(timestamps[i], result.timestamps[i]);
  }

  timeseries_query_free_result(&result);
}

TEST_CASE("Can handle duplicate timestamps", "[esp_idf_timeseries]") {
  const char* field_names[] = {"reading"};

  // Insert multiple values with same timestamp
  uint64_t timestamps[] = {1000, 1000, 2000, 2000, 2000, 3000};
  timeseries_field_value_t field_values[6];

  for (int i = 0; i < 6; i++) {
    field_values[i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[i].data.int_val = i;
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = "dup_time_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = 6,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  // Query without aggregation - should get all points
  timeseries_query_t query = {
      .measurement_name = "dup_time_test",
      .limit = 10,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  // Should only return 3 points, removing duplicates
  TEST_ASSERT_EQUAL(3, result.num_points);

  timeseries_query_free_result(&result);
}

TEST_CASE("Can handle very long strings", "[esp_idf_timeseries]") {
  const char* field_names[] = {"log_entry"};

  // Create a very long string (1KB)
  char long_string[1024];
  for (int i = 0; i < 1023; i++) {
    long_string[i] = 'A' + (i % 26);
  }
  long_string[1023] = '\0';

  timeseries_field_value_t field_values[1];
  field_values[0].type = TIMESERIES_FIELD_TYPE_STRING;
  field_values[0].data.string_val.str = strdup(long_string);
  field_values[0].data.string_val.length = strlen(long_string);

  uint64_t timestamps[1] = {1000};

  timeseries_insert_data_t insert_data = {
      .measurement_name = "long_string_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = 1,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));
  free(field_values[0].data.string_val.str);

  // Query and verify
  timeseries_query_t query = {
      .measurement_name = "long_string_test",
      .limit = 1,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(1, result.num_points);
  TEST_ASSERT_EQUAL(1023, result.columns[0].values[0].data.string_val.length);
  TEST_ASSERT_EQUAL_STRING_LEN(long_string, result.columns[0].values[0].data.string_val.str, 1023);

  timeseries_query_free_result(&result);
}

TEST_CASE("Can handle edge case numeric values", "[esp_idf_timeseries]") {
  const char* field_names[] = {"int_edge", "float_edge", "bool_edge"};

  timeseries_field_value_t field_values[3 * 5];
  uint64_t timestamps[5];

  // Test various edge cases
  int int_values[] = {INT_MIN, -1, 0, 1, INT_MAX};
  float float_values[] = {-FLT_MAX, -0.0f, 0.0f, FLT_MIN, FLT_MAX};
  bool bool_values[] = {false, true, false, true, false};

  for (int i = 0; i < 5; i++) {
    timestamps[i] = 1000 * i;

    field_values[0 * 5 + i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[0 * 5 + i].data.int_val = int_values[i];

    field_values[1 * 5 + i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[1 * 5 + i].data.float_val = float_values[i];

    field_values[2 * 5 + i].type = TIMESERIES_FIELD_TYPE_BOOL;
    field_values[2 * 5 + i].data.bool_val = bool_values[i];
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = "edge_values",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 3,
      .timestamps_ms = timestamps,
      .num_points = 5,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  // Query and verify edge values are preserved
  timeseries_query_t query = {
      .measurement_name = "edge_values",
      .limit = 10,
      .start_ms = 0,
      .end_ms = 2737381864000ULL,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(5, result.num_points);

  // Verify edge values
  TEST_ASSERT_EQUAL(INT_MIN, result.columns[0].values[0].data.int_val);
  TEST_ASSERT_EQUAL(INT_MAX, result.columns[0].values[4].data.int_val);
  TEST_ASSERT_EQUAL_FLOAT(-FLT_MAX, result.columns[1].values[0].data.float_val);
  TEST_ASSERT_EQUAL_FLOAT(FLT_MAX, result.columns[1].values[4].data.float_val);

  timeseries_query_free_result(&result);
}

/*
TEST_CASE("Can query the data with strings", "[esp_idf_timeseries]") {
  const char* measurement_name = "logs";
  const char* tags_keys[] = {"severity", "deviceId"};
  const char* tags_values[] = {"error", "poioiuyty"};
  size_t num_tags = 2;

  const char* field_names[] = {"message"};
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
  query.tag_keys = tags_keys;
  query.tag_values = tags_values;
  query.num_tags = num_tags;
  query.field_names = field_names;
  query.num_fields = num_fields;
  query.limit = 250;
  query.start_ms = 0;
  query.end_ms = 2737381864000ULL;
  query.rollup_interval = 0;  // No aggregation

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

    ESP_LOGI("TAG", "String value: %.*s", (int)result.columns[0].values[i].data.string_val.length,
             result.columns[0].values[i].data.string_val.str);

    ESP_LOGI("TAG", "Timestamp: %llu", result.timestamps[i]);

    TEST_ASSERT_TRUE(result.timestamps[i] == 1000 * i);
    TEST_ASSERT_TRUE(result.columns[0].values[i].type == TIMESERIES_FIELD_TYPE_STRING);
    TEST_ASSERT_EQUAL_STRING_LEN(message, result.columns[0].values[i].data.string_val.str,
                                 result.columns[0].values[i].data.string_val.length);
  }

  // Finally, free the result memory
  timeseries_query_free_result(&result);
}

TEST_CASE("Can store a large amount of unique strings", "[esp_idf_timeseries]") {
  // Insert 1000 unique strings into the database that will trigger a compaction
  const size_t NUM_POINTS = 1000;

  const char* tags_keys[] = {"severity", "deviceId"};
  const char* tags_values[] = {"info", "abcdefgh"};
  const size_t num_tags = 2;

  const char* field_names[] = {"message"};
  const size_t num_fields = 1;

  for (size_t i = 0; i < NUM_POINTS; i++) {
    uint64_t timestamps[1];
    timestamps[0] = 1000 * i;

    timeseries_field_value_t field_values[1];
    field_values[0].type = TIMESERIES_FIELD_TYPE_STRING;

    // Generate a unique string using the loop index
    char unique_message[100];
    snprintf(unique_message, sizeof(unique_message), "Unique test string %zu", i);

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
  query.tag_keys = tags_keys;
  query.tag_values = tags_values;
  query.num_tags = num_tags;
  query.field_names = field_names;
  query.num_fields = num_fields;
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
    snprintf(unique_message, sizeof(unique_message), "Unique test string %zu", i);

    ESP_LOGI("TAG", "String value: %.*s", (int)result.columns[0].values[i].data.string_val.length,
             result.columns[0].values[i].data.string_val.str);

    ESP_LOGI("TAG", "Timestamp: %llu", result.timestamps[i]);

    TEST_ASSERT_TRUE(result.columns[0].values[i].type == TIMESERIES_FIELD_TYPE_STRING);
    TEST_ASSERT_EQUAL_STRING_LEN(unique_message, result.columns[0].values[i].data.string_val.str,
                                 result.columns[0].values[i].data.string_val.length);
  }
}

TEST_CASE("Can store a large amount of unique floats", "[esp_idf_timeseries]") {
  // Insert 10000 unique floats into the database that will trigger a compaction
  const size_t NUM_POINTS = 2000;

  for (size_t i = 0; i < NUM_POINTS; i++) {
    const char* tags_keys[] = {"severity", "deviceId"};
    const char* tags_values[] = {"info", "abcdefgh"};
    size_t num_tags = 2;

    const char* field_names[] = {"temperature"};
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
}
*/

TEST_CASE("Can cache metadata to improve query performance", "[esp_idf_timeseries]") {
  const char* measurement_name = "perf_test";
  const char* tags_keys[] = {"suburb", "city"};
  const char* tags_values[] = {"beldon", "perth"};
  size_t num_tags = 2;

  const char* field_names[] = {"temperature", "valid", "status"};
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

  // Initial uncached query

  int64_t start_time, end_time;

  {
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));

    // 2) Set the measurement name
    query.measurement_name = measurement_name;
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
    start_time = esp_timer_get_time();
    bool success = timeseries_query(&query, &result);
    end_time = esp_timer_get_time();

    TEST_ASSERT_TRUE(success);
    TEST_ASSERT_TRUE(result.num_points == 10);
    TEST_ASSERT_TRUE(result.num_columns == 3);

    // Ensure the data is correct
    for (size_t i = 0; i < 10; i++) {
      TEST_ASSERT_TRUE(result.timestamps[i] == 1000 * i);
      TEST_ASSERT_TRUE(result.columns[0].values[i].data.float_val == 1.23f * i);
      TEST_ASSERT_TRUE(result.columns[1].values[i].data.bool_val == ((int)i % 2 == 0));
      TEST_ASSERT_TRUE(result.columns[2].values[i].data.int_val == (int)i * 4);
    }

    // Finally, free the result memory
    timeseries_query_free_result(&result);
  }

  int64_t uncached_duration = end_time - start_time;

  ESP_LOGI(TAG, "Uncached query duration: %" PRId64 " us", uncached_duration);

  // Repeat the query to test cache performance
  int64_t start_time2, end_time2;
  {
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = measurement_name;
    query.tag_keys = NULL;
    query.tag_values = NULL;
    query.num_tags = 0;
    query.num_fields = 0;
    query.limit = 250;
    query.start_ms = 0;
    query.end_ms = 2737381864000ULL;
    query.rollup_interval = 0;

    timeseries_query_result_t result;
    memset(&result, 0, sizeof(result));

    start_time2 = esp_timer_get_time();
    bool success = timeseries_query(&query, &result);
    end_time2 = esp_timer_get_time();

    TEST_ASSERT_TRUE(success);
    TEST_ASSERT_TRUE(result.num_points == 10);
    TEST_ASSERT_TRUE(result.num_columns == 3);

    for (size_t i = 0; i < 10; i++) {
      TEST_ASSERT_TRUE(result.timestamps[i] == 1000 * i);
      TEST_ASSERT_TRUE(result.columns[0].values[i].data.float_val == 1.23f * i);
      TEST_ASSERT_TRUE(result.columns[1].values[i].data.bool_val == ((int)i % 2 == 0));
      TEST_ASSERT_TRUE(result.columns[2].values[i].data.int_val == (int)i * 4);
    }

    timeseries_query_free_result(&result);
  }

  int64_t cached_duration = end_time2 - start_time2;

  ESP_LOGI(TAG, "Cached query duration: %" PRId64 " us", cached_duration);

  // Ensure the cached query is faster than the uncached query
  TEST_ASSERT_TRUE(cached_duration < uncached_duration);
}

TEST_CASE("60‑second timeseries round‑trip", "[esp_idf_timeseries]") {
  /* Tag & field metadata */
  const char* tag_keys[] = {"device"};
  const char* tag_values[] = {"weather_001"};
  const char* field_names[] = {"temperature_celsius"};

  /* Data buffers */
  const size_t NUM_POINTS = 60;
  uint64_t timestamps[NUM_POINTS];
  timeseries_field_value_t field_values[NUM_POINTS];

  /* Populate 1‑Hz datapoints covering 0 ms … 59 000 ms */
  for (size_t i = 0; i < NUM_POINTS; ++i) {
    timestamps[i] = 1750749962000 + (uint64_t)i * 1000ULL;  // milliseconds
    field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[i].data.float_val = 20.0f + i;  // deterministic pseudo‑data
  }

  /* Insert them into the DB */
  timeseries_insert_data_t insert_data = {
      .measurement_name = "weather_test",
      .tag_keys = tag_keys,
      .tag_values = tag_values,
      .num_tags = 1,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };

  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  /* Query the full range [0 ms, 60 000 ms) — end is exclusive */
  timeseries_query_t query = {
      .measurement_name = "weather_test",
      .start_ms = 1750749962000,
      .end_ms = 1750749962000 + 60000,  // exclusive
      .limit = 1000,                    // comfortably above NUM_POINTS
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  ESP_LOGI("TS_TEST", "Query returned %d points", result.num_points);

  /* Verify cardinality and boundary timestamps */
  TEST_ASSERT_EQUAL(NUM_POINTS, result.num_points);
  TEST_ASSERT_EQUAL_UINT64(timestamps[0], result.timestamps[0]);
  TEST_ASSERT_EQUAL_UINT64(timestamps[NUM_POINTS - 1], result.timestamps[NUM_POINTS - 1]);

  for (int i = 0; i < NUM_POINTS; ++i) {
    TEST_ASSERT_EQUAL_FLOAT(field_values[i].data.float_val, result.columns[0].values[i].data.float_val);
  }

  timeseries_query_free_result(&result);
}

TEST_CASE("Timeseries query parser - happy path", "[tsdb_query_string_parse]") {
  const char* QUERY = "avg:weather(temp, humidity){device:weather_001, site:perth}";
  timeseries_query_t q;

  TEST_ASSERT_EQUAL(ESP_OK, tsdb_query_string_parse(QUERY, &q));

  TEST_ASSERT_NOT_NULL(q.measurement_name);
  TEST_ASSERT_EQUAL_STRING("weather", q.measurement_name);

  TEST_ASSERT_EQUAL(2, q.num_fields);
  TEST_ASSERT_EQUAL_STRING("temp", q.field_names[0]);
  TEST_ASSERT_EQUAL_STRING("humidity", q.field_names[1]);

  TEST_ASSERT_EQUAL(2, q.num_tags);
  TEST_ASSERT_EQUAL_STRING("device", q.tag_keys[0]);
  TEST_ASSERT_EQUAL_STRING("weather_001", q.tag_values[0]);
  TEST_ASSERT_EQUAL_STRING("site", q.tag_keys[1]);
  TEST_ASSERT_EQUAL_STRING("perth", q.tag_values[1]);

  tsdb_query_string_free(&q);
}

TEST_CASE("Timeseries query parser - rejects trailing parameters", "[tsdb_query_string_parse]") {
  const char* BAD = "avg:weather(temp){site:x} start=1";
  timeseries_query_t q;

  TEST_ASSERT_EQUAL(ESP_ERR_INVALID_ARG, tsdb_query_string_parse(BAD, &q));
}

static bool tag_pair_matches(const tsdb_tag_pair_t* p, const char* key, const char* val) {
  return (strcmp(p->key, key) == 0) && (strcmp(p->val, val) == 0);
}

TEST_CASE("Tag list API returns unique key/value pairs", "[esp_idf_timeseries][tags]") {
  /* ------------------------------------------------- 1.  test fixture */
  const char* measurement_name = "taglist_test";

  /* two different tags */
  const char* tag_keys[] = {"device", "sensor"};
  const char* tag_values[] = {"weather_1", "dht22"};

  const char* field_names[] = {"temperature_c"};

  /* one single field value is enough for the tag-index to be created */
  timeseries_field_value_t value = {
      .type = TIMESERIES_FIELD_TYPE_FLOAT,
      .data.float_val = 21.5f,
  };
  uint64_t ts_ms = 1750750000000ULL;

  timeseries_insert_data_t ins = {
      .measurement_name = measurement_name,
      .tag_keys = tag_keys,
      .tag_values = tag_values,
      .num_tags = 2,

      .field_names = field_names,
      .field_values = &value,
      .num_fields = 1,

      .timestamps_ms = &ts_ms,
      .num_points = 1,
  };
  TEST_ASSERT_TRUE_MESSAGE(timeseries_insert(&ins), "data insert failed");

  /* do another insert with the *same* tag pairs – should *not*
     create duplicates in the tag index                         */
  TEST_ASSERT_TRUE(timeseries_insert(&ins));

  /* ------------------------------------------------- 2.  call API   */
  tsdb_tag_pair_t* pairs = NULL; /* ← now a single pointer        */
  size_t n = 0;

  TEST_ASSERT_TRUE_MESSAGE(timeseries_get_tags_for_measurement(measurement_name, &pairs, &n),
                           "timeseries_get_tags_for_measurement() returned false");

  /* ------------------------------------------------- 3.  assertions */
  TEST_ASSERT_NOT_NULL(pairs);
  TEST_ASSERT_EQUAL_UINT32(2, n); /* expect exactly two pairs   */

  bool have_device = false;
  bool have_sensor = false;

  for (size_t i = 0; i < n; ++i) {
    if (tag_pair_matches(&pairs[i], "device", "weather_1")) have_device = true;
    if (tag_pair_matches(&pairs[i], "sensor", "dht22")) have_sensor = true;
  }
  TEST_ASSERT_TRUE_MESSAGE(have_device, "'device=weather_1' pair missing");
  TEST_ASSERT_TRUE_MESSAGE(have_sensor, "'sensor=dht22' pair missing");

  /* ------------------------------------------------- 4.  free heap  */
  if (pairs) {
    for (size_t i = 0; i < n; ++i) {
      free(pairs[i].key);
      free(pairs[i].val);
    }
    free(pairs);
  }
}

TEST_CASE("Timeseries usage summary returns sane values", "[esp_idf_timeseries][usage]") {
  /* ---------------------------------------------------------------------- */
  /* 1. Test fixture – insert a handful of points so pages get created      */
  /* ---------------------------------------------------------------------- */
  const char* measurement_name = "usage_test";

  const char* tag_keys[] = {"device"};
  const char* tag_values[] = {"weather_1"};
  const char* field_names[] = {"temperature_c"};

  timeseries_field_value_t field_val = {
      .type = TIMESERIES_FIELD_TYPE_FLOAT,
      .data.float_val = 25.0f,
  };

  /* ten points, one per second */
  uint64_t ts_ms[10];
  for (size_t i = 0; i < 10; ++i) {
    ts_ms[i] = 1750750000000ULL + i * 1000ULL;
  }

  timeseries_insert_data_t ins = {
      .measurement_name = measurement_name,
      .tag_keys = tag_keys,
      .tag_values = tag_values,
      .num_tags = 1,

      .field_names = field_names,
      .field_values = &field_val,
      .num_fields = 1,

      .timestamps_ms = ts_ms,
      .num_points = sizeof(ts_ms) / sizeof(ts_ms[0]),
  };
  TEST_ASSERT_TRUE_MESSAGE(timeseries_insert(&ins), "data insert failed");

  /* ---------------------------------------------------------------------- */
  /* 2. Call API under test                                                 */
  /* ---------------------------------------------------------------------- */
  tsdb_usage_summary_t summary;
  TEST_ASSERT_TRUE_MESSAGE(timeseries_get_usage_summary(&summary), "timeseries_get_usage_summary() failed");

  /* ---------------------------------------------------------------------- */
  /* Print summary to console                                               */
  /* ---------------------------------------------------------------------- */
  ESP_LOGW("USAGE", "Total space: %lu bytes, Used space: %lu bytes", summary.total_space_bytes,
           summary.used_space_bytes);
  ESP_LOGW("USAGE", "Metadata: %lu pages, %lu bytes", summary.metadata_summary.num_pages,
           summary.metadata_summary.size_bytes);
  for (int lvl = 0; lvl < 5; ++lvl) {
    ESP_LOGW("USAGE", "Level %d: %lu pages, %lu bytes", lvl, summary.page_summaries[lvl].num_pages,
             summary.page_summaries[lvl].size_bytes);
  }

  /* ---------------------------------------------------------------------- */
  /* 3. Sanity checks                                                       */
  /* ---------------------------------------------------------------------- */
  TEST_ASSERT_GREATER_THAN_UINT32(0, summary.total_space_bytes);
  TEST_ASSERT_GREATER_THAN_UINT32(0, summary.used_space_bytes);
  TEST_ASSERT_LESS_OR_EQUAL_UINT32(summary.total_space_bytes, summary.used_space_bytes);

  /* sum of every category must equal used_space_bytes */
  uint64_t computed_used = summary.metadata_summary.size_bytes;
  for (int lvl = 0; lvl < 5; ++lvl) {
    computed_used += summary.page_summaries[lvl].size_bytes;
  }
  TEST_ASSERT_EQUAL_UINT64_MESSAGE(computed_used, summary.used_space_bytes,
                                   "Sum of category sizes does not match used_space_bytes");

  /* page counts should also be reasonable (all non-negative) */
  TEST_ASSERT_GREATER_OR_EQUAL_UINT32(0, summary.metadata_summary.num_pages);
  for (int lvl = 0; lvl < 4; ++lvl) {
    TEST_ASSERT_GREATER_OR_EQUAL_UINT32(0, summary.page_summaries[lvl].num_pages);
  }
}

TEST_CASE("Delete by measurement removes all data", "[esp_idf_timeseries][delete]") {
  /* ------------------------------------------------- 1. Test fixture */
  const char* measurement_name = "delete_test_measurement";

  /* Insert data with multiple series (different tags) */
  const char* tag_keys[] = {"device", "location"};
  const char* tag_values_1[] = {"sensor_1", "room_a"};
  const char* tag_values_2[] = {"sensor_2", "room_b"};

  const char* field_names[] = {"temperature", "humidity"};

  const size_t NUM_POINTS = 10;
  uint64_t timestamps[NUM_POINTS];
  timeseries_field_value_t temp_values[NUM_POINTS];
  timeseries_field_value_t humidity_values[NUM_POINTS];

  /* Create test data */
  for (size_t i = 0; i < NUM_POINTS; ++i) {
    timestamps[i] = 1750750000000ULL + (uint64_t)i * 1000ULL;
    temp_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    temp_values[i].data.float_val = 20.0f + i;
    humidity_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    humidity_values[i].data.float_val = 50.0f + i;
  }

  /* Combine field values for insert */
  timeseries_field_value_t field_values[NUM_POINTS * 2];
  for (size_t i = 0; i < NUM_POINTS; ++i) {
    field_values[i] = temp_values[i];
    field_values[NUM_POINTS + i] = humidity_values[i];
  }

  /* Insert first series */
  timeseries_insert_data_t insert_data_1 = {
      .measurement_name = measurement_name,
      .tag_keys = tag_keys,
      .tag_values = tag_values_1,
      .num_tags = 2,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 2,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };
  TEST_ASSERT_TRUE(timeseries_insert(&insert_data_1));

  /* Insert second series */
  timeseries_insert_data_t insert_data_2 = {
      .measurement_name = measurement_name,
      .tag_keys = tag_keys,
      .tag_values = tag_values_2,
      .num_tags = 2,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 2,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };
  TEST_ASSERT_TRUE(timeseries_insert(&insert_data_2));

  /* ------------------------------------------------- 2. Verify data exists */
  timeseries_query_t query = {
      .measurement_name = measurement_name,
      .start_ms = timestamps[0],
      .end_ms = timestamps[NUM_POINTS - 1] + 1000,
      .limit = 1000,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));
  TEST_ASSERT_GREATER_THAN(0, result.num_points);
  ESP_LOGI("DELETE_TEST", "Before delete: %d points found", result.num_points);
  timeseries_query_free_result(&result);

  /* ------------------------------------------------- 3. Delete measurement */
  TEST_ASSERT_TRUE(timeseries_delete_measurement(measurement_name));

  /* ------------------------------------------------- 4. Verify data is gone */
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));
  TEST_ASSERT_EQUAL(0, result.num_points);
  ESP_LOGI("DELETE_TEST", "After delete: %d points found", result.num_points);
  timeseries_query_free_result(&result);

  /* ------------------------------------------------- 5. Test deleting non-existent measurement */
  TEST_ASSERT_TRUE(timeseries_delete_measurement("non_existent_measurement"));
}

TEST_CASE("Delete by measurement and field removes specific field data", "[esp_idf_timeseries][delete]") {
  const char* measurement_name = "delete_field_test";

  const char* tag_keys[] = {"device"};
  const char* tag_values[] = {"multi_sensor"};

  const char* field_names[] = {"temperature", "humidity", "pressure"};

  const size_t NUM_POINTS = 5;
  uint64_t timestamps[NUM_POINTS];
  timeseries_field_value_t field_values[NUM_POINTS * 3];

  for (size_t i = 0; i < NUM_POINTS; ++i) {
    timestamps[i] = 1750751000000ULL + (uint64_t)i * 1000ULL;

    field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[i].data.float_val = 22.0f + i;

    field_values[NUM_POINTS + i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[NUM_POINTS + i].data.float_val = 60.0f + i;

    field_values[2 * NUM_POINTS + i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[2 * NUM_POINTS + i].data.float_val = 1013.0f + i;
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = measurement_name,
      .tag_keys = tag_keys,
      .tag_values = tag_values,
      .num_tags = 1,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 3,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };
  TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

  timeseries_query_t query_all = {
      .measurement_name = measurement_name,
      .start_ms = timestamps[0],
      .end_ms = timestamps[NUM_POINTS - 1] + 1000,
      .limit = 1000,
  };

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query_all, &result));
  TEST_ASSERT_EQUAL(3, result.num_columns);
  TEST_ASSERT_EQUAL(NUM_POINTS, result.num_points);
  timeseries_query_free_result(&result);

  TEST_ASSERT_TRUE(timeseries_delete_measurement_and_field(measurement_name, "humidity"));

  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query_all, &result));
  TEST_ASSERT_EQUAL(2, result.num_columns);
  TEST_ASSERT_EQUAL(NUM_POINTS, result.num_points);

  bool has_temperature = false;
  bool has_pressure = false;
  bool has_humidity = false;

  for (size_t i = 0; i < result.num_columns; ++i) {
    if (strcmp(result.columns[i].name, "temperature") == 0) has_temperature = true;
    if (strcmp(result.columns[i].name, "pressure") == 0) has_pressure = true;
    if (strcmp(result.columns[i].name, "humidity") == 0) has_humidity = true;
  }

  TEST_ASSERT_TRUE(has_temperature);
  TEST_ASSERT_TRUE(has_pressure);
  TEST_ASSERT_FALSE(has_humidity);

  timeseries_query_free_result(&result);

  TEST_ASSERT_TRUE(timeseries_delete_measurement_and_field(measurement_name, "non_existent_field"));
}

TEST_CASE("Delete functions handle edge cases gracefully", "[esp_idf_timeseries][delete]") {
  // Test NULL measurement name
  TEST_ASSERT_FALSE(timeseries_delete_measurement(NULL));

  // Test empty measurement name (should succeed)
  TEST_ASSERT_TRUE(timeseries_delete_measurement(""));

  // Test NULL measurement name for field deletion
  TEST_ASSERT_FALSE(timeseries_delete_measurement_and_field(NULL, "field"));

  // Test NULL field name
  TEST_ASSERT_FALSE(timeseries_delete_measurement_and_field("test", NULL));

  // Test both NULL (should fail)
  TEST_ASSERT_FALSE(timeseries_delete_measurement_and_field(NULL, NULL));

  // Test empty strings (should succeed)
  TEST_ASSERT_TRUE(timeseries_delete_measurement_and_field("", "field"));

  // Test non-existent measurement (should succeed)
  TEST_ASSERT_TRUE(timeseries_delete_measurement("non_existent_measurement"));
  TEST_ASSERT_TRUE(timeseries_delete_measurement_and_field("non_existent_measurement", "field"));
}

static void query_and_check(const char* measurement, const char* tag_key, /* NULL ⇒ no tag filter */
                            const char* tag_value,                        /* NULL ⇒ no tag filter */
                            uint64_t start_ms, uint64_t end_ms, size_t expect_points, size_t expect_columns) {
  /* build query struct ------------------------------------------------ */
  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = measurement;
  q.limit = 100;
  q.start_ms = start_ms;
  q.end_ms = end_ms;

  const char* keys[1];
  const char* vals[1];
  if (tag_key && tag_value) {
    keys[0] = tag_key;
    vals[0] = tag_value;
    q.tag_keys = keys;
    q.tag_values = vals;
    q.num_tags = 1;
  }

  // Log start and end timestamps
  ESP_LOGI(TAG, "Querying measurement '%s' from %llu ms to %llu ms", measurement, start_ms, end_ms);

  /* execute ----------------------------------------------------------- */
  timeseries_query_result_t res;
  memset(&res, 0, sizeof(res));

  TEST_ASSERT_TRUE_MESSAGE(timeseries_query(&q, &res), "timeseries_query() failed");

  TEST_ASSERT_EQUAL_UINT(expect_points, res.num_points);
  TEST_ASSERT_EQUAL_UINT(expect_columns, res.num_columns);

  timeseries_query_free_result(&res);
}

/* -------------------------------------------------------------------- */
/*  The test case                                                       */
/* -------------------------------------------------------------------- */
TEST_CASE("Insert & query by device_id tag", "[esp_idf_timeseries]") {
  /* -------- constants ----------------------------------------------- */
  static const char* MEASUREMENT = "Demo_Data";
  static const char* TAG_KEY = "device_id";
  static const char* DEVICES[2] = {"Gen_001", "pump_001"};

  static const char* FIELD_NAMES[] = {"pressure", "temperature", "flow"};
  const size_t NUM_FIELDS = 3;
  const size_t PTS_PER_SER = 10;

  /* -------- timestamps over the last hour --------------------------- */
  uint64_t now_ms = 1752737543ULL;                   /* current time in ms */
  uint64_t hour_ago_ms = now_ms - 60ULL * 60ULL;     /* 1 h ago */
  uint64_t delta_ms = (60ULL * 60ULL) / PTS_PER_SER; /* ≈6 min   */

  /* -------- insert two series (Gen_001 / pump_001) ------------------ */
  for (int dev = 0; dev < 2; ++dev) {
    const char* tag_keys[1] = {TAG_KEY};
    const char* tag_values[1] = {DEVICES[dev]};

    for (size_t i = 0; i < PTS_PER_SER; ++i) {
      uint64_t ts = hour_ago_ms + i * delta_ms + 10 * dev;
      ESP_LOGI(TAG, "Inserting data for %s at %llu ms", DEVICES[dev], ts);

      /* prepare field values */
      timeseries_field_value_t fvals[3];

      fvals[0].type = TIMESERIES_FIELD_TYPE_FLOAT;
      fvals[0].data.float_val = 1.0f + dev * 0.1f + (float)i; /* pressure   */

      fvals[1].type = TIMESERIES_FIELD_TYPE_FLOAT;
      fvals[1].data.float_val = 20.0f + dev * 0.5f + (float)i * 0.1f; /* temperature*/

      fvals[2].type = TIMESERIES_FIELD_TYPE_FLOAT;
      fvals[2].data.float_val = 100.0f + dev * 2.0f + (float)i * 0.5f; /* flow       */

      timeseries_insert_data_t ins = {
          .measurement_name = MEASUREMENT,
          .tag_keys = tag_keys,
          .tag_values = tag_values,
          .num_tags = 1,
          .field_names = FIELD_NAMES,
          .field_values = fvals,
          .num_fields = NUM_FIELDS,
          .timestamps_ms = &ts,
          .num_points = 1,
      };

      TEST_ASSERT_TRUE_MESSAGE(timeseries_insert(&ins), "timeseries_insert() failed");
    }
  }

  /* -------- queries -------------------------------------------------- */
  /* 1) aggregate (all device_ids) */
  query_and_check(MEASUREMENT,
                  /*no tag filter*/ NULL, NULL, hour_ago_ms, now_ms + 1, PTS_PER_SER * 2, /* expected points */
                  NUM_FIELDS);

  /* 2) device_id == "Gen_001" */
  query_and_check(MEASUREMENT, TAG_KEY, "Gen_001", hour_ago_ms, now_ms + 1, PTS_PER_SER, NUM_FIELDS);

  /* 3) device_id == "pump_001" */
  query_and_check(MEASUREMENT, TAG_KEY, "pump_001", hour_ago_ms, now_ms + 1, PTS_PER_SER, NUM_FIELDS);
}

TEST_CASE("Field cache is invalidated when new fields are added", "[esp_idf_timeseries]") {
  const char* measurement = "cache_test";
  
  // 1) Insert data with initial field
  const char* initial_field = "temperature";
  const char* initial_field_names[] = {initial_field};
  
  uint64_t timestamp = 1000000000ULL;
  timeseries_field_value_t initial_field_values[1];
  initial_field_values[0].type = TIMESERIES_FIELD_TYPE_FLOAT;
  initial_field_values[0].data.float_val = 25.5f;
  
  timeseries_insert_data_t initial_insert = {
      .measurement_name = measurement,
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = initial_field_names,
      .field_values = initial_field_values,
      .num_fields = 1,
      .timestamps_ms = &timestamp,
      .num_points = 1,
  };
  
  TEST_ASSERT_TRUE(timeseries_insert(&initial_insert));
  
  // 2) Call get_fields_for_measurement to populate cache
  char** fields_before = NULL;
  size_t num_fields_before = 0;
  TEST_ASSERT_TRUE(timeseries_get_fields_for_measurement(measurement, &fields_before, &num_fields_before));
  TEST_ASSERT_EQUAL(1, num_fields_before);
  TEST_ASSERT_EQUAL_STRING("temperature", fields_before[0]);
  
  // Free the returned fields
  for (size_t i = 0; i < num_fields_before; i++) {
    free(fields_before[i]);
  }
  free(fields_before);
  
  // 3) Insert data with a new field (this should invalidate the cache)
  const char* new_field = "humidity";
  const char* new_field_names[] = {new_field};
  
  timestamp = 1000000001ULL;
  timeseries_field_value_t new_field_values[1];
  new_field_values[0].type = TIMESERIES_FIELD_TYPE_FLOAT;
  new_field_values[0].data.float_val = 60.0f;
  
  timeseries_insert_data_t new_insert = {
      .measurement_name = measurement,
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = new_field_names,
      .field_values = new_field_values,
      .num_fields = 1,
      .timestamps_ms = &timestamp,
      .num_points = 1,
  };
  
  TEST_ASSERT_TRUE(timeseries_insert(&new_insert));
  
  // 4) Call get_fields_for_measurement again - should now return both fields
  char** fields_after = NULL;
  size_t num_fields_after = 0;
  TEST_ASSERT_TRUE(timeseries_get_fields_for_measurement(measurement, &fields_after, &num_fields_after));
  
  // This assertion will FAIL because the cache is not invalidated
  // The cache still contains only "temperature" even though we added "humidity"
  TEST_ASSERT_EQUAL_MESSAGE(2, num_fields_after, "Expected 2 fields but cache returned stale data");
  
  // Check that both fields are present
  bool found_temp = false, found_humidity = false;
  for (size_t i = 0; i < num_fields_after; i++) {
    if (strcmp(fields_after[i], "temperature") == 0) found_temp = true;
    if (strcmp(fields_after[i], "humidity") == 0) found_humidity = true;
  }
  
  TEST_ASSERT_TRUE_MESSAGE(found_temp, "temperature field missing from cached results");
  TEST_ASSERT_TRUE_MESSAGE(found_humidity, "humidity field missing from cached results - cache not invalidated");
  
  // Free the returned fields
  for (size_t i = 0; i < num_fields_after; i++) {
    free(fields_after[i]);
  }
  free(fields_after);
}
