#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "timeseries.h"
#include "timeseries_query.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <limits.h>

#include "esp_timer.h"

static const char *TAG = "solar_example";

// Solar field names (6 fields)
static const char *solar_fields[] = {
    "grid_import_w",
    "grid_export_w",
    "solar_w",
    "battery_charge_w",
    "battery_discharge_w",
    "house_w"
};

#define NUM_SOLAR_FIELDS 6
#define SECONDS_PER_5MIN 300
#define MS_PER_5MIN (SECONDS_PER_5MIN * 1000ULL)
#define POINTS_PER_DAY (24 * 60 / 5)  // 288 points per day
#define DAYS_TO_GENERATE 14
#define TOTAL_POINTS (POINTS_PER_DAY * DAYS_TO_GENERATE)  // 4032 points

// Simple random number generator for demo data
static float rand_float(float min, float max) {
    return min + ((float)rand() / (float)RAND_MAX) * (max - min);
}

// Get current time aligned to 5-minute boundary
static uint64_t get_aligned_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    uint64_t ms = (uint64_t)tv.tv_sec * 1000ULL + (uint64_t)tv.tv_usec / 1000ULL;
    // Align to 5-minute boundary
    return ms - (ms % MS_PER_5MIN);
}

// Get midnight of a given timestamp (rounds down to start of day)
static uint64_t get_midnight_ms(uint64_t timestamp_ms) {
    uint64_t ms_per_day = 24ULL * 60ULL * 60ULL * 1000ULL;
    return (timestamp_ms / ms_per_day) * ms_per_day;
}

void app_main(void) {
  // 1) Initialize TSDB
  if (!timeseries_init()) {
    ESP_LOGE(TAG, "Timeseries DB init failed!");
    return;
  }

  // Clear all data before benchmark to prevent flash fragmentation
  ESP_LOGI(TAG, "Clearing flash data...");
  if (!timeseries_clear_all()) {
    ESP_LOGW(TAG, "Failed to clear data (continuing anyway)");
  }

  ESP_LOGI(TAG, "=== Solar Data Timeseries Example ===");

  // Tags for solar system
  const char *tags_keys[] = {"location", "system_id"};
  const char *tags_values[] = {"home", "solar_001"};
  size_t num_tags = 2;

  // ---------------------------------------------------------
  // Generate and insert 14 days of solar data at 5-min intervals
  // ---------------------------------------------------------

  // Use timestamps aligned to midnight (ESP32 clock may not be set)
  // Pick day 20390 as the end day (Oct 28, 2025 at midnight UTC)
  uint64_t ms_per_day = 24ULL * 60ULL * 60ULL * 1000ULL;
  uint64_t end_time = 20390ULL * ms_per_day; // 1761696000000 ms = Oct 28, 2025 00:00:00 UTC
  uint64_t start_time = end_time - (14ULL * ms_per_day); // 14 days earlier at midnight

  ESP_LOGI(TAG, "Generating %d days of solar data (%u points at 5-min intervals)...",
           DAYS_TO_GENERATE, TOTAL_POINTS);
  ESP_LOGI(TAG, "Start time: %llu", (unsigned long long)start_time);
  ESP_LOGI(TAG, "End time: %llu", (unsigned long long)end_time);

  // Allocate arrays for timestamps and field values
  uint64_t *timestamps = malloc(TOTAL_POINTS * sizeof(uint64_t));
  timeseries_field_value_t *field_values = malloc(NUM_SOLAR_FIELDS * TOTAL_POINTS * sizeof(timeseries_field_value_t));

  if (!timestamps || !field_values) {
    ESP_LOGE(TAG, "Failed to allocate memory for data!");
    return;
  }

  // Generate data
  for (size_t i = 0; i < TOTAL_POINTS; i++) {
    // Aligned timestamp at 5-minute intervals
    timestamps[i] = start_time + (i * MS_PER_5MIN);

    // Calculate hour of day for solar simulation (0-23)
    uint64_t day_ms = timestamps[i] % (24ULL * 60ULL * 60ULL * 1000ULL);
    float hour_of_day = (float)day_ms / (60.0f * 60.0f * 1000.0f);

    // Simulate solar production (peaks at noon)
    float solar_factor = (hour_of_day >= 6.0f && hour_of_day <= 18.0f) ?
                         sinf((hour_of_day - 6.0f) * 3.14159f / 12.0f) : 0.0f;

    // Field 0: grid_import_w
    size_t idx = 0 * TOTAL_POINTS + i;
    field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[idx].data.float_val = rand_float(0.0f, 500.0f) * (1.0f - solar_factor * 0.8f);

    // Field 1: grid_export_w
    idx = 1 * TOTAL_POINTS + i;
    field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[idx].data.float_val = rand_float(1000.0f, 3000.0f) * solar_factor;

    // Field 2: solar_w
    idx = 2 * TOTAL_POINTS + i;
    field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[idx].data.float_val = rand_float(3000.0f, 10000.0f) * solar_factor;

    // Field 3: battery_charge_w
    idx = 3 * TOTAL_POINTS + i;
    field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[idx].data.float_val = rand_float(0.0f, 5000.0f) * solar_factor;

    // Field 4: battery_discharge_w
    idx = 4 * TOTAL_POINTS + i;
    field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[idx].data.float_val = rand_float(0.0f, 2000.0f) * (1.0f - solar_factor);

    // Field 5: house_w
    idx = 5 * TOTAL_POINTS + i;
    field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[idx].data.float_val = rand_float(1000.0f, 5000.0f);
  }

  // Insert all data
  timeseries_insert_data_t insert_data = {
      .measurement_name = "solar",
      .tag_keys = tags_keys,
      .tag_values = tags_values,
      .num_tags = num_tags,
      .field_names = solar_fields,
      .field_values = field_values,
      .num_fields = NUM_SOLAR_FIELDS,
      .timestamps_ms = timestamps,
      .num_points = TOTAL_POINTS,
  };

  int64_t insert_start = esp_timer_get_time();
  if (!timeseries_insert(&insert_data)) {
    ESP_LOGE(TAG, "Failed to insert solar data!");
    free(timestamps);
    free(field_values);
    return;
  }
  int64_t insert_end = esp_timer_get_time();

  ESP_LOGI(TAG, "Inserted %u points in %lld ms",
           TOTAL_POINTS, (insert_end - insert_start) / 1000);

  // Compact the data
  ESP_LOGI(TAG, "Starting compaction...");
  int64_t compact_start = esp_timer_get_time();
  if (!timeseries_compact()) {
    ESP_LOGE(TAG, "Compaction failed!");
  } else {
    int64_t compact_end = esp_timer_get_time();
    ESP_LOGI(TAG, "Compaction completed in %lld ms",
             (compact_end - compact_start) / 1000);
  }

  // ---------------------------------------------------------
  // Query each day and measure performance
  // ---------------------------------------------------------

  ESP_LOGI(TAG, "\n=== Querying 14 days of data ===");

  int64_t query_times[DAYS_TO_GENERATE];

  // Get the midnight at the start of the data range
  uint64_t first_midnight = get_midnight_ms(start_time);

  ESP_LOGI(TAG, "Data range: %llu to %llu",
           (unsigned long long)start_time,
           (unsigned long long)end_time);
  ESP_LOGI(TAG, "First midnight: %llu", (unsigned long long)first_midnight);

  // Query each day starting from the most recent day
  for (int day = 0; day < DAYS_TO_GENERATE; day++) {
    // Calculate the day index from the end (most recent = 0)
    int day_index = DAYS_TO_GENERATE - 1 - day;

    // Calculate day boundaries
    uint64_t day_start = first_midnight + (day_index * ms_per_day);
    uint64_t day_end = day_start + ms_per_day - 1;

    // Build query
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "solar";
    query.tag_keys = tags_keys;
    query.tag_values = tags_values;
    query.num_tags = num_tags;
    query.field_names = solar_fields;
    query.num_fields = NUM_SOLAR_FIELDS;
    query.start_ms = day_start;
    query.end_ms = day_end;
    query.limit = 0; // No limit

    // Execute query
    timeseries_query_result_t result;
    memset(&result, 0, sizeof(result));

    int64_t q_start = esp_timer_get_time();
    bool success = timeseries_query(&query, &result);
    int64_t q_end = esp_timer_get_time();

    query_times[day] = q_end - q_start;

    if (!success) {
      ESP_LOGE(TAG, "Day %d query failed!", day);
      query_times[day] = -1;
    } else {
      ESP_LOGI(TAG, "Day %d (day_index=%d): %llu to %llu -> %u points in %lld ms",
               day,
               day_index,
               (unsigned long long)day_start,
               (unsigned long long)day_end,
               (unsigned)result.num_points,
               query_times[day] / 1000);
      timeseries_query_free_result(&result);
    }
  }

  // ---------------------------------------------------------
  // Calculate and display statistics
  // ---------------------------------------------------------

  ESP_LOGI(TAG, "\n=== Query Performance Statistics ===");

  int64_t sum = 0;
  int64_t min_time = LLONG_MAX;
  int64_t max_time = LLONG_MIN;
  int valid_queries = 0;

  for (int day = 0; day < DAYS_TO_GENERATE; day++) {
    if (query_times[day] >= 0) {
      sum += query_times[day];
      if (query_times[day] < min_time) min_time = query_times[day];
      if (query_times[day] > max_time) max_time = query_times[day];
      valid_queries++;
    }
  }

  if (valid_queries > 0) {
    int64_t avg_time = sum / valid_queries;
    ESP_LOGI(TAG, "Average query time: %lld ms", avg_time / 1000);
    ESP_LOGI(TAG, "Minimum query time: %lld ms", min_time / 1000);
    ESP_LOGI(TAG, "Maximum query time: %lld ms", max_time / 1000);
  } else {
    ESP_LOGE(TAG, "No valid queries to calculate statistics!");
  }

  // Clean up
  free(timestamps);
  free(field_values);

  ESP_LOGI(TAG, "\n=== Example complete ===");

  // Keep running
  while (1) {
    vTaskDelay(pdMS_TO_TICKS(10000));
  }
}
