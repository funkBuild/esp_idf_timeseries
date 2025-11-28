#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "timeseries.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include "esp_timer.h"

static const char *TAG = "chunk_test";

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

typedef struct {
    size_t chunk_size;
    int64_t insert_time_ms;
    int64_t compact_time_ms;
    bool success;
} chunk_test_result_t;

static bool test_chunk_size(size_t chunk_size, chunk_test_result_t *result) {
    ESP_LOGI(TAG, "========================================");
    ESP_LOGI(TAG, "Testing chunk size: %zu", chunk_size);
    ESP_LOGI(TAG, "========================================");

    result->chunk_size = chunk_size;
    result->success = false;

    // Clear database before test
    if (!timeseries_clear_all()) {
        ESP_LOGE(TAG, "Failed to clear database");
        return false;
    }

    // Set chunk size
    timeseries_set_chunk_size(chunk_size);

    // Generate test data
    uint64_t *timestamps = (uint64_t *)malloc(TOTAL_POINTS * sizeof(uint64_t));
    timeseries_field_value_t *field_values = (timeseries_field_value_t *)malloc(
        NUM_SOLAR_FIELDS * TOTAL_POINTS * sizeof(timeseries_field_value_t));

    if (!timestamps || !field_values) {
        ESP_LOGE(TAG, "Failed to allocate memory for test data");
        free(timestamps);
        free(field_values);
        return false;
    }

    // Generate timestamps
    uint64_t base_time = get_aligned_time_ms();
    for (size_t i = 0; i < TOTAL_POINTS; i++) {
        timestamps[i] = base_time + (i * MS_PER_5MIN);
    }

    // Generate field data
    for (size_t i = 0; i < TOTAL_POINTS; i++) {
        size_t idx;

        idx = 0 * TOTAL_POINTS + i;
        field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[idx].data.float_val = rand_float(0.0f, 3000.0f);

        idx = 1 * TOTAL_POINTS + i;
        field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[idx].data.float_val = rand_float(0.0f, 5000.0f);

        idx = 2 * TOTAL_POINTS + i;
        field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[idx].data.float_val = rand_float(0.0f, 8000.0f);

        idx = 3 * TOTAL_POINTS + i;
        field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[idx].data.float_val = rand_float(0.0f, 2000.0f);

        idx = 4 * TOTAL_POINTS + i;
        field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[idx].data.float_val = rand_float(0.0f, 3000.0f);

        idx = 5 * TOTAL_POINTS + i;
        field_values[idx].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[idx].data.float_val = rand_float(1000.0f, 5000.0f);
    }

    // Insert data
    const char *tags_keys[] = {"location"};
    const char *tags_values[] = {"home"};
    size_t num_tags = 1;

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
    bool insert_success = timeseries_insert(&insert_data);
    int64_t insert_end = esp_timer_get_time();

    free(timestamps);
    free(field_values);

    if (!insert_success) {
        ESP_LOGE(TAG, "Failed to insert data");
        return false;
    }

    result->insert_time_ms = (insert_end - insert_start) / 1000;

    // Compact
    int64_t compact_start = esp_timer_get_time();
    bool compact_success = timeseries_compact();
    int64_t compact_end = esp_timer_get_time();

    if (!compact_success) {
        ESP_LOGE(TAG, "Compaction failed");
        return false;
    }

    result->compact_time_ms = (compact_end - compact_start) / 1000;
    result->success = true;

    ESP_LOGI(TAG, "Chunk %zu: Insert=%lldms, Compact=%lldms, Total=%lldms",
             chunk_size, result->insert_time_ms, result->compact_time_ms,
             result->insert_time_ms + result->compact_time_ms);

    // Give system time to settle
    vTaskDelay(pdMS_TO_TICKS(1000));

    return true;
}

void chunk_size_performance_test_task(void *pvParameters) {
    ESP_LOGI(TAG, "===========================================");
    ESP_LOGI(TAG, "  CHUNK SIZE PERFORMANCE TEST");
    ESP_LOGI(TAG, "===========================================");
    ESP_LOGI(TAG, "Test configuration:");
    ESP_LOGI(TAG, "  - Data points: %d", TOTAL_POINTS);
    ESP_LOGI(TAG, "  - Fields: %d", NUM_SOLAR_FIELDS);
    ESP_LOGI(TAG, "  - Total values: %d", TOTAL_POINTS * NUM_SOLAR_FIELDS);
    ESP_LOGI(TAG, "");

    // Initialize timeseries DB
    if (!timeseries_init()) {
        ESP_LOGE(TAG, "Failed to initialize timeseries DB");
        vTaskDelete(NULL);
        return;
    }

    // Test chunk sizes: 100, 200, 300, 500, 750, 1000, 1500, 2000
    const size_t chunk_sizes[] = {100, 200, 300, 500, 750, 1000, 1500, 2000};
    const size_t num_tests = sizeof(chunk_sizes) / sizeof(chunk_sizes[0]);
    chunk_test_result_t results[num_tests];

    for (size_t i = 0; i < num_tests; i++) {
        if (!test_chunk_size(chunk_sizes[i], &results[i])) {
            ESP_LOGE(TAG, "Test failed for chunk size %zu", chunk_sizes[i]);
        }
    }

    // Print summary
    ESP_LOGI(TAG, "");
    ESP_LOGI(TAG, "===========================================");
    ESP_LOGI(TAG, "  TEST RESULTS SUMMARY");
    ESP_LOGI(TAG, "===========================================");
    ESP_LOGI(TAG, "Chunk  | Insert  | Compact | Total   | Throughput");
    ESP_LOGI(TAG, "Size   | (ms)    | (ms)    | (ms)    | (values/s)");
    ESP_LOGI(TAG, "-------|---------|---------|---------|------------");

    int64_t best_total = INT64_MAX;
    size_t best_chunk_size = 0;

    for (size_t i = 0; i < num_tests; i++) {
        if (results[i].success) {
            int64_t total = results[i].insert_time_ms + results[i].compact_time_ms;
            int throughput = (TOTAL_POINTS * NUM_SOLAR_FIELDS * 1000) / total;

            ESP_LOGI(TAG, "%6zu | %7lld | %7lld | %7lld | %10d",
                     results[i].chunk_size,
                     results[i].insert_time_ms,
                     results[i].compact_time_ms,
                     total,
                     throughput);

            if (total < best_total) {
                best_total = total;
                best_chunk_size = results[i].chunk_size;
            }
        }
    }

    ESP_LOGI(TAG, "");
    ESP_LOGI(TAG, "OPTIMAL CHUNK SIZE: %zu (total time: %lldms)",
             best_chunk_size, best_total);
    ESP_LOGI(TAG, "");
    ESP_LOGI(TAG, "=== Test Complete ===");

    vTaskDelete(NULL);
}

void app_main(void) {
    // Create task with 4096 byte stack as specified
    xTaskCreate(chunk_size_performance_test_task,
                "chunk_test",
                4096,  // Stack size in bytes
                NULL,
                5,     // Priority
                NULL);
}
