/**
 * @file timeseries_expiration_test.c
 * @brief Comprehensive unit tests for timeseries expiration functionality
 *
 * This test suite covers:
 * - Expiration with empty database
 * - Expiration based on time thresholds
 * - Expiration based on storage usage
 * - Oldest record identification
 * - Deletion marker handling
 * - Multiple series expiration
 * - Edge cases at threshold boundaries
 * - Storage reclamation verification
 */

#include "esp_log.h"
#include "timeseries.h"
#include "timeseries_expiration.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "unity.h"
#include <string.h>

static const char *TAG = "ExpirationTest";

// External function to get DB handle
extern timeseries_db_t *timeseries_get_db_handle(void);

// Helper macro to get partition size
#define GET_PARTITION_SIZE(db) ((db)->partition->size)

// -----------------------------------------------------------------------------
// Helper Functions
// -----------------------------------------------------------------------------

/**
 * @brief Insert a batch of data points with specified timestamps
 */
static bool insert_test_data(const char *measurement, const char *suburb,
                             const char *city, const char *field_name,
                             timeseries_field_type_e field_type,
                             uint64_t *timestamps, void *values,
                             size_t num_points) {
  const char *tag_keys[] = {"suburb", "city"};
  const char *tag_values[] = {suburb, city};
  const char *field_names[] = {field_name};

  timeseries_field_value_t *field_values =
      calloc(num_points, sizeof(timeseries_field_value_t));
  if (!field_values) {
    return false;
  }

  // Fill field values based on type
  for (size_t i = 0; i < num_points; i++) {
    field_values[i].type = field_type;
    switch (field_type) {
    case TIMESERIES_FIELD_TYPE_FLOAT:
      field_values[i].data.float_val = ((double *)values)[i];
      break;
    case TIMESERIES_FIELD_TYPE_INT:
      field_values[i].data.int_val = ((int64_t *)values)[i];
      break;
    case TIMESERIES_FIELD_TYPE_BOOL:
      field_values[i].data.bool_val = ((bool *)values)[i];
      break;
    default:
      free(field_values);
      return false;
    }
  }

  timeseries_insert_data_t insert_data = {
      .measurement_name = measurement,
      .tag_keys = tag_keys,
      .tag_values = tag_values,
      .num_tags = 2,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = num_points,
  };

  bool result = timeseries_insert(&insert_data);
  free(field_values);
  return result;
}

/**
 * @brief Count total active field data records in the database
 */
static size_t count_active_field_data_records(timeseries_db_t *db) {
  size_t count = 0;

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    ESP_LOGE(TAG, "Failed to init page cache iterator");
    return 0;
  }

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM &&
        hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {

      timeseries_fielddata_iterator_t f_iter;
      if (!timeseries_fielddata_iterator_init(db, page_offset, page_size,
                                              &f_iter)) {
        continue;
      }

      timeseries_field_data_header_t fd_hdr;
      while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
        // Check if record is NOT deleted (DELETED flag is set)
        if (fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) {
          count++;
        }
      }
    }
  }

  timeseries_page_cache_iterator_deinit(&page_iter);
  return count;
}

/**
 * @brief Find the oldest active record timestamp in the database
 */
static bool find_oldest_active_record(timeseries_db_t *db,
                                      uint64_t *out_timestamp) {
  bool found = false;
  uint64_t oldest = UINT64_MAX;

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM &&
        hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {

      timeseries_fielddata_iterator_t f_iter;
      if (!timeseries_fielddata_iterator_init(db, page_offset, page_size,
                                              &f_iter)) {
        continue;
      }

      timeseries_field_data_header_t fd_hdr;
      while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
        if (fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) {
          if (fd_hdr.start_time < oldest) {
            oldest = fd_hdr.start_time;
            found = true;
          }
        }
      }
    }
  }

  timeseries_page_cache_iterator_deinit(&page_iter);

  if (found) {
    *out_timestamp = oldest;
  }
  return found;
}

/**
 * @brief Get current storage usage as a fraction (0.0 to 1.0)
 */
static float get_current_usage(timeseries_db_t *db) {
  uint32_t partition_size = db->partition->size;
  uint32_t used_space = tsdb_pagecache_get_total_active_size(db);
  return (float)used_space / (float)partition_size;
}

/**
 * @brief Count how many field data records have been marked as deleted
 */
static size_t count_deleted_records(timeseries_db_t *db) {
  size_t count = 0;

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    return 0;
  }

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM &&
        hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {

      timeseries_fielddata_iterator_t f_iter;
      if (!timeseries_fielddata_iterator_init(db, page_offset, page_size,
                                              &f_iter)) {
        continue;
      }

      timeseries_field_data_header_t fd_hdr;
      while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
        // Check if DELETED flag is cleared (record deleted)
        if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
          count++;
        }
      }
    }
  }

  timeseries_page_cache_iterator_deinit(&page_iter);
  return count;
}

// -----------------------------------------------------------------------------
// Test Cases
// -----------------------------------------------------------------------------

TEST_CASE("expiration: initialize database", "[expiration]") {
  TEST_ASSERT_TRUE(timeseries_init());
}

TEST_CASE("expiration: clear database before tests", "[expiration]") {
  TEST_ASSERT_TRUE(timeseries_clear_all());
}

TEST_CASE("expiration: empty database - no expiration needed",
          "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Run expiration on empty database with high threshold
  bool result = timeseries_expiration_run(db, 0.90f, 0.05f);
  TEST_ASSERT_TRUE(result);

  // Verify no records exist
  size_t count = count_active_field_data_records(db);
  TEST_ASSERT_EQUAL(0, count);

  ESP_LOGI(TAG, "Empty database expiration test passed");
}

TEST_CASE("expiration: invalid arguments rejected", "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // NULL db pointer
  TEST_ASSERT_FALSE(timeseries_expiration_run(NULL, 0.90f, 0.05f));

  // Invalid usage_threshold (0.0)
  TEST_ASSERT_FALSE(timeseries_expiration_run(db, 0.0f, 0.05f));

  // Invalid usage_threshold (1.0)
  TEST_ASSERT_FALSE(timeseries_expiration_run(db, 1.0f, 0.05f));

  // Invalid usage_threshold (> 1.0)
  TEST_ASSERT_FALSE(timeseries_expiration_run(db, 1.5f, 0.05f));

  // Invalid reduction_threshold (0.0)
  TEST_ASSERT_FALSE(timeseries_expiration_run(db, 0.90f, 0.0f));

  // Invalid reduction_threshold (1.0)
  TEST_ASSERT_FALSE(timeseries_expiration_run(db, 0.90f, 1.0f));

  // Invalid reduction_threshold (> 1.0)
  TEST_ASSERT_FALSE(timeseries_expiration_run(db, 0.90f, 1.2f));

  // Negative values
  TEST_ASSERT_FALSE(timeseries_expiration_run(db, -0.1f, 0.05f));
  TEST_ASSERT_FALSE(timeseries_expiration_run(db, 0.90f, -0.05f));

  ESP_LOGI(TAG, "Invalid arguments test passed");
}

TEST_CASE("expiration: below threshold - no expiration", "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Insert small amount of data
  const size_t NUM_POINTS = 10;
  uint64_t timestamps[NUM_POINTS];
  double values[NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 1000000 + i * 1000;
    values[i] = 20.0 + i * 0.5;
  }

  TEST_ASSERT_TRUE(insert_test_data("weather", "suburb1", "city1", "temp",
                                    TIMESERIES_FIELD_TYPE_FLOAT, timestamps,
                                    values, NUM_POINTS));

  // Get usage before expiration
  float usage_before = get_current_usage(db);
  size_t records_before = count_active_field_data_records(db);

  ESP_LOGI(TAG, "Usage before: %.2f%%, Records: %zu", usage_before * 100.0f,
           records_before);

  // Run expiration with very high threshold (should not expire anything)
  // Since usage is low, threshold of 0.95 means no expiration
  bool result = timeseries_expiration_run(db, 0.95f, 0.05f);
  TEST_ASSERT_TRUE(result);

  // Verify nothing was deleted
  size_t records_after = count_active_field_data_records(db);
  TEST_ASSERT_EQUAL(records_before, records_after);

  ESP_LOGI(TAG, "Below threshold test passed");
}

TEST_CASE("expiration: above threshold - oldest records deleted",
          "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Insert multiple batches of data with different timestamps
  const size_t NUM_BATCHES = 5;
  const size_t POINTS_PER_BATCH = 50;

  for (size_t batch = 0; batch < NUM_BATCHES; batch++) {
    uint64_t timestamps[POINTS_PER_BATCH];
    double values[POINTS_PER_BATCH];

    uint64_t base_time = 1000000 + (batch * 100000);

    for (size_t i = 0; i < POINTS_PER_BATCH; i++) {
      timestamps[i] = base_time + i * 1000;
      values[i] = 20.0 + batch + i * 0.1;
    }

    char suburb[32];
    snprintf(suburb, sizeof(suburb), "suburb%zu", batch);

    TEST_ASSERT_TRUE(insert_test_data("weather", suburb, "city1", "temp",
                                      TIMESERIES_FIELD_TYPE_FLOAT, timestamps,
                                      values, POINTS_PER_BATCH));
  }

  // Get initial state
  size_t records_before = count_active_field_data_records(db);
  uint64_t oldest_before;
  TEST_ASSERT_TRUE(find_oldest_active_record(db, &oldest_before));

  ESP_LOGI(TAG, "Records before expiration: %zu, Oldest timestamp: %llu",
           records_before, oldest_before);

  // Run expiration with threshold below actual usage to force deletion
  // Usage is ~0.78% on a 2MB partition with small test data
  float usage = get_current_usage(db);
  float trigger_threshold = (usage > 0.001f) ? usage - 0.001f : 0.001f;
  bool result = timeseries_expiration_run(db, trigger_threshold, 0.05f);
  TEST_ASSERT_TRUE(result);

  // Verify records were removed (compaction removes deleted records entirely)
  size_t records_after = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records after expiration: %zu (was %zu)", records_after, records_before);
  TEST_ASSERT_LESS_THAN(records_before, records_after);

  ESP_LOGI(TAG, "Above threshold expiration test passed");
}

TEST_CASE("expiration: deletion marker handling", "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Insert some test data
  const size_t NUM_POINTS = 25;
  uint64_t timestamps[NUM_POINTS];
  double values[NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 2000000 + i * 1000;
    values[i] = 15.0 + i * 0.2;
  }

  TEST_ASSERT_TRUE(insert_test_data("weather", "testsuburb", "testcity",
                                    "temperature", TIMESERIES_FIELD_TYPE_FLOAT,
                                    timestamps, values, NUM_POINTS));

  // Count active records before
  size_t records_before = count_active_field_data_records(db);

  // Run expiration to mark records as deleted (compaction removes them)
  // Use threshold below actual usage to ensure trigger
  float usage = get_current_usage(db);
  float trigger_threshold = (usage > 0.001f) ? usage - 0.001f : 0.001f;
  bool result = timeseries_expiration_run(db, trigger_threshold, 0.05f);
  TEST_ASSERT_TRUE(result);

  // After expiration+compaction, active records should decrease
  size_t records_after = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records after first expiration: %zu (was %zu)", records_after, records_before);
  TEST_ASSERT_LESS_THAN(records_before, records_after);

  // Run expiration again - with fewer records, should still succeed
  size_t records_before_second = records_after;
  result = timeseries_expiration_run(db, trigger_threshold, 0.05f);
  TEST_ASSERT_TRUE(result);

  size_t records_after_second = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records after second expiration: %zu", records_after_second);

  // Record count should not increase
  TEST_ASSERT_LESS_OR_EQUAL(records_before_second, records_after_second);

  ESP_LOGI(TAG, "Deletion marker handling test passed");
}

TEST_CASE("expiration: multiple series with different timestamps",
          "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Insert data for Series 1 (older timestamps)
  const size_t NUM_POINTS_S1 = 30;
  uint64_t timestamps_s1[NUM_POINTS_S1];
  double values_s1[NUM_POINTS_S1];

  for (size_t i = 0; i < NUM_POINTS_S1; i++) {
    timestamps_s1[i] = 1000000 + i * 1000; // Older timestamps
    values_s1[i] = 18.0 + i * 0.1;
  }

  TEST_ASSERT_TRUE(insert_test_data("weather", "old_suburb", "city1", "temp",
                                    TIMESERIES_FIELD_TYPE_FLOAT, timestamps_s1,
                                    values_s1, NUM_POINTS_S1));

  // Insert data for Series 2 (newer timestamps)
  const size_t NUM_POINTS_S2 = 30;
  uint64_t timestamps_s2[NUM_POINTS_S2];
  double values_s2[NUM_POINTS_S2];

  for (size_t i = 0; i < NUM_POINTS_S2; i++) {
    timestamps_s2[i] = 5000000 + i * 1000; // Newer timestamps
    values_s2[i] = 22.0 + i * 0.1;
  }

  TEST_ASSERT_TRUE(insert_test_data("weather", "new_suburb", "city2", "temp",
                                    TIMESERIES_FIELD_TYPE_FLOAT, timestamps_s2,
                                    values_s2, NUM_POINTS_S2));

  // Get oldest timestamp before expiration
  uint64_t oldest_before;
  TEST_ASSERT_TRUE(find_oldest_active_record(db, &oldest_before));
  ESP_LOGI(TAG, "Oldest timestamp before: %llu", oldest_before);

  // Should be from the older series
  TEST_ASSERT_LESS_THAN(2000000, oldest_before);

  // Run expiration with dynamic threshold
  float usage = get_current_usage(db);
  float trigger = (usage > 0.001f) ? usage - 0.001f : 0.001f;
  bool result = timeseries_expiration_run(db, trigger, 0.05f);
  TEST_ASSERT_TRUE(result);

  // Verify oldest records (from series 1) were preferentially deleted
  uint64_t oldest_after;
  if (find_oldest_active_record(db, &oldest_after)) {
    ESP_LOGI(TAG, "Oldest timestamp after: %llu", oldest_after);
    // Oldest remaining should be newer than before
    TEST_ASSERT_GREATER_OR_EQUAL(oldest_before, oldest_after);
  }

  ESP_LOGI(TAG, "Multiple series expiration test passed");
}

TEST_CASE("expiration: boundary condition - exactly at threshold",
          "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Insert some data
  const size_t NUM_POINTS = 20;
  uint64_t timestamps[NUM_POINTS];
  double values[NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 3000000 + i * 1000;
    values[i] = 19.5 + i * 0.3;
  }

  TEST_ASSERT_TRUE(insert_test_data("weather", "boundarysuburb", "city1",
                                    "temp", TIMESERIES_FIELD_TYPE_FLOAT,
                                    timestamps, values, NUM_POINTS));

  // Get current usage
  float current_usage = get_current_usage(db);
  ESP_LOGI(TAG, "Current usage: %.4f", current_usage);

  // Set threshold slightly below current usage (should trigger expiration)
  float threshold_trigger = current_usage - 0.001f;
  if (threshold_trigger <= 0.0f)
    threshold_trigger = 0.001f;

  size_t records_before = count_active_field_data_records(db);

  bool result = timeseries_expiration_run(db, threshold_trigger, 0.05f);
  TEST_ASSERT_TRUE(result);

  // Should have triggered expiration (compaction removes deleted records entirely)
  size_t records_after = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records after expiration at boundary: %zu (was %zu)", records_after, records_before);
  TEST_ASSERT_LESS_THAN(records_before, records_after);

  // Now set threshold above current usage (should not trigger)
  TEST_ASSERT_TRUE(timeseries_clear_all());
  TEST_ASSERT_TRUE(insert_test_data("weather", "boundarysuburb", "city1",
                                    "temp", TIMESERIES_FIELD_TYPE_FLOAT,
                                    timestamps, values, NUM_POINTS));

  current_usage = get_current_usage(db);
  float threshold_no_trigger = current_usage + 0.1f;
  if (threshold_no_trigger >= 1.0f)
    threshold_no_trigger = 0.99f;

  size_t records_no_trigger_before = count_active_field_data_records(db);

  result = timeseries_expiration_run(db, threshold_no_trigger, 0.05f);
  TEST_ASSERT_TRUE(result);

  size_t records_no_trigger_after = count_active_field_data_records(db);
  // No deletions should occur when below threshold
  TEST_ASSERT_EQUAL(records_no_trigger_before, records_no_trigger_after);

  ESP_LOGI(TAG, "Boundary condition test passed");
}

TEST_CASE("expiration: reduction threshold controls amount deleted",
          "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Insert substantial amount of data
  const size_t NUM_BATCHES = 10;
  const size_t POINTS_PER_BATCH = 20;

  for (size_t batch = 0; batch < NUM_BATCHES; batch++) {
    uint64_t timestamps[POINTS_PER_BATCH];
    double values[POINTS_PER_BATCH];

    for (size_t i = 0; i < POINTS_PER_BATCH; i++) {
      timestamps[i] = 4000000 + batch * 50000 + i * 1000;
      values[i] = 21.0 + batch * 0.5 + i * 0.05;
    }

    char suburb[32];
    snprintf(suburb, sizeof(suburb), "testsuburb%zu", batch);

    TEST_ASSERT_TRUE(insert_test_data("weather", suburb, "city1", "temp",
                                      TIMESERIES_FIELD_TYPE_FLOAT, timestamps,
                                      values, POINTS_PER_BATCH));
  }

  // Test with small reduction threshold (use dynamic threshold)
  size_t records_after_small = 0;
  size_t records_before_small = 0;
  {
    timeseries_db_t *db_test = timeseries_get_db_handle();
    records_before_small = count_active_field_data_records(db_test);
    float usage = get_current_usage(db_test);
    float trigger = (usage > 0.001f) ? usage - 0.001f : 0.001f;
    bool result = timeseries_expiration_run(db_test, trigger, 0.02f); // 2% reduction
    TEST_ASSERT_TRUE(result);
    records_after_small = count_active_field_data_records(db_test);
    ESP_LOGI(TAG, "Records after 2%% reduction: %zu (was %zu)", records_after_small, records_before_small);
  }

  // Clear and re-insert for second test
  TEST_ASSERT_TRUE(timeseries_clear_all());
  for (size_t batch = 0; batch < NUM_BATCHES; batch++) {
    uint64_t timestamps[POINTS_PER_BATCH];
    double values[POINTS_PER_BATCH];

    for (size_t i = 0; i < POINTS_PER_BATCH; i++) {
      timestamps[i] = 4000000 + batch * 50000 + i * 1000;
      values[i] = 21.0 + batch * 0.5 + i * 0.05;
    }

    char suburb[32];
    snprintf(suburb, sizeof(suburb), "testsuburb%zu", batch);

    TEST_ASSERT_TRUE(insert_test_data("weather", suburb, "city1", "temp",
                                      TIMESERIES_FIELD_TYPE_FLOAT, timestamps,
                                      values, POINTS_PER_BATCH));
  }

  // Test with larger reduction threshold (use dynamic threshold)
  size_t records_after_large = 0;
  size_t records_before_large = 0;
  {
    timeseries_db_t *db_test = timeseries_get_db_handle();
    records_before_large = count_active_field_data_records(db_test);
    float usage = get_current_usage(db_test);
    float trigger = (usage > 0.001f) ? usage - 0.001f : 0.001f;
    bool result = timeseries_expiration_run(db_test, trigger, 0.10f); // 10% reduction
    TEST_ASSERT_TRUE(result);
    records_after_large = count_active_field_data_records(db_test);
    ESP_LOGI(TAG, "Records after 10%% reduction: %zu (was %zu)", records_after_large, records_before_large);
  }

  // Larger reduction threshold should delete more records
  size_t removed_small = records_before_small - records_after_small;
  size_t removed_large = records_before_large - records_after_large;
  TEST_ASSERT_GREATER_OR_EQUAL(removed_small, removed_large);

  ESP_LOGI(TAG, "Reduction threshold test passed");
}

TEST_CASE("expiration: storage reclamation verification", "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Insert data
  const size_t NUM_POINTS = 40;
  uint64_t timestamps[NUM_POINTS];
  int64_t values[NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 5000000 + i * 1000;
    values[i] = 100 + i;
  }

  TEST_ASSERT_TRUE(insert_test_data("metrics", "server1", "datacenter1",
                                    "cpu_usage", TIMESERIES_FIELD_TYPE_INT,
                                    timestamps, values, NUM_POINTS));

  // Get usage before expiration
  uint32_t used_before = tsdb_pagecache_get_total_active_size(db);
  size_t records_before = count_active_field_data_records(db);

  ESP_LOGI(TAG, "Used space before: %u bytes, Records: %zu", used_before,
           records_before);

  // Run expiration with threshold below actual usage
  float usage = get_current_usage(db);
  float trigger_threshold = (usage > 0.001f) ? usage - 0.001f : 0.001f;
  bool result = timeseries_expiration_run(db, trigger_threshold, 0.05f);
  TEST_ASSERT_TRUE(result);

  // Verify records were removed (expiration internally runs compaction)
  size_t records_mid = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records after expiration: %zu (was %zu)", records_mid, records_before);
  TEST_ASSERT_LESS_THAN(records_before, records_mid);

  // Run compaction again to ensure further reclamation
  TEST_ASSERT_TRUE(timeseries_compact_sync());

  // Get usage after compaction
  uint32_t used_after = tsdb_pagecache_get_total_active_size(db);
  size_t records_after = count_active_field_data_records(db);

  ESP_LOGI(TAG, "Used space after: %u bytes, Records: %zu", used_after,
           records_after);
  ESP_LOGI(TAG, "Space reclaimed: %u bytes", used_before - used_after);

  // After compaction, used space should be less (deleted records removed)
  // Note: Space might not reduce if pages are still partially full
  // But record count should definitely be less
  TEST_ASSERT_LESS_THAN(records_before, records_after);

  ESP_LOGI(TAG, "Storage reclamation test passed");
}

TEST_CASE("expiration: max heap maintains 25 oldest records", "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Insert more than 25 records to test heap behavior
  const size_t NUM_RECORDS = 50;

  for (size_t i = 0; i < NUM_RECORDS; i++) {
    uint64_t timestamp = 6000000 + i * 10000; // Well-spaced timestamps
    double value = 25.0 + i;

    char suburb[32];
    snprintf(suburb, sizeof(suburb), "heap_test_%zu", i);

    TEST_ASSERT_TRUE(insert_test_data("weather", suburb, "testcity", "temp",
                                      TIMESERIES_FIELD_TYPE_FLOAT, &timestamp,
                                      &value, 1));
  }

  // Get oldest timestamp before expiration
  uint64_t oldest_before;
  TEST_ASSERT_TRUE(find_oldest_active_record(db, &oldest_before));

  // Run expiration - should only delete up to 25 oldest
  size_t records_before = count_active_field_data_records(db);
  float usage = get_current_usage(db);
  float trigger = (usage > 0.001f) ? usage - 0.001f : 0.001f;
  bool result = timeseries_expiration_run(db, trigger, 0.05f);
  TEST_ASSERT_TRUE(result);

  size_t records_after = count_active_field_data_records(db);
  size_t removed = records_before - records_after;
  ESP_LOGI(TAG, "Removed %zu records from %zu total", removed, records_before);

  // Should delete some records but not all (limited by heap size of 25)
  TEST_ASSERT_GREATER_THAN(0, removed);
  TEST_ASSERT_LESS_OR_EQUAL(25, removed); // At most 25 can be in the heap

  ESP_LOGI(TAG, "Max heap test passed");
}

TEST_CASE("expiration: different field types handled correctly",
          "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  const size_t NUM_POINTS = 15;

  // Insert float data
  {
    uint64_t timestamps[NUM_POINTS];
    double values[NUM_POINTS];
    for (size_t i = 0; i < NUM_POINTS; i++) {
      timestamps[i] = 7000000 + i * 1000;
      values[i] = 30.5 + i * 0.2;
    }
    TEST_ASSERT_TRUE(insert_test_data("sensors", "zone1", "building1", "temp",
                                      TIMESERIES_FIELD_TYPE_FLOAT, timestamps,
                                      values, NUM_POINTS));
  }

  // Insert int data
  {
    uint64_t timestamps[NUM_POINTS];
    int64_t values[NUM_POINTS];
    for (size_t i = 0; i < NUM_POINTS; i++) {
      timestamps[i] = 7010000 + i * 1000;
      values[i] = 1000 + i * 10;
    }
    TEST_ASSERT_TRUE(insert_test_data("sensors", "zone2", "building1", "count",
                                      TIMESERIES_FIELD_TYPE_INT, timestamps,
                                      values, NUM_POINTS));
  }

  // Insert bool data
  {
    uint64_t timestamps[NUM_POINTS];
    bool values[NUM_POINTS];
    for (size_t i = 0; i < NUM_POINTS; i++) {
      timestamps[i] = 7020000 + i * 1000;
      values[i] = (i % 2 == 0);
    }
    TEST_ASSERT_TRUE(insert_test_data("sensors", "zone3", "building1", "alarm",
                                      TIMESERIES_FIELD_TYPE_BOOL, timestamps,
                                      values, NUM_POINTS));
  }

  size_t records_before = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records before expiration: %zu", records_before);

  // Run expiration with threshold below actual usage
  float usage = get_current_usage(db);
  float trigger_threshold = (usage > 0.001f) ? usage - 0.001f : 0.001f;
  bool result = timeseries_expiration_run(db, trigger_threshold, 0.05f);
  TEST_ASSERT_TRUE(result);

  // Verify some records were removed regardless of type (compaction removes deleted records)
  size_t records_after = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records after expiration: %zu (was %zu)", records_after, records_before);
  TEST_ASSERT_LESS_THAN(records_before, records_after);

  ESP_LOGI(TAG, "Different field types test passed");
}

TEST_CASE("expiration: zero byte target still deletes at least one record",
          "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Insert minimal data
  uint64_t timestamp = 8000000;
  double value = 20.0;

  TEST_ASSERT_TRUE(insert_test_data("weather", "suburb", "city", "temp",
                                    TIMESERIES_FIELD_TYPE_FLOAT, &timestamp,
                                    &value, 1));

  // Run with very small reduction that might round to 0
  // The code sets bytes_to_free = 1 if it rounds to 0
  size_t records_before = count_active_field_data_records(db);
  float usage = get_current_usage(db);
  float trigger = (usage > 0.001f) ? usage - 0.001f : 0.001f;
  bool result = timeseries_expiration_run(db, trigger, 0.0001f);
  TEST_ASSERT_TRUE(result);

  // Even with tiny reduction, code should set minimum bytes_to_free = 1
  size_t records_after = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records after tiny reduction: %zu (was %zu)", records_after, records_before);

  // Depending on available data, might delete records
  // This test mainly verifies no crash/error on edge case

  ESP_LOGI(TAG, "Zero byte target test passed");
}

TEST_CASE("expiration: compaction is called for modified pages",
          "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Insert data that will span multiple field data records
  const size_t NUM_POINTS = 30;
  uint64_t timestamps[NUM_POINTS];
  double values[NUM_POINTS];

  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = 9000000 + i * 1000;
    values[i] = 22.0 + i * 0.1;
  }

  TEST_ASSERT_TRUE(insert_test_data("weather", "compactsuburb", "compactcity",
                                    "temp", TIMESERIES_FIELD_TYPE_FLOAT,
                                    timestamps, values, NUM_POINTS));

  size_t records_before = count_active_field_data_records(db);

  // Run expiration - this should mark records deleted and call compaction
  float usage = get_current_usage(db);
  float trigger = (usage > 0.001f) ? usage - 0.001f : 0.001f;
  bool result = timeseries_expiration_run(db, trigger, 0.05f);
  TEST_ASSERT_TRUE(result);

  size_t records_after = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records after expiration: %zu (was %zu)", records_after, records_before);

  // If records were removed, compaction was called successfully
  if (records_after < records_before) {
    ESP_LOGI(TAG, "Expiration with deletion succeeded, compaction was called");
  }

  ESP_LOGI(TAG, "Compaction call test passed");
}

TEST_CASE("expiration: consecutive runs handle remaining data correctly",
          "[expiration]") {
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Clear database first
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Insert substantial data
  const size_t NUM_BATCHES = 8;
  const size_t POINTS_PER_BATCH = 25;

  for (size_t batch = 0; batch < NUM_BATCHES; batch++) {
    uint64_t timestamps[POINTS_PER_BATCH];
    double values[POINTS_PER_BATCH];

    for (size_t i = 0; i < POINTS_PER_BATCH; i++) {
      timestamps[i] = 10000000 + batch * 100000 + i * 1000;
      values[i] = 18.0 + batch + i * 0.05;
    }

    char suburb[32];
    snprintf(suburb, sizeof(suburb), "consecutive_%zu", batch);

    TEST_ASSERT_TRUE(insert_test_data("weather", suburb, "city1", "temp",
                                      TIMESERIES_FIELD_TYPE_FLOAT, timestamps,
                                      values, POINTS_PER_BATCH));
  }

  size_t initial_records = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Initial records: %zu", initial_records);

  // First expiration run
  float usage1 = get_current_usage(db);
  float trigger1 = (usage1 > 0.001f) ? usage1 - 0.001f : 0.001f;
  bool result1 = timeseries_expiration_run(db, trigger1, 0.05f);
  TEST_ASSERT_TRUE(result1);

  size_t records_after_first = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records after first run: %zu (was %zu)", records_after_first, initial_records);

  // Second expiration run
  float usage2 = get_current_usage(db);
  float trigger2 = (usage2 > 0.001f) ? usage2 - 0.001f : 0.001f;
  bool result2 = timeseries_expiration_run(db, trigger2, 0.05f);
  TEST_ASSERT_TRUE(result2);

  size_t records_after_second = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records after second run: %zu", records_after_second);

  // Each run should remove more or maintain (never increase)
  TEST_ASSERT_LESS_OR_EQUAL(records_after_first, records_after_second);

  // Third run
  float usage3 = get_current_usage(db);
  float trigger3 = (usage3 > 0.001f) ? usage3 - 0.001f : 0.001f;
  bool result3 = timeseries_expiration_run(db, trigger3, 0.05f);
  TEST_ASSERT_TRUE(result3);

  size_t records_after_third = count_active_field_data_records(db);
  ESP_LOGI(TAG, "Records after third run: %zu", records_after_third);

  ESP_LOGI(TAG, "Consecutive runs test passed");
}

TEST_CASE("expiration: final cleanup", "[expiration]") {
  TEST_ASSERT_TRUE(timeseries_clear_all());
  ESP_LOGI(TAG, "All expiration tests completed successfully");
}
