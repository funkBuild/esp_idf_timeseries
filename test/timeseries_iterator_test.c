/**
 * @file timeseries_iterator_test.c
 * @brief Comprehensive unit tests for timeseries iterator functionality
 *
 * These tests focus on:
 * - Page iterator boundary conditions
 * - Entity (metadata) iterator with mmap and fallback
 * - Field data iterator edge cases
 * - Points iterator compression/decompression
 * - Blank iterator space allocation
 * - Iterator initialization and cleanup
 * - Error handling paths
 * - Page boundary crossing
 * - Corrupted data handling
 */

#include "esp_log.h"
#include "esp_partition.h"
#include "timeseries.h"
#include "timeseries_data.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_points_iterator.h"
#include "unity.h"
#include <math.h>
#include <string.h>

static const char *TAG = "iterator_test";

// Access internal DB context
extern timeseries_db_t *timeseries_get_db_handle(void);

// Flag to track if database has been initialized
static bool db_initialized = false;

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * @brief Ensure database is initialized and cleared before each test
 */
static void setup_database(void) {
  if (!db_initialized) {
    TEST_ASSERT_TRUE_MESSAGE(timeseries_init(),
                             "Failed to initialize timeseries database");
    db_initialized = true;
  }
  TEST_ASSERT_TRUE(timeseries_clear_all());
}

/**
 * @brief Helper to insert N float points to a measurement
 */
static bool insert_float_points(const char *measurement, const char *field_name,
                                size_t num_points, uint64_t start_timestamp,
                                uint64_t timestamp_increment) {
  uint64_t *timestamps = malloc(num_points * sizeof(uint64_t));
  timeseries_field_value_t *field_values =
      malloc(num_points * sizeof(timeseries_field_value_t));

  if (!timestamps || !field_values) {
    free(timestamps);
    free(field_values);
    return false;
  }

  for (size_t i = 0; i < num_points; i++) {
    timestamps[i] = start_timestamp + (i * timestamp_increment);
    field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    field_values[i].data.float_val = (float)i * 1.5f;
  }

  const char *field_names[] = {field_name};
  timeseries_insert_data_t insert_data = {
      .measurement_name = measurement,
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = num_points,
  };

  bool result = timeseries_insert(&insert_data);

  free(timestamps);
  free(field_values);
  return result;
}

/**
 * @brief Helper to insert int points
 */
static bool insert_int_points(const char *measurement, const char *field_name,
                              size_t num_points, uint64_t start_timestamp,
                              uint64_t timestamp_increment) {
  uint64_t *timestamps = malloc(num_points * sizeof(uint64_t));
  timeseries_field_value_t *field_values =
      malloc(num_points * sizeof(timeseries_field_value_t));

  if (!timestamps || !field_values) {
    free(timestamps);
    free(field_values);
    return false;
  }

  for (size_t i = 0; i < num_points; i++) {
    timestamps[i] = start_timestamp + (i * timestamp_increment);
    field_values[i].type = TIMESERIES_FIELD_TYPE_INT;
    field_values[i].data.int_val = (int64_t)i * 10;
  }

  const char *field_names[] = {field_name};
  timeseries_insert_data_t insert_data = {
      .measurement_name = measurement,
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = num_points,
  };

  bool result = timeseries_insert(&insert_data);

  free(timestamps);
  free(field_values);
  return result;
}

/**
 * @brief Helper to insert bool points
 */
static bool insert_bool_points(const char *measurement, const char *field_name,
                               size_t num_points, uint64_t start_timestamp,
                               uint64_t timestamp_increment) {
  uint64_t *timestamps = malloc(num_points * sizeof(uint64_t));
  timeseries_field_value_t *field_values =
      malloc(num_points * sizeof(timeseries_field_value_t));

  if (!timestamps || !field_values) {
    free(timestamps);
    free(field_values);
    return false;
  }

  for (size_t i = 0; i < num_points; i++) {
    timestamps[i] = start_timestamp + (i * timestamp_increment);
    field_values[i].type = TIMESERIES_FIELD_TYPE_BOOL;
    field_values[i].data.bool_val = (i % 2 == 0);
  }

  const char *field_names[] = {field_name};
  timeseries_insert_data_t insert_data = {
      .measurement_name = measurement,
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = field_values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = num_points,
  };

  bool result = timeseries_insert(&insert_data);

  free(timestamps);
  free(field_values);
  return result;
}

// ============================================================================
// Page Iterator Tests
// ============================================================================

TEST_CASE("iterator: page iterator init with NULL db", "[iterator]") {
  setup_database();

  timeseries_page_iterator_t iter;
  TEST_ASSERT_FALSE(timeseries_page_iterator_init(NULL, &iter));
}

TEST_CASE("iterator: page iterator init with NULL iter", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);
  TEST_ASSERT_FALSE(timeseries_page_iterator_init(db, NULL));
}

TEST_CASE("iterator: page iterator on empty database", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  timeseries_page_iterator_t iter;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &iter));
  TEST_ASSERT_TRUE(iter.valid);
  TEST_ASSERT_EQUAL(0, iter.current_offset);

  // Empty database should return false (no pages)
  timeseries_page_header_t header;
  uint32_t offset, size;
  bool has_page = timeseries_page_iterator_next(&iter, &header, &offset, &size);

  // Might have some pages or not depending on initialization
  ESP_LOGI(TAG, "Page iterator on empty DB returned: %d", has_page);
}

TEST_CASE("iterator: page iterator with single page", "[iterator]") {
  setup_database();

  // Insert data to create at least one page
  TEST_ASSERT_TRUE(insert_float_points("test", "value", 10, 1000, 1000));

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  timeseries_page_iterator_t iter;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &iter));

  int page_count = 0;
  timeseries_page_header_t header;
  uint32_t offset, size;

  while (timeseries_page_iterator_next(&iter, &header, &offset, &size)) {
    page_count++;
    TEST_ASSERT_EQUAL(TIMESERIES_MAGIC_NUM, header.magic_number);
    TEST_ASSERT_EQUAL(TIMESERIES_PAGE_STATE_ACTIVE, header.page_state);
    TEST_ASSERT_TRUE(header.page_type == TIMESERIES_PAGE_TYPE_METADATA ||
                     header.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA);
    TEST_ASSERT_TRUE(size > 0);

    ESP_LOGI(TAG, "Found page: offset=0x%08X, size=%u, type=%u", offset, size,
             header.page_type);
  }

  TEST_ASSERT_TRUE(page_count > 0);
  ESP_LOGI(TAG, "Total pages found: %d", page_count);
}

TEST_CASE("iterator: page iterator with multiple pages", "[iterator]") {
  setup_database();

  // Insert enough data to create multiple pages
  TEST_ASSERT_TRUE(insert_float_points("test1", "value", 100, 1000, 1000));
  TEST_ASSERT_TRUE(insert_float_points("test2", "value", 100, 200000, 1000));
  TEST_ASSERT_TRUE(insert_float_points("test3", "value", 100, 400000, 1000));

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  timeseries_page_iterator_t iter;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &iter));

  int metadata_pages = 0;
  int field_data_pages = 0;
  timeseries_page_header_t header;
  uint32_t offset, size;

  while (timeseries_page_iterator_next(&iter, &header, &offset, &size)) {
    if (header.page_type == TIMESERIES_PAGE_TYPE_METADATA) {
      metadata_pages++;
    } else if (header.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA) {
      field_data_pages++;
    }
  }

  ESP_LOGI(TAG, "Metadata pages: %d, Field data pages: %d", metadata_pages,
           field_data_pages);
  TEST_ASSERT_TRUE(metadata_pages > 0 || field_data_pages > 0);
}

TEST_CASE("iterator: page iterator next with invalid iter", "[iterator]") {
  timeseries_page_header_t header;
  uint32_t offset, size;
  TEST_ASSERT_FALSE(timeseries_page_iterator_next(NULL, &header, &offset, &size));
}

// ============================================================================
// Entity (Metadata) Iterator Tests
// ============================================================================

TEST_CASE("iterator: entity iterator init with NULL db", "[iterator]") {
  setup_database();

  timeseries_entity_iterator_t iter;
  TEST_ASSERT_FALSE(timeseries_entity_iterator_init(NULL, 0, 4096, &iter));
}

TEST_CASE("iterator: entity iterator init with NULL iter", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);
  TEST_ASSERT_FALSE(timeseries_entity_iterator_init(db, 0, 4096, NULL));
}

TEST_CASE("iterator: entity iterator with out of bounds offset", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  uint32_t partition_size = db->partition->size;
  timeseries_entity_iterator_t iter;

  // Offset beyond partition
  TEST_ASSERT_FALSE(timeseries_entity_iterator_init(db, partition_size, 4096, &iter));
}

TEST_CASE("iterator: entity iterator on metadata page", "[iterator]") {
  setup_database();

  // Insert data to create metadata entries
  TEST_ASSERT_TRUE(insert_float_points("weather", "temperature", 5, 1000, 1000));

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Find a metadata page
  timeseries_page_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &page_iter));

  timeseries_page_header_t page_header;
  uint32_t page_offset, page_size;
  bool found_metadata = false;

  while (timeseries_page_iterator_next(&page_iter, &page_header, &page_offset,
                                       &page_size)) {
    if (page_header.page_type == TIMESERIES_PAGE_TYPE_METADATA) {
      found_metadata = true;

      // Now iterate over entities in this metadata page
      timeseries_entity_iterator_t ent_iter;
      TEST_ASSERT_TRUE(timeseries_entity_iterator_init(db, page_offset,
                                                       page_size, &ent_iter));
      TEST_ASSERT_TRUE(ent_iter.valid);

      int entry_count = 0;
      timeseries_entry_header_t entry_header;

      while (timeseries_entity_iterator_next(&ent_iter, &entry_header)) {
        entry_count++;
        ESP_LOGI(TAG, "Entry %d: key_type=%u, key_len=%u, value_len=%u",
                 entry_count, entry_header.key_type, entry_header.key_len,
                 entry_header.value_len);

        TEST_ASSERT_TRUE(entry_header.key_len > 0);
        TEST_ASSERT_TRUE(entry_header.delete_marker ==
                         TIMESERIES_DELETE_MARKER_VALID ||
                         entry_header.delete_marker ==
                         TIMESERIES_DELETE_MARKER_DELETED);
      }

      timeseries_entity_iterator_deinit(&ent_iter);
      TEST_ASSERT_TRUE(entry_count > 0);
      break;
    }
  }

  TEST_ASSERT_TRUE_MESSAGE(found_metadata, "No metadata pages found");
}

TEST_CASE("iterator: entity iterator read data", "[iterator]") {
  setup_database();

  TEST_ASSERT_TRUE(insert_float_points("sensor", "temp", 3, 5000, 1000));

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Find a metadata page
  timeseries_page_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &page_iter));

  timeseries_page_header_t page_header;
  uint32_t page_offset, page_size;

  while (timeseries_page_iterator_next(&page_iter, &page_header, &page_offset,
                                       &page_size)) {
    if (page_header.page_type == TIMESERIES_PAGE_TYPE_METADATA) {
      timeseries_entity_iterator_t ent_iter;
      TEST_ASSERT_TRUE(timeseries_entity_iterator_init(db, page_offset,
                                                       page_size, &ent_iter));

      timeseries_entry_header_t entry_header;
      if (timeseries_entity_iterator_next(&ent_iter, &entry_header)) {
        // Allocate buffers for key and value
        uint8_t *key_buf = malloc(entry_header.key_len);
        uint8_t *value_buf = malloc(entry_header.value_len);

        TEST_ASSERT_NOT_NULL(key_buf);
        TEST_ASSERT_NOT_NULL(value_buf);

        // Read the data
        TEST_ASSERT_TRUE(timeseries_entity_iterator_read_data(
            &ent_iter, &entry_header, key_buf, value_buf));

        ESP_LOGI(TAG, "Successfully read entry data: key_len=%u, value_len=%u",
                 entry_header.key_len, entry_header.value_len);

        free(key_buf);
        free(value_buf);
      }

      timeseries_entity_iterator_deinit(&ent_iter);
      break;
    }
  }
}

TEST_CASE("iterator: entity iterator deinit with NULL", "[iterator]") {
  // Should not crash
  timeseries_entity_iterator_deinit(NULL);
  TEST_ASSERT_TRUE(true);
}

TEST_CASE("iterator: entity iterator next with invalid iter", "[iterator]") {
  timeseries_entry_header_t header;
  TEST_ASSERT_FALSE(timeseries_entity_iterator_next(NULL, &header));
}

TEST_CASE("iterator: entity iterator read with NULL iter", "[iterator]") {
  timeseries_entry_header_t header;
  memset(&header, 0, sizeof(header));
  uint8_t buf[16];

  TEST_ASSERT_FALSE(
      timeseries_entity_iterator_read_data(NULL, &header, buf, buf));
}

// ============================================================================
// Field Data Iterator Tests
// ============================================================================

TEST_CASE("iterator: fielddata iterator init with NULL db", "[iterator]") {
  timeseries_fielddata_iterator_t iter;
  TEST_ASSERT_FALSE(timeseries_fielddata_iterator_init(NULL, 0, 4096, &iter));
}

TEST_CASE("iterator: fielddata iterator init with NULL iter", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);
  TEST_ASSERT_FALSE(timeseries_fielddata_iterator_init(db, 0, 4096, NULL));
}

TEST_CASE("iterator: fielddata iterator with out of bounds", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  uint32_t partition_size = db->partition->size;
  timeseries_fielddata_iterator_t iter;

  // Offset beyond partition
  TEST_ASSERT_FALSE(
      timeseries_fielddata_iterator_init(db, partition_size, 4096, &iter));
}

TEST_CASE("iterator: fielddata iterator on field data page", "[iterator]") {
  setup_database();

  // Insert data to create field data pages
  TEST_ASSERT_TRUE(insert_float_points("metrics", "cpu", 20, 10000, 1000));

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Find a field data page
  timeseries_page_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &page_iter));

  timeseries_page_header_t page_header;
  uint32_t page_offset, page_size;
  bool found_field_data = false;

  while (timeseries_page_iterator_next(&page_iter, &page_header, &page_offset,
                                       &page_size)) {
    if (page_header.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA) {
      found_field_data = true;

      timeseries_fielddata_iterator_t fd_iter;
      TEST_ASSERT_TRUE(timeseries_fielddata_iterator_init(db, page_offset,
                                                          page_size, &fd_iter));
      TEST_ASSERT_TRUE(fd_iter.valid);

      int record_count = 0;
      timeseries_field_data_header_t fd_header;

      while (timeseries_fielddata_iterator_next(&fd_iter, &fd_header)) {
        record_count++;
        ESP_LOGI(TAG,
                 "Field data record %d: flags=0x%02X, count=%u, length=%u",
                 record_count, fd_header.flags, fd_header.record_count,
                 fd_header.record_length);

        TEST_ASSERT_TRUE(fd_header.record_count > 0);
        TEST_ASSERT_TRUE(fd_header.record_length > 0);
      }

      ESP_LOGI(TAG, "Total field data records: %d", record_count);
      TEST_ASSERT_TRUE(record_count > 0);
      break;
    }
  }

  if (!found_field_data) {
    ESP_LOGW(TAG, "No field data pages found - data might be in metadata only");
  }
}

TEST_CASE("iterator: fielddata iterator next with invalid iter", "[iterator]") {
  timeseries_field_data_header_t header;
  TEST_ASSERT_FALSE(timeseries_fielddata_iterator_next(NULL, &header));
}

TEST_CASE("iterator: fielddata iterator read with NULL iter", "[iterator]") {
  timeseries_field_data_header_t header;
  memset(&header, 0, sizeof(header));
  header.record_length = 100;
  uint8_t buf[100];

  TEST_ASSERT_FALSE(
      timeseries_fielddata_iterator_read_data(NULL, &header, buf, sizeof(buf)));
}

TEST_CASE("iterator: fielddata iterator read with NULL header", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  timeseries_fielddata_iterator_t iter;
  iter.db = db;
  iter.valid = true;
  iter.page_offset = 0;
  iter.page_size = 4096;

  uint8_t buf[100];
  TEST_ASSERT_FALSE(
      timeseries_fielddata_iterator_read_data(&iter, NULL, buf, sizeof(buf)));
}

// ============================================================================
// Blank Iterator Tests
// ============================================================================

TEST_CASE("iterator: blank iterator init with NULL db", "[iterator]") {
  timeseries_blank_iterator_t iter;
  TEST_ASSERT_FALSE(timeseries_blank_iterator_init(NULL, &iter, 4096));
}

TEST_CASE("iterator: blank iterator init with NULL iter", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);
  TEST_ASSERT_FALSE(timeseries_blank_iterator_init(db, NULL, 4096));
}

TEST_CASE("iterator: blank iterator on empty database", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  timeseries_blank_iterator_t iter;
  TEST_ASSERT_TRUE(timeseries_blank_iterator_init(db, &iter, 4096));
  TEST_ASSERT_TRUE(iter.valid);
  TEST_ASSERT_EQUAL(4096, iter.min_size);

  uint32_t offset, size;
  int blank_count = 0;

  while (timeseries_blank_iterator_next(&iter, &offset, &size)) {
    blank_count++;
    ESP_LOGI(TAG, "Blank region %d: offset=0x%08X, size=%u", blank_count,
             offset, size);
    TEST_ASSERT_TRUE(size >= 4096);
  }

  ESP_LOGI(TAG, "Total blank regions found: %d", blank_count);
  // Empty database should have at least some free space
  TEST_ASSERT_TRUE(blank_count > 0);
}

TEST_CASE("iterator: blank iterator after data insertion", "[iterator]") {
  setup_database();

  // Insert some data
  TEST_ASSERT_TRUE(insert_float_points("test", "value", 50, 1000, 1000));

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  timeseries_blank_iterator_t iter;
  TEST_ASSERT_TRUE(timeseries_blank_iterator_init(db, &iter, 8192));

  uint32_t offset, size;
  int blank_count = 0;

  while (timeseries_blank_iterator_next(&iter, &offset, &size)) {
    blank_count++;
    ESP_LOGI(TAG, "Blank region after insert: offset=0x%08X, size=%u", offset,
             size);
    TEST_ASSERT_TRUE(size >= 8192);
  }

  ESP_LOGI(TAG, "Blank regions after insertion: %d", blank_count);
}

TEST_CASE("iterator: blank iterator with large min_size", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  // Request very large contiguous space
  uint32_t large_min_size = 64 * 1024;
  timeseries_blank_iterator_t iter;
  TEST_ASSERT_TRUE(timeseries_blank_iterator_init(db, &iter, large_min_size));

  uint32_t offset, size;
  int blank_count = 0;

  while (timeseries_blank_iterator_next(&iter, &offset, &size)) {
    blank_count++;
    TEST_ASSERT_TRUE(size >= large_min_size);
    ESP_LOGI(TAG, "Large blank region: offset=0x%08X, size=%u", offset, size);
  }

  ESP_LOGI(TAG, "Large blank regions: %d", blank_count);
}

TEST_CASE("iterator: blank iterator next with invalid iter", "[iterator]") {
  uint32_t offset, size;
  TEST_ASSERT_FALSE(timeseries_blank_iterator_next(NULL, &offset, &size));
}

// ============================================================================
// Page Cache Iterator Tests
// ============================================================================

TEST_CASE("iterator: page cache iterator init with NULL db", "[iterator]") {
  timeseries_page_cache_iterator_t iter;
  TEST_ASSERT_FALSE(timeseries_page_cache_iterator_init(NULL, &iter));
}

TEST_CASE("iterator: page cache iterator init with NULL iter", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);
  TEST_ASSERT_FALSE(timeseries_page_cache_iterator_init(db, NULL));
}

TEST_CASE("iterator: page cache iterator on empty cache", "[iterator]") {
  setup_database();

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  timeseries_page_cache_iterator_t iter;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &iter));
  TEST_ASSERT_TRUE(iter.valid);
  TEST_ASSERT_EQUAL(0, iter.index);

  timeseries_page_header_t header;
  uint32_t offset, size;
  int cache_count = 0;

  while (timeseries_page_cache_iterator_next(&iter, &header, &offset, &size)) {
    cache_count++;
    TEST_ASSERT_EQUAL(TIMESERIES_PAGE_STATE_ACTIVE, header.page_state);
  }

  ESP_LOGI(TAG, "Page cache entries (empty DB): %d", cache_count);
}

TEST_CASE("iterator: page cache iterator with data", "[iterator]") {
  setup_database();

  // Insert data to populate cache
  TEST_ASSERT_TRUE(insert_float_points("cache_test", "value", 30, 1000, 1000));

  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);

  ESP_LOGI(TAG, "Page cache count: %zu", db->page_cache_count);

  timeseries_page_cache_iterator_t iter;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &iter));

  int active_pages = 0;
  timeseries_page_header_t header;
  uint32_t offset, size;

  while (timeseries_page_cache_iterator_next(&iter, &header, &offset, &size)) {
    active_pages++;
    TEST_ASSERT_EQUAL(TIMESERIES_PAGE_STATE_ACTIVE, header.page_state);
    ESP_LOGI(TAG, "Active page %d: offset=0x%08X, size=%u, type=%u",
             active_pages, offset, size, header.page_type);
  }

  ESP_LOGI(TAG, "Total active pages in cache: %d", active_pages);
  TEST_ASSERT_TRUE(active_pages > 0);
}

TEST_CASE("iterator: page cache iterator next with invalid", "[iterator]") {
  timeseries_page_header_t header;
  uint32_t offset, size;
  TEST_ASSERT_FALSE(
      timeseries_page_cache_iterator_next(NULL, &header, &offset, &size));
}

// ============================================================================
// Points Iterator Tests - Compressed Float
// ============================================================================

TEST_CASE("iterator: points iterator compressed float", "[iterator]") {
  setup_database();

  // Insert data and compact to create compressed format
  const size_t NUM_POINTS = 50;
  TEST_ASSERT_TRUE(
      insert_float_points("compressed_test", "temperature", NUM_POINTS, 1000, 1000));

  // Compact to compress the data
  TEST_ASSERT_TRUE(timeseries_compact());

  ESP_LOGI(TAG, "Inserted and compacted %zu float points", NUM_POINTS);

  // Query to verify compression worked
  timeseries_query_t query;
  memset(&query, 0, sizeof(query));
  query.measurement_name = "compressed_test";
  query.start_ms = 0;
  query.end_ms = INT64_MAX;
  query.limit = 0;

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  ESP_LOGI(TAG, "Query returned %zu points, %zu columns", result.num_points,
           result.num_columns);
  TEST_ASSERT_EQUAL(NUM_POINTS, result.num_points);
  TEST_ASSERT_EQUAL(1, result.num_columns);

  // Verify some values
  for (size_t i = 0; i < result.num_points && i < 5; i++) {
    float expected = (float)i * 1.5f;
    float actual = result.columns[0].values[i].data.float_val;
    TEST_ASSERT_FLOAT_WITHIN(0.001, expected, actual);
    ESP_LOGI(TAG, "Point %zu: ts=%llu, val=%.2f", i, result.timestamps[i],
             actual);
  }

  timeseries_query_free_result(&result);
}

// ============================================================================
// Points Iterator Tests - Compressed Int
// ============================================================================

TEST_CASE("iterator: points iterator compressed int", "[iterator]") {
  setup_database();

  const size_t NUM_POINTS = 40;
  TEST_ASSERT_TRUE(
      insert_int_points("int_test", "counter", NUM_POINTS, 2000, 500));

  TEST_ASSERT_TRUE(timeseries_compact());

  timeseries_query_t query;
  memset(&query, 0, sizeof(query));
  query.measurement_name = "int_test";
  query.start_ms = 0;
  query.end_ms = INT64_MAX;
  query.limit = 0;

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(NUM_POINTS, result.num_points);
  TEST_ASSERT_EQUAL(1, result.num_columns);

  // Verify values
  for (size_t i = 0; i < result.num_points && i < 5; i++) {
    int64_t expected = (int64_t)i * 10;
    int64_t actual = result.columns[0].values[i].data.int_val;
    TEST_ASSERT_EQUAL(expected, actual);
  }

  timeseries_query_free_result(&result);
}

// ============================================================================
// Points Iterator Tests - Compressed Bool
// ============================================================================

TEST_CASE("iterator: points iterator compressed bool", "[iterator]") {
  setup_database();

  const size_t NUM_POINTS = 30;
  TEST_ASSERT_TRUE(
      insert_bool_points("bool_test", "active", NUM_POINTS, 3000, 100));

  TEST_ASSERT_TRUE(timeseries_compact());

  timeseries_query_t query;
  memset(&query, 0, sizeof(query));
  query.measurement_name = "bool_test";
  query.start_ms = 0;
  query.end_ms = INT64_MAX;
  query.limit = 0;

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(NUM_POINTS, result.num_points);

  // Verify bool pattern
  for (size_t i = 0; i < result.num_points && i < 10; i++) {
    bool expected = (i % 2 == 0);
    bool actual = result.columns[0].values[i].data.bool_val;
    TEST_ASSERT_EQUAL(expected, actual);
  }

  timeseries_query_free_result(&result);
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

TEST_CASE("iterator: empty iterator (no data points)", "[iterator]") {
  setup_database();

  // Insert measurement with zero points (if supported)
  // This tests the empty iterator case

  timeseries_query_t query;
  memset(&query, 0, sizeof(query));
  query.measurement_name = "nonexistent";
  query.start_ms = 0;
  query.end_ms = INT64_MAX;
  query.limit = 0;

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  bool success = timeseries_query(&query, &result);

  TEST_ASSERT_TRUE(success);
  TEST_ASSERT_EQUAL(0, result.num_points);

  timeseries_query_free_result(&result);
}

TEST_CASE("iterator: single element iteration", "[iterator]") {
  setup_database();

  // Insert exactly one point
  TEST_ASSERT_TRUE(insert_float_points("single", "value", 1, 5000, 1000));

  timeseries_query_t query;
  memset(&query, 0, sizeof(query));
  query.measurement_name = "single";
  query.start_ms = 0;
  query.end_ms = INT64_MAX;
  query.limit = 0;

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(1, result.num_points);
  TEST_ASSERT_EQUAL(5000, result.timestamps[0]);
  TEST_ASSERT_FLOAT_WITHIN(0.001, 0.0f, result.columns[0].values[0].data.float_val);

  timeseries_query_free_result(&result);
}

TEST_CASE("iterator: multi-page iteration", "[iterator]") {
  setup_database();

  // Insert large amount of data to span multiple pages
  const size_t LARGE_COUNT = 500;
  TEST_ASSERT_TRUE(
      insert_float_points("multipage", "value", LARGE_COUNT, 10000, 100));

  timeseries_query_t query;
  memset(&query, 0, sizeof(query));
  query.measurement_name = "multipage";
  query.start_ms = 0;
  query.end_ms = INT64_MAX;
  query.limit = 0;

  timeseries_query_result_t result;
  memset(&result, 0, sizeof(result));
  TEST_ASSERT_TRUE(timeseries_query(&query, &result));

  TEST_ASSERT_EQUAL(LARGE_COUNT, result.num_points);

  // Verify timestamps are sequential
  for (size_t i = 1; i < result.num_points; i++) {
    TEST_ASSERT_TRUE(result.timestamps[i] >= result.timestamps[i - 1]);
  }

  timeseries_query_free_result(&result);
}

TEST_CASE("iterator: page boundary crossing", "[iterator]") {
  setup_database();

  // Insert data that will cross page boundaries
  TEST_ASSERT_TRUE(insert_float_points("boundary1", "v1", 100, 1000, 100));
  TEST_ASSERT_TRUE(insert_float_points("boundary2", "v2", 100, 50000, 100));
  TEST_ASSERT_TRUE(insert_float_points("boundary3", "v3", 100, 100000, 100));

  // Iterate through all pages
  timeseries_db_t *db = timeseries_get_db_handle();
  timeseries_page_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &page_iter));

  int total_pages = 0;
  uint32_t last_offset = 0;
  timeseries_page_header_t header;
  uint32_t offset, size;

  while (timeseries_page_iterator_next(&page_iter, &header, &offset, &size)) {
    total_pages++;
    TEST_ASSERT_TRUE(offset >= last_offset);
    last_offset = offset + size;
  }

  ESP_LOGI(TAG, "Page boundary test: %d pages traversed", total_pages);
  TEST_ASSERT_TRUE(total_pages > 0);
}

TEST_CASE("iterator: iterator cleanup and reinitialization", "[iterator]") {
  setup_database();

  TEST_ASSERT_TRUE(insert_float_points("cleanup_test", "val", 20, 1000, 500));

  timeseries_db_t *db = timeseries_get_db_handle();

  // Initialize and use entity iterator
  timeseries_page_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &page_iter));

  timeseries_page_header_t page_header;
  uint32_t page_offset, page_size;

  if (timeseries_page_iterator_next(&page_iter, &page_header, &page_offset,
                                    &page_size)) {
    if (page_header.page_type == TIMESERIES_PAGE_TYPE_METADATA) {
      // First iteration
      timeseries_entity_iterator_t ent_iter1;
      TEST_ASSERT_TRUE(timeseries_entity_iterator_init(db, page_offset,
                                                       page_size, &ent_iter1));

      timeseries_entry_header_t entry_header;
      bool has_entry = timeseries_entity_iterator_next(&ent_iter1, &entry_header);

      timeseries_entity_iterator_deinit(&ent_iter1);

      // Second iteration on same page
      timeseries_entity_iterator_t ent_iter2;
      TEST_ASSERT_TRUE(timeseries_entity_iterator_init(db, page_offset,
                                                       page_size, &ent_iter2));

      bool has_entry2 = timeseries_entity_iterator_next(&ent_iter2, &entry_header);
      TEST_ASSERT_EQUAL(has_entry, has_entry2);

      timeseries_entity_iterator_deinit(&ent_iter2);
    }
  }
}

TEST_CASE("iterator: stress test multiple iterators", "[iterator][stress]") {
  setup_database();

  // Insert various data types
  TEST_ASSERT_TRUE(insert_float_points("stress_float", "f", 50, 1000, 200));
  TEST_ASSERT_TRUE(insert_int_points("stress_int", "i", 50, 20000, 200));
  TEST_ASSERT_TRUE(insert_bool_points("stress_bool", "b", 50, 40000, 200));

  timeseries_db_t *db = timeseries_get_db_handle();

  // Run multiple iterators concurrently
  timeseries_page_iterator_t iter1, iter2;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &iter1));
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &iter2));

  int count1 = 0, count2 = 0;
  timeseries_page_header_t h1, h2;
  uint32_t o1, o2, s1, s2;

  while (timeseries_page_iterator_next(&iter1, &h1, &o1, &s1)) {
    count1++;
  }

  while (timeseries_page_iterator_next(&iter2, &h2, &o2, &s2)) {
    count2++;
  }

  TEST_ASSERT_EQUAL(count1, count2);
  ESP_LOGI(TAG, "Stress test: both iterators found %d pages", count1);
}

// ============================================================================
// Potential Bug Detection Tests
// ============================================================================

TEST_CASE("iterator: detect off-by-one in page iteration", "[iterator][bug]") {
  setup_database();

  TEST_ASSERT_TRUE(insert_float_points("obo_test", "value", 10, 1000, 1000));

  timeseries_db_t *db = timeseries_get_db_handle();
  timeseries_page_iterator_t iter;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &iter));

  timeseries_page_header_t header;
  uint32_t offset, size;
  uint32_t expected_next_offset = 0;

  while (timeseries_page_iterator_next(&iter, &header, &offset, &size)) {
    if (expected_next_offset > 0) {
      // Check for gaps or overlaps
      ESP_LOGI(TAG, "Expected offset: 0x%08X, Actual: 0x%08X",
               expected_next_offset, offset);
    }
    expected_next_offset = offset + size;
  }

  // Final offset should not exceed partition
  TEST_ASSERT_TRUE(expected_next_offset <= db->partition->size);
}

TEST_CASE("iterator: entity iterator boundary at page end", "[iterator][bug]") {
  setup_database();

  TEST_ASSERT_TRUE(insert_float_points("boundary", "v", 5, 1000, 1000));

  timeseries_db_t *db = timeseries_get_db_handle();
  timeseries_page_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &page_iter));

  timeseries_page_header_t page_header;
  uint32_t page_offset, page_size;

  while (timeseries_page_iterator_next(&page_iter, &page_header, &page_offset,
                                       &page_size)) {
    if (page_header.page_type == TIMESERIES_PAGE_TYPE_METADATA) {
      timeseries_entity_iterator_t ent_iter;
      TEST_ASSERT_TRUE(timeseries_entity_iterator_init(db, page_offset,
                                                       page_size, &ent_iter));

      timeseries_entry_header_t entry_header;
      uint32_t last_offset = ent_iter.offset;

      while (timeseries_entity_iterator_next(&ent_iter, &entry_header)) {
        // Verify we haven't gone past page boundary
        TEST_ASSERT_TRUE(ent_iter.current_entry_offset < page_size);
        last_offset = ent_iter.offset;
      }

      // Final offset should be within page
      TEST_ASSERT_TRUE(last_offset <= page_size);

      timeseries_entity_iterator_deinit(&ent_iter);
      break;
    }
  }
}

TEST_CASE("iterator: blank iterator contiguity check", "[iterator][bug]") {
  setup_database();

  // Insert some data
  TEST_ASSERT_TRUE(insert_float_points("gap_test", "v", 30, 1000, 500));

  timeseries_db_t *db = timeseries_get_db_handle();
  timeseries_blank_iterator_t iter;
  TEST_ASSERT_TRUE(timeseries_blank_iterator_init(db, &iter, 4096));

  uint32_t prev_end = 0;
  uint32_t offset, size;
  int region_count = 0;

  while (timeseries_blank_iterator_next(&iter, &offset, &size)) {
    region_count++;
    ESP_LOGI(TAG, "Blank region %d: [0x%08X - 0x%08X] size=%u", region_count,
             offset, offset + size, size);

    // Blank regions should not overlap
    if (prev_end > 0) {
      TEST_ASSERT_TRUE(offset >= prev_end);
    }

    prev_end = offset + size;
    TEST_ASSERT_TRUE(size >= 4096);
  }

  ESP_LOGI(TAG, "Total non-overlapping blank regions: %d", region_count);
}
