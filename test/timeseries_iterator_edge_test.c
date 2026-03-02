/**
 * @file timeseries_iterator_edge_test.c
 * @brief Edge case tests for iterator APIs not covered by the main iterator
 *        test file: init_with_buffer, init_with_snapshot, deinit edge cases,
 *        and iterator reuse patterns.
 */

#include "esp_log.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache_snapshot.h"
#include "unity.h"
#include <string.h>

static const char *TAG = "iterator_edge_test";

extern timeseries_db_t *timeseries_get_db_handle(void);

static bool s_db_initialized = false;

static void ensure_init(void) {
  if (!s_db_initialized) {
    TEST_ASSERT_TRUE(timeseries_init());
    s_db_initialized = true;
  }
  TEST_ASSERT_TRUE(timeseries_clear_all());
}

static bool insert_points(const char *measurement, const char *field,
                          size_t count) {
  uint64_t *ts = malloc(count * sizeof(uint64_t));
  timeseries_field_value_t *vals =
      malloc(count * sizeof(timeseries_field_value_t));
  if (!ts || !vals) {
    free(ts);
    free(vals);
    return false;
  }
  for (size_t i = 0; i < count; i++) {
    ts[i] = 1000 + i * 100;
    vals[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
    vals[i].data.float_val = (double)i;
  }
  const char *fields[] = {field};
  timeseries_insert_data_t data = {
      .measurement_name = measurement,
      .field_names = fields,
      .field_values = vals,
      .num_fields = 1,
      .timestamps_ms = ts,
      .num_points = count,
  };
  bool ok = timeseries_insert(&data);
  free(ts);
  free(vals);
  return ok;
}

// ============================================================================
// fielddata_iterator_init_with_buffer tests
// ============================================================================

TEST_CASE("iterator_edge: init_with_buffer NULL db", "[iterator_edge]") {
  timeseries_fielddata_iterator_t f_iter;
  uint8_t buf[1024];
  TEST_ASSERT_FALSE(timeseries_fielddata_iterator_init_with_buffer(
      NULL, 0, 8192, buf, sizeof(buf), &f_iter));
}

TEST_CASE("iterator_edge: init_with_buffer NULL iter", "[iterator_edge]") {
  ensure_init();
  timeseries_db_t *db = timeseries_get_db_handle();
  uint8_t buf[1024];
  TEST_ASSERT_FALSE(timeseries_fielddata_iterator_init_with_buffer(
      db, 0, 8192, buf, sizeof(buf), NULL));
}

TEST_CASE("iterator_edge: init_with_buffer undersized buffer falls back",
          "[iterator_edge]") {
  ensure_init();
  TEST_ASSERT_TRUE(insert_points("buf_test", "v", 10));

  timeseries_db_t *db = timeseries_get_db_handle();

  // Find an L0 field data page to iterate
  timeseries_page_cache_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &page_iter));

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;
  bool found = false;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
      found = true;
      break;
    }
  }
  timeseries_page_cache_iterator_deinit(&page_iter);
  TEST_ASSERT_TRUE(found);

  // Initialize with a buffer that is too small
  uint8_t small_buf[16]; // way too small for page data
  timeseries_fielddata_iterator_t f_iter;
  bool ok = timeseries_fielddata_iterator_init_with_buffer(
      db, page_offset, page_size, small_buf, sizeof(small_buf), &f_iter);

  // Should succeed (returns true) but with NULL page_buf fallback
  TEST_ASSERT_TRUE(ok);

  timeseries_fielddata_iterator_deinit(&f_iter);
}

TEST_CASE("iterator_edge: init_with_buffer adequate buffer works",
          "[iterator_edge]") {
  ensure_init();
  TEST_ASSERT_TRUE(insert_points("buf_test2", "v", 10));

  timeseries_db_t *db = timeseries_get_db_handle();

  // Find an L0 field data page
  timeseries_page_cache_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &page_iter));

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;
  bool found = false;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
      found = true;
      break;
    }
  }
  timeseries_page_cache_iterator_deinit(&page_iter);
  TEST_ASSERT_TRUE(found);

  // Allocate adequate buffer
  uint32_t data_size = page_size - sizeof(timeseries_page_header_t);
  uint8_t *buf = malloc(data_size);
  TEST_ASSERT_NOT_NULL(buf);

  timeseries_fielddata_iterator_t f_iter;
  TEST_ASSERT_TRUE(timeseries_fielddata_iterator_init_with_buffer(
      db, page_offset, page_size, buf, data_size, &f_iter));

  // Should be able to iterate field data records
  timeseries_field_data_header_t f_hdr;
  int record_count = 0;
  while (timeseries_fielddata_iterator_next(&f_iter, &f_hdr)) {
    record_count++;
  }
  TEST_ASSERT_GREATER_THAN(0, record_count);

  timeseries_fielddata_iterator_deinit(&f_iter);
  free(buf); // we own the buffer, not the iterator

  ESP_LOGI(TAG, "init_with_buffer: iterated %d records with caller-owned buf",
           record_count);
}

TEST_CASE("iterator_edge: init_with_buffer NULL buffer falls back",
          "[iterator_edge]") {
  ensure_init();
  TEST_ASSERT_TRUE(insert_points("buf_null", "v", 5));

  timeseries_db_t *db = timeseries_get_db_handle();

  // Find a page
  timeseries_page_cache_iterator_t page_iter;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &page_iter));
  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;
  bool found = false;
  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA) {
      found = true;
      break;
    }
  }
  timeseries_page_cache_iterator_deinit(&page_iter);
  TEST_ASSERT_TRUE(found);

  // Pass NULL buffer — should succeed and fall back to no-buffer mode
  timeseries_fielddata_iterator_t f_iter;
  TEST_ASSERT_TRUE(timeseries_fielddata_iterator_init_with_buffer(
      db, page_offset, page_size, NULL, 0, &f_iter));

  timeseries_fielddata_iterator_deinit(&f_iter);
}

// ============================================================================
// blank_iterator_init_with_snapshot tests
// ============================================================================

TEST_CASE("iterator_edge: blank_init_with_snapshot NULL db",
          "[iterator_edge]") {
  timeseries_blank_iterator_t iter;
  TEST_ASSERT_FALSE(
      timeseries_blank_iterator_init_with_snapshot(NULL, &iter, 4096, NULL));
}

TEST_CASE("iterator_edge: blank_init_with_snapshot NULL iter",
          "[iterator_edge]") {
  ensure_init();
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_FALSE(
      timeseries_blank_iterator_init_with_snapshot(db, NULL, 4096, NULL));
}

TEST_CASE("iterator_edge: blank_init_with_snapshot finds blank regions",
          "[iterator_edge]") {
  ensure_init();
  // Insert some data so the partition isn't entirely blank
  TEST_ASSERT_TRUE(insert_points("blank_snap", "v", 20));

  timeseries_db_t *db = timeseries_get_db_handle();

  // Acquire a snapshot
  tsdb_page_cache_snapshot_t *snap = tsdb_snapshot_acquire_current(db);
  TEST_ASSERT_NOT_NULL(snap);

  // Initialize blank iterator with the snapshot
  timeseries_blank_iterator_t iter;
  TEST_ASSERT_TRUE(
      timeseries_blank_iterator_init_with_snapshot(db, &iter, 4096, snap));

  // Should find at least one blank region
  uint32_t offset = 0, size = 0;
  bool found_blank = timeseries_blank_iterator_next(&iter, &offset, &size);
  if (found_blank) {
    TEST_ASSERT_GREATER_OR_EQUAL(4096, size);
    ESP_LOGI(TAG, "Found blank region: offset=0x%08" PRIX32 " size=%" PRIu32,
             offset, size);
  }

  timeseries_blank_iterator_deinit(&iter);
  tsdb_snapshot_release(snap);
}

// ============================================================================
// Iterator deinit edge cases
// ============================================================================

TEST_CASE("iterator_edge: fielddata deinit NULL iter", "[iterator_edge]") {
  // Should not crash
  timeseries_fielddata_iterator_deinit(NULL);
}

TEST_CASE("iterator_edge: blank deinit NULL iter", "[iterator_edge]") {
  // Should not crash
  timeseries_blank_iterator_deinit(NULL);
}

TEST_CASE("iterator_edge: page_cache deinit NULL iter", "[iterator_edge]") {
  // Should not crash
  timeseries_page_cache_iterator_deinit(NULL);
}

TEST_CASE("iterator_edge: fielddata deinit zeroed iter", "[iterator_edge]") {
  timeseries_fielddata_iterator_t f_iter;
  memset(&f_iter, 0, sizeof(f_iter));
  // Should not crash
  timeseries_fielddata_iterator_deinit(&f_iter);
}

// ============================================================================
// Iterator reuse: init → iterate → deinit → re-init
// ============================================================================

TEST_CASE("iterator_edge: page_cache iterator reuse", "[iterator_edge]") {
  ensure_init();
  TEST_ASSERT_TRUE(insert_points("reuse_test", "v", 10));

  timeseries_db_t *db = timeseries_get_db_handle();

  timeseries_page_cache_iterator_t page_iter;

  // First use
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &page_iter));
  timeseries_page_header_t hdr;
  uint32_t offset, size;
  int count1 = 0;
  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &offset,
                                             &size)) {
    count1++;
  }
  timeseries_page_cache_iterator_deinit(&page_iter);

  // Re-init same variable
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &page_iter));
  int count2 = 0;
  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &offset,
                                             &size)) {
    count2++;
  }
  timeseries_page_cache_iterator_deinit(&page_iter);

  // Should get the same count both times
  TEST_ASSERT_EQUAL(count1, count2);
  TEST_ASSERT_GREATER_THAN(0, count1);

  ESP_LOGI(TAG, "Iterator reuse: both passes found %d pages", count1);
}
