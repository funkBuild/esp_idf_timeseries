/**
 * @file timeseries_recovery_test.c
 * @brief timeseries_init_with_recovery: the recovery ladder.
 *
 * A region of flash is made unreadable (esp_partition_read fails for any read
 * overlapping it; the wraps live in timeseries_audit_alloc.c for Linux and in
 * timeseries_meta_scan_reads_test.c for the target). How the fault clears
 * models the damage: an erase covering the region (a page whose content was
 * bad), only an erase of the whole partition (damage the page erase does not
 * reach), or never (a flash fault). Erases are counted by the erase wrap in
 * timeseries_tenth_pass_test.c, which can also be told to fail them.
 */
#include "sdkconfig.h"
#include "esp_partition.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_metadata.h"
#include "unity.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern timeseries_db_t *timeseries_get_db_handle(void);

typedef enum { CLEAR_ON_COVERING_ERASE, CLEAR_ON_FULL_ERASE, CLEAR_NEVER } fault_clear_t;
static struct {
  bool armed;
  size_t start, end;
  fault_clear_t clear;
} s_fault;
static size_t s_erases, s_full_erases, s_last_erase_offset, s_last_erase_length;
static bool s_fail_erases;

/* Called by the esp_partition_read wraps: true fails the read. */
bool tsdb_test_read_fault(size_t offset, size_t size) {
  return s_fault.armed && offset < s_fault.end && s_fault.start < offset + size;
}

/* Called by the esp_partition_erase_range wrap: false fails the erase. */
bool tsdb_test_on_erase(const esp_partition_t *part, size_t offset, size_t length) {
  if (s_fail_erases) return false;
  s_erases++;
  s_last_erase_offset = offset;
  s_last_erase_length = length;
  bool full = offset == 0 && part && length >= part->size;
  if (full) s_full_erases++;
  bool covers = offset <= s_fault.start && s_fault.end <= offset + length;
  if (s_fault.armed && covers &&
      (s_fault.clear == CLEAR_ON_COVERING_ERASE || (s_fault.clear == CLEAR_ON_FULL_ERASE && full)))
    s_fault.armed = false;
  return true;
}

static void arm_fault(size_t start, size_t end, fault_clear_t clear) {
  s_fault.start = start;
  s_fault.end = end;
  s_fault.clear = clear;
  s_fault.armed = true;
}

static void reset_counters(void) {
  s_erases = s_full_erases = s_last_erase_offset = s_last_erase_length = 0;
}

static void disarm(void) {
  s_fault.armed = false;
  s_fail_erases = false;
}

static bool insert_points(const char *measurement, size_t count, uint64_t first) {
  uint64_t ts[64];
  timeseries_field_value_t values[64];
  if (count > 64) return false;
  for (size_t i = 0; i < count; i++) {
    ts[i] = first + i;
    values[i].type = TIMESERIES_FIELD_TYPE_INT;
    values[i].data.int_val = (int64_t)(first + i);
  }
  const char *field = "v";
  timeseries_insert_data_t in = {.measurement_name = measurement, .field_names = &field, .field_values = values,
                                 .num_fields = 1, .timestamps_ms = ts, .num_points = count};
  return timeseries_insert(&in);
}

/* Points stored for a measurement, or -1 if the query fails. */
static int count_points(const char *measurement) {
  timeseries_query_t q = {.measurement_name = measurement};
  timeseries_query_result_t r = {0};
  if (!timeseries_query(&q, &r)) return -1;
  int n = (int)r.num_points;
  timeseries_query_free_result(&r);
  return n;
}

/* Page at `offset` in the page cache, or false. */
static bool find_page(timeseries_db_t *db, uint32_t offset, timeseries_page_header_t *out) {
  timeseries_page_cache_iterator_t it;
  if (!timeseries_page_cache_iterator_init(db, &it)) return false;
  timeseries_page_header_t h;
  uint32_t off, size;
  bool found = false;
  while (!found && timeseries_page_cache_iterator_next(&it, &h, &off, &size)) {
    if (off == offset) {
      *out = h;
      found = true;
    }
  }
  timeseries_page_cache_iterator_deinit(&it);
  return found;
}

static uint32_t first_metadata_page(timeseries_db_t *db) {
  timeseries_page_cache_iterator_t it;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &it));
  timeseries_page_header_t h;
  uint32_t off, size, found = UINT32_MAX;
  while (found == UINT32_MAX && timeseries_page_cache_iterator_next(&it, &h, &off, &size)) {
    if (h.page_type == TIMESERIES_PAGE_TYPE_METADATA) found = off;
  }
  timeseries_page_cache_iterator_deinit(&it);
  TEST_ASSERT_NOT_EQUAL(UINT32_MAX, found);
  return found;
}

/* "rec_keep" compacted into a level-1 page, then "rec_bad" in a fresh L0
 * page. Returns the L0 page (offset and size). */
static void two_page_fixture(uint32_t *bad_page, uint32_t *bad_size) {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  TEST_ASSERT_TRUE(insert_points("rec_keep", 40, 1000));
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());
  TEST_ASSERT_TRUE(insert_points("rec_bad", 20, 5000));
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);
  TEST_ASSERT_TRUE(db->last_l0_cache_valid);
  *bad_page = db->last_l0_page_offset;
  timeseries_page_header_t h;
  TEST_ASSERT_TRUE(find_page(db, *bad_page, &h));
  TEST_ASSERT_EQUAL(TIMESERIES_PAGE_TYPE_FIELD_DATA, h.page_type);
  TEST_ASSERT_EQUAL(0, h.field_data_level);
  *bad_size = h.page_size;
  TEST_ASSERT_EQUAL(40, count_points("rec_keep"));
  TEST_ASSERT_EQUAL(20, count_points("rec_bad"));
}

TEST_CASE("recovery: unreadable data page is erased alone; other data survives", "[recovery]") {
  uint32_t bad_page, bad_size;
  two_page_fixture(&bad_page, &bad_size);
  timeseries_deinit();

  // The records of rec_bad's page cannot be read (the page header can).
  const size_t body = bad_page + sizeof(timeseries_page_header_t);
  arm_fault(body, body + 1, CLEAR_ON_COVERING_ERASE);
  reset_counters();
  // Before recovery existed this was the whole story: init fails (and the
  // firmware entered safe mode). Plain init still never erases.
  TEST_ASSERT_FALSE(timeseries_init());
  TEST_ASSERT_EQUAL(0, s_erases);

  timeseries_recovery_report_t report;
  bool ok = timeseries_init_with_recovery(&report);
  disarm();
  TEST_ASSERT_TRUE(ok);
  TEST_ASSERT_EQUAL(TIMESERIES_INIT_OK, report.status);
  TEST_ASSERT_EQUAL(TIMESERIES_RECOVERY_PAGES_ERASED, report.action);
  TEST_ASSERT_EQUAL_UINT32(1, report.pages_erased);
  TEST_ASSERT_EQUAL(1, s_erases);
  TEST_ASSERT_EQUAL(0, s_full_erases);
  TEST_ASSERT_EQUAL(bad_page, s_last_erase_offset);
  TEST_ASSERT_EQUAL(bad_size, s_last_erase_length);

  TEST_ASSERT_EQUAL(40, count_points("rec_keep"));  // the other page survived
  TEST_ASSERT_EQUAL(0, count_points("rec_bad"));    // its metadata remains, its points are gone
  TEST_ASSERT_TRUE(insert_points("rec_bad", 5, 9000));
  TEST_ASSERT_EQUAL(5, count_points("rec_bad"));
}

TEST_CASE("recovery: unreadable page scan resumes past the bad sector", "[recovery]") {
  // The page header itself cannot be read: the scan used to stop there and
  // hide every later page. Now the sector is reported, erased, and the scan
  // still finds the pages after it.
  uint32_t bad_page, bad_size;
  two_page_fixture(&bad_page, &bad_size);
  timeseries_db_t *db = timeseries_get_db_handle();
  uint32_t meta = first_metadata_page(db);
  TEST_ASSERT_NOT_EQUAL(bad_page, meta);
  timeseries_deinit();

  arm_fault(bad_page, bad_page + 1, CLEAR_ON_COVERING_ERASE);
  reset_counters();
  timeseries_recovery_report_t report;
  bool ok = timeseries_init_with_recovery(&report);
  disarm();
  TEST_ASSERT_TRUE(ok);
  TEST_ASSERT_EQUAL(TIMESERIES_RECOVERY_PAGES_ERASED, report.action);
  TEST_ASSERT_EQUAL_UINT32(1, report.pages_erased);
  TEST_ASSERT_EQUAL(0, s_full_erases);
  TEST_ASSERT_EQUAL(bad_page & ~0xFFFu, s_last_erase_offset);
  TEST_ASSERT_EQUAL(4096, s_last_erase_length);
  TEST_ASSERT_EQUAL(40, count_points("rec_keep"));
  TEST_ASSERT_EQUAL(0, count_points("rec_bad"));
}

TEST_CASE("recovery: unreadable metadata page is erased alone; data pages survive", "[recovery]") {
  uint32_t bad_page, bad_size;
  two_page_fixture(&bad_page, &bad_size);
  timeseries_db_t *db = timeseries_get_db_handle();
  uint32_t meta = first_metadata_page(db);
  timeseries_deinit();

  // The metadata page's entries cannot be read (its header can).
  const size_t body = meta + sizeof(timeseries_page_header_t);
  arm_fault(body, body + 1, CLEAR_ON_COVERING_ERASE);
  reset_counters();
  TEST_ASSERT_FALSE(timeseries_init());
  timeseries_recovery_report_t report;
  bool ok = timeseries_init_with_recovery(&report);
  disarm();
  TEST_ASSERT_TRUE(ok);
  TEST_ASSERT_EQUAL(TIMESERIES_RECOVERY_PAGES_ERASED, report.action);
  TEST_ASSERT_EQUAL_UINT32(1, report.pages_erased);
  TEST_ASSERT_EQUAL(0, s_full_erases);
  TEST_ASSERT_EQUAL(meta, s_last_erase_offset);
  TEST_ASSERT_EQUAL(TIMESERIES_METADATA_PAGE_SIZE, s_last_erase_length);

  // The names are gone with their page; the data pages were not touched, so
  // writing a measurement again (same series IDs) brings its points back.
  TEST_ASSERT_EQUAL(0, count_points("rec_keep"));
  TEST_ASSERT_TRUE(insert_points("rec_keep", 1, 999999));
  TEST_ASSERT_EQUAL(41, count_points("rec_keep"));
}

TEST_CASE("recovery: an index entry's measurement ID is never handed out again", "[recovery]") {
  // After a metadata page is erased, tag/field index entries on other pages
  // can outlive their measurement entry. Their ID must stay taken: a new
  // measurement given it would inherit their series.
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  TEST_ASSERT_TRUE(insert_points("rec_keep", 3, 1));
  timeseries_db_t *db = timeseries_get_db_handle();
  uint32_t keep_id = 0, max_id = 0;
  TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "rec_keep", &keep_id));
  TEST_ASSERT_LESS_THAN_UINT32(77, keep_id);
  TEST_ASSERT_TRUE(tsdb_find_max_measurement_id(db, &max_id));
  TEST_ASSERT_EQUAL_UINT32(keep_id, max_id);

  const char *keys[] = {"site"}, *values[] = {"orphan"};
  const unsigned char series[16] = {0x5a};
  TEST_ASSERT_TRUE(xSemaphoreTake(db->flash_write_mutex, portMAX_DELAY) == pdTRUE);
  bool indexed = tsdb_index_tags_for_series(db, 77, keys, values, 1, series);
  xSemaphoreGive(db->flash_write_mutex);
  TEST_ASSERT_TRUE(indexed);
  TEST_ASSERT_TRUE(tsdb_find_max_measurement_id(db, &max_id));
  TEST_ASSERT_EQUAL_UINT32_MESSAGE(77, max_id, "ID named only by an index key was not counted");

  timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(insert_points("rec_new", 1, 1));
  uint32_t new_id = 0;
  TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "rec_new", &new_id));
  TEST_ASSERT_GREATER_THAN_UINT32(77, new_id);
}

TEST_CASE("recovery: metadata the page erase cannot fix -> whole partition erased", "[recovery]") {
  uint32_t bad_page, bad_size;
  two_page_fixture(&bad_page, &bad_size);
  timeseries_db_t *db = timeseries_get_db_handle();
  uint32_t meta = first_metadata_page(db);
  timeseries_deinit();

  // The whole metadata page is unreadable and erasing that page does not
  // cure it; only the partition erase does.
  arm_fault(meta, meta + TIMESERIES_METADATA_PAGE_SIZE, CLEAR_ON_FULL_ERASE);
  reset_counters();
  TEST_ASSERT_FALSE(timeseries_init());
  timeseries_recovery_report_t report;
  bool ok = timeseries_init_with_recovery(&report);
  disarm();
  TEST_ASSERT_TRUE(ok);
  TEST_ASSERT_EQUAL(TIMESERIES_INIT_OK, report.status);
  TEST_ASSERT_EQUAL(TIMESERIES_RECOVERY_PARTITION_ERASED, report.action);
  TEST_ASSERT_GREATER_OR_EQUAL_UINT32(1, report.pages_erased);  // minimal recovery was tried first
  TEST_ASSERT_EQUAL(1, s_full_erases);

  // Empty and usable.
  char **names = NULL;
  size_t n = 99;
  TEST_ASSERT_TRUE(timeseries_get_measurements(&names, &n));
  TEST_ASSERT_EQUAL(0, n);
  free(names);
  TEST_ASSERT_EQUAL(0, count_points("rec_keep"));
  TEST_ASSERT_TRUE(insert_points("rec_fresh", 7, 100));
  TEST_ASSERT_EQUAL(7, count_points("rec_fresh"));
}

TEST_CASE("recovery: flash that stays unreadable fails cleanly, bounded", "[recovery]") {
  uint32_t bad_page, bad_size;
  two_page_fixture(&bad_page, &bad_size);
  timeseries_deinit();

  // The page's first byte: every scan, even of an erased partition, reads it.
  arm_fault(bad_page, bad_page + 1, CLEAR_NEVER);
  reset_counters();
  timeseries_recovery_report_t report;
  bool ok = timeseries_init_with_recovery(&report);
  TEST_ASSERT_FALSE(ok);
  TEST_ASSERT_EQUAL(TIMESERIES_INIT_ERR_CORRUPT, report.status);
  TEST_ASSERT_EQUAL(TIMESERIES_RECOVERY_PARTITION_ERASED, report.action);
  TEST_ASSERT_EQUAL(1, s_full_erases);  // one partition erase, not a loop
  TEST_ASSERT_NULL(timeseries_get_db_handle());
  TEST_ASSERT_FALSE(insert_points("rec_keep", 1, 1));
  TEST_ASSERT_EQUAL(-1, count_points("rec_keep"));
  disarm();
  TEST_ASSERT_TRUE(timeseries_init());
}

TEST_CASE("recovery: failing erases report a flash fault", "[recovery]") {
  uint32_t bad_page, bad_size;
  two_page_fixture(&bad_page, &bad_size);
  timeseries_deinit();

  const size_t body = bad_page + sizeof(timeseries_page_header_t);
  arm_fault(body, body + 1, CLEAR_ON_COVERING_ERASE);
  s_fail_erases = true;
  reset_counters();
  timeseries_recovery_report_t report;
  bool ok = timeseries_init_with_recovery(&report);
  s_fail_erases = false;
  TEST_ASSERT_FALSE(ok);
  TEST_ASSERT_EQUAL(TIMESERIES_INIT_ERR_FLASH, report.status);
  TEST_ASSERT_NULL(timeseries_get_db_handle());
  // The page was never erased: its neighbour's data is still there.
  disarm();
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_EQUAL(40, count_points("rec_keep"));
  TEST_ASSERT_EQUAL(20, count_points("rec_bad"));
}

TEST_CASE("recovery: a database closed by failed init refuses every call cleanly", "[recovery]") {
  uint32_t bad_page, bad_size;
  two_page_fixture(&bad_page, &bad_size);
  timeseries_deinit();
  TEST_ASSERT_NULL(timeseries_get_db_handle());

  TEST_ASSERT_FALSE(insert_points("rec_keep", 3, 1));
  timeseries_query_t q = {.measurement_name = "rec_keep"};
  timeseries_query_result_t r = {0};
  TEST_ASSERT_FALSE(timeseries_query(&q, &r));
  TEST_ASSERT_NULL(r.timestamps);
  TEST_ASSERT_EQUAL(0, r.num_points);
  timeseries_query_free_result(&r);
  char **names = NULL;
  size_t n = 0;
  TEST_ASSERT_FALSE(timeseries_get_measurements(&names, &n));
  TEST_ASSERT_FALSE(timeseries_get_fields_for_measurement("rec_keep", &names, &n));
  tsdb_tag_pair_t *tags = NULL;
  TEST_ASSERT_FALSE(timeseries_get_tags_for_measurement("rec_keep", &tags, &n));
  tsdb_usage_summary_t usage;
  TEST_ASSERT_FALSE(timeseries_get_usage_summary(&usage));
  TEST_ASSERT_FALSE(timeseries_delete_measurement("rec_keep"));
  TEST_ASSERT_FALSE(timeseries_delete_measurement_and_field("rec_keep", "v"));
  TEST_ASSERT_FALSE(timeseries_compact());
  TEST_ASSERT_FALSE(timeseries_compact_sync());
  TEST_ASSERT_FALSE(timeseries_expire());
  TEST_ASSERT_FALSE(timeseries_clear_all());
  timeseries_set_chunk_size(100);

  // The data is untouched and the next open sees it.
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_EQUAL(40, count_points("rec_keep"));
}

TEST_CASE("recovery: erase_partition refuses an open database and wipes a closed one", "[recovery]") {
  uint32_t bad_page, bad_size;
  two_page_fixture(&bad_page, &bad_size);
  reset_counters();
  TEST_ASSERT_FALSE(timeseries_erase_partition());
  TEST_ASSERT_EQUAL(0, s_erases);
  TEST_ASSERT_EQUAL(40, count_points("rec_keep"));
  timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_erase_partition());
  TEST_ASSERT_EQUAL(1, s_full_erases);
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_EQUAL(0, count_points("rec_keep"));
}

#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_fail_malloc_always(size_t size);
void tsdb_audit_allocation_probe(size_t fail_nth, bool measure);
bool tsdb_audit_allocation_hit(void);

TEST_CASE("recovery: out of memory never erases; bounded retries; data intact", "[recovery]") {
  uint32_t bad_page, bad_size;
  two_page_fixture(&bad_page, &bad_size);
  timeseries_deinit();

  // The revision scan's 4 KiB window cannot be allocated, on every attempt.
  tsdb_audit_fail_malloc_always(4096);
  reset_counters();
  timeseries_recovery_report_t report;
  bool ok = timeseries_init_with_recovery(&report);
  tsdb_audit_fail_malloc_always(0);
  TEST_ASSERT_FALSE(ok);
  TEST_ASSERT_EQUAL(TIMESERIES_INIT_ERR_TRANSIENT, report.status);
  TEST_ASSERT_EQUAL(TIMESERIES_RECOVERY_NONE, report.action);
  TEST_ASSERT_EQUAL_UINT32(0, report.pages_erased);
  TEST_ASSERT_EQUAL_UINT32(TIMESERIES_RECOVERY_TRANSIENT_ATTEMPTS, report.attempts);
  TEST_ASSERT_EQUAL(0, s_erases);
  TEST_ASSERT_NULL(timeseries_get_db_handle());

  // Memory is back: everything is still there.
  ok = timeseries_init_with_recovery(&report);
  TEST_ASSERT_TRUE(ok);
  TEST_ASSERT_EQUAL(TIMESERIES_RECOVERY_NONE, report.action);
  TEST_ASSERT_EQUAL(40, count_points("rec_keep"));
  TEST_ASSERT_EQUAL(20, count_points("rec_bad"));
}

TEST_CASE("recovery: allocation failure at every step of init never erases", "[recovery]") {
  uint32_t bad_page, bad_size;
  two_page_fixture(&bad_page, &bad_size);
  timeseries_deinit();
  reset_counters();
  // Fail the nth allocation of the first init attempt, for every n init
  // makes. Recovery must treat each as transient (retry, never erase).
  size_t steps = 0;
  for (size_t nth = 1; nth < 400; nth++) {
    timeseries_recovery_report_t report;
    tsdb_audit_allocation_probe(nth, false);
    bool ok = timeseries_init_with_recovery(&report);
    bool hit = tsdb_audit_allocation_hit();
    tsdb_audit_allocation_probe(0, false);
    TEST_ASSERT_EQUAL(TIMESERIES_RECOVERY_NONE, report.action);
    TEST_ASSERT_EQUAL(0, s_erases);
    if (ok) timeseries_deinit();
    if (!hit) break;
    steps++;
  }
  TEST_ASSERT_GREATER_THAN(10, steps);
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_EQUAL(40, count_points("rec_keep"));
  TEST_ASSERT_EQUAL(20, count_points("rec_bad"));
}

TEST_CASE("recovery: allocation failures in insert and query return errors", "[recovery]") {
  uint32_t bad_page, bad_size;
  two_page_fixture(&bad_page, &bad_size);
  for (size_t nth = 1; nth < 200; nth++) {
    tsdb_audit_allocation_probe(nth, false);
    (void)insert_points("rec_oom", 10, 20000 + nth * 10);
    bool hit_insert = tsdb_audit_allocation_hit();
    tsdb_audit_allocation_probe(nth, false);
    (void)count_points("rec_keep");
    bool hit_query = tsdb_audit_allocation_hit();
    tsdb_audit_allocation_probe(0, false);
    if (!hit_insert && !hit_query) break;
  }
  TEST_ASSERT_EQUAL(40, count_points("rec_keep"));
}
#endif
