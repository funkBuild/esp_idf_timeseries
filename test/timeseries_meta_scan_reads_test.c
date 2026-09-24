/**
 * @file timeseries_meta_scan_reads_test.c
 * @brief Flash-read cost of metadata scans.
 *
 * Read-only metadata scans copy each metadata page to RAM in doubling blocks
 * and serve entries from the copy; before that they issued one read per entry
 * header plus separate key/value reads. These tests count
 * esp_partition_read calls made by the calling task and check the buffered
 * iterator returns exactly what the flash-reading iterator returns.
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

extern timeseries_db_t* timeseries_get_db_handle(void);

// -----------------------------------------------------------------------------
// esp_partition_read counter (reads made by the calling task only)
// -----------------------------------------------------------------------------
#if CONFIG_IDF_TARGET_LINUX
/* timeseries_audit_alloc.c wraps esp_partition_read on Linux; its counter is
 * thread-local, so it already counts only this task's reads. */
void tsdb_audit_reset_reads(void);
size_t tsdb_audit_read_calls(void);
static void reads_start(void) { tsdb_audit_reset_reads(); }
static size_t reads_stop(void) { return tsdb_audit_read_calls(); }
#else
static TaskHandle_t volatile s_count_task;
static volatile size_t s_read_calls;
esp_err_t __real_esp_partition_read(const esp_partition_t* part, size_t offset, void* dst, size_t size);
esp_err_t __wrap_esp_partition_read(const esp_partition_t* part, size_t offset, void* dst, size_t size) {
  if (s_count_task && xTaskGetCurrentTaskHandle() == s_count_task) s_read_calls++;
  return __real_esp_partition_read(part, offset, dst, size);
}
static void reads_start(void) {
  s_read_calls = 0;
  s_count_task = xTaskGetCurrentTaskHandle();
}
static size_t reads_stop(void) {
  s_count_task = NULL;
  return s_read_calls;
}
#endif

// -----------------------------------------------------------------------------
// Fixture: one measurement, F fields x T tag values = F*T series
// -----------------------------------------------------------------------------
#define MSR_NAME "msr_scan_reads"
#define MSR_FIELDS 6
#define MSR_TAGVALS 8

static void setup_db(void) {
  static bool inited = false;
  if (!inited) {
    TEST_ASSERT_TRUE(timeseries_init());
    inited = true;
  }
  TEST_ASSERT_TRUE(timeseries_clear_all());

  for (int t = 0; t < MSR_TAGVALS; t++) {
    char tv[16];
    snprintf(tv, sizeof(tv), "dev%d", t);
    const char* tag_keys[] = {"device", "site"};
    const char* tag_values[] = {tv, (t & 1) ? "north" : "south"};
    char names[MSR_FIELDS][16];
    const char* field_names[MSR_FIELDS];
    for (int f = 0; f < MSR_FIELDS; f++) {
      snprintf(names[f], sizeof(names[f]), "field%d", f);
      field_names[f] = names[f];
    }
    uint64_t ts[2] = {1000 + (uint64_t)t, 2000 + (uint64_t)t};
    timeseries_field_value_t vals[MSR_FIELDS * 2];
    for (int i = 0; i < MSR_FIELDS * 2; i++) {
      vals[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
      vals[i].data.float_val = (double)(t * 100 + i);
    }
    timeseries_insert_data_t data = {
        .measurement_name = MSR_NAME,
        .tag_keys = tag_keys,
        .tag_values = tag_values,
        .num_tags = 2,
        .field_names = field_names,
        .field_values = vals,
        .num_fields = MSR_FIELDS,
        .timestamps_ms = ts,
        .num_points = 2,
    };
    TEST_ASSERT_TRUE(timeseries_insert(&data));
  }
}

/* Active metadata pages, from the flash page iterator (not counted). */
static size_t metadata_pages(timeseries_db_t* db, uint32_t* offsets, size_t cap) {
  timeseries_page_iterator_t it;
  TEST_ASSERT_TRUE(timeseries_page_iterator_init(db, &it));
  timeseries_page_header_t hdr;
  uint32_t off = 0, size = 0;
  size_t n = 0;
  while (timeseries_page_iterator_next(&it, &hdr, &off, &size)) {
    if (hdr.page_type == TIMESERIES_PAGE_TYPE_METADATA && n < cap) offsets[n++] = off;
  }
  return n;
}

TEST_CASE("meta scan: buffered entity iterator returns the same entries as flash reads",
          "[metadata][meta_scan_reads]") {
  setup_db();
  timeseries_db_t* db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);
  uint32_t pages[16];
  size_t npages = metadata_pages(db, pages, 16);
  TEST_ASSERT_GREATER_THAN(0, npages);

  size_t total_entries = 0;
  for (size_t p = 0; p < npages; p++) {
    timeseries_entity_iterator_t direct, buffered;
    TEST_ASSERT_TRUE(timeseries_entity_iterator_init(db, pages[p], TIMESERIES_METADATA_PAGE_SIZE, &direct));
    TEST_ASSERT_TRUE(timeseries_entity_iterator_init(db, pages[p], TIMESERIES_METADATA_PAGE_SIZE, &buffered));
    TEST_ASSERT_TRUE(timeseries_entity_iterator_buffer(&buffered));
    TEST_ASSERT_NOT_NULL(buffered.page_ptr);
    TEST_ASSERT_NULL(direct.page_ptr);

    for (;;) {
      timeseries_entry_header_t hd, hb;
      bool nd = timeseries_entity_iterator_next(&direct, &hd);
      bool nb = timeseries_entity_iterator_next(&buffered, &hb);
      TEST_ASSERT_EQUAL(nd, nb);
      TEST_ASSERT_EQUAL(direct.valid, buffered.valid);
      TEST_ASSERT_EQUAL(direct.offset, buffered.offset);
      if (!nd) break;
      TEST_ASSERT_EQUAL_MEMORY(&hd, &hb, sizeof(hd));
      TEST_ASSERT_EQUAL(direct.current_entry_offset, buffered.current_entry_offset);
      size_t len = (size_t)hd.key_len + hd.value_len;
      uint8_t* kd = malloc(len + 1);
      uint8_t* kb = malloc(len + 1);
      TEST_ASSERT_NOT_NULL(kd);
      TEST_ASSERT_NOT_NULL(kb);
      TEST_ASSERT_TRUE(timeseries_entity_iterator_read_data(&direct, &hd, kd, kd + hd.key_len));
      TEST_ASSERT_TRUE(timeseries_entity_iterator_read_data(&buffered, &hb, kb, kb + hb.key_len));
      if (len) TEST_ASSERT_EQUAL_MEMORY(kd, kb, len);
      free(kd);
      free(kb);
      total_entries++;
    }
    TEST_ASSERT_FALSE(direct.read_error);
    TEST_ASSERT_FALSE(buffered.read_error);
    timeseries_entity_iterator_deinit(&direct);
    timeseries_entity_iterator_deinit(&buffered);
    TEST_ASSERT_NULL(buffered.page_ptr);
  }
  printf("META_SCAN_ENTRIES pages=%zu entries=%zu\n", npages, total_entries);
  // Measurement + tag index + field lists + one type entry per series.
  TEST_ASSERT_GREATER_OR_EQUAL(1 + MSR_TAGVALS + 2 + MSR_FIELDS + MSR_FIELDS * MSR_TAGVALS, total_entries);
}

TEST_CASE("meta scan: read-only lookups read a metadata page in a few blocks", "[metadata][meta_scan_reads]") {
  setup_db();
  timeseries_db_t* db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);
  uint32_t pages[16];
  size_t npages = metadata_pages(db, pages, 16);
  TEST_ASSERT_GREATER_THAN(0, npages);

  uint32_t meas_id = 0;
  reads_start();
  TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, MSR_NAME, &meas_id));
  size_t r_find_hit = reads_stop();

  uint32_t missing_id = 0;
  reads_start();
  TEST_ASSERT_FALSE(tsdb_find_measurement_id(db, "no_such_measurement", &missing_id));
  size_t r_find_miss = reads_stop();

  timeseries_string_list_t fields;
  tsdb_string_list_init(&fields);
  reads_start();
  TEST_ASSERT_TRUE(tsdb_list_fields_for_measurement(db, meas_id, &fields));
  size_t r_list_fields = reads_stop();
  TEST_ASSERT_EQUAL(MSR_FIELDS, fields.count);
  tsdb_string_list_free(&fields);

  timeseries_series_id_list_t ids;
  tsdb_series_id_list_init(&ids);
  reads_start();
  TEST_ASSERT_TRUE(tsdb_find_series_ids_for_field(db, meas_id, "field3", &ids));
  size_t r_field = reads_stop();
  TEST_ASSERT_EQUAL(MSR_TAGVALS, ids.count);

  tsdb_series_id_list_clear(&ids);
  reads_start();
  TEST_ASSERT_TRUE(tsdb_find_series_ids_for_tag(db, meas_id, "site", "north", &ids));
  size_t r_tag = reads_stop();
  TEST_ASSERT_EQUAL(MSR_FIELDS * MSR_TAGVALS / 2, ids.count);

  tsdb_series_id_list_clear(&ids);
  reads_start();
  TEST_ASSERT_TRUE(tsdb_find_all_series_ids_for_measurement(db, meas_id, &ids));
  size_t r_all = reads_stop();
  TEST_ASSERT_EQUAL(MSR_FIELDS * MSR_TAGVALS, ids.count);
  tsdb_series_id_list_free(&ids);

  tsdb_tag_pair_t* tags = NULL;
  size_t ntags = 0;
  reads_start();
  TEST_ASSERT_TRUE(timeseries_metadata_get_tags_for_measurement(db, meas_id, &tags, &ntags));
  size_t r_tags = reads_stop();
  TEST_ASSERT_EQUAL(MSR_TAGVALS + 2, ntags);
  for (size_t i = 0; i < ntags; i++) {
    free(tags[i].key);
    free(tags[i].val);
  }
  free(tags);

  timeseries_string_list_t meas;
  tsdb_string_list_init(&meas);
  reads_start();
  TEST_ASSERT_TRUE(tsdb_list_all_measurements(db, &meas));
  size_t r_list_meas = reads_stop();
  TEST_ASSERT_EQUAL(1, meas.count);
  tsdb_string_list_free(&meas);

  uint32_t max_id = 0;
  reads_start();
  TEST_ASSERT_TRUE(tsdb_find_max_measurement_id(db, &max_id));
  size_t r_max = reads_stop();
  TEST_ASSERT_EQUAL(meas_id, max_id);

  printf("META_SCAN_READS pages=%zu find_hit=%zu find_miss=%zu list_fields=%zu field=%zu tag=%zu all=%zu "
         "tags=%zu list_meas=%zu max_id=%zu\n",
         npages, r_find_hit, r_find_miss, r_list_fields, r_field, r_tag, r_all, r_tags, r_list_meas, r_max);

  // Buffered pages load in doubling blocks (64 B .. 8 KB): at most 8 reads a
  // page, however many entries it holds. Per-entry reads cost one read per
  // entry header plus key/value reads, so a full scan read more times than
  // there are entries.
  const size_t per_page = 8;
  TEST_ASSERT_LESS_OR_EQUAL(npages * per_page, r_find_hit);
  TEST_ASSERT_LESS_OR_EQUAL(npages * per_page, r_find_miss);
  TEST_ASSERT_LESS_OR_EQUAL(npages * per_page, r_list_fields);
  TEST_ASSERT_LESS_OR_EQUAL(npages * per_page, r_field);
  TEST_ASSERT_LESS_OR_EQUAL(npages * per_page, r_tag);
  TEST_ASSERT_LESS_OR_EQUAL(npages * per_page, r_all);
  TEST_ASSERT_LESS_OR_EQUAL(npages * per_page, r_tags);
  TEST_ASSERT_LESS_OR_EQUAL(npages * per_page, r_list_meas);
  TEST_ASSERT_LESS_OR_EQUAL(npages * per_page, r_max);
  // A full scan reads the same blocks whichever lookup runs it.
  TEST_ASSERT_EQUAL(r_find_miss, r_all);
  TEST_ASSERT_EQUAL(r_find_miss, r_list_fields);
}

// -----------------------------------------------------------------------------
// Query: one metadata scan, measurement ID from the cache
// -----------------------------------------------------------------------------

static const timeseries_query_result_column_t* find_column(const timeseries_query_result_t* r, const char* name) {
  for (size_t c = 0; c < r->num_columns; c++) {
    if (strcmp(r->columns[c].name, name) == 0) return &r->columns[c];
  }
  return NULL;
}

TEST_CASE("meta scan: query metadata phase scans the metadata pages once", "[metadata][meta_scan_reads]") {
  setup_db();
  timeseries_db_t* db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);
  uint32_t pages[16];
  size_t npages = metadata_pages(db, pages, 16);
  TEST_ASSERT_GREATER_THAN(0, npages);

  // Reads of one full metadata scan, for scale.
  uint32_t meas_id = 0;
  TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, MSR_NAME, &meas_id));
  timeseries_series_id_list_t ids;
  tsdb_series_id_list_init(&ids);
  reads_start();
  TEST_ASSERT_TRUE(tsdb_find_all_series_ids_for_measurement(db, meas_id, &ids));
  size_t r_one_scan = reads_stop();
  tsdb_series_id_list_free(&ids);

  // A field with no series: the query ends after the metadata phase, so every
  // read counted here is a metadata read. The measurement ID comes from the
  // cache the inserts filled; tags and fields share one scan.
  const char* tk[] = {"site", "device"};
  const char* tv[] = {"north", "dev3"};
  const char* nofield[] = {"no_such_field"};
  timeseries_query_t q = {
      .measurement_name = MSR_NAME, .tag_keys = tk, .tag_values = tv, .num_tags = 2,
      .field_names = nofield, .num_fields = 1,
  };
  timeseries_query_result_t r;
  reads_start();
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  size_t r_meta_only = reads_stop();
  TEST_ASSERT_EQUAL(0, r.num_points);
  TEST_ASSERT_EQUAL(0, r.num_columns);
  timeseries_query_free_result(&r);

  // Full query (all fields, two tag filters): same answer as before the change.
  q.field_names = NULL;
  q.num_fields = 0;
  reads_start();
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  size_t r_full = reads_stop();
  TEST_ASSERT_EQUAL(MSR_FIELDS, r.num_columns);
  TEST_ASSERT_EQUAL(2, r.num_points);
  TEST_ASSERT_EQUAL_UINT64(1003, r.timestamps[0]);
  TEST_ASSERT_EQUAL_UINT64(2003, r.timestamps[1]);
  for (int f = 0; f < MSR_FIELDS; f++) {
    char name[16];
    snprintf(name, sizeof(name), "field%d", f);
    const timeseries_query_result_column_t* col = find_column(&r, name);
    TEST_ASSERT_NOT_NULL(col);
    for (int p = 0; p < 2; p++) {
      TEST_ASSERT_EQUAL(TIMESERIES_FIELD_TYPE_FLOAT, col->values[p].type);
      TEST_ASSERT_EQUAL_DOUBLE((double)(3 * 100 + f * 2 + p), col->values[p].data.float_val);
    }
  }
  timeseries_query_free_result(&r);

  // No tags, two named fields: every series of those fields.
  const char* two[] = {"field1", "field4"};
  timeseries_query_t q2 = {.measurement_name = MSR_NAME, .field_names = two, .num_fields = 2};
  reads_start();
  TEST_ASSERT_TRUE(timeseries_query(&q2, &r));
  size_t r_untagged = reads_stop();
  TEST_ASSERT_EQUAL(2, r.num_columns);
  TEST_ASSERT_EQUAL(2 * MSR_TAGVALS, r.num_points);
  TEST_ASSERT_NOT_NULL(find_column(&r, "field1"));
  TEST_ASSERT_NOT_NULL(find_column(&r, "field4"));
  timeseries_query_free_result(&r);

  printf("META_SCAN_QUERY_READS pages=%zu one_scan=%zu meta_only=%zu full=%zu untagged=%zu\n", npages, r_one_scan,
         r_meta_only, r_full, r_untagged);
  // Cached measurement ID + one scan for both tag filters and the field
  // (separately that is a measurement scan, two tag scans and a field scan).
  TEST_ASSERT_EQUAL(r_one_scan, r_meta_only);
}

TEST_CASE("meta scan: query sees a re-created measurement, not a stale cached ID", "[metadata][meta_scan_reads]") {
  setup_db();
  const char* fields[] = {"v"};
  uint64_t ts[1] = {5000};
  timeseries_field_value_t val = {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 1.0};
  timeseries_insert_data_t d = {
      .measurement_name = "msr_recreate", .field_names = fields, .field_values = &val, .num_fields = 1,
      .timestamps_ms = ts, .num_points = 1,
  };
  TEST_ASSERT_TRUE(timeseries_insert(&d));

  timeseries_query_t q = {.measurement_name = "msr_recreate"};
  timeseries_query_result_t r;
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));  // caches the ID
  TEST_ASSERT_EQUAL(1, r.num_points);
  timeseries_query_free_result(&r);

  TEST_ASSERT_TRUE(timeseries_delete_measurement("msr_recreate"));
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  TEST_ASSERT_EQUAL(0, r.num_points);
  timeseries_query_free_result(&r);

  ts[0] = 6000;
  val.data.float_val = 2.0;
  TEST_ASSERT_TRUE(timeseries_insert(&d));  // new measurement ID
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_EQUAL_UINT64(6000, r.timestamps[0]);
  TEST_ASSERT_EQUAL_DOUBLE(2.0, r.columns[0].values[0].data.float_val);
  timeseries_query_free_result(&r);
}
