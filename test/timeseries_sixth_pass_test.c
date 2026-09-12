#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_compaction.h"
#include "timeseries_data.h"
#include "timeseries_iterator.h"
#include "timeseries_page_stream_writer.h"
#include "timeseries_points_iterator.h"
#include "esp_timer.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

extern timeseries_db_t* timeseries_get_db_handle(void);
#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_allocation_probe(size_t, bool);
bool tsdb_audit_allocation_hit(void);
size_t tsdb_audit_largest_allocation(void);
void tsdb_audit_fail_calloc(size_t, size_t);
bool tsdb_audit_calloc_failed(void);
void tsdb_audit_fail_read_at(size_t, bool);
#endif

static timeseries_field_value_t make_value(timeseries_field_type_e type, int n) {
  timeseries_field_value_t value = {.type = type};
  switch (type) {
    case TIMESERIES_FIELD_TYPE_INT:
      value.data.int_val = n;
      break;
    case TIMESERIES_FIELD_TYPE_FLOAT:
      value.data.float_val = n + .25;
      break;
    case TIMESERIES_FIELD_TYPE_BOOL:
      value.data.bool_val = n % 2;
      break;
    case TIMESERIES_FIELD_TYPE_STRING:
      value.data.string_val.str = n >= 30 ? "newest" : "older";
      value.data.string_val.length = strlen(value.data.string_val.str);
      break;
  }
  return value;
}

static timeseries_db_t* prepare(timeseries_field_type_e type, uint8_t series[16]) {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t* db = timeseries_get_db_handle();
  atomic_store(&db->compaction_in_progress, true);
  uint64_t ts = 999999;
  timeseries_field_value_t value = make_value(type, 0);
  const char* field = "v";
  timeseries_insert_data_t insert = {.measurement_name = "sixth",
                                     .field_names = &field,
                                     .field_values = &value,
                                     .num_fields = 1,
                                     .timestamps_ms = &ts,
                                     .num_points = 1};
  TEST_ASSERT_TRUE(timeseries_insert(&insert));
  timeseries_field_data_header_t h;
  TEST_ASSERT_EQUAL(
      ESP_OK,
      esp_partition_read(db->partition, db->last_l0_page_offset + sizeof(timeseries_page_header_t), &h, sizeof(h)));
  memcpy(series, h.series_id, 16);
  return db;
}
static void cleanup(timeseries_db_t* db) {
  atomic_store(&db->compaction_in_progress, false);
  timeseries_deinit();
}

static size_t stored_points(timeseries_db_t* db, unsigned level) {
  timeseries_page_cache_iterator_t it;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &it));
  timeseries_page_header_t h;
  uint32_t offset, size;
  size_t count = 0;
  while (timeseries_page_cache_iterator_next(&it, &h, &offset, &size)) {
    if (h.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA || h.field_data_level != level) continue;
    timeseries_fielddata_iterator_t fields;
    TEST_ASSERT_TRUE(timeseries_fielddata_iterator_init(db, offset, size, &fields));
    timeseries_field_data_header_t f;
    while (timeseries_fielddata_iterator_next(&fields, &f))
      if (f.flags & TSDB_FIELDDATA_FLAG_DELETED) count += f.record_count;
    timeseries_fielddata_iterator_deinit(&fields);
  }
  timeseries_page_cache_iterator_deinit(&it);
  return count;
}

static uint32_t write_record(timeseries_db_t* db, const uint8_t* series, timeseries_field_type_e type, int generation,
                             bool overlap) {
  timeseries_page_stream_writer_t writer = {0};
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, 1, 4096));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(&writer, series, type));
  uint32_t data_offset = writer.col_hdr_offset;
  for (int i = 0; i < 2; i++)
    TEST_ASSERT_TRUE(
        timeseries_page_stream_writer_write_timestamp(&writer, (overlap ? 1 : generation * 2 + 1) + i * 2));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));
  for (int i = 0; i < 2; i++) {
    timeseries_field_value_t v = make_value(type, generation * 10 + i);
    TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&writer, &v));
  }
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));
  return data_offset;
}

static void overlap_test(timeseries_field_type_e type) {
  uint8_t series[16];
  timeseries_db_t* db = prepare(type, series);
  for (int gen = 0; gen < 4; gen++) write_record(db, series, type, gen, true);
  TEST_ASSERT_TRUE(timeseries_compact_level_pages(db, 1, 2));
  size_t stored = stored_points(db, 2);
  timeseries_query_t q = {.measurement_name = "sixth", .start_ms = 1, .end_ms = 3};
  timeseries_query_result_t r = {0};
  bool queried = timeseries_query(&q, &r);
  size_t count = r.num_points;
  bool values_ok = queried && count == 2;
  for (size_t i = 0; values_ok && i < 2; i++) {
    timeseries_field_value_t* v = &r.columns[0].values[i];
    values_ok = r.timestamps[i] == 1 + i * 2;
    if (type == TIMESERIES_FIELD_TYPE_INT)
      values_ok &= v->data.int_val == 30 + (int)i;
    else if (type == TIMESERIES_FIELD_TYPE_FLOAT)
      values_ok &= v->data.float_val == 30.25 + i;
    else if (type == TIMESERIES_FIELD_TYPE_BOOL)
      values_ok &= v->data.bool_val == (bool)i;
    else
      values_ok &= v->data.string_val.length == 6 && memcmp(v->data.string_val.str, "newest", 6) == 0;
  }
  timeseries_query_free_result(&r);
  cleanup(db);
  TEST_ASSERT_EQUAL(2, stored);
  TEST_ASSERT_EQUAL(2, count);
  TEST_ASSERT_TRUE(values_ok);
}
TEST_CASE("sixth: overlapping INT records merge by timestamp", "[sixth][sixth_overlap]") {
  overlap_test(TIMESERIES_FIELD_TYPE_INT);
}
TEST_CASE("sixth: overlapping FLOAT records merge by timestamp", "[sixth][sixth_overlap]") {
  overlap_test(TIMESERIES_FIELD_TYPE_FLOAT);
}
TEST_CASE("sixth: overlapping BOOL records merge by timestamp", "[sixth][sixth_overlap]") {
  overlap_test(TIMESERIES_FIELD_TYPE_BOOL);
}
TEST_CASE("sixth: overlapping STRING records release discarded values", "[sixth][sixth_overlap]") {
  overlap_test(TIMESERIES_FIELD_TYPE_STRING);
}

static void raw_merge_test(bool overlap) {
  uint8_t series[16];
  timeseries_db_t* db = prepare(TIMESERIES_FIELD_TYPE_INT, series);
  uint64_t* ts = malloc(500 * sizeof(*ts));
  timeseries_field_value_t* values = calloc(500, sizeof(*values));
  TEST_ASSERT_NOT_NULL(ts);
  TEST_ASSERT_NOT_NULL(values);
  const char* field = "v";
  timeseries_insert_data_t insert = {.measurement_name = "sixth",
                                     .field_names = &field,
                                     .field_values = values,
                                     .num_fields = 1,
                                     .timestamps_ms = ts,
                                     .num_points = 500};
  for (size_t page = 0; page < 32; page++) {
    for (size_t i = 0; i < 500; i++) {
      ts[i] = (overlap ? 0 : page * 500) + i + 1;
      values[i] = make_value(TIMESERIES_FIELD_TYPE_INT, page * 500 + i + 1);
    }
    TEST_ASSERT_TRUE(timeseries_insert(&insert));
  }
  free(ts);
  free(values);
#if CONFIG_IDF_TARGET_LINUX
  tsdb_audit_allocation_probe(0, true);
#endif
  int64_t start = esp_timer_get_time();
  bool compacted = timeseries_compact_level_pages(db, 0, 1);
  int64_t elapsed = esp_timer_get_time() - start;
  size_t largest = 0;
#if CONFIG_IDF_TARGET_LINUX
  largest = tsdb_audit_largest_allocation();
  tsdb_audit_allocation_probe(0, false);
#endif
  size_t count = stored_points(db, 1);
  timeseries_query_t q = {
      .measurement_name = "sixth", .start_ms = overlap ? 498 : 15998, .end_ms = overlap ? 500 : 16000};
  timeseries_query_result_t r = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  TEST_ASSERT_EQUAL(3, r.num_points);
  for (size_t i = 0; i < 3; i++) TEST_ASSERT_EQUAL_INT64(15998 + i, r.columns[0].values[i].data.int_val);
  timeseries_query_free_result(&r);
  cleanup(db);
  printf("SIXTH_MERGE overlap=%d us=%lld largest=%zu\n", overlap, (long long)elapsed, largest);
  TEST_ASSERT_TRUE(compacted);
  TEST_ASSERT_EQUAL(overlap ? 501 : 16001, count);
#if CONFIG_IDF_TARGET_LINUX
  TEST_ASSERT_LESS_OR_EQUAL(65536, largest);
#endif
}
TEST_CASE("sixth: raw merge uses bounded allocations", "[sixth][sixth_memory]") { raw_merge_test(false); }
TEST_CASE("sixth: overlapping raw merge uses bounded allocations", "[sixth][sixth_memory]") { raw_merge_test(true); }

#if CONFIG_IDF_TARGET_LINUX
TEST_CASE("sixth: decoder allocation failure preserves source pages", "[sixth][sixth_decoder_oom]") {
  uint8_t series[16];
  timeseries_db_t* db = prepare(TIMESERIES_FIELD_TYPE_INT, series);
  for (int gen = 0; gen < 4; gen++) write_record(db, series, TIMESERIES_FIELD_TYPE_INT, gen, true);
  tsdb_audit_fail_calloc(1, sizeof(timeseries_points_iterator_t));
  bool compacted = timeseries_compact_level_pages(db, 1, 2);
  bool failed = tsdb_audit_calloc_failed();
  tsdb_audit_fail_calloc(0, 0);
  size_t retained = stored_points(db, 1);
  cleanup(db);
  TEST_ASSERT_TRUE(failed);
  TEST_ASSERT_FALSE(compacted);
  TEST_ASSERT_EQUAL(8, retained);
}
TEST_CASE("sixth: decoder read failure preserves source pages", "[sixth][sixth_read_error]") {
  uint8_t series[16];
  timeseries_db_t* db = prepare(TIMESERIES_FIELD_TYPE_INT, series);
  uint32_t offset = 0;
  for (int gen = 0; gen < 4; gen++) {
    uint32_t data = write_record(db, series, TIMESERIES_FIELD_TYPE_INT, gen, true);
    if (!gen) offset = data;
  }
  tsdb_audit_fail_read_at(offset, true);
  bool compacted = timeseries_compact_level_pages(db, 1, 2);
  tsdb_audit_fail_read_at(0, false);
  size_t retained = stored_points(db, 1);
  cleanup(db);
  TEST_ASSERT_FALSE(compacted);
  TEST_ASSERT_EQUAL(8, retained);
}
TEST_CASE("sixth: every string merge allocation failure preserves query results", "[sixth][sixth_fault_sweep]") {
  bool exhausted = false;
  size_t failures = 0;
  for (size_t nth = 1; nth <= 256; nth++) {
    uint8_t series[16];
    timeseries_db_t* db = prepare(TIMESERIES_FIELD_TYPE_STRING, series);
    for (int gen = 0; gen < 4; gen++) write_record(db, series, TIMESERIES_FIELD_TYPE_STRING, gen, true);
    tsdb_audit_allocation_probe(nth, false);
    (void)timeseries_compact_level_pages(db, 1, 2);
    bool hit = tsdb_audit_allocation_hit();
    tsdb_audit_allocation_probe(0, false);
    timeseries_query_t q = {.measurement_name = "sixth", .start_ms = 1, .end_ms = 3};
    timeseries_query_result_t r = {0};
    bool queried = timeseries_query(&q, &r);
    bool valid = queried && r.num_points == 2;
    for (size_t i = 0; valid && i < 2; i++)
      valid = r.columns[0].values[i].data.string_val.length == 6 &&
              !memcmp(r.columns[0].values[i].data.string_val.str, "newest", 6);
    timeseries_query_free_result(&r);
    cleanup(db);
    TEST_ASSERT_TRUE_MESSAGE(valid, "allocation failure changed data");
    if (!hit) {
      exhausted = true;
      break;
    }
    failures++;
  }
  printf("SIXTH_FAULTS tested=%zu\n", failures);
  TEST_ASSERT_TRUE(exhausted);
  TEST_ASSERT_GREATER_THAN(20, failures);
}
#endif

TEST_CASE("sixth: newest record on the same page wins duplicate timestamps", "[sixth][sixth_same_page]") {
  uint8_t series[16];
  timeseries_db_t* db = prepare(TIMESERIES_FIELD_TYPE_INT, series);
  const char* field = "v";
  uint64_t ts = 7;
  for (int i = 1; i <= 5; i++) {
    timeseries_field_value_t v = make_value(TIMESERIES_FIELD_TYPE_INT, i);
    timeseries_insert_data_t insert = {.measurement_name = "sixth",
                                       .field_names = &field,
                                       .field_values = &v,
                                       .num_fields = 1,
                                       .timestamps_ms = &ts,
                                       .num_points = 1};
    TEST_ASSERT_TRUE(timeseries_insert(&insert));
  }
  TEST_ASSERT_TRUE(timeseries_compact_all_levels_force(db));
  timeseries_query_t q = {.measurement_name = "sixth", .start_ms = 7, .end_ms = 7};
  timeseries_query_result_t r = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  TEST_ASSERT_EQUAL_INT64(5, r.columns[0].values[0].data.int_val);
  timeseries_query_free_result(&r);
  cleanup(db);
}

TEST_CASE("sixth: string chunks preserve long binary and empty values", "[sixth][sixth_long_strings]") {
  uint8_t series[16];
  timeseries_db_t* db = prepare(TIMESERIES_FIELD_TYPE_STRING, series);
  char* bytes = malloc(3000);
  TEST_ASSERT_NOT_NULL(bytes);
  memset(bytes, 'x', 3000);
  bytes[7] = 0;
  uint64_t ts[] = {1, 2, 3, 4};
  timeseries_field_value_t values[4];
  for (int i = 0; i < 4; i++) {
    values[i].type = TIMESERIES_FIELD_TYPE_STRING;
    values[i].data.string_val.str = bytes;
    values[i].data.string_val.length = i == 3 ? 0 : 3000;
  }
  const char* field = "v";
  timeseries_insert_data_t insert = {.measurement_name = "sixth",
                                     .field_names = &field,
                                     .field_values = values,
                                     .num_fields = 1,
                                     .timestamps_ms = ts,
                                     .num_points = 4};
  TEST_ASSERT_TRUE(timeseries_insert(&insert));
  TEST_ASSERT_TRUE(timeseries_compact_all_levels_force(db));
  timeseries_query_t q = {.measurement_name = "sixth", .start_ms = 1, .end_ms = 4};
  timeseries_query_result_t r = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  TEST_ASSERT_EQUAL(4, r.num_points);
  for (int i = 0; i < 4; i++) {
    TEST_ASSERT_EQUAL(i == 3 ? 0 : 3000, r.columns[0].values[i].data.string_val.length);
    if (i != 3) TEST_ASSERT_EQUAL_MEMORY(bytes, r.columns[0].values[i].data.string_val.str, 3000);
  }
  free(bytes);
  timeseries_query_free_result(&r);
  cleanup(db);
}

TEST_CASE("sixth: malformed record header aborts before source retirement", "[sixth][sixth_bad_header]") {
  uint8_t series[16];
  timeseries_db_t* db = prepare(TIMESERIES_FIELD_TYPE_INT, series);
  uint32_t offset = db->last_l0_page_offset;
  timeseries_page_header_t header;
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(db->partition, offset, &header, sizeof(header)));
  uint8_t* page = malloc(header.page_size);
  TEST_ASSERT_NOT_NULL(page);
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(db->partition, offset, page, header.page_size));
  uint16_t bad_length = 0x8000;
  memcpy(page + sizeof(header) + offsetof(timeseries_field_data_header_t, record_length), &bad_length,
         sizeof(bad_length));
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_erase_range(db->partition, offset, header.page_size));
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_write(db->partition, offset, page, header.page_size));
  free(page);
  bool compacted = timeseries_compact_all_levels_force(db);
  tsdb_usage_summary_t usage;
  TEST_ASSERT_TRUE(timeseries_get_usage_summary(&usage));
  // Leave a valid database for the next test; revision recovery now checks records at open.
  atomic_store(&db->compaction_in_progress, false);
  TEST_ASSERT_TRUE(timeseries_clear_all());
  cleanup(db);
  TEST_ASSERT_FALSE(compacted);
  TEST_ASSERT_EQUAL(1, usage.page_summaries[0].num_pages);
}

#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_fail_timestamp_finalize(bool);
bool tsdb_audit_timestamp_finalize_hit(void);
TEST_CASE("sixth: failed timestamp finalization preserves source pages", "[sixth][sixth_timestamp_error]") {
  uint8_t series[16];
  timeseries_db_t* db = prepare(TIMESERIES_FIELD_TYPE_BOOL, series);
  for (int gen = 0; gen < 4; gen++) write_record(db, series, TIMESERIES_FIELD_TYPE_BOOL, gen, true);
  tsdb_audit_fail_timestamp_finalize(true);
  bool compacted = timeseries_compact_level_pages(db, 1, 2);
  bool hit = tsdb_audit_timestamp_finalize_hit();
  tsdb_audit_fail_timestamp_finalize(false);
  size_t retained = stored_points(db, 1);
  cleanup(db);
  TEST_ASSERT_TRUE(hit);
  TEST_ASSERT_FALSE(compacted);
  TEST_ASSERT_EQUAL(8, retained);
}
#endif
