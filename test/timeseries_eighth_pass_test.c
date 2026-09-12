#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_compaction.h"
#include "timeseries_iterator.h"
#include "timeseries_page_stream_writer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include <stdlib.h>
#include <string.h>

extern timeseries_db_t *timeseries_get_db_handle(void);
static TaskHandle_t hook_owner, hook_task;
static SemaphoreHandle_t hook_done;
static bool hook_progress;
static void (*writer_callback)(timeseries_db_t *);
static TaskHandle_t writer_callback_owner;
void tsdb_audit_writer_callback(void (*callback)(timeseries_db_t *)) {
  writer_callback = callback;
  writer_callback_owner = callback ? xTaskGetCurrentTaskHandle() : NULL;
}
bool __real_timeseries_page_stream_writer_init(timeseries_db_t *, timeseries_page_stream_writer_t *, uint8_t, uint32_t);
bool __wrap_timeseries_page_stream_writer_init(timeseries_db_t *db, timeseries_page_stream_writer_t *writer,
                                               uint8_t level, uint32_t size) {
  if (writer_callback && writer_callback_owner == xTaskGetCurrentTaskHandle()) {
    void (*callback)(timeseries_db_t *) = writer_callback;
    writer_callback = NULL;
    callback(db);
  }
  if (hook_owner == xTaskGetCurrentTaskHandle() && hook_task) {
    TaskHandle_t task = hook_task;
    hook_task = NULL;
    xTaskNotifyGive(task);
    hook_progress = xSemaphoreTake(hook_done, pdMS_TO_TICKS(2000)) == pdTRUE;
  }
  return __real_timeseries_page_stream_writer_init(db, writer, level, size);
}
static bool insert(uint64_t *ts, timeseries_field_value_t *v, size_t n) {
  const char *field = "v";
  timeseries_insert_data_t in = {.measurement_name = "eighth", .field_names = &field,
      .field_values = v, .num_fields = 1, .timestamps_ms = ts, .num_points = n};
  return timeseries_insert(&in);
}
static bool int_value_matches(int64_t expected) {
  timeseries_query_t q = {.measurement_name = "eighth", .start_ms = 1, .end_ms = 1};
  timeseries_query_result_t r = {0};
  bool ok = timeseries_query(&q, &r) && r.num_points == 1 && r.num_columns == 1 &&
            r.columns[0].values[0].data.int_val == expected;
  timeseries_query_free_result(&r);
  return ok;
}
static bool newest_value(void) { return int_value_matches(99); }
typedef struct { SemaphoreHandle_t done; bool inserted; } concurrent_t;
static void insert_worker(void *arg) {
  concurrent_t *c = arg;
  ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
  uint64_t ts = 1;
  timeseries_field_value_t v = {.type = TIMESERIES_FIELD_TYPE_INT, .data.int_val = 99};
  c->inserted = insert(&ts, &v, 1);
  xSemaphoreGive(c->done);
  vTaskDelete(NULL);
}
TEST_CASE("eighth: concurrent update wins before output page reservation", "[eighth][eighth_concurrent]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  atomic_store(&db->compaction_in_progress, true);
  uint64_t *ts = malloc(500*sizeof(*ts));
  timeseries_field_value_t *v = calloc(500, sizeof(*v));
  TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(v);
  for (size_t page = 0; page < 4; page++) {
    for (size_t i = 0; i < 500; i++) {
      ts[i] = page*500+i+1; v[i].type = TIMESERIES_FIELD_TYPE_INT; v[i].data.int_val = 1;
    }
    TEST_ASSERT_TRUE(insert(ts, v, 500));
  }
  free(ts); free(v);
  concurrent_t c = {.done = xSemaphoreCreateBinary()};
  TEST_ASSERT_NOT_NULL(c.done);
  TEST_ASSERT_EQUAL(pdPASS, xTaskCreate(insert_worker, "eighth_insert", 8192, &c, tskIDLE_PRIORITY+1, &hook_task));
  hook_owner = xTaskGetCurrentTaskHandle(); hook_done = c.done; hook_progress = false;
  bool compacted = timeseries_compact_level0_pages(db);
  bool progress = hook_progress;
  hook_owner = NULL; hook_task = NULL; hook_done = NULL;
  bool done = progress || xSemaphoreTake(c.done, pdMS_TO_TICKS(2000)) == pdTRUE;
  bool before = done && newest_value();
  if (done) vSemaphoreDelete(c.done);
  atomic_store(&db->compaction_in_progress, false);
  timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_init());
  bool after = newest_value();
  timeseries_deinit();
  TEST_ASSERT_TRUE(done); TEST_ASSERT_TRUE(progress); TEST_ASSERT_TRUE(c.inserted);
  TEST_ASSERT_TRUE(compacted); TEST_ASSERT_TRUE(before); TEST_ASSERT_TRUE(after);
}

TEST_CASE("eighth: append to an older page wins after a cold reopen", "[eighth][eighth_reuse]") {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  atomic_store(&db->compaction_in_progress, true);
  uint64_t *ts = malloc(500*sizeof(*ts));
  timeseries_field_value_t *v = calloc(500, sizeof(*v));
  TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(v);
  uint32_t older = 0;
  for (int page = 0; page < 2; page++) {
    for (size_t i = 0; i < 500; i++) { ts[i] = i+1; v[i].type = TIMESERIES_FIELD_TYPE_INT; v[i].data.int_val = page+1; }
    TEST_ASSERT_TRUE(insert(ts, v, 500));
    if (!page) {
      older = db->last_l0_page_offset;
      // Model a compactor claiming the older page while the newer page is
      // inserted, then aborting and leaving the old free space reusable.
      db->compaction_claimed_pages[0] = older;
      db->compaction_claimed_count = 1;
      db->last_l0_cache_valid = false;
    }
  }
  db->compaction_claimed_count = 0;
  free(ts); free(v);
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_init()); atomic_store(&db->compaction_in_progress, true);
  uint64_t timestamp = 1;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_INT, .data.int_val = 99};
  bool inserted = insert(&timestamp, &value, 1);
  bool reused = db->last_l0_page_offset == older;
  bool matched = newest_value();
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE_MESSAGE(reused, "fixture must reuse the older page");
  TEST_ASSERT_TRUE_MESSAGE(matched, "latest append must win");
}

static timeseries_field_value_t typed_value(timeseries_field_type_e type, int value) {
  timeseries_field_value_t v = {.type = type};
  switch (type) {
    case TIMESERIES_FIELD_TYPE_INT: v.data.int_val = value; break;
    case TIMESERIES_FIELD_TYPE_FLOAT: v.data.float_val = value + .25; break;
    case TIMESERIES_FIELD_TYPE_BOOL: v.data.bool_val = value == 99; break;
    case TIMESERIES_FIELD_TYPE_STRING:
      v.data.string_val.str = value == 99 ? "new\0value" : "old";
      v.data.string_val.length = value == 99 ? 9 : 3;
      break;
  }
  return v;
}
static bool typed_match_at(timeseries_field_type_e type, uint64_t timestamp) {
  timeseries_query_t q = {.measurement_name = "eighth", .start_ms = timestamp, .end_ms = timestamp};
  timeseries_query_result_t r = {0};
  bool ok = timeseries_query(&q, &r) && r.num_points == 1 && r.num_columns == 1;
  if (ok) {
    timeseries_field_value_t *v = &r.columns[0].values[0];
    if (type == TIMESERIES_FIELD_TYPE_INT) ok = v->data.int_val == 99;
    if (type == TIMESERIES_FIELD_TYPE_FLOAT) ok = v->data.float_val == 99.25;
    if (type == TIMESERIES_FIELD_TYPE_BOOL) ok = v->data.bool_val;
    if (type == TIMESERIES_FIELD_TYPE_STRING)
      ok = v->data.string_val.length == 9 && memcmp(v->data.string_val.str, "new\0value", 9) == 0;
  }
  timeseries_query_free_result(&r); return ok;
}
static bool typed_match(timeseries_field_type_e type) { return typed_match_at(type, 1) && typed_match_at(type, 3); }
static timeseries_db_t *prepare_typed(timeseries_field_type_e type, uint8_t series[16]) {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  atomic_store(&db->compaction_in_progress, true);
  uint64_t seed = 999999;
  timeseries_field_value_t value = typed_value(type, 0);
  TEST_ASSERT_TRUE(insert(&seed, &value, 1));
  timeseries_field_data_header_t header;
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(db->partition,
      db->last_l0_page_offset + sizeof(timeseries_page_header_t), &header, sizeof(header)));
  memcpy(series, header.series_id, 16);
  return db;
}
static uint32_t write_pair(timeseries_db_t *db, const uint8_t *series, timeseries_field_type_e type,
                           uint8_t level, uint64_t first, uint64_t last, int value) {
  timeseries_page_stream_writer_t writer = {0};
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &writer, level, 4096));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(&writer, series, type));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&writer, first));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&writer, last));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&writer));
  timeseries_field_value_t v = typed_value(type, value);
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&writer, &v));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&writer, &v));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&writer));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&writer));
  return writer.base_offset;
}
static void mixed_revisions(timeseries_field_type_e type) {
  uint8_t series[16]; timeseries_db_t *db = prepare_typed(type, series);
  write_pair(db, series, type, 1, 1, 3, 1);
  // This page is outside the L1 selection; its update is newer than the
  // timestamp-1 source, but older than the other selected source records.
  write_pair(db, series, type, 2, 1, 3, 99);
  write_pair(db, series, type, 1, 2, 4, 2);
  write_pair(db, series, type, 1, 6, 7, 3);
  write_pair(db, series, type, 1, 8, 9, 4);
  bool before = typed_match(type);
  bool compacted = timeseries_compact_level_pages(db, 1, 2);
  bool after = typed_match(type);
  bool forced = timeseries_compact_all_levels_force(db);
  bool final = typed_match(type);
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_init());
  bool reopened = typed_match(type);
  timeseries_deinit();
  TEST_ASSERT_TRUE(before); TEST_ASSERT_TRUE(compacted); TEST_ASSERT_TRUE(after);
  TEST_ASSERT_TRUE(forced); TEST_ASSERT_TRUE(final); TEST_ASSERT_TRUE(reopened);
}
TEST_CASE("eighth: INT compaction preserves per-point revisions", "[eighth][eighth_mixed]") {
  mixed_revisions(TIMESERIES_FIELD_TYPE_INT);
}
TEST_CASE("eighth: FLOAT compaction preserves per-point revisions", "[eighth][eighth_mixed]") {
  mixed_revisions(TIMESERIES_FIELD_TYPE_FLOAT);
}
TEST_CASE("eighth: BOOL compaction preserves per-point revisions", "[eighth][eighth_mixed]") {
  mixed_revisions(TIMESERIES_FIELD_TYPE_BOOL);
}
TEST_CASE("eighth: STRING compaction preserves per-point revisions", "[eighth][eighth_mixed]") {
  mixed_revisions(TIMESERIES_FIELD_TYPE_STRING);
}
TEST_CASE("eighth: rewriting an old page cannot promote its values", "[eighth][eighth_rewrite]") {
  uint8_t series[16]; timeseries_db_t *db = prepare_typed(TIMESERIES_FIELD_TYPE_INT, series);
  uint32_t old = write_pair(db, series, TIMESERIES_FIELD_TYPE_INT, 1, 1, 3, 1);
  write_pair(db, series, TIMESERIES_FIELD_TYPE_INT, 1, 1, 3, 99);
  bool before = newest_value();
  bool rewritten = timeseries_compact_page_list(db, &old, 1);
  bool after = newest_value();
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_init()); bool reopened = newest_value(); timeseries_deinit();
  TEST_ASSERT_TRUE(before); TEST_ASSERT_TRUE(rewritten); TEST_ASSERT_TRUE(after); TEST_ASSERT_TRUE(reopened);
}

static uint32_t write_legacy_pair(timeseries_db_t *db, const uint8_t *series, int value) {
  uint32_t offset = write_pair(db, series, TIMESERIES_FIELD_TYPE_INT, 1, 1, 3, value);
  uint8_t *page = malloc(4096);
  TEST_ASSERT_NOT_NULL(page);
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(db->partition, offset, page, 4096));
  timeseries_field_data_header_t h;
  memcpy(&h, page + sizeof(timeseries_page_header_t), sizeof(h));
  if (!(h.flags & 0x08)) {
    TEST_ASSERT_GREATER_OR_EQUAL(sizeof(uint64_t), h.record_length);
    h.flags |= 0x08;
    h.record_length -= sizeof(uint64_t);
    memcpy(page + sizeof(timeseries_page_header_t), &h, sizeof(h));
    memset(page + sizeof(timeseries_page_header_t) + sizeof(h) + h.record_length, 0xff, sizeof(uint64_t));
    TEST_ASSERT_EQUAL(ESP_OK, esp_partition_erase_range(db->partition, offset, 4096));
    TEST_ASSERT_EQUAL(ESP_OK, esp_partition_write(db->partition, offset, page, 4096));
  }
  free(page);
  return offset;
}
TEST_CASE("eighth: legacy records retain order through rewrite and new inserts", "[eighth][eighth_legacy]") {
  uint8_t series[16]; timeseries_db_t *db = prepare_typed(TIMESERIES_FIELD_TYPE_INT, series);
  uint32_t old = write_legacy_pair(db, series, 1);
  write_legacy_pair(db, series, 99);
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_init()); atomic_store(&db->compaction_in_progress, true);
  bool before = newest_value();
  bool rewritten = timeseries_compact_page_list(db, &old, 1);
  bool after = newest_value();
  uint64_t ts = 1;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_INT, .data.int_val = 199};
  bool inserted = insert(&ts, &value, 1);
  bool updated = int_value_matches(199);
  bool compacted = timeseries_compact_all_levels_force(db);
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_init()); bool reopened = int_value_matches(199); timeseries_deinit();
  TEST_ASSERT_TRUE(before); TEST_ASSERT_TRUE(rewritten); TEST_ASSERT_TRUE(after);
  TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE(updated); TEST_ASSERT_TRUE(compacted); TEST_ASSERT_TRUE(reopened);
}

static TaskHandle_t rewrite_owner, rewrite_task;
static SemaphoreHandle_t rewrite_done;
static uint32_t rewrite_offset;
static bool rewrite_progress;
void tsdb_audit_on_field_scan(TaskHandle_t task, SemaphoreHandle_t done, uint32_t offset) {
  rewrite_owner = task ? xTaskGetCurrentTaskHandle() : NULL;
  rewrite_task = task; rewrite_done = done; rewrite_offset = offset; rewrite_progress = false;
}
bool tsdb_audit_field_scan_progress(void) { return rewrite_progress; }
void __real_timeseries_fielddata_iterator_deinit(timeseries_fielddata_iterator_t *);
void __wrap_timeseries_fielddata_iterator_deinit(timeseries_fielddata_iterator_t *iter) {
  if (rewrite_owner == xTaskGetCurrentTaskHandle() && rewrite_task && iter->page_offset == rewrite_offset) {
    TaskHandle_t task = rewrite_task; rewrite_task = NULL;
    xTaskNotifyGive(task);
    rewrite_progress = xSemaphoreTake(rewrite_done, pdMS_TO_TICKS(200)) == pdTRUE;
  }
  __real_timeseries_fielddata_iterator_deinit(iter);
}
TEST_CASE("eighth: rewrite sizing and copying exclude concurrent appends", "[eighth][eighth_rewrite_lock]") {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  atomic_store(&db->compaction_in_progress, true);
  uint64_t *ts = malloc(249*sizeof(*ts));
  timeseries_field_value_t *v = calloc(249, sizeof(*v));
  TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(v);
  for (size_t i = 0; i < 249; i++) { ts[i] = i+1; v[i].type = TIMESERIES_FIELD_TYPE_INT; v[i].data.int_val = 1; }
  TEST_ASSERT_TRUE(insert(ts, v, 249));
  free(ts); free(v);
  uint32_t old = db->last_l0_page_offset;
  concurrent_t c = {.done = xSemaphoreCreateBinary()}; TEST_ASSERT_NOT_NULL(c.done);
  TEST_ASSERT_EQUAL(pdPASS, xTaskCreate(insert_worker, "eighth_rewrite", 8192, &c, tskIDLE_PRIORITY+1, &rewrite_task));
  rewrite_owner = xTaskGetCurrentTaskHandle(); rewrite_done = c.done; rewrite_offset = old; rewrite_progress = false;
  bool rewritten = timeseries_compact_page_list(db, &old, 1);
  bool early = rewrite_progress;
  rewrite_owner = NULL; rewrite_task = NULL; rewrite_done = NULL;
  bool done = early || xSemaphoreTake(c.done, pdMS_TO_TICKS(2000)) == pdTRUE;
  bool matched = done && newest_value();
  if (done) vSemaphoreDelete(c.done);
  bool count_ok = atomic_load(&db->l0_page_count) == tsdb_count_l0_pages(db);
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(done); TEST_ASSERT_TRUE(c.inserted); TEST_ASSERT_TRUE(rewritten);
  TEST_ASSERT_FALSE_MESSAGE(early, "append must wait until the rewrite finishes");
  TEST_ASSERT_TRUE(matched); TEST_ASSERT_TRUE(count_ok);
}
TEST_CASE("eighth: L0 rewrite refreshes append cache and page count", "[eighth][eighth_rewrite_cache]") {
  uint8_t series[16]; timeseries_db_t *db = prepare_typed(TIMESERIES_FIELD_TYPE_INT, series);
  uint32_t old = db->last_l0_page_offset;
  bool rewritten = timeseries_compact_page_list(db, &old, 1);
  bool count_ok = atomic_load(&db->l0_page_count) == tsdb_count_l0_pages(db);
  uint64_t ts = 1;
  timeseries_field_value_t v = {.type = TIMESERIES_FIELD_TYPE_INT, .data.int_val = 99};
  bool inserted = insert(&ts, &v, 1);
  bool matched = newest_value();
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(rewritten); TEST_ASSERT_TRUE(count_ok); TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE(matched);
}
