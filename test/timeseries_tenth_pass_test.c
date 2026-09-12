#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_expiration.h"
#include "timeseries_compaction.h"
#include "timeseries_iterator.h"
#include "freertos/task.h"
#include "esp_timer.h"
#include "esp_log.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

extern timeseries_db_t *timeseries_get_db_handle(void);
void tsdb_audit_writer_callback(void (*)(timeseries_db_t *));
static TaskHandle_t delay_owner, erase_owner;
static bool fail_erase, erase_owned;
static unsigned delays;
static TaskHandle_t mark_owner;
static bool mark_failed;
esp_err_t __real_tsdb_flash_write_byte(const esp_partition_t *, uint32_t, uint8_t);
esp_err_t __wrap_tsdb_flash_write_byte(const esp_partition_t *p, uint32_t offset, uint8_t value) {
  if (mark_owner == xTaskGetCurrentTaskHandle()) { mark_failed = true; return ESP_FAIL; }
  return __real_tsdb_flash_write_byte(p, offset, value);
}
void __real_vTaskDelay(TickType_t);
bool tsdb_audit_fourteenth_skip_delay(void);
void __wrap_vTaskDelay(TickType_t ticks) {
  if (tsdb_audit_fourteenth_skip_delay()) return;
  if (delay_owner == xTaskGetCurrentTaskHandle()) { delays++; return; }
  __real_vTaskDelay(ticks);
}
esp_err_t __real_esp_partition_erase_range(const esp_partition_t *, size_t, size_t);
bool tsdb_audit_fourteenth_erase_gate(void);
esp_err_t __wrap_esp_partition_erase_range(const esp_partition_t *p, size_t offset, size_t length) {
  if (!tsdb_audit_fourteenth_erase_gate()) return ESP_FAIL;
  if (erase_owner == xTaskGetCurrentTaskHandle()) {
    erase_owned = atomic_load(&timeseries_get_db_handle()->compaction_in_progress);
    if (fail_erase) return ESP_FAIL;
  }
  return __real_esp_partition_erase_range(p, offset, length);
}
static bool insert_points(size_t count, uint64_t first) {
  uint64_t *ts = malloc(count * sizeof(*ts));
  timeseries_field_value_t *values = calloc(count, sizeof(*values));
  if (!ts || !values) { free(ts); free(values); return false; }
  for (size_t i = 0; i < count; i++) { ts[i] = first+i; values[i].type = TIMESERIES_FIELD_TYPE_INT; values[i].data.int_val = ts[i]; }
  const char *field = "v";
  timeseries_insert_data_t in = {.measurement_name = "tenth", .field_names = &field,
    .field_values = values, .num_fields = 1, .timestamps_ms = ts, .num_points = count};
  bool ok = timeseries_insert(&in); free(ts); free(values); return ok;
}
static bool query_count(size_t count) {
  timeseries_query_t q = {.measurement_name = "tenth"}; timeseries_query_result_t r = {0};
  bool ok = timeseries_query(&q, &r) && r.num_points == count;
  for (size_t i = 0; ok && i < r.num_points; i++) ok = r.columns[0].values[i].data.int_val == (int64_t)r.timestamps[i];
  timeseries_query_free_result(&r); return ok;
}
static timeseries_db_t *prepare(void) {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  atomic_store(&db->compaction_in_progress, true);
  TEST_ASSERT_TRUE(insert_points(1, 1));
  return db;
}
static void cleanup(timeseries_db_t *db) {
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
}
static bool callback_seen, expiration_rejected, source_preserved;
static void expire_during_merge(timeseries_db_t *db) {
  callback_seen = db->compaction_claimed_count > 0;
  uint32_t offset = db->compaction_claimed_pages[0] + sizeof(timeseries_page_header_t);
  timeseries_field_data_header_t before, after;
  bool read = esp_partition_read(db->partition, offset, &before, sizeof(before)) == ESP_OK;
  expiration_rejected = !timeseries_expiration_run(db, .00001f, .000001f);
  source_preserved = read && esp_partition_read(db->partition, offset, &after, sizeof(after)) == ESP_OK &&
      !memcmp(&before, &after, sizeof(before));
}
TEST_CASE("tenth: expiration cannot delete a running compactor source", "[tenth][tenth_expire_race]") {
  timeseries_db_t *db = prepare();
  for (size_t i = 0; i < 4; i++) TEST_ASSERT_TRUE(insert_points(500, 2+i*500));
  callback_seen = expiration_rejected = source_preserved = false;
  tsdb_audit_writer_callback(expire_during_merge);
  bool compacted = timeseries_compact_level0_pages(db);
  tsdb_audit_writer_callback(NULL);
  bool matched = query_count(2001);
  cleanup(db);
  TEST_ASSERT_TRUE(callback_seen); TEST_ASSERT_TRUE(expiration_rejected);
  TEST_ASSERT_TRUE(source_preserved); TEST_ASSERT_TRUE(compacted); TEST_ASSERT_TRUE(matched);
}
TEST_CASE("tenth: clear timeout preserves data and existing compaction owner", "[tenth][tenth_clear_timeout]") {
  timeseries_db_t *db = prepare(); uint64_t revision = db->write_revision;
  delay_owner = xTaskGetCurrentTaskHandle(); delays = 0;
  bool cleared = timeseries_clear_all();
  delay_owner = NULL;
  bool preserved = atomic_load(&db->compaction_in_progress) && db->write_revision == revision && query_count(1);
  cleanup(db);
  TEST_ASSERT_FALSE(cleared); TEST_ASSERT_TRUE(preserved); TEST_ASSERT_EQUAL(6000, delays);
}
TEST_CASE("tenth: clear owns compaction slot through erase and releases on failure", "[tenth][tenth_clear_owner]") {
  timeseries_db_t *db = prepare(); atomic_store(&db->compaction_in_progress, false);
  erase_owner = xTaskGetCurrentTaskHandle(); fail_erase = true; erase_owned = false;
  bool failed = !timeseries_clear_all(); bool owned_failure = erase_owned;
  erase_owner = NULL; fail_erase = false;
  bool retained = query_count(1) && !atomic_load(&db->compaction_in_progress);
  erase_owner = xTaskGetCurrentTaskHandle(); erase_owned = false;
  bool cleared = timeseries_clear_all(); bool owned_success = erase_owned;
  erase_owner = NULL;
  bool released = !atomic_load(&db->compaction_in_progress) && db->write_revision == 0;
  cleanup(db);
  TEST_ASSERT_TRUE(failed); TEST_ASSERT_TRUE(owned_failure); TEST_ASSERT_TRUE(retained);
  TEST_ASSERT_TRUE(cleared); TEST_ASSERT_TRUE(owned_success); TEST_ASSERT_TRUE(released);
}
TEST_CASE("tenth: expiration rejects NaN fractions", "[tenth][tenth_expire_args]") {
  timeseries_db_t *db = prepare(); atomic_store(&db->compaction_in_progress, false);
  bool rejected = !timeseries_expiration_run(db, NAN, .05f) && !timeseries_expiration_run(db, .5f, NAN);
  cleanup(db);
  TEST_ASSERT_TRUE(rejected);
}
#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_fail_read_at(size_t, bool);
void tsdb_audit_fail_reads_at(size_t);
void tsdb_audit_allocation_probe(size_t, bool);
size_t tsdb_audit_largest_allocation(void);
size_t tsdb_audit_allocation_count(void);
TEST_CASE("tenth: expiration aborts failed scan with bounded scratch memory", "[tenth][tenth_expire_read]") {
  timeseries_db_t *db = prepare(); atomic_store(&db->compaction_in_progress, false);
  tsdb_audit_fail_reads_at(db->last_l0_page_offset + sizeof(timeseries_page_header_t));
  tsdb_audit_allocation_probe(0, true);
  bool rejected = !timeseries_expiration_run(db, .00001f, .000001f);
  size_t largest = tsdb_audit_largest_allocation();
  tsdb_audit_allocation_probe(0, false); tsdb_audit_fail_read_at(0, false);
  bool preserved = query_count(1) && !atomic_load(&db->compaction_in_progress);
  cleanup(db);
  TEST_ASSERT_TRUE(rejected); TEST_ASSERT_TRUE(preserved); TEST_ASSERT_LESS_OR_EQUAL(4096, largest);
}
TEST_CASE("tenth: busy expiration does not allocate scan buffers", "[tenth][tenth_expire_busy]") {
  timeseries_db_t *db = prepare();
  tsdb_audit_allocation_probe(0, true);
  bool rejected = !timeseries_expiration_run(db, .00001f, .000001f);
  size_t allocations = tsdb_audit_allocation_count(); tsdb_audit_allocation_probe(0, false);
  bool preserved = query_count(1) && atomic_load(&db->compaction_in_progress);
  cleanup(db);
  TEST_ASSERT_TRUE(rejected); TEST_ASSERT_TRUE(preserved); TEST_ASSERT_EQUAL(0, allocations);
}
TEST_CASE("tenth: expiration OOM releases slot without deleting data", "[tenth][tenth_expire_oom]") {
  timeseries_db_t *db = prepare(); atomic_store(&db->compaction_in_progress, false);
  bool ok = true;
  for (size_t allocation = 1; allocation <= 2; allocation++) {
    tsdb_audit_allocation_probe(allocation, false);
    bool rejected = !timeseries_expiration_run(db, .00001f, .000001f);
    tsdb_audit_allocation_probe(0, false);
    ok = ok && rejected && !atomic_load(&db->compaction_in_progress) && query_count(1);
  }
  cleanup(db); TEST_ASSERT_TRUE(ok);
}
#endif

TEST_CASE("tenth: expiration throughput and allocation benchmark", "[tenth][tenth_expire_bench]") {
  timeseries_db_t *db = prepare();
  size_t old_chunk = db->chunk_size; db->chunk_size = 1;
  TEST_ASSERT_TRUE(insert_points(255, 2)); db->chunk_size = old_chunk;
  atomic_store(&db->compaction_in_progress, false);
  esp_log_level_set("*", ESP_LOG_NONE);
#if CONFIG_IDF_TARGET_LINUX
  tsdb_audit_allocation_probe(0, true);
#endif
  int64_t start = esp_timer_get_time();
  bool expired = timeseries_expiration_run(db, .00001f, .00000001f);
  int64_t elapsed = esp_timer_get_time() - start;
#if CONFIG_IDF_TARGET_LINUX
  size_t allocations = tsdb_audit_allocation_count(), largest = tsdb_audit_largest_allocation();
  tsdb_audit_allocation_probe(0, false);
  printf("TENTH_EXPIRE us=%lld allocations=%zu largest=%zu\n", (long long)elapsed, allocations, largest);
#else
  printf("TENTH_EXPIRE us=%lld\n", (long long)elapsed);
#endif
  esp_log_level_set("*", ESP_LOG_INFO);
  bool matched = query_count(255) && !atomic_load(&db->compaction_in_progress);
  cleanup(db);
  TEST_ASSERT_TRUE(expired); TEST_ASSERT_TRUE(matched);
}

void tsdb_audit_on_field_scan(TaskHandle_t, SemaphoreHandle_t, uint32_t);
bool tsdb_audit_field_scan_progress(void);
typedef struct { SemaphoreHandle_t done; bool inserted, owned; } scan_insert_t;
static void scan_insert_worker(void *arg) {
  scan_insert_t *c = arg;
  ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
  c->owned = atomic_load(&timeseries_get_db_handle()->compaction_in_progress);
  c->inserted = insert_points(1, 1000);
  xSemaphoreGive(c->done); vTaskDelete(NULL);
}
TEST_CASE("tenth: expiration scan owns maintenance slot but permits inserts", "[tenth][tenth_expire_insert]") {
  timeseries_db_t *db = prepare(); atomic_store(&db->compaction_in_progress, false);
  scan_insert_t c = {.done = xSemaphoreCreateBinary()}; TEST_ASSERT_NOT_NULL(c.done);
  TaskHandle_t worker;
  TEST_ASSERT_EQUAL(pdPASS, xTaskCreate(scan_insert_worker, "expire_insert", 8192, &c, tskIDLE_PRIORITY+1, &worker));
  tsdb_audit_on_field_scan(worker, c.done, db->last_l0_page_offset);
  bool expired = timeseries_expiration_run(db, .00001f, .00000001f);
  bool progress = tsdb_audit_field_scan_progress();
  tsdb_audit_on_field_scan(NULL, NULL, 0);
  bool done = progress || xSemaphoreTake(c.done, pdMS_TO_TICKS(2000)) == pdTRUE;
  timeseries_query_t q = {.measurement_name = "tenth"};
  timeseries_query_result_t r = {0};
  bool matched = done && timeseries_query(&q, &r) && r.num_points == 1 && r.num_columns == 1 &&
      r.timestamps[0] == 1000 && r.columns[0].values[0].data.int_val == 1000;
  timeseries_query_free_result(&r);
  if (done) vSemaphoreDelete(c.done);
  cleanup(db);
  TEST_ASSERT_TRUE(done); TEST_ASSERT_TRUE(progress); TEST_ASSERT_TRUE(c.inserted);
  TEST_ASSERT_TRUE(c.owned); TEST_ASSERT_TRUE(expired); TEST_ASSERT_TRUE(matched);
}

TEST_CASE("tenth: expiration reports deletion write failure and permits retry", "[tenth][tenth_expire_write]") {
  timeseries_db_t *db = prepare(); atomic_store(&db->compaction_in_progress, false);
  mark_owner = xTaskGetCurrentTaskHandle(); mark_failed = false;
  bool rejected = !timeseries_expiration_run(db, .00001f, .00000001f);
  mark_owner = NULL;
  bool retained = query_count(1) && !atomic_load(&db->compaction_in_progress);
  bool retried = timeseries_expiration_run(db, .00001f, .00000001f) && query_count(0);
  cleanup(db);
  TEST_ASSERT_TRUE(mark_failed); TEST_ASSERT_TRUE(rejected); TEST_ASSERT_TRUE(retained); TEST_ASSERT_TRUE(retried);
}
