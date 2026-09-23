/*
 * TSDB review 2026-09-23 (TSDB_REVIEW.md): deterministic phases R1-R12.
 * Interleaving tests park one task at a hook (linker --wrap) and let the main
 * task run a conflicting public-API operation, then check a user-visible
 * oracle. Fault tests inject one flash-write or allocation failure.
 * run_tests.sh runs all of them before the churn; REVIEW=<id> runs one.
 * R10 (torn appends seen by unlocked readers) has no phase: see the note at
 * the end of this file.
 */
#include <inttypes.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "esp_partition.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_metadata.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_stream_writer.h"
#include "timeseries_compaction.h"
#include "timeseries_data.h"

extern timeseries_db_t* review_db(void);

/* ---- gates --------------------------------------------------------------- */
enum { G_STREAM_INIT, G_TYPE_LOOKUP, G_ENTITY_INIT, G_PCI_NEXT, G_FD_INIT, G_COUNT };
typedef struct {
  atomic_bool armed;
  TaskHandle_t _Atomic target;
  SemaphoreHandle_t hit, go;
} gate_t;
static gate_t g[G_COUNT];

static void gates_init(void) {
  for (int i = 0; i < G_COUNT; i++) {
    g[i].hit = xSemaphoreCreateBinary();
    g[i].go = xSemaphoreCreateBinary();
  }
}
static void gate_pass(int id) {
  if (!atomic_load(&g[id].armed)) return;
  if (xTaskGetCurrentTaskHandle() != atomic_load(&g[id].target)) return;
  atomic_store(&g[id].armed, false);
  xSemaphoreGive(g[id].hit);
  xSemaphoreTake(g[id].go, portMAX_DELAY);
}
static void gate_arm(int id, TaskHandle_t t) {
  atomic_store(&g[id].target, t);
  atomic_store(&g[id].armed, true);
}
static bool gate_wait(int id, int ms) { return xSemaphoreTake(g[id].hit, pdMS_TO_TICKS(ms)) == pdTRUE; }
static void gate_release(int id) { xSemaphoreGive(g[id].go); }

bool __real_timeseries_page_stream_writer_init(timeseries_db_t* db, timeseries_page_stream_writer_t* w,
                                               uint8_t to_level, uint32_t prev);
bool __wrap_timeseries_page_stream_writer_init(timeseries_db_t* db, timeseries_page_stream_writer_t* w,
                                               uint8_t to_level, uint32_t prev) {
  gate_pass(G_STREAM_INIT);
  return __real_timeseries_page_stream_writer_init(db, w, to_level, prev);
}
bool __real_tsdb_lookup_series_type_cached(timeseries_db_t* db, const unsigned char id[16],
                                           timeseries_field_type_e* t);
bool __wrap_tsdb_lookup_series_type_cached(timeseries_db_t* db, const unsigned char id[16],
                                           timeseries_field_type_e* t) {
  gate_pass(G_TYPE_LOOKUP);
  return __real_tsdb_lookup_series_type_cached(db, id, t);
}
bool __real_timeseries_entity_iterator_init(timeseries_db_t* db, uint32_t off, uint32_t sz,
                                            timeseries_entity_iterator_t* it);
bool __wrap_timeseries_entity_iterator_init(timeseries_db_t* db, uint32_t off, uint32_t sz,
                                            timeseries_entity_iterator_t* it) {
  gate_pass(G_ENTITY_INIT);
  return __real_timeseries_entity_iterator_init(db, off, sz, it);
}
static uint32_t s_parked_offset, s_parked_seq;
// Data-page scans of the unlocked readers (timestamp range) open each page
// with this; parking here means the reader holds its page list and is about
// to read a page from it.
bool __real_timeseries_fielddata_iterator_init_with_buffer(timeseries_db_t* db, uint32_t off, uint32_t sz, uint8_t* b,
                                                           uint32_t cap, timeseries_fielddata_iterator_t* it);
bool __wrap_timeseries_fielddata_iterator_init_with_buffer(timeseries_db_t* db, uint32_t off, uint32_t sz, uint8_t* b,
                                                           uint32_t cap, timeseries_fielddata_iterator_t* it) {
  if (atomic_load(&g[G_FD_INIT].armed) && xTaskGetCurrentTaskHandle() == atomic_load(&g[G_FD_INIT].target))
    s_parked_offset = off;
  gate_pass(G_FD_INIT);
  return __real_timeseries_fielddata_iterator_init_with_buffer(db, off, sz, b, cap, it);
}
bool __real_timeseries_page_cache_iterator_next(timeseries_page_cache_iterator_t* it, timeseries_page_header_t* h,
                                                uint32_t* o, uint32_t* s);
bool __wrap_timeseries_page_cache_iterator_next(timeseries_page_cache_iterator_t* it, timeseries_page_header_t* h,
                                                uint32_t* o, uint32_t* s) {
  bool r = __real_timeseries_page_cache_iterator_next(it, h, o, s);
  // Park only once the caller holds a snapshot and is about to read a data page.
  if (r && h && o && h->page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA) {
    if (atomic_load(&g[G_PCI_NEXT].armed) && xTaskGetCurrentTaskHandle() == atomic_load(&g[G_PCI_NEXT].target))
      s_parked_offset = *o, s_parked_seq = h->sequence_num;
    gate_pass(G_PCI_NEXT);
  }
  return r;
}

extern TaskHandle_t _Atomic review_fail_alloc_task;
// One-shot flash-write failure (tsdb_churn.c): the next esp_partition_write by
// review_fail_write_task with length review_fail_write_len (and, unless
// UINT32_MAX, at offset review_fail_write_offset) returns ESP_FAIL.
extern TaskHandle_t _Atomic review_fail_write_task;
extern _Atomic uint32_t review_fail_write_len, review_fail_write_offset;
static void fail_next_write(size_t len, uint32_t offset) {
  atomic_store(&review_fail_write_len, (uint32_t)len);
  atomic_store(&review_fail_write_offset, offset);
  atomic_store(&review_fail_write_task, xTaskGetCurrentTaskHandle());
}
static atomic_bool s_fail_after_finalize;
bool __real_timeseries_page_stream_writer_finalize(timeseries_page_stream_writer_t* w);
bool __wrap_timeseries_page_stream_writer_finalize(timeseries_page_stream_writer_t* w) {
  bool ok = __real_timeseries_page_stream_writer_finalize(w);
  // R6: the output page is complete on flash; fail the next allocation the
  // compaction makes (publication in finish_compaction_batch).
  if (ok && atomic_exchange(&s_fail_after_finalize, false))
    atomic_store(&review_fail_alloc_task, xTaskGetCurrentTaskHandle());
  return ok;
}

/* ---- helpers ------------------------------------------------------------- */
static uint64_t s_ts = 1800000000000ULL;

static bool put(const char* m, const char* f, timeseries_field_value_t v, size_t n) {
  const char* fn[1] = {f};
  timeseries_field_value_t* fv = calloc(n, sizeof(*fv));
  uint64_t* ts = calloc(n, sizeof(*ts));
  for (size_t i = 0; i < n; i++) {
    fv[i] = v;
    if (v.type == TIMESERIES_FIELD_TYPE_FLOAT) fv[i].data.float_val = v.data.float_val + (double)i;
    ts[i] = (s_ts += 1000);
  }
  timeseries_insert_data_t d = {.measurement_name = m, .field_names = fn, .field_values = fv, .num_fields = 1,
                                .timestamps_ms = ts, .num_points = n};
  bool ok = timeseries_insert(&d);
  free(fv);
  free(ts);
  return ok;
}
static timeseries_field_value_t F(double x) {
  return (timeseries_field_value_t){.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = x};
}
static size_t count_points(const char* m, const char* f) {
  const char* fn[1] = {f};
  timeseries_query_t q = {.measurement_name = m, .field_names = fn, .num_fields = 1};
  timeseries_query_result_t r;
  memset(&r, 0, sizeof r);
  if (!timeseries_query(&q, &r)) return (size_t)-1;
  size_t n = r.num_points;
  timeseries_query_free_result(&r);
  return n;
}
static void level_pages(uint32_t out[5]) {
  tsdb_usage_summary_t s;
  memset(&s, 0, sizeof s);
  timeseries_get_usage_summary(&s);
  for (int i = 0; i < 5; i++) out[i] = s.page_summaries[i].num_pages;
}

/* ---- worker tasks -------------------------------------------------------- */
typedef struct {
  int kind;
  volatile bool result;
  SemaphoreHandle_t done, start;
  const char* m;
  const char* f;
  timeseries_field_value_t v;
  uint64_t lo, hi;
  uint32_t cnt;
} job_t;
enum { J_FORCE_COMPACT, J_INSERT, J_RANGE, J_LIST, J_DELETE, J_CLEAR };
static void worker(void* arg) {
  job_t* j = arg;
  xSemaphoreTake(j->start, portMAX_DELAY);
  switch (j->kind) {
    case J_FORCE_COMPACT: j->result = timeseries_compact_force_sync(); break;
    case J_INSERT: j->result = put(j->m, j->f, j->v, 1); break;
    case J_DELETE: j->result = timeseries_delete_measurement(j->m); break;
    case J_CLEAR: j->result = timeseries_clear_all(); break;
    case J_RANGE: j->result = timeseries_get_timestamp_range(j->m, &j->lo, &j->hi, &j->cnt); break;
    case J_LIST: {
      char** ms = NULL;
      size_t n = 0;
      j->result = timeseries_get_measurements(&ms, &n);
      j->cnt = (uint32_t)n;
      for (size_t i = 0; i < n; i++) free(ms[i]);
      free(ms);
      break;
    }
  }
  xSemaphoreGive(j->done);
  vTaskDelete(NULL);
}
static TaskHandle_t spawn(job_t* j) {
  j->done = xSemaphoreCreateBinary();
  j->start = xSemaphoreCreateBinary();
  TaskHandle_t h = NULL;
  xTaskCreate(worker, "rv_worker", 32768, j, 1, &h);
  return h;
}

/* ---- R1: delete during an in-flight level compaction resurrects data ----- */
static int t_delete_during_compaction(void) {
  int bad = 0;
  timeseries_clear_all();
  if (!put("dr", "v", F(1.0), 200)) { printf("R1: FAIL setup insert\n"); return 1; }
  printf("R1: before: points=%zu\n", count_points("dr", "v"));
  job_t j = {.kind = J_FORCE_COMPACT};
  TaskHandle_t h = spawn(&j);
  gate_arm(G_STREAM_INIT, h);
  xSemaphoreGive(j.start);
  if (!gate_wait(G_STREAM_INIT, 20000)) { printf("R1: FAIL compaction did not reach the merge writer\n"); return 1; }
  // Compaction has read every live record header of the L0 page and is about
  // to write them into an L1 page. Delete the measurement now.
  // Delete from another task (a fixed library may make it wait for the
  // compaction); give it time to run, then let the compaction continue.
  job_t d = {.kind = J_DELETE, .m = "dr"};
  spawn(&d);
  xSemaphoreGive(d.start);
  bool del_done_first = xSemaphoreTake(d.done, pdMS_TO_TICKS(500)) == pdTRUE;
  gate_release(G_STREAM_INIT);
  xSemaphoreTake(j.done, portMAX_DELAY);
  if (!del_done_first) xSemaphoreTake(d.done, portMAX_DELAY);
  printf("R1: delete_measurement during compaction -> %d (completed while compaction parked: %d)\n", d.result,
         del_done_first);
  uint32_t lv[5];
  level_pages(lv);
  printf("R1: compaction -> %d; pages L0=%" PRIu32 " L1=%" PRIu32 " L2=%" PRIu32 "\n", j.result, lv[0], lv[1], lv[2]);
  // (a) A later compaction must still work: the deleted series has no type
  // metadata any more, so records that survived the delete block the merge.
  bool c2 = timeseries_compact_force_sync();
  level_pages(lv);
  printf("R1: next force compaction -> %d; pages L0=%" PRIu32 " L1=%" PRIu32 " L2=%" PRIu32 "\n", c2, lv[0], lv[1],
         lv[2]);
  if (!c2) {
    printf("R1: FAIL compaction fails after a delete that raced a compaction\n");
    bad++;
  }
  // (b) Re-creating the same series (same name/field -> same series id) must
  // not bring the deleted points back.
  put("dr", "v", F(1000.0), 1);
  size_t n = count_points("dr", "v");
  printf("R1: after delete + 1 new point: points=%zu (want 1)\n", n);
  if (n != 1) {
    printf("R1: FAIL %zu deleted points resurrected\n", n - 1);
    bad++;
  }
  return bad;
}

/* ---- R2: insert racing delete_measurement writes an orphan record -------- */
static int t_insert_racing_delete(void) {
  int bad = 0;
  timeseries_clear_all();
  if (!put("id", "v", F(1.0), 1)) { printf("R2: FAIL setup\n"); return 1; }  // series cache now holds id:v
  job_t j = {.kind = J_INSERT, .m = "id", .f = "v", .v = F(2.0)};
  TaskHandle_t h = spawn(&j);
  gate_arm(G_TYPE_LOOKUP, h);
  xSemaphoreGive(j.start);
  if (!gate_wait(G_TYPE_LOOKUP, 20000)) { printf("R2: FAIL insert did not reach the type check\n"); return 1; }
  bool del = timeseries_delete_measurement("id");
  gate_release(G_TYPE_LOOKUP);
  xSemaphoreTake(j.done, portMAX_DELAY);
  printf("R2: delete -> %d, racing insert -> %d, points visible=%zu\n", del, j.result, count_points("id", "v"));
  uint32_t lv[5];
  bool c = timeseries_compact_force_sync();
  level_pages(lv);
  printf("R2: force compaction -> %d; pages L0=%" PRIu32 " L1=%" PRIu32 "\n", c, lv[0], lv[1]);
  bool c2 = timeseries_compact_force_sync();
  printf("R2: force compaction again -> %d\n", c2);
  if (!c || !c2) {
    printf("R2: FAIL an insert that raced delete_measurement left a record compaction cannot merge\n");
    bad++;
  }
  return bad;
}

/* ---- R3: unlocked type check reads a retired+reused metadata page -------- */
static uint32_t first_meta_page(void) {
  timeseries_page_cache_iterator_t it;
  uint32_t found = UINT32_MAX;
  if (!timeseries_page_cache_iterator_init(review_db(), &it)) return found;
  timeseries_page_header_t h;
  uint32_t o, s;
  while (__real_timeseries_page_cache_iterator_next(&it, &h, &o, &s))
    if (h.page_type == TIMESERIES_PAGE_TYPE_METADATA && found == UINT32_MAX) found = o;
  timeseries_page_cache_iterator_deinit(&it);
  return found;
}
static int page_type_at(uint32_t off) {
  timeseries_page_cache_iterator_t it;
  int t = -1;
  if (!timeseries_page_cache_iterator_init(review_db(), &it)) return t;
  timeseries_page_header_t h;
  uint32_t o, s;
  while (__real_timeseries_page_cache_iterator_next(&it, &h, &o, &s))
    if (o == off) t = h.page_type;
  timeseries_page_cache_iterator_deinit(&it);
  return t;
}
static bool page_at(uint32_t off, timeseries_page_header_t* out) {
  timeseries_page_cache_iterator_t it;
  bool f = false;
  if (!timeseries_page_cache_iterator_init(review_db(), &it)) return f;
  timeseries_page_header_t h;
  uint32_t o, s;
  while (__real_timeseries_page_cache_iterator_next(&it, &h, &o, &s))
    if (o == off) f = true, *out = h;
  timeseries_page_cache_iterator_deinit(&it);
  return f;
}
static bool reused_as_l0(uint32_t off, uint32_t old_seq) {
  timeseries_page_header_t h;
  return page_at(off, &h) && h.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA && h.field_data_level == 0 &&
         h.sequence_num != old_seq;
}
static unsigned s_churn_i;
static void churn_metadata_until_moved(uint32_t gen0) {
  timeseries_db_t* db = review_db();
  for (unsigned k = 0; k < 4000 && db->metadata_page_gen == gen0; k++) {
    char node[16];
    snprintf(node, sizeof node, "r%u", s_churn_i++);
    const char* tk[2] = {"node", "grp"};
    const char* tv[2] = {node, (k & 1) ? "x" : "y"};
    const char* fn[1] = {"a"};
    timeseries_field_value_t fv[1] = {F(k)};
    uint64_t ts = (s_ts += 1000);
    timeseries_insert_data_t d = {.measurement_name = "churn", .tag_keys = tk, .tag_values = tv, .num_tags = 2,
                                  .field_names = fn, .field_values = fv, .num_fields = 1, .timestamps_ms = &ts,
                                  .num_points = 1};
    timeseries_insert(&d);
  }
}
static int t_type_check_reads_reused_page(void) {
  int bad = 0;
  timeseries_db_t* db = review_db();
  timeseries_clear_all();
  if (!put("tb", "v", F(1.5), 3)) { printf("R3: FAIL setup\n"); return 1; }
  uint32_t meta = first_meta_page();
  // Evict the series' type from the 128-slot type cache (routine on the
  // device: sync_heartbeat alone writes 40 series), keeping the series-id
  // cache entry, so the next insert takes the unlocked metadata type scan.
  tsdb_clear_type_cache(db);
  timeseries_field_value_t wrong = {.type = TIMESERIES_FIELD_TYPE_BOOL, .data.bool_val = true};
  job_t j = {.kind = J_INSERT, .m = "tb", .f = "v", .v = wrong};
  TaskHandle_t h = spawn(&j);
  gate_arm(G_ENTITY_INIT, h);
  xSemaphoreGive(j.start);
  if (!gate_wait(G_ENTITY_INIT, 20000)) { printf("R3: FAIL insert did not scan metadata\n"); return 1; }
  // The inserting task holds the metadata page list {meta,...} and no lock.
  const uint32_t gen0 = db->metadata_page_gen;
  churn_metadata_until_moved(gen0);
  printf("R3: metadata page @0x%05" PRIx32 " gen %" PRIu32 "->%" PRIu32 ", type now at that offset=%d\n", meta, gen0,
         db->metadata_page_gen, page_type_at(meta));
  // Reuse the retired region: new L0 pages go to the lowest blank region.
  for (int k = 0; k < 64 && page_type_at(meta) != TIMESERIES_PAGE_TYPE_FIELD_DATA; k++) put("filler", "f", F(k), 400);
  printf("R3: page type at 0x%05" PRIx32 " before resume=%d (field data=%d)\n", meta, page_type_at(meta),
         TIMESERIES_PAGE_TYPE_FIELD_DATA);
  gate_release(G_ENTITY_INIT);
  xSemaphoreTake(j.done, portMAX_DELAY);
  printf("R3: BOOL insert into FLOAT series 'tb.v' -> %d (want 0: type is immutable)\n", j.result);
  if (j.result) {
    printf("R3: FAIL type validation bypassed by a scan of a reused metadata region\n");
    bad++;
  }
  size_t n = count_points("tb", "v");
  bool c = timeseries_compact_force_sync();
  printf("R3: query points=%zu (3 float + %d bool), force compaction -> %d\n", n, j.result ? 1 : 0, c);
  return bad;
}

/* ---- R4: unlocked timestamp range reads a retired+reused data page ------ */
static int t_range_reads_reused_page(void) {
  int bad = 0;
  timeseries_clear_all();
  // Keep the auto-trigger from compacting while the scene is set up.
  atomic_store(&review_db()->compaction_in_progress, true);
  put("tr", "v", F(1.0), 50);
  // Three more L0 pages so the normal (threshold 4) L0->L1 compaction runs.
  for (int k = 0; k < 64; k++) {
    uint32_t lv0[5];
    level_pages(lv0);
    if (lv0[0] >= 4) break;
    put("other", "f", F(k), 300);
  }
  uint64_t lo0 = 0, hi0 = 0;
  uint32_t c0 = 0;
  timeseries_get_timestamp_range("tr", &lo0, &hi0, &c0);
  job_t j = {.kind = J_RANGE, .m = "tr"};
  TaskHandle_t h = spawn(&j);
  gate_arm(G_FD_INIT, h);
  xSemaphoreGive(j.start);
  if (!gate_wait(G_FD_INIT, 20000)) { printf("R4: FAIL reader did not scan pages\n"); return 1; }
  {
    timeseries_page_header_t ph;
    s_parked_seq = page_at(s_parked_offset, &ph) ? ph.sequence_num : 0;
  }
  // Reader holds a page-cache snapshot naming the L0 page. Compact it away and
  // let new L0 pages take the region.
  atomic_store(&review_db()->compaction_in_progress, false);
  bool cc = timeseries_compact_sync();
  {
    timeseries_page_header_t ph;
    bool f = page_at(s_parked_offset, &ph);
    printf("R4: after compaction page@parked: present=%d type=%u level=%u seq=%" PRIu32 "\n", f, ph.page_type,
           ph.field_data_level, ph.sequence_num);
  }
  int k = 0;
  for (; k < 64 && !reused_as_l0(s_parked_offset, s_parked_seq); k++) put("other", "f", F(k), 300);
  printf("R4: reader parked on page @0x%05" PRIx32 " seq %" PRIu32 "; reused as a new L0 page=%d after %d inserts\n",
         s_parked_offset, s_parked_seq, reused_as_l0(s_parked_offset, s_parked_seq), k);
  uint32_t lv[5];
  level_pages(lv);
  printf("R4: compaction -> %d, pages L0=%" PRIu32 " L1=%" PRIu32 " L2=%" PRIu32 "\n", cc, lv[0], lv[1], lv[2]);
  gate_release(G_FD_INIT);
  xSemaphoreTake(j.done, portMAX_DELAY);
  printf("R4: range before: ok count=%" PRIu32 " [%" PRIu64 ",%" PRIu64 "]; racing: ok=%d count=%" PRIu32 "\n", c0,
         lo0, hi0, j.result, j.cnt);
  if (!j.result || j.cnt != c0) {
    printf("R4: FAIL timestamp range answered from a reused page (no lock pins flash regions)\n");
    bad++;
  }
  return bad;
}

/* ---- R5: metadata offset cache survives deinit/init ---------------------- */
static int t_reinit_stale_metadata_cache(void) {
  int bad = 0;
  timeseries_db_t* db = review_db();
  timeseries_clear_all();
  put("m1", "v", F(1.0), 1);
  char** ms = NULL;
  size_t n = 0;
  timeseries_get_measurements(&ms, &n);  // populates the metadata page cache
  for (size_t i = 0; i < n; i++) free(ms[i]);
  free(ms);
  uint32_t meta = first_meta_page();
  const esp_partition_t* part = db->partition;
  uint8_t* page = malloc(TIMESERIES_METADATA_PAGE_SIZE);
  esp_partition_read(part, meta, page, TIMESERIES_METADATA_PAGE_SIZE);
  timeseries_deinit();
  // A different on-flash image, e.g. restored from a backup: same metadata,
  // but at another offset.
  uint32_t other = meta + 0x10000;
  esp_partition_erase_range(part, 0, part->size);
  esp_partition_write(part, other, page, TIMESERIES_METADATA_PAGE_SIZE);
  free(page);
  if (!timeseries_init()) { printf("R5: FAIL re-init\n"); return 1; }
  ms = NULL;
  n = 0;
  timeseries_get_measurements(&ms, &n);
  printf("R5: metadata moved 0x%05" PRIx32 "->0x%05" PRIx32 ", measurements after re-init=%zu (want 1)\n", meta,
         other, n);
  for (size_t i = 0; i < n; i++) free(ms[i]);
  free(ms);
  if (n != 1) {
    printf("R5: FAIL a metadata page list from before timeseries_deinit/init was used\n");
    bad++;
  }
  return bad;
}

/* ---- R6: compaction that fails after writing its output duplicates data -- */
static int t_compaction_oom_after_output(void) {
  int bad = 0;
  timeseries_clear_all();
  put("oom", "v", F(1.0), 100);
  size_t n0 = count_points("oom", "v");
  atomic_store(&s_fail_after_finalize, true);
  bool c = timeseries_compact_force_sync();
  atomic_store(&s_fail_after_finalize, false);
  atomic_store(&review_fail_alloc_task, (TaskHandle_t)NULL);
  size_t n1 = count_points("oom", "v");
  uint32_t lv[5];
  level_pages(lv);
  tsdb_usage_summary_t us;
  timeseries_get_usage_summary(&us);
  printf("R6: after failed compaction: L0=%" PRIu32 " L1=%" PRIu32 " used=%" PRIu32 " B\n", lv[0], lv[1], us.used_space_bytes);
  // Reboot: rebuild the page cache from flash.
  timeseries_deinit();
  if (!timeseries_init()) { printf("R6: FAIL re-init\n"); return 1; }
  size_t n2 = count_points("oom", "v");
  level_pages(lv);
  printf("R6: after restart: L0=%" PRIu32 " L1=%" PRIu32 "\n", lv[0], lv[1]);
  if (lv[1] != 0) {
    printf("R6: FAIL the failed compaction's output page is ACTIVE after restart (source still ACTIVE too)\n");
    bad++;
  }
  printf("R6: points before=%zu, compaction(with one OOM)->%d, after=%zu, after restart=%zu\n", n0, c, n1, n2);
  if (n2 != n0) {
    printf("R6: FAIL a failed compaction left its output page ACTIVE on flash (%zu duplicate points after restart)\n",
           n2 - n0);
    bad++;
  }
  return bad;
}


/* ---- R7: metadata lookups see at most 4 metadata pages ------------------- */
static unsigned active_metadata_pages(void) {
  tsdb_usage_summary_t us;
  memset(&us, 0, sizeof us);
  timeseries_get_usage_summary(&us);
  return us.metadata_summary.num_pages;
}
static int t_metadata_page_cap(void) {
  int bad = 0;
  timeseries_clear_all();
  unsigned n = 0, failed = 0;
  char name[96];
  // ~150 bytes of live metadata per measurement: 6 pages need ~330.
  for (; n < 700 && active_metadata_pages() < 6; n++) {
    snprintf(name, sizeof name, "metadata_capacity_%04u_padding_padding_padding_padding", n);
    if (!put(name, "v", F(n), 1) && failed++ < 3) printf("R7: insert %u failed (metadata pages=%u)\n", n, active_metadata_pages());
  }
  char** ms = NULL;
  size_t listed = 0;
  timeseries_get_measurements(&ms, &listed);
  for (size_t i = 0; i < listed; i++) free(ms[i]);
  free(ms);
  unsigned missing = 0;
  for (unsigned i = 0; i < n; i += 7) {
    snprintf(name, sizeof name, "metadata_capacity_%04u_padding_padding_padding_padding", i);
    if (count_points(name, "v") != 1) missing++;
  }
  printf("R7: measurements=%u metadata pages=%u failed inserts=%u listed=%zu sampled-missing=%u\n", n,
         active_metadata_pages(), failed, listed, missing);
  if (failed || listed != n || missing) {
    printf("R7: FAIL metadata beyond the 4 lowest-offset metadata pages is invisible or unwritable\n");
    bad++;
  }
  return bad;
}

/* ---- R8: a failed L0 page creation leaks its reserved region ------------- */
static unsigned placeholder_pages(void) {
  timeseries_page_cache_iterator_t it;
  unsigned n = 0;
  if (!timeseries_page_cache_iterator_init(review_db(), &it)) return 0;
  timeseries_page_header_t h;
  uint32_t o, s;
  while (__real_timeseries_page_cache_iterator_next(&it, &h, &o, &s))
    if (h.page_type == 0xFF) n++;
  timeseries_page_cache_iterator_deinit(&it);
  return n;
}
static int t_l0_create_failure_leak(void) {
  int bad = 0;
  timeseries_clear_all();
  put("lk", "v", F(1.0), 1);  // metadata, caches and the first L0 page exist
  // The next 20-byte write by this task is the new L0 page header.
  fail_next_write(sizeof(timeseries_page_header_t), UINT32_MAX);
  bool ok = put("lk", "v", F(2.0), 1500);  // needs new L0 pages
  atomic_store(&review_fail_write_task, (TaskHandle_t)NULL);
  unsigned leaked = placeholder_pages();
  printf("R8: insert with one failed L0 header write -> %d; reserved-but-unused regions left=%u (want 0)\n", ok, leaked);
  if (leaked) {
    printf("R8: FAIL create_level0_field_page leaked its reservation\n");
    bad++;
  }
  return bad;
}

/* ---- R9: failed obsolete-mark in metadata compaction leaves 2 copies ----- */
static unsigned live_measurement_entries(const char* name) {
  timeseries_page_cache_iterator_t it;
  unsigned n = 0;
  if (!timeseries_page_cache_iterator_init(review_db(), &it)) return 0;
  timeseries_page_header_t h;
  uint32_t o, s;
  while (__real_timeseries_page_cache_iterator_next(&it, &h, &o, &s)) {
    if (h.page_type != TIMESERIES_PAGE_TYPE_METADATA) continue;
    timeseries_entity_iterator_t e;
    if (!__real_timeseries_entity_iterator_init(review_db(), o, TIMESERIES_METADATA_PAGE_SIZE, &e)) continue;
    timeseries_entry_header_t eh;
    while (timeseries_entity_iterator_next(&e, &eh)) {
      if (eh.key_type != TIMESERIES_KEYTYPE_MEASUREMENT || eh.delete_marker != TIMESERIES_DELETE_MARKER_VALID ||
          eh.key_len != strlen(name) || eh.value_len != 4)
        continue;
      char k[64];
      uint32_t id;
      if (timeseries_entity_iterator_read_data(&e, &eh, k, &id) && !memcmp(k, name, eh.key_len)) n++;
    }
    timeseries_entity_iterator_deinit(&e);
  }
  timeseries_page_cache_iterator_deinit(&it);
  return n;
}
static int t_metadata_fallback_duplicate(void) {
  int bad = 0;
  timeseries_db_t* db = review_db();
  timeseries_clear_all();
  put("fb", "v", F(1.0), 1);
  uint32_t meta = first_meta_page();
  // The header write that marks the old page OBSOLETE fails once.
  fail_next_write(sizeof(timeseries_page_header_t), meta);
  const uint32_t gen0 = db->metadata_page_gen;
  churn_metadata_until_moved(gen0);
  bool injected = atomic_load(&review_fail_write_task) == NULL;
  atomic_store(&review_fail_write_task, (TaskHandle_t)NULL);
  unsigned live = live_measurement_entries("fb");
  timeseries_deinit();
  if (!timeseries_init()) { printf("R9: FAIL re-init\n"); return 1; }
  unsigned after = live_measurement_entries("fb");
  printf("R9: failure injected=%d; live copies of measurement 'fb': %u, after restart %u (want 1)\n", injected, live,
         after);
  if (!injected) {
    printf("R9: FAIL no metadata compaction reached the obsolete-mark write\n");
    bad++;
  } else if (live != 1 || after != 1) {
    printf("R9: FAIL a metadata compaction whose obsolete-mark failed left two ACTIVE copies of every entry\n");
    bad++;
  }
  return bad;
}

/* ---- R11: near-full partition, free space only in pinned retired regions - */
static size_t fill_blank(uint32_t* held, size_t cap) {
  size_t n = 0;
  uint32_t o, sz;
  while (n < cap && tsdb_reserve_blank_region(review_db(), 4096, NULL, &o, &sz)) held[n++] = o;
  return n;
}
static int t_near_full_with_reader(void) {
  int bad = 0;
  timeseries_db_t* db = review_db();
  timeseries_clear_all();
  atomic_store(&db->compaction_in_progress, true);  // no auto-compaction while the scene is set
  for (int k = 0; k < 64; k++) {
    uint32_t lv0[5];
    level_pages(lv0);
    if (lv0[0] >= 4) break;
    put("nf", "v", F(k), 300);
  }
  uint32_t l0[8];
  size_t nl0 = 0;
  {
    timeseries_page_cache_iterator_t it;
    timeseries_page_header_t h;
    uint32_t o, s2;
    timeseries_page_cache_iterator_init(db, &it);
    while (__real_timeseries_page_cache_iterator_next(&it, &h, &o, &s2))
      if (h.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA && h.field_data_level == 0 && nl0 < 8) l0[nl0++] = o;
    timeseries_page_cache_iterator_deinit(&it);
  }
  // Take every blank region, then give back 64 KiB for the compaction output.
  static uint32_t held[1024];
  size_t nheld = fill_blank(held, 1024);
  for (size_t i = 0, run = 0; i < nheld && run < 16; i++) {
    tsdb_release_reserved_region(db, held[i], NULL);
    held[i] = UINT32_MAX;
    run++;
  }
  // A reader holds a snapshot naming the L0 pages.
  job_t j = {.kind = J_RANGE, .m = "nf"};
  TaskHandle_t h = spawn(&j);
  gate_arm(G_FD_INIT, h);
  xSemaphoreGive(j.start);
  if (!gate_wait(G_FD_INIT, 20000)) { printf("R11: FAIL reader did not scan pages\n"); return 1; }
  atomic_store(&db->compaction_in_progress, false);
  bool c = timeseries_compact_level0_pages(db);  // retires the L0 pages
  // Take all blank space again, except the retired L0 pages: the only space
  // left for an insert is what the reader may still be reading.
  size_t n2 = fill_blank(held + nheld, 1024 - nheld);
  for (size_t i = nheld; i < nheld + n2; i++)
    for (size_t k = 0; k < nl0; k++)
      if (held[i] >= l0[k] && held[i] < l0[k] + TIMESERIES_FIELD_DATA_PAGE_SIZE) {
        tsdb_release_reserved_region(db, held[i], NULL);
        held[i] = UINT32_MAX;
        break;
      }
  nheld += n2;
  // An insert that needs a new L0 page must get one once the reader is done.
  job_t ins = {.kind = J_INSERT, .m = "nf2", .f = "v", .v = F(7.0)};
  spawn(&ins);
  xSemaphoreGive(ins.start);
  vTaskDelay(pdMS_TO_TICKS(300));
  gate_release(G_FD_INIT);
  xSemaphoreTake(j.done, portMAX_DELAY);
  xSemaphoreTake(ins.done, portMAX_DELAY);
  printf("R11: L0 pages=%zu compaction -> %d; insert needing a page while a reader held the only free space -> %d (want 1)\n",
         nl0, c, ins.result);
  if (!c || !ins.result) {
    printf("R11: FAIL insert failed although retired space became reusable when the reader finished\n");
    bad++;
  }
  for (size_t i = 0; i < nheld; i++)
    if (held[i] != UINT32_MAX) tsdb_release_reserved_region(db, held[i], NULL);
  return bad;
}

/* ---- R13: clear_all erases pages a reader is still reading ------------- */
static int t_clear_all_under_reader(void) {
  int bad = 0;
  timeseries_clear_all();
  put("cr", "v", F(1.0), 50);
  uint64_t lo, hi;
  uint32_t c0 = 0;
  timeseries_get_timestamp_range("cr", &lo, &hi, &c0);
  job_t j = {.kind = J_RANGE, .m = "cr"};
  TaskHandle_t h = spawn(&j);
  gate_arm(G_FD_INIT, h);
  xSemaphoreGive(j.start);
  if (!gate_wait(G_FD_INIT, 20000)) { printf("R13: FAIL reader did not scan pages\n"); return 1; }
  job_t c = {.kind = J_CLEAR};
  spawn(&c);
  xSemaphoreGive(c.start);
  bool cleared_under_reader = xSemaphoreTake(c.done, pdMS_TO_TICKS(300)) == pdTRUE;
  gate_release(G_FD_INIT);
  xSemaphoreTake(j.done, portMAX_DELAY);
  if (!cleared_under_reader) xSemaphoreTake(c.done, portMAX_DELAY);
  printf("R13: clear_all finished while a reader was active=%d; reader count=%" PRIu32 " (want %" PRIu32
         "); clear_all -> %d\n", cleared_under_reader, j.cnt, c0, c.result);
  if (j.cnt != c0 || !c.result) {
    printf("R13: FAIL clear_all erased pages an active reader was reading\n");
    bad++;
  }
  return bad;
}

/* ---- R12: a record whose series has no metadata blocks compaction -------- */
static int t_orphan_record_blocks_compaction(void) {
  int bad = 0;
  timeseries_db_t* db = review_db();
  timeseries_clear_all();
  put("real", "v", F(1.0), 5);
  // A record of a series whose metadata was deleted (left on flash by the R2
  // race before it was fixed, or by a delete during a pre-fix compaction).
  unsigned char orphan[16];
  memset(orphan, 0xA5, sizeof orphan);
  uint64_t ts = (s_ts += 1000);
  timeseries_field_value_t v = F(3.0);
  bool w = tsdb_append_multiple_points(db, orphan, &ts, &v, 1);
  bool c1 = timeseries_compact_force_sync();
  bool c2 = timeseries_compact_force_sync();
  size_t n = count_points("real", "v");
  printf("R12: orphan written=%d; force compaction -> %d, again -> %d; real points=%zu (want 5)\n", w, c1, c2, n);
  if (!c1 || !c2 || n != 5) {
    printf("R12: FAIL a record of a deleted series makes every compaction of its level fail\n");
    bad++;
  }
  return bad;
}

/* R10, not a phase. Unlocked readers can observe an append in progress (the
 * device flash driver programs <=256-byte pages and releases its lock between
 * them). Record headers end with series_id, so a torn record header never
 * matches a series and is skipped; a record whose header is complete but whose
 * data is not yet written is only read by the locked query path. A torn
 * metadata entry is always the newest entry, so no reader can depend on it
 * yet. No reader returns a wrong answer from it, so there is no oracle. */

int review_main(const char* which) {
  gates_init();
  struct {
    const char* name;
    int (*fn)(void);
  } t[] = {{"R1", t_delete_during_compaction},
           {"R2", t_insert_racing_delete},
           {"R3", t_type_check_reads_reused_page},
           {"R4", t_range_reads_reused_page},
           {"R5", t_reinit_stale_metadata_cache},
           {"R6", t_compaction_oom_after_output},
           {"R7", t_metadata_page_cap},
           {"R8", t_l0_create_failure_leak},
           {"R9", t_metadata_fallback_duplicate},
           {"R11", t_near_full_with_reader},
           {"R12", t_orphan_record_blocks_compaction},
           {"R13", t_clear_all_under_reader}};
  int bad = 0;
  for (size_t i = 0; i < sizeof t / sizeof t[0]; i++) {
    if (strcmp(which, "all") && strcmp(which, t[i].name)) continue;
    int b = t[i].fn();
    printf("%s %s\n", b ? "FAIL" : "PASS", t[i].name);
    fflush(stdout);
    bad += b;
  }
  return bad;
}
