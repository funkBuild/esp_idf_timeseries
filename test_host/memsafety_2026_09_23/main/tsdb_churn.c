/*
 * memsafety 2026-09-23 (PROTO area): seeded multi-task churn of the real
 * esp_idf_timeseries component, modelled on the product's concurrent users:
 *   - "hb"    : sync_heartbeat node-stats insert (one point, many int/float fields)
 *   - "lua"   : app_runtime timeseries.insert (tags, strings, small batches)
 *   - "query" : web timeseries_api queries (rollups, aggregation, limits)
 *   - "meta"  : web listing endpoints (measurements/fields/tags/usage/range)
 *   - "admin" : background compaction trigger, expiry, field/measurement delete
 * Each task runs on its own FreeRTOS (pthread) task. Late stores into freed
 * blocks are caught by ASan with a large quarantine (see run_tests.sh); races
 * by TSan. Exit 0 = no sanitizer report and every API call returned sanely.
 */
#include <inttypes.h>
#include <pthread.h>
#include <sys/types.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "timeseries.h"
#include "esp_log.h"
#include "esp_partition.h"
#include "spi_flash_mmap.h"
#include "freertos/semphr.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "mbedtls/md.h"

/* Flash-integrity oracle (PROTO-TS-1). The linux esp_partition_write returns
 * ESP_ERR_FLASH_OP_FAIL when a write needs a 0->1 bit transition (a write over
 * non-erased NOR flash; CONFIG_ESP_PARTITION_ERASE_CHECK). Count them here:
 * logging is disabled because the linux FreeRTOS port can preempt a task inside
 * printf and deadlock the next printer on the libc stdout lock (sim artifact). */
static atomic_uint s_bad_writes;
static atomic_uint s_writes;
esp_err_t __real_esp_partition_write(const esp_partition_t* p, size_t off, const void* src, size_t len);
TaskHandle_t _Atomic review_fail_write_task;  // TSDB_REVIEW fault injection (tsdb_review_phases.c)
_Atomic uint32_t review_fail_write_len, review_fail_write_offset;
esp_err_t __wrap_esp_partition_write(const esp_partition_t* p, size_t off, const void* src, size_t len) {
  TaskHandle_t t = atomic_load(&review_fail_write_task);
  if (t && t == xTaskGetCurrentTaskHandle() && len == atomic_load(&review_fail_write_len) &&
      (atomic_load(&review_fail_write_offset) == UINT32_MAX || off == atomic_load(&review_fail_write_offset))) {
    atomic_store(&review_fail_write_task, (TaskHandle_t)NULL);
    return ESP_FAIL;
  }
  esp_err_t r = __real_esp_partition_write(p, off, src, len);
  atomic_fetch_add(&s_writes, 1);
  if (r == ESP_ERR_FLASH_OP_FAIL) atomic_fetch_add(&s_bad_writes, 1);
  return r;
}

/* PROTO-TS-1b hooks. tsdb_snapshot_acquire_current() hands us the db pointer;
 * timeseries_page_cache_iterator_deinit() is where tsdb_find_metadata_pages()
 * finishes scanning a page-cache snapshot (it used to publish the scan as a
 * cached page list, removed by TSDB_REVIEW S3). When armed for one task, that
 * task is parked there until the test lets it go. */
static timeseries_db_t* _Atomic s_db;
static TaskHandle_t _Atomic s_race_reader;
static atomic_bool s_race_armed;
static SemaphoreHandle_t s_race_scanned, s_race_resume, s_race_done;
tsdb_page_cache_snapshot_t* __real_tsdb_snapshot_acquire_current(timeseries_db_t* db);
tsdb_page_cache_snapshot_t* __wrap_tsdb_snapshot_acquire_current(timeseries_db_t* db) {
  atomic_store(&s_db, db);
  return __real_tsdb_snapshot_acquire_current(db);
}
void __real_timeseries_page_cache_iterator_deinit(timeseries_page_cache_iterator_t* iter);
void __wrap_timeseries_page_cache_iterator_deinit(timeseries_page_cache_iterator_t* iter) {
  if (atomic_load(&s_race_armed) && xTaskGetCurrentTaskHandle() == atomic_load(&s_race_reader)) {
    atomic_store(&s_race_armed, false);
    xSemaphoreGive(s_race_scanned);
    xSemaphoreTake(s_race_resume, portMAX_DELAY);
  }
  __real_timeseries_page_cache_iterator_deinit(iter);
}

/* PROTO-TS-1c (simulator artifact, not product code). The linux FreeRTOS port
 * runs every task on a pthread and switches tasks from its SIGALRM tick
 * handler, at any instruction. If the switched-out thread holds a
 * process-wide lock that is not a FreeRTOS object, the next task that needs
 * that lock blocks in the kernel, the scheduler never runs again, and the
 * whole simulation stops. Two such locks were caught with gdb:
 *  - glibc's thread-stack lock, held by the idle task in pthread_join()
 *    (vPortCancelThread reaping a deleted task, outside a critical section)
 *    while the next xTaskCreate() needed it in pthread_create();
 *  - an ASan allocator size-class mutex, held by the idle task freeing a
 *    deleted task's memory, while another task needed it in malloc().
 * On the device FreeRTOS owns every lock, so this cannot happen there. Run
 * these calls with all signals blocked, as the port does for pthread_create
 * (vPortEnterCritical). The previous mask is restored, so a call made inside
 * a critical section stays inside it. */
#define SIG_GUARD_BEGIN     \
  sigset_t sg_all_, sg_old_; \
  sigfillset(&sg_all_);      \
  pthread_sigmask(SIG_BLOCK, &sg_all_, &sg_old_)
#define SIG_GUARD_END pthread_sigmask(SIG_SETMASK, &sg_old_, NULL)
int __real_pthread_join(pthread_t thread, void** retval);
int __wrap_pthread_join(pthread_t thread, void** retval) {
  SIG_GUARD_BEGIN;
  int rc = __real_pthread_join(thread, retval);
  SIG_GUARD_END;
  return rc;
}
// TSDB_REVIEW R6: fail the next malloc/calloc/realloc made by this task.
TaskHandle_t _Atomic review_fail_alloc_task;
static bool review_fail_this_alloc(void) {
  TaskHandle_t t = atomic_load(&review_fail_alloc_task);
  if (!t || t != xTaskGetCurrentTaskHandle()) return false;
  atomic_store(&review_fail_alloc_task, (TaskHandle_t)NULL);
  return true;
}
void* __real_malloc(size_t n);
void* __wrap_malloc(size_t n) {
  if (review_fail_this_alloc()) return NULL;
  SIG_GUARD_BEGIN;
  void* p = __real_malloc(n);
  SIG_GUARD_END;
  return p;
}
void* __real_calloc(size_t n, size_t sz);
void* __wrap_calloc(size_t n, size_t sz) {
  if (review_fail_this_alloc()) return NULL;
  SIG_GUARD_BEGIN;
  void* p = __real_calloc(n, sz);
  SIG_GUARD_END;
  return p;
}
void* __real_realloc(void* ptr, size_t n);
void* __wrap_realloc(void* ptr, size_t n) {
  if (review_fail_this_alloc()) return NULL;
  SIG_GUARD_BEGIN;
  void* p = __real_realloc(ptr, n);
  SIG_GUARD_END;
  return p;
}
void* __real_aligned_alloc(size_t align, size_t n);
void* __wrap_aligned_alloc(size_t align, size_t n) {
  SIG_GUARD_BEGIN;
  void* p = __real_aligned_alloc(align, n);
  SIG_GUARD_END;
  return p;
}
void __real_free(void* ptr);
void __wrap_free(void* ptr) {
  SIG_GUARD_BEGIN;
  __real_free(ptr);
  SIG_GUARD_END;
}
/* PROTO-TS-1d (simulator artifact, not product code). ESP-IDF's linux log lock
 * (components/log/src/linux/log_lock.c) is a plain pthread mutex, taken by
 * every ESP_LOGx call even at level NONE (esp_log_is_tag_loggable). A task
 * switched out while holding it stalls the next logger in the kernel, and the
 * scheduler with it. Caught with gdb: seeds hung with one task in
 * esp_log_impl_lock -> futex_wait on mutex1 and every other task suspended.
 * Hold the lock with all signals blocked, as above; the mask is restored when
 * the outermost lock is released. */
static _Thread_local sigset_t s_log_mask;
static _Thread_local int s_log_depth;
static void log_guard_enter(void) {
  sigset_t all, old;
  sigfillset(&all);
  pthread_sigmask(SIG_BLOCK, &all, &old);
  if (s_log_depth++ == 0) s_log_mask = old;
}
void __real_esp_log_impl_lock(void);
void __wrap_esp_log_impl_lock(void) {
  log_guard_enter();
  __real_esp_log_impl_lock();
}
bool __real_esp_log_impl_lock_timeout(void);
bool __wrap_esp_log_impl_lock_timeout(void) {
  log_guard_enter();
  bool locked = __real_esp_log_impl_lock_timeout();
  if (!locked && --s_log_depth == 0) pthread_sigmask(SIG_SETMASK, &s_log_mask, NULL);
  return locked;
}
void __real_esp_log_impl_unlock(void);
void __wrap_esp_log_impl_unlock(void) {
  __real_esp_log_impl_unlock();
  if (s_log_depth > 0 && --s_log_depth == 0) pthread_sigmask(SIG_SETMASK, &s_log_mask, NULL);
}
char* __real_strdup(const char* str);
char* __wrap_strdup(const char* str) {
  SIG_GUARD_BEGIN;
  char* p = __real_strdup(str);
  SIG_GUARD_END;
  return p;
}
char* __real_strndup(const char* str, size_t n);
char* __wrap_strndup(const char* str, size_t n) {
  SIG_GUARD_BEGIN;
  char* p = __real_strndup(str, n);
  SIG_GUARD_END;
  return p;
}

static atomic_bool s_stop;
static atomic_int s_done;
static atomic_uint_fast64_t s_ops[5];
static unsigned s_seed = 1;

static uint32_t rnd(uint32_t* s) {
  *s ^= *s << 13;
  *s ^= *s >> 17;
  *s ^= *s << 5;
  return *s;
}

#define HB_FIELDS 40
static const char* hb_names[HB_FIELDS];
static char hb_name_buf[HB_FIELDS][16];

/* Data-correctness oracle: stat_00 of node_stats is the insert's sequence
 * number k at timestamp HB_TS0 + k*1000; node_stats is never deleted through
 * the API, only by expiration. After the churn every point the query returns
 * must be distinct and carry its own k, and every point whose insert returned
 * true must either be returned or lie inside a node_stats.stat_00 record that
 * expiration soft-deleted while the churn ran (recorded as it happens, below,
 * because expiration then rewrites the page without the record).
 * Expiration works on whole records ordered by their start time, and
 * compaction merges pages chosen by offset, so an expired record can cover
 * points newer than a surviving one: "records, oldest start first" is the
 * contract, not "the oldest points only". */
#define HB_TS0 1700000000000ULL
static uint8_t* s_hb_ok;  // per k: 1 = timeseries_insert returned true
static size_t s_hb_count, s_hb_cap;

static void hb_task(void* arg) {
  (void)arg;
  uint64_t ts = HB_TS0;
  uint32_t s = s_seed * 2654435761u + 1;
  timeseries_field_value_t fv[HB_FIELDS];
  while (!atomic_load(&s_stop)) {
    for (int i = 0; i < HB_FIELDS; i++) {
      if (i & 1) {
        fv[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        fv[i].data.float_val = (float)(rnd(&s) % 100000) / 7.0f;
      } else {
        fv[i].type = TIMESERIES_FIELD_TYPE_INT;
        fv[i].data.int_val = (int64_t)rnd(&s);
      }
    }
    size_t k = s_hb_count;
    fv[0].type = TIMESERIES_FIELD_TYPE_INT;
    fv[0].data.int_val = (int64_t)k;
    ts = HB_TS0 + (uint64_t)k * 1000;
    timeseries_insert_data_t d = {.measurement_name = "node_stats",
                                  .field_names = hb_names,
                                  .field_values = fv,
                                  .num_fields = HB_FIELDS,
                                  .timestamps_ms = &ts,
                                  .num_points = 1};
    bool ok = timeseries_insert(&d);
    if (k == s_hb_cap) {
      s_hb_cap = s_hb_cap ? s_hb_cap * 2 : 4096;
      s_hb_ok = realloc(s_hb_ok, s_hb_cap);
      if (!s_hb_ok) abort();
    }
    s_hb_ok[k] = ok;
    s_hb_count = k + 1;
    atomic_fetch_add(&s_ops[0], 1);
    if ((rnd(&s) & 7) == 0) vTaskDelay(1);
  }
  atomic_fetch_add(&s_done, 1);
  vTaskDelete(NULL);
}

static const char* meas[] = {"m0", "m1", "m2", "m3"};
static const char* fnames[] = {"temp", "hum", "state", "label", "count", "flag"};
static const char* tkeys[] = {"dev", "site"};
static const char* tvals[] = {"a", "b", "c"};

static void lua_task(void* arg) {
  (void)arg;
  uint32_t s = s_seed * 40503u + 7;
  uint64_t ts = 1700000000000ULL;
  char strbuf[20][24];
  while (!atomic_load(&s_stop)) {
    size_t np = 1 + rnd(&s) % 20;
    size_t nf = 1 + rnd(&s) % 6;
    const char* fn[6];
    uint32_t start = rnd(&s) % 6;
    for (size_t f = 0; f < nf; f++) fn[f] = fnames[(start + f) % 6];
    timeseries_field_value_t* fv = calloc(np * nf, sizeof(*fv));
    uint64_t* tss = calloc(np, sizeof(*tss));
    if (!fv || !tss) abort();
    for (size_t p = 0; p < np; p++) {
      ts += 1 + rnd(&s) % 3000;
      tss[p] = ts;
      for (size_t f = 0; f < nf; f++) {
        timeseries_field_value_t* v = &fv[f * np + p];
        const char* name = fn[f];
        if (!strcmp(name, "label")) {
          snprintf(strbuf[p], sizeof strbuf[p], "s%" PRIu32, rnd(&s) % 50);
          v->type = TIMESERIES_FIELD_TYPE_STRING;
          v->data.string_val.str = strbuf[p];
          v->data.string_val.length = strlen(strbuf[p]);
        } else if (!strcmp(name, "flag")) {
          v->type = TIMESERIES_FIELD_TYPE_BOOL;
          v->data.bool_val = rnd(&s) & 1;
        } else if (!strcmp(name, "count") || !strcmp(name, "state")) {
          v->type = TIMESERIES_FIELD_TYPE_INT;
          v->data.int_val = (int64_t)(rnd(&s) % 1000);
        } else {
          v->type = TIMESERIES_FIELD_TYPE_FLOAT;
          v->data.float_val = (double)(rnd(&s) % 10000) / 3.0;
        }
      }
    }
    const char* tk[2] = {tkeys[0], tkeys[1]};
    const char* tv[2] = {tvals[rnd(&s) % 3], tvals[rnd(&s) % 3]};
    timeseries_insert_data_t d = {.measurement_name = meas[rnd(&s) % 4],
                                  .tag_keys = tk,
                                  .tag_values = tv,
                                  .num_tags = rnd(&s) % 3,
                                  .field_names = fn,
                                  .field_values = fv,
                                  .num_fields = nf,
                                  .timestamps_ms = tss,
                                  .num_points = np};
    timeseries_insert(&d);
    free(fv);
    free(tss);
    atomic_fetch_add(&s_ops[1], 1);
    if ((rnd(&s) & 3) == 0) vTaskDelay(1);
  }
  atomic_fetch_add(&s_done, 1);
  vTaskDelete(NULL);
}

static void query_task(void* arg) {
  (void)arg;
  uint32_t s = s_seed * 69069u + 3;
  while (!atomic_load(&s_stop)) {
    const char* fn[2] = {fnames[rnd(&s) % 6], fnames[rnd(&s) % 6]};
    const char* hbf[2] = {hb_names[rnd(&s) % HB_FIELDS], hb_names[rnd(&s) % HB_FIELDS]};
    bool hb = (rnd(&s) % 3) == 0;
    const char* tk[1] = {tkeys[rnd(&s) % 2]};
    const char* tv[1] = {tvals[rnd(&s) % 3]};
    timeseries_query_t q = {.measurement_name = hb ? "node_stats" : meas[rnd(&s) % 4],
                            .tag_keys = tk,
                            .tag_values = tv,
                            .num_tags = hb ? 0 : rnd(&s) % 2,
                            .field_names = hb ? hbf : fn,
                            .num_fields = rnd(&s) % 3,
                            .start_ms = (rnd(&s) & 1) ? 0 : 1700000000000LL + (int64_t)(rnd(&s) % 2000000),
                            .end_ms = (rnd(&s) & 1) ? 0 : 1700000000000LL + 2000000LL + (int64_t)(rnd(&s) % 20000000),
                            .limit = (rnd(&s) & 1) ? 0 : 1 + rnd(&s) % 500,
                            .rollup_interval = (rnd(&s) & 1) ? 0 : 1000 * (1 + rnd(&s) % 60),
                            .aggregate_method = (timeseries_aggregation_method_e)(rnd(&s) % 7)};
    timeseries_query_result_t r;
    memset(&r, 0, sizeof r);
    if (timeseries_query(&q, &r)) {
      for (size_t c = 0; c < r.num_columns; c++) {
        volatile const char* n = r.columns[c].name;
        (void)n;
        for (size_t p = 0; p < r.num_points; p++) {
          volatile int t = r.columns[c].values[p].type;
          (void)t;
        }
      }
      timeseries_query_free_result(&r);
    }
    atomic_fetch_add(&s_ops[2], 1);
    vTaskDelay(1);
  }
  atomic_fetch_add(&s_done, 1);
  vTaskDelete(NULL);
}

static void meta_task(void* arg) {
  (void)arg;
  uint32_t s = s_seed * 1103515245u + 11;
  while (!atomic_load(&s_stop)) {
    char** ms = NULL;
    size_t n = 0;
    if (timeseries_get_measurements(&ms, &n)) {
      for (size_t i = 0; i < n; i++) free(ms[i]);
      free(ms);
    }
    const char* m = (rnd(&s) & 1) ? "node_stats" : meas[rnd(&s) % 4];
    char** fs = NULL;
    if (timeseries_get_fields_for_measurement(m, &fs, &n)) {
      for (size_t i = 0; i < n; i++) free(fs[i]);
      free(fs);
    }
    tsdb_tag_pair_t* tags = NULL;
    if (timeseries_get_tags_for_measurement(m, &tags, &n)) {
      for (size_t i = 0; i < n; i++) {
        free(tags[i].key);
        free(tags[i].val);
      }
      free(tags);
    }
    tsdb_usage_summary_t sum;
    timeseries_get_usage_summary(&sum);
    uint64_t lo, hi;
    uint32_t cnt;
    timeseries_get_timestamp_range(m, &lo, &hi, &cnt);
    atomic_fetch_add(&s_ops[3], 1);
    vTaskDelay(2);
  }
  atomic_fetch_add(&s_done, 1);
  vTaskDelete(NULL);
}

static void admin_task(void* arg) {
  (void)arg;
  uint32_t s = s_seed * 214013u + 2531011u;
  while (!atomic_load(&s_stop)) {
    uint32_t r = rnd(&s) % 100;
    if (r < 40) {
      timeseries_compact();  // async, background task (product path)
    } else if (r < 55) {
      timeseries_expire();
    } else if (r < 65) {
      timeseries_delete_measurement_and_field(meas[3], fnames[rnd(&s) % 6]);
    } else if (r < 70) {
      timeseries_delete_measurement(meas[2 + (rnd(&s) & 1)]);
    } else if (r < 73) {
      timeseries_compact_sync();
    }
    atomic_fetch_add(&s_ops[4], 1);
    vTaskDelay(5 + rnd(&s) % 40);
  }
  atomic_fetch_add(&s_done, 1);
  vTaskDelete(NULL);
}

/* Deterministic single-task phase (PROTO-TS-1). Every insert brings a new
 * series (a new "node" tag value), so the shared tag and field-list index
 * entries are rewritten on every insert and the metadata pages fill and get
 * compacted in the middle of inserts. A field delete every 50 inserts runs the
 * series-delete path, which rewrites shared index entries too. Afterwards the
 * tag index must still find exactly the points written for every
 * (measurement, grp) pair and for sampled node values: a replaced entry that
 * was left live, or a new entry written into a retired page, shows up as a
 * wrong count. The default of 160 inserts keeps the live metadata within the
 * metadata pages the component uses; far larger runs make inserts fail for
 * lack of metadata space, which is not this defect. Returns the number of
 * failed checks (the first 10 are printed). */
static int det_phase(unsigned inserts) {
  static const char* dmeas[] = {"det0", "det1", "det2", "det3"};
  static const char* dfields[] = {"a", "b"};
  static const char* dgrp[] = {"g0", "g1", "g2"};
  unsigned per[4][3];
  memset(per, 0, sizeof per);
  uint64_t ts = 1600000000000ULL;
  int bad = 0;
  for (unsigned i = 0; i < inserts; i++) {
    char node[16];
    snprintf(node, sizeof node, "n%u", i);
    // Every 25th insert carries 17 tags, which takes the per-tag upsert path
    // (more than 16 tags) instead of the batched tag scan.
    const char* tk[17] = {"node", "grp", "k2", "k3", "k4", "k5", "k6", "k7", "k8",
                          "k9", "k10", "k11", "k12", "k13", "k14", "k15", "k16"};
    const char* tv[17] = {node, dgrp[i % 3], "v", "v", "v", "v", "v", "v", "v", "v", "v", "v", "v", "v", "v", "v", "v"};
    timeseries_field_value_t fv[2];
    fv[0].type = TIMESERIES_FIELD_TYPE_FLOAT;
    fv[0].data.float_val = (double)i;
    fv[1].type = TIMESERIES_FIELD_TYPE_FLOAT;
    fv[1].data.float_val = (double)i / 2.0;
    ts += 1000;
    timeseries_insert_data_t d = {.measurement_name = dmeas[i % 4],
                                  .tag_keys = tk,
                                  .tag_values = tv,
                                  .num_tags = (i % 25 == 0) ? 17 : 2,
                                  .field_names = dfields,
                                  .field_values = fv,
                                  .num_fields = 2,
                                  .timestamps_ms = &ts,
                                  .num_points = 1};
    if (!timeseries_insert(&d)) {
      if (bad++ < 10) printf("det: insert %u failed\n", i);
    } else {
      per[i % 4][i % 3]++;
    }
    if (i % 50 == 49) timeseries_delete_measurement_and_field("det3", "b");
  }
  printf("det: inserts=%u flash writes=%u over-non-erased=%u\n", inserts, atomic_load(&s_writes),
         atomic_load(&s_bad_writes));
  for (unsigned m = 0; m < 3; m++) {
    for (unsigned g = 0; g < 3; g++) {
      const char* tk[1] = {"grp"};
      const char* tv[1] = {dgrp[g]};
      const char* qf[1] = {"a"};
      timeseries_query_t q = {.measurement_name = dmeas[m], .tag_keys = tk, .tag_values = tv, .num_tags = 1,
                              .field_names = qf, .num_fields = 1};
      timeseries_query_result_t r;
      memset(&r, 0, sizeof r);
      size_t got = 0;
      if (timeseries_query(&q, &r)) {
        got = r.num_points;
        timeseries_query_free_result(&r);
      }
      if (got != per[m][g]) {
        if (bad++ < 10) printf("det: FAIL %s grp=%s points=%zu want=%u\n", dmeas[m], dgrp[g], got, per[m][g]);
      }
    }
  }
  for (unsigned i = 0; i < inserts; i += 7) {
    if (i % 4 == 3) continue;  // det3 has field deletes
    char node[16];
    snprintf(node, sizeof node, "n%u", i);
    const char* tk[1] = {"node"};
    const char* tv[1] = {node};
    timeseries_query_t q = {.measurement_name = dmeas[i % 4], .tag_keys = tk, .tag_values = tv, .num_tags = 1};
    timeseries_query_result_t r;
    memset(&r, 0, sizeof r);
    size_t got = 0, cols = 0;
    if (timeseries_query(&q, &r)) {
      got = r.num_points;
      cols = r.num_columns;
      timeseries_query_free_result(&r);
    }
    if (got != 1 || cols != 2) {
      if (bad++ < 10) printf("det: FAIL %s node=%s points=%zu columns=%zu want 1/2\n", dmeas[i % 4], node, got, cols);
    }
  }
  return bad;
}

/* Deterministic PROTO-TS-1b phase, part 1: the background compaction publishes
 * its private page-cache batch (cloned from the live cache when the batch
 * began) with tsdb_pagecache_commit_batch(). A page that an insert removed
 * from the live cache while the batch was open (a metadata page retired by a
 * metadata compaction) must stay removed: if the commit brings it back, it is
 * listed ACTIVE again while its flash region is free for reuse. Uses a dummy
 * cache entry only; nothing is written to flash. Returns failed checks. */
static int batch_phase(void) {
  timeseries_db_t* db = atomic_load(&s_db);
  if (!db) {
    printf("batch: FAIL no db pointer captured\n");
    return 1;
  }
  const uint32_t off = (uint32_t)db->partition->size - 8192;
  timeseries_page_header_t h;
  memset(&h, 0xFF, sizeof h);
  h.magic_number = TIMESERIES_MAGIC_NUM;
  h.page_type = TIMESERIES_PAGE_TYPE_METADATA;
  h.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  h.sequence_num = 1;
  h.page_size = 8192;
  int bad = 0;
  if (!tsdb_pagecache_add_entry(db, off, &h)) {
    printf("batch: FAIL could not add the dummy page\n");
    return 1;
  }
  tsdb_page_cache_snapshot_t* batch = tsdb_pagecache_begin_batch(db);
  if (!batch) abort();
  tsdb_pagecache_remove_entry(db, off);  // live removal while the batch is open
  if (!tsdb_pagecache_commit_batch(db, batch)) {
    printf("batch: FAIL commit failed\n");
    bad++;
  }
  timeseries_page_cache_iterator_t it;
  if (timeseries_page_cache_iterator_init(db, &it)) {
    timeseries_page_header_t ph;
    uint32_t po = 0, ps = 0;
    while (timeseries_page_cache_iterator_next(&it, &ph, &po, &ps)) {
      if (po == off) {
        printf("batch: FAIL page @0x%05" PRIx32 " removed from the live cache is listed again after the batch commit\n",
               off);
        bad++;
      }
    }
    timeseries_page_cache_iterator_deinit(&it);
  }
  tsdb_pagecache_remove_entry(db, off);
  return bad;
}

static void race_reader_task(void* arg) {
  (void)arg;
  atomic_store(&s_race_reader, xTaskGetCurrentTaskHandle());
  atomic_store(&s_race_armed, true);
  char** ms = NULL;
  size_t n = 0;
  if (timeseries_get_measurements(&ms, &n)) {
    for (size_t i = 0; i < n; i++) free(ms[i]);
    free(ms);
  }
  atomic_store(&s_race_armed, false);
  xSemaphoreGive(s_race_done);
  vTaskDelete(NULL);
}

/* Deterministic PROTO-TS-1b phase: a metadata reader that does not hold
 * flash_write_mutex (here timeseries_get_measurements; the insert type lookup,
 * the other listings and the timestamp range behave the same) rescans the
 * page cache for metadata pages. It is parked after the scan, before it
 * publishes the page list. Meanwhile an insert compacts a metadata page, which
 * retires a page the parked reader saw. When the reader resumes, the metadata
 * offset cache must not name that retired page, or later index writes go into
 * it (writes over reused flash, or lost index entries). Returns failed checks. */
static int race_phase(void) {
  s_race_scanned = xSemaphoreCreateBinary();
  s_race_resume = xSemaphoreCreateBinary();
  s_race_done = xSemaphoreCreateBinary();
  if (!s_race_scanned || !s_race_resume || !s_race_done) abort();
  int bad = 0;
  // Leaves the metadata page-offset cache invalid, so the reader rescans.
  timeseries_delete_measurement("race_none");
  timeseries_db_t* db = atomic_load(&s_db);
  if (!db) {
    printf("race: FAIL no db pointer captured\n");
    return 1;
  }
  xTaskCreate(race_reader_task, "race_rd", 32768, NULL, 1, NULL);
  if (xSemaphoreTake(s_race_scanned, pdMS_TO_TICKS(10000)) != pdTRUE) {
    printf("race: FAIL reader did not rescan the metadata page list\n");
    atomic_store(&s_race_armed, false);
    xSemaphoreTake(s_race_done, pdMS_TO_TICKS(10000));
    return 1;
  }
  const uint32_t gen0 = db->metadata_page_gen;
  uint64_t ts = 1500000000000ULL;
  unsigned i;
  for (i = 0; i < 3000 && db->metadata_page_gen == gen0; i++) {
    char node[16];
    snprintf(node, sizeof node, "r%u", i);
    const char* tk[2] = {"node", "grp"};
    const char* tv[2] = {node, (i & 1) ? "x" : "y"};
    const char* fn[2] = {"a", "b"};
    timeseries_field_value_t fv[2] = {{.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = i},
                                      {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = i}};
    ts += 1000;
    timeseries_insert_data_t d = {.measurement_name = "race", .tag_keys = tk, .tag_values = tv, .num_tags = 2,
                                  .field_names = fn, .field_values = fv, .num_fields = 2,
                                  .timestamps_ms = &ts, .num_points = 1};
    timeseries_insert(&d);
  }
  if (db->metadata_page_gen == gen0) {
    printf("race: FAIL no metadata page compaction after %u inserts\n", i);
    bad++;
  }
  xSemaphoreGive(s_race_resume);
  if (xSemaphoreTake(s_race_done, pdMS_TO_TICKS(10000)) != pdTRUE) {
    printf("race: FAIL reader did not finish\n");
    return bad + 1;
  }
  // Every metadata page the page cache lists (the only source of the page
  // list since TSDB_REVIEW S3) must be an ACTIVE metadata page on flash.
  timeseries_page_cache_iterator_t it;
  if (timeseries_page_cache_iterator_init(db, &it)) {
    timeseries_page_header_t ch, h;
    uint32_t off, sz;
    while (timeseries_page_cache_iterator_next(&it, &ch, &off, &sz)) {
      if (ch.page_type != TIMESERIES_PAGE_TYPE_METADATA) continue;
      if (esp_partition_read(db->partition, off, &h, sizeof h) != ESP_OK || h.magic_number != TIMESERIES_MAGIC_NUM ||
          h.page_type != TIMESERIES_PAGE_TYPE_METADATA || h.page_state != TIMESERIES_PAGE_STATE_ACTIVE) {
        printf("race: FAIL page cache lists page @0x%05" PRIx32 " (state=%u), not an active metadata page\n", off,
               (unsigned)h.page_state);
        bad++;
      }
    }
    timeseries_page_cache_iterator_deinit(&it);
  }
  printf("race: inserts=%u compaction_gen %" PRIu32 "->%" PRIu32 " failed=%d\n", i, gen0, db->metadata_page_gen, bad);
  return bad;
}

// On a data-check failure, keep what is needed to find out where the points
// went: the whole partition image and the live page cache (run_tests.sh keeps
// both with KEEP_FAILED_LOGS). Analyse offline with the page/record layout.
static void dump_forensics(void) {
  timeseries_db_t* db = atomic_load(&s_db);
  if (!db || !db->partition) return;
  FILE* f = fopen("tsdb_flash.bin", "wb");
  if (f) {
    static uint8_t chunk[4096];
    for (uint32_t off = 0; off < db->partition->size; off += sizeof chunk) {
      if (esp_partition_read(db->partition, off, chunk, sizeof chunk) != ESP_OK) break;
      fwrite(chunk, 1, sizeof chunk, f);
    }
    fclose(f);
  }
  f = fopen("tsdb_cache.txt", "w");
  if (!f) return;
  tsdb_page_cache_snapshot_t* snap = tsdb_snapshot_acquire_current(db);
  for (size_t i = 0; snap && i < snap->count; i++) {
    const timeseries_page_header_t* h = &snap->entries[i].header;
    fprintf(f, "0x%06" PRIx32 " type=%u state=%u level=%u seq=%" PRIu32 " size=%" PRIu32 "\n", snap->entries[i].offset,
            h->page_type, h->page_state, h->field_data_level, h->sequence_num, h->page_size);
  }
  tsdb_snapshot_release(snap);
  fclose(f);
  printf("data: wrote tsdb_flash.bin and tsdb_cache.txt\n");
}

// Expiration and deletes clear a record's DELETED bit with
// tsdb_flash_write_byte() on the header's flags byte. Record the time range of
// every node_stats.stat_00 record that gets deleted this way.
static unsigned char s_hb_sid[16];
static struct {
  uint64_t start, end;
} s_hb_expired[4096];
static atomic_size_t s_hb_expired_n;
esp_err_t __real_tsdb_flash_write_byte(const esp_partition_t* part, uint32_t offset, uint8_t value);
esp_err_t __wrap_tsdb_flash_write_byte(const esp_partition_t* part, uint32_t offset, uint8_t value) {
  timeseries_field_data_header_t fh;
  bool track = !(value & TSDB_FIELDDATA_FLAG_DELETED) &&
               esp_partition_read(part, offset, &fh, sizeof fh) == ESP_OK && !memcmp(fh.series_id, s_hb_sid, 16) &&
               (fh.flags & TSDB_FIELDDATA_FLAG_DELETED) && fh.start_time <= fh.end_time;
  esp_err_t err = __real_tsdb_flash_write_byte(part, offset, value);
  if (track && err == ESP_OK) {
    size_t i = atomic_fetch_add(&s_hb_expired_n, 1);
    if (i < sizeof s_hb_expired / sizeof s_hb_expired[0]) {
      s_hb_expired[i].start = fh.start_time;
      s_hb_expired[i].end = fh.end_time;
    }
  }
  return err;
}
static bool hb_point_expired(uint64_t ts) {
  size_t n = atomic_load(&s_hb_expired_n);
  if (n > sizeof s_hb_expired / sizeof s_hb_expired[0]) return true;  // log overflowed: cannot tell
  for (size_t i = 0; i < n; i++)
    if (ts >= s_hb_expired[i].start && ts <= s_hb_expired[i].end) return true;
  return false;
}

static int hb_oracle(void) {
  const char* fn[1] = {"stat_00"};
  timeseries_query_t q = {.measurement_name = "node_stats", .field_names = fn, .num_fields = 1};
  timeseries_query_result_t r;
  memset(&r, 0, sizeof r);
  if (!timeseries_query(&q, &r)) {
    printf("data: FAIL query of node_stats.stat_00 failed (%zu inserts)\n", s_hb_count);
    return 1;
  }
  int bad = 0;
  size_t got = r.num_points, first = SIZE_MAX, succeeded = 0, missing = 0;
  uint8_t* seen = calloc(s_hb_count + 1, 1);
  for (size_t i = 0; i < got; i++) {
    uint64_t t = r.timestamps[i];
    size_t k = (size_t)((t - HB_TS0) / 1000);
    const timeseries_field_value_t* v = &r.columns[0].values[i];
    if (t < HB_TS0 || (t - HB_TS0) % 1000 || k >= s_hb_count || v->type != TIMESERIES_FIELD_TYPE_INT ||
        v->data.int_val != (int64_t)k || seen[k]) {
      if (bad++ < 5)
        printf("data: FAIL point %zu ts=%" PRIu64 " type=%d value=%" PRId64 " dup=%d\n", i, t, (int)v->type,
               v->data.int_val, k < s_hb_count ? seen[k] : 0);
      continue;
    }
    seen[k] = 1;
    if (k < first) first = k;
  }
  size_t expired = 0;
  for (size_t k = 0; k < s_hb_count; k++) {
    succeeded += s_hb_ok[k];
    if (!s_hb_ok[k] || seen[k]) continue;
    if (hb_point_expired(HB_TS0 + (uint64_t)k * 1000)) {
      expired++;
    } else if (missing++ < 5) {
      printf("data: FAIL successful insert k=%zu missing (no expired record covered it)\n", k);
    }
  }
  if (!got && succeeded && bad++ < 5) printf("data: FAIL no node_stats points stored\n");
  printf("data: node_stats inserts=%zu succeeded=%zu stored=%zu expired=%zu (records %zu) oldest_k=%zd bad=%d "
         "missing=%zu\n",
         s_hb_count, succeeded, got, expired, atomic_load(&s_hb_expired_n),
         first == SIZE_MAX ? (ssize_t)-1 : (ssize_t)first, bad, missing);
  free(seen);
  timeseries_query_free_result(&r);
  if (bad || missing) dump_forensics();
  return bad + (int)missing;
}

timeseries_db_t* review_db(void) { return atomic_load(&s_db); }
int review_main(const char* which);

void app_main(void) {
  const char* e = getenv("CHURN_SEED");
  if (e) s_seed = (unsigned)strtoul(e, NULL, 0);
  if (!s_seed) s_seed = 1;
  int secs = 60;
  e = getenv("CHURN_SECONDS");
  if (e) secs = atoi(e);
  if (!getenv("CHURN_LOG")) esp_log_level_set("*", ESP_LOG_NONE);
  mbedtls_md(mbedtls_md_info_from_type(MBEDTLS_MD_MD5), (const unsigned char*)"node_stats:stat_00",
             strlen("node_stats:stat_00"), s_hb_sid);
  for (int i = 0; i < HB_FIELDS; i++) {
    snprintf(hb_name_buf[i], sizeof hb_name_buf[i], "stat_%02d", i);
    hb_names[i] = hb_name_buf[i];
  }
  if (!timeseries_init()) {
    printf("FAIL timeseries_init\n");
    exit(2);
  }
  timeseries_clear_all();
  if (getenv("REVIEW")) {  // one TSDB_REVIEW phase (or "all"), then exit
    int b = review_main(getenv("REVIEW"));
    printf("review: %d failed\n", b);
    fflush(stdout);
    exit(b ? 7 : 0);
  }
  if (!getenv("CHURN_NO_REVIEW")) {
    int b = review_main("all");
    if (b) {
      printf("FAIL TSDB_REVIEW phases: %d failed checks\n", b);
      exit(7);
    }
    printf("PASS review phases\n");
    timeseries_clear_all();
  }
  if (!getenv("CHURN_NO_RACE")) {
    int batch_bad = batch_phase();
    if (batch_bad) {
      printf("FAIL PROTO-TS-1b batch phase: %d failed checks\n", batch_bad);
      exit(6);
    }
    printf("PASS batch phase\n");
    int race_bad = race_phase();
    if (race_bad) {
      printf("FAIL PROTO-TS-1b race phase: %d failed checks\n", race_bad);
      exit(6);
    }
    printf("PASS race phase\n");
    timeseries_clear_all();
  }
  e = getenv("CHURN_DET_INSERTS");
  unsigned det_inserts = e ? (unsigned)strtoul(e, NULL, 0) : 160;
  if (det_inserts) {
    int det_bad = det_phase(det_inserts);
    unsigned det_bad_writes = atomic_load(&s_bad_writes);
    if (det_bad || det_bad_writes) {
      printf("FAIL PROTO-TS-1 deterministic phase: %d failed checks, %u writes over non-erased flash\n", det_bad,
             det_bad_writes);
      exit(5);
    }
    printf("PASS deterministic phase: %u inserts\n", det_inserts);
    timeseries_clear_all();
  }
  printf("churn: seed=%u seconds=%d\n", s_seed, secs);
  unsigned mask = 31;
  e = getenv("CHURN_TASKS");  // bit0 hb, bit1 lua, bit2 query, bit3 meta, bit4 admin
  if (e) mask = (unsigned)strtoul(e, NULL, 0) & 31;
  int want = 0;
  TaskFunction_t fns[5] = {hb_task, lua_task, query_task, meta_task, admin_task};
  const char* names[5] = {"hb", "lua", "query", "meta", "admin"};
  for (int i = 0; i < 5; i++) {
    if (mask & (1u << i)) {
      xTaskCreate(fns[i], names[i], 32768, NULL, 1, NULL);
      want++;
    }
  }
  int64_t end = esp_timer_get_time() + (int64_t)secs * 1000000;
  while (esp_timer_get_time() < end) {
    vTaskDelay(pdMS_TO_TICKS(5000));
    printf("progress: hb=%" PRIu64 " lua=%" PRIu64 " query=%" PRIu64 " meta=%" PRIu64 " admin=%" PRIu64 "\n",
           (uint64_t)s_ops[0], (uint64_t)s_ops[1], (uint64_t)s_ops[2], (uint64_t)s_ops[3], (uint64_t)s_ops[4]);
    fflush(stdout);
  }
  atomic_store(&s_stop, true);
  int64_t deadline = esp_timer_get_time() + 60LL * 1000000;
  while (atomic_load(&s_done) < want && esp_timer_get_time() < deadline) vTaskDelay(pdMS_TO_TICKS(50));
  timeseries_compact_sync();
  printf("churn ops: hb=%" PRIu64 " lua=%" PRIu64 " query=%" PRIu64 " meta=%" PRIu64 " admin=%" PRIu64 "\n",
         (uint64_t)s_ops[0], (uint64_t)s_ops[1], (uint64_t)s_ops[2], (uint64_t)s_ops[3], (uint64_t)s_ops[4]);
  printf("flash writes=%u over-non-erased=%u\n", atomic_load(&s_writes), atomic_load(&s_bad_writes));
  if ((mask & 1u) && hb_oracle()) {
    printf("FAIL churn data check (node_stats.stat_00)\n");
    exit(8);
  }
  if (atomic_load(&s_bad_writes)) {
    printf("FAIL PROTO-TS-1: %u esp_partition_write calls needed a 0->1 bit transition (write over non-erased flash)\n",
           atomic_load(&s_bad_writes));
    exit(4);
  }
  if (atomic_load(&s_done) < want) {
    printf("FAIL churn tasks did not stop (hang?)\n");
    exit(3);
  }
  printf("PASS tsdb churn seed=%u\n", s_seed);
  exit(0);
}
