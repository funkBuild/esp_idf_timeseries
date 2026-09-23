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

/* Flash-integrity oracle (PROTO-TS-1). The linux esp_partition_write returns
 * ESP_ERR_FLASH_OP_FAIL when a write needs a 0->1 bit transition (a write over
 * non-erased NOR flash; CONFIG_ESP_PARTITION_ERASE_CHECK). Count them here:
 * logging is disabled because the linux FreeRTOS port can preempt a task inside
 * printf and deadlock the next printer on the libc stdout lock (sim artifact). */
static atomic_uint s_bad_writes;
static atomic_uint s_writes;
esp_err_t __real_esp_partition_write(const esp_partition_t* p, size_t off, const void* src, size_t len);
esp_err_t __wrap_esp_partition_write(const esp_partition_t* p, size_t off, const void* src, size_t len) {
  esp_err_t r = __real_esp_partition_write(p, off, src, len);
  atomic_fetch_add(&s_writes, 1);
  if (r == ESP_ERR_FLASH_OP_FAIL) atomic_fetch_add(&s_bad_writes, 1);
  return r;
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

static void hb_task(void* arg) {
  (void)arg;
  uint64_t ts = 1700000000000ULL;
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
    ts += 1000;
    timeseries_insert_data_t d = {.measurement_name = "node_stats",
                                  .field_names = hb_names,
                                  .field_values = fv,
                                  .num_fields = HB_FIELDS,
                                  .timestamps_ms = &ts,
                                  .num_points = 1};
    timeseries_insert(&d);
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

void app_main(void) {
  const char* e = getenv("CHURN_SEED");
  if (e) s_seed = (unsigned)strtoul(e, NULL, 0);
  if (!s_seed) s_seed = 1;
  int secs = 60;
  e = getenv("CHURN_SECONDS");
  if (e) secs = atoi(e);
  if (!getenv("CHURN_LOG")) esp_log_level_set("*", ESP_LOG_NONE);
  for (int i = 0; i < HB_FIELDS; i++) {
    snprintf(hb_name_buf[i], sizeof hb_name_buf[i], "stat_%02d", i);
    hb_names[i] = hb_name_buf[i];
  }
  if (!timeseries_init()) {
    printf("FAIL timeseries_init\n");
    exit(2);
  }
  timeseries_clear_all();
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
