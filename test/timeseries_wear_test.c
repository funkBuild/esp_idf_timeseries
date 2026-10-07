/**
 * @file timeseries_wear_test.c
 * @brief Flash wear: erases are spread over the partition
 *
 * A logger that keeps inserting and expiring cycles L0 pages through the
 * allocator. The erase hook counts erases per 4 KB sector.
 */

#include "timeseries.h"
#include "timeseries_internal.h"
#include "unity.h"
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern timeseries_db_t *timeseries_get_db_handle(void);

static uint16_t *s_sector_erases;
static size_t s_sectors;

static void count_erase(uint32_t offset, uint32_t size) {
  for (uint32_t s = offset / 4096; s < (offset + size) / 4096 && s < s_sectors; s++) {
    if (s_sector_erases[s] < UINT16_MAX) s_sector_erases[s]++;
  }
}

typedef struct {
  uint32_t total, touched, max, insert_failures;
  float mean;
} wear_result_t;

// A monitor writing 6 fields at 10 s intervals, flushed every 10 minutes,
// holding usage under a retention ceiling
static wear_result_t run_logger(bool zoned, int rounds, float ceiling) {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  uint32_t zone_start = db->alloc_zone_start;
  if (!zoned) db->alloc_zone_start = db->partition->size;  // plain first fit
  s_sectors = db->partition->size / 4096;
  s_sector_erases = calloc(s_sectors, sizeof(*s_sector_erases));
  TEST_ASSERT_NOT_NULL(s_sector_erases);
  timeseries_set_erase_hook(count_erase);

  enum { FIELDS = 6, POINTS = 60 };
  const char *fields[FIELDS] = {"te_mean", "te_rms", "freq", "steer", "temp", "noise"};
  timeseries_field_value_t *vals = calloc(FIELDS * POINTS, sizeof(*vals));
  uint64_t *ts = calloc(POINTS, sizeof(*ts));
  TEST_ASSERT_NOT_NULL(vals);
  TEST_ASSERT_NOT_NULL(ts);
  uint64_t t = 1790000000000ULL;
  uint32_t seed = 1;
  wear_result_t res = {0};
  for (int r = 0; r < rounds; r++) {
    for (int p = 0; p < POINTS; p++) {
      ts[p] = t;
      t += 10000;
      for (int f = 0; f < FIELDS; f++) {
        seed = seed * 1103515245u + 12345u;
        timeseries_field_value_t *v = &vals[f * POINTS + p];
        v->type = TIMESERIES_FIELD_TYPE_FLOAT;
        v->data.float_val = 100.0 * sin((double)(r * POINTS + p) / 300.0 + f) + (double)(seed >> 16) / 6553.6;
      }
    }
    timeseries_insert_data_t d = {
        .measurement_name = "clk",
        .field_names = fields,
        .field_values = vals,
        .num_fields = FIELDS,
        .timestamps_ms = ts,
        .num_points = POINTS,
    };
    if (!timeseries_insert(&d)) res.insert_failures++;
    // A logger writes a batch a minute: compaction keeps up between batches.
    // Back-to-back inserts here would outrun the background compaction.
    if (r % 10 == 9) {
      timeseries_compact_sync();
      timeseries_expire_ex(ceiling, 0.05f);
    }
  }
  timeseries_compact_sync();
  timeseries_set_erase_hook(NULL);

  for (size_t s = 0; s < s_sectors; s++) {
    res.total += s_sector_erases[s];
    if (s_sector_erases[s]) res.touched++;
    if (s_sector_erases[s] > res.max) res.max = s_sector_erases[s];
  }
  res.mean = (float)res.total / (float)s_sectors;
  // Erase-count histogram over 16 equal slices of the partition
  printf("wear[%s]: %" PRIu32 " erases over %u sectors, %" PRIu32 " touched, max %" PRIu32
         ", mean %.1f, insert failures %" PRIu32 "\nwear[%s] slices:",
         zoned ? "zoned" : "first-fit", res.total, (unsigned)s_sectors,
         res.touched, res.max, res.mean, res.insert_failures,
         zoned ? "zoned" : "first-fit");
  for (int i = 0; i < 16; i++) {
    uint32_t m = 0;
    for (size_t s = i * s_sectors / 16; s < (i + 1) * s_sectors / 16; s++) {
      if (s_sector_erases[s] > m) m = s_sector_erases[s];
    }
    printf(" %" PRIu32, m);
  }
  printf("\n");

  free(vals);
  free(ts);
  free(s_sector_erases);
  s_sector_erases = NULL;
  db->alloc_zone_start = zone_start;
  TEST_ASSERT_TRUE(timeseries_clear_all());
  return res;
}

// Reference figures for plain first fit (zone disabled); not run by default
TEST_CASE("wear: first-fit baseline", "[wear_baseline]") {
  wear_result_t r = run_logger(false, 1500, 0.4f);
  TEST_ASSERT_EQUAL_UINT32(0, r.insert_failures);
}

TEST_CASE("wear: recycled pages rotate through the zone", "[wear]") {
  wear_result_t r = run_logger(true, 1500, 0.4f);
  TEST_ASSERT_EQUAL_UINT32(0, r.insert_failures);
  // Every sector takes a share, and none wears far faster than the average
  // (plain first fit on this workload: busiest sector 188 erases, 4.5x the mean)
  TEST_ASSERT_GREATER_OR_EQUAL_UINT32(s_sectors * 9 / 10, r.touched);
  TEST_ASSERT_LESS_OR_EQUAL_UINT32((uint32_t)(4.0f * r.mean) + 2, r.max);
}
