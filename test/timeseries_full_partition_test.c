#include "unity.h"
#include "timeseries.h"
#include "timeseries_compaction.h"
#include "timeseries_data.h"
#include "timeseries_expiration.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

// A full partition (timeseries_data.h, free space and reserves): reserves for
// maintenance, dropping the oldest data, and output pages sized exactly.

extern timeseries_db_t *timeseries_get_db_handle(void);

#define FULL_POINTS 1000
#define FULL_FIELDS 4
#define FULL_STEP_MS 1000
#define FULL_SPACE (256 * 1024)  // usable space the full-partition tests leave

static const char *full_fields[FULL_FIELDS] = {"a", "b", "c", "d"};

static void wait_for_maintenance(timeseries_db_t *db) {
  for (int i = 0; i < 3000 && atomic_load(&db->compaction_in_progress); i++) vTaskDelay(pdMS_TO_TICKS(10));
}

// FULL_POINTS points per field from t0, random doubles (they compress
// poorly, so the partition fills in few inserts).
static bool full_insert(uint64_t t0, uint32_t *seed) {
  static uint64_t ts[FULL_POINTS];
  static timeseries_field_value_t values[FULL_FIELDS * FULL_POINTS];
  for (size_t i = 0; i < FULL_POINTS; i++) ts[i] = t0 + i * FULL_STEP_MS;
  for (size_t v = 0; v < FULL_FIELDS * FULL_POINTS; v++) {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 17;
    *seed ^= *seed << 5;
    values[v].type = TIMESERIES_FIELD_TYPE_FLOAT;
    values[v].data.float_val = (double)*seed / 7.0;
  }
  timeseries_insert_data_t in = {.measurement_name = "full", .field_names = full_fields, .field_values = values,
                                 .num_fields = FULL_FIELDS, .timestamps_ms = ts, .num_points = FULL_POINTS};
  return timeseries_insert(&in);
}

// Fills a 2 MiB partition faster than QEMU can afford: holds blank regions
// (cache placeholders, not data: never dropped or compacted) until about
// `keep` bytes of usable space are left. clear_all() gives them back.
static void hold_all_but(timeseries_db_t *db, uint32_t keep) {
  typeof(db->space) space = db->space;
  memset(&db->space, 0, sizeof(db->space));
  uint32_t o, sz;
  while (tsdb_usable_free(db) > keep + 64 * 1024 && tsdb_reserve_blank_region(db, 64 * 1024, NULL, &o, &sz)) {
  }
  db->space = space;
  tsdb_usable_free(db);
}

static size_t full_count(uint64_t start, uint64_t end) {
  const char *field = "a";
  timeseries_query_t q = {.measurement_name = "full", .field_names = &field, .num_fields = 1,
                          .start_ms = (int64_t)start, .end_ms = (int64_t)end};
  timeseries_query_result_t r = {0};
  size_t n = timeseries_query(&q, &r) ? r.num_points : 0;
  timeseries_query_free_result(&r);
  return n;
}

TEST_CASE("full: a full partition keeps recording by dropping its oldest data", "[full][full_drop]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  TEST_ASSERT_TRUE(db->space.drop_oldest);
  hold_all_but(db, FULL_SPACE);
  uint32_t seed = 12345;
  const uint64_t t0 = 1000000;
  // About 36 KB per insert once compacted: ~8 inserts fill the space left,
  // so this runs at full for some 16 more.
  const int inserts = 24;
  int failed = 0;
  uint64_t t = t0;
  for (int i = 0; i < inserts; i++, t += FULL_POINTS * FULL_STEP_MS)
    if (!full_insert(t, &seed)) failed++;
  wait_for_maintenance(db);
  uint64_t last = t - FULL_POINTS * FULL_STEP_MS;
  size_t newest = full_count(last, last + (FULL_POINTS - 1) * FULL_STEP_MS);
  size_t oldest = full_count(t0, t0 + (FULL_POINTS - 1) * FULL_STEP_MS);
  uint32_t used = tsdb_pagecache_get_total_active_size(db);
  printf("full_drop: %d inserts, %d failed; newest insert stored %zu of %d points, oldest %zu; %u KiB in use\n",
         inserts, failed, newest, FULL_POINTS, oldest, (unsigned)(used / 1024));
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_deinit();
  TEST_ASSERT_EQUAL(0, failed);
  TEST_ASSERT_EQUAL(FULL_POINTS, newest);
  TEST_ASSERT_EQUAL(0, oldest);
}

TEST_CASE("full: with drop_oldest off a full partition stops, and expiration recovers it", "[full][full_stop]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  typeof(db->space) space = db->space;
  db->space.drop_oldest = false;
  db->space.clear_free = 0;
  db->space.max_usage_percent = 0;
  hold_all_but(db, FULL_SPACE);
  uint32_t seed = 777;
  uint64_t t = 1000000;
  int stored = 0;
  bool filled = false;
  for (int i = 0; i < 100 && !filled; i++, t += FULL_POINTS * FULL_STEP_MS) {
    filled = !full_insert(t, &seed);
    stored += !filled;
  }
  wait_for_maintenance(db);
  uint32_t usable = tsdb_usable_free(db);
  bool expired = timeseries_expiration_run(db, 0.01f, 0.10f);
  wait_for_maintenance(db);
  bool again = full_insert(t, &seed);
  wait_for_maintenance(db);
  printf("full_stop: full after %d inserts (filled=%d), usable free %u KiB; expiration -> %d; insert after -> %d\n",
         stored, filled, (unsigned)(usable / 1024), expired, again);
  db->space = space;
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_deinit();
  TEST_ASSERT_TRUE(filled);
  TEST_ASSERT_TRUE(usable >= space.maintenance_reserve);  // left for the expiration
  TEST_ASSERT_TRUE(expired);
  TEST_ASSERT_TRUE(again);
}

TEST_CASE("full: dropping a page never brings back an overwritten value", "[full][full_drop_revision]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  const char *field = "v";
  static uint64_t ts[1000];
  static timeseries_field_value_t v[1000];
  for (int k = 0; k < 1000; k++) {
    ts[k] = 1000000 + (uint64_t)k * 1000;
    v[k] = (timeseries_field_value_t){.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 1.0};
  }
  timeseries_insert_data_t in = {.measurement_name = "rev", .field_names = &field, .field_values = v,
                                 .num_fields = 1, .timestamps_ms = ts, .num_points = 1000};
  TEST_ASSERT_TRUE(timeseries_insert(&in));
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());
  // A newer revision of point 500, in an L0 page whose newest point is older
  // than the compacted page's: that L0 page is the one dropped.
  timeseries_field_value_t newer = {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 2.0};
  timeseries_insert_data_t over = {.measurement_name = "rev", .field_names = &field, .field_values = &newer,
                                   .num_fields = 1, .timestamps_ms = &ts[500], .num_points = 1};
  TEST_ASSERT_TRUE(timeseries_insert(&over));
  wait_for_maintenance(db);
  bool expected = false, dropped = false;
  TEST_ASSERT_TRUE(atomic_compare_exchange_strong(&db->compaction_in_progress, &expected, true));
  bool ok = tsdb_drop_oldest_page_held(db, &dropped);
  atomic_store(&db->compaction_in_progress, false);
  timeseries_query_t q = {.measurement_name = "rev", .field_names = &field, .num_fields = 1};
  timeseries_query_result_t r = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  int stale = 0;
  for (size_t p = 0; p < r.num_points; p++)
    if (r.timestamps[p] == ts[500] && r.columns[0].values[p].data.float_val == 1.0) stale++;
  size_t left = r.num_points;
  timeseries_query_free_result(&r);
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_deinit();
  TEST_ASSERT_TRUE(ok);
  TEST_ASSERT_TRUE(dropped);
  TEST_ASSERT_EQUAL(0, stale);
  TEST_ASSERT_TRUE(left < 1000);  // the overlapping compacted record went too
}

// The end of the last record in a field-data page.
static uint32_t records_end(timeseries_db_t *db, uint32_t offset, uint32_t size) {
  timeseries_fielddata_iterator_t it;
  timeseries_field_data_header_t h;
  uint32_t end = sizeof(timeseries_page_header_t);
  TEST_ASSERT_TRUE(timeseries_fielddata_iterator_init(db, offset, size, &it));
  while (timeseries_fielddata_iterator_next(&it, &h)) {
    uint32_t e = it.current_record_offset + sizeof(h) + h.record_length;
    if (e > end) end = e;
  }
  timeseries_fielddata_iterator_deinit(&it);
  return end;
}

// Counts the ACTIVE field-data pages above level 0 and their largest size;
// with exact_sizes_only, asserts each is sized to its records.
static size_t compacted_pages(timeseries_db_t *db, uint32_t *max_size, bool exact_sizes_only) {
  timeseries_page_cache_iterator_t it;
  timeseries_page_header_t h;
  uint32_t off, size;
  size_t n = 0;
  *max_size = 0;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &it));
  while (timeseries_page_cache_iterator_next(&it, &h, &off, &size)) {
    if (h.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA || h.page_state != TIMESERIES_PAGE_STATE_ACTIVE ||
        !h.field_data_level)
      continue;
    n++;
    if (size > *max_size) *max_size = size;
    if (exact_sizes_only) TEST_ASSERT_EQUAL_UINT32((records_end(db, off, size) + 4095) & ~4095u, size);
  }
  timeseries_page_cache_iterator_deinit(&it);
  return n;
}

TEST_CASE("full: compaction output pages are sized exactly to their records", "[full][full_exact]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  uint32_t seed = 4242;
  for (int i = 0; i < 6; i++) TEST_ASSERT_TRUE(full_insert(1000000 + (uint64_t)i * FULL_POINTS * FULL_STEP_MS, &seed));
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());
  uint32_t largest = 0;
  size_t pages = compacted_pages(db, &largest, true);
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_deinit();
  TEST_ASSERT_TRUE(pages > 0);
}

TEST_CASE("full: a compaction output splits between records when free space is in small holes",
          "[full][full_split]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  typeof(db->space) space = db->space;
  memset(&db->space, 0, sizeof(db->space));  // the scene takes free space by hand
  atomic_store(&db->compaction_in_progress, true);  // no background compaction
  uint32_t seed = 99;
  for (int i = 0; i < 3; i++) TEST_ASSERT_TRUE(full_insert(1000000 + (uint64_t)i * FULL_POINTS * FULL_STEP_MS, &seed));
  // Take every free sector, then give back 16 KiB holes (12 KiB apart):
  // plenty of space in total, none of it in a run of more than 16 KiB.
  static uint32_t held[1024];
  size_t n = 0;
  uint32_t o, sz;
  while (n < 1024 && tsdb_reserve_blank_region(db, 4096, NULL, &o, &sz)) held[n++] = o;
  size_t freed = 0;
  for (size_t i = 0; i + 7 < n && freed < 48; i += 7) {
    if (held[i + 3] != held[i] + 3 * 4096) continue;  // four adjacent sectors
    for (size_t k = 0; k < 4; k++, freed++) {
      tsdb_release_reserved_region(db, held[i + k], NULL);
      held[i + k] = UINT32_MAX;
    }
  }
  // One L0 -> L1 run: later levels would also use the space it frees.
  bool compacted = timeseries_compact_level0_pages(db);
  atomic_store(&db->compaction_in_progress, false);
  uint32_t largest = 0;
  size_t pages = compacted_pages(db, &largest, false);
  for (size_t i = 0; i < n; i++)
    if (held[i] != UINT32_MAX) tsdb_release_reserved_region(db, held[i], NULL);
  printf("full_split: %zu sectors held, %zu freed in 16 KiB holes; compaction -> %d, %zu output pages, largest %u\n",
         n, freed, compacted, pages, (unsigned)largest);
  db->space = space;
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_deinit();
  TEST_ASSERT_TRUE(freed >= 32);
  TEST_ASSERT_TRUE(compacted);
  TEST_ASSERT_TRUE(pages > 1);
  TEST_ASSERT_TRUE(largest <= 16384);
}
