/**
 * @file timeseries_error_path_test.c
 * @brief Tests for error paths: double-init, double-deinit, operations on
 *        cleared DB, insert validation, and edge case robustness.
 */

#include "timeseries.h"
#include "timeseries_internal.h"
#include "esp_partition.h"
#include "esp_heap_caps.h"
#include "unity.h"
#include <inttypes.h>
#include <string.h>

#include "esp_log.h"

static const char *TAG = "error_path_test";

// Helper to get the internal DB context (declared in timeseries.c)
extern timeseries_db_t *timeseries_get_db_handle(void);

static bool s_db_initialized = false;

static void ensure_init(void) {
  if (!s_db_initialized) {
    TEST_ASSERT_TRUE(timeseries_init());
    s_db_initialized = true;
  }
}

static void ensure_clean(void) {
  ensure_init();
  TEST_ASSERT_TRUE(timeseries_clear_all());
}

// ============================================================================
// Lifecycle error paths
// ============================================================================

TEST_CASE("error: double init does not crash", "[error][lifecycle]") {
  ensure_init();
  // Second init should either succeed or fail gracefully, not crash
  bool ok = timeseries_init();
  (void)ok; // we don't care about the return, just that it doesn't crash
  s_db_initialized = true;
}

TEST_CASE("error: deinit then operations do not crash",
          "[error][lifecycle]") {
  ensure_init();
  timeseries_deinit();
  s_db_initialized = false;

  // Operations after deinit should not crash (return value is undefined)
  uint64_t ts = 1000;
  timeseries_field_value_t val = {.type = TIMESERIES_FIELD_TYPE_INT,
                                  .data.int_val = 42};
  const char *fields[] = {"temp"};
  timeseries_insert_data_t data = {
      .measurement_name = "test",
      .field_names = fields,
      .field_values = &val,
      .num_fields = 1,
      .timestamps_ms = &ts,
      .num_points = 1,
  };
  timeseries_insert(&data); // may succeed or fail, must not crash

  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = "test";
  q.start_ms = 0;
  q.end_ms = INT64_MAX;
  timeseries_query_result_t r = {0};
  bool query_ok = timeseries_query(&q, &r);
  if (query_ok) {
    timeseries_query_free_result(&r);
  }

  // Re-init for other tests
  TEST_ASSERT_TRUE(timeseries_init());
  s_db_initialized = true;
}

// ============================================================================
// Insert validation edge cases
// ============================================================================

TEST_CASE("error: insert NULL data pointer", "[error][insert]") {
  ensure_init();
  TEST_ASSERT_FALSE(timeseries_insert(NULL));
}

TEST_CASE("error: insert with 0 points", "[error][insert]") {
  ensure_clean();

  uint64_t ts = 1000;
  timeseries_field_value_t val = {.type = TIMESERIES_FIELD_TYPE_INT,
                                  .data.int_val = 42};
  const char *fields[] = {"v"};
  timeseries_insert_data_t data = {
      .measurement_name = "test",
      .field_names = fields,
      .field_values = &val,
      .num_fields = 1,
      .timestamps_ms = &ts,
      .num_points = 0, // zero points
  };
  // Should either succeed as no-op or return false, not crash
  timeseries_insert(&data);
}

TEST_CASE("error: insert with 0 fields", "[error][insert]") {
  ensure_clean();

  uint64_t ts = 1000;
  timeseries_insert_data_t data = {
      .measurement_name = "test",
      .field_names = NULL,
      .field_values = NULL,
      .num_fields = 0, // zero fields
      .timestamps_ms = &ts,
      .num_points = 1,
  };
  // Should either succeed as no-op or return false, not crash
  timeseries_insert(&data);
}

TEST_CASE("error: insert with NULL measurement name", "[error][insert]") {
  ensure_clean();

  uint64_t ts = 1000;
  timeseries_field_value_t val = {.type = TIMESERIES_FIELD_TYPE_INT,
                                  .data.int_val = 42};
  const char *fields[] = {"v"};
  timeseries_insert_data_t data = {
      .measurement_name = NULL,
      .field_names = fields,
      .field_values = &val,
      .num_fields = 1,
      .timestamps_ms = &ts,
      .num_points = 1,
  };
  TEST_ASSERT_FALSE(timeseries_insert(&data));
}

TEST_CASE("error: insert with empty measurement name", "[error][insert]") {
  ensure_clean();

  uint64_t ts = 1000;
  timeseries_field_value_t val = {.type = TIMESERIES_FIELD_TYPE_INT,
                                  .data.int_val = 42};
  const char *fields[] = {"v"};
  timeseries_insert_data_t data = {
      .measurement_name = "",
      .field_names = fields,
      .field_values = &val,
      .num_fields = 1,
      .timestamps_ms = &ts,
      .num_points = 1,
  };
  // Should either succeed or fail, not crash
  timeseries_insert(&data);
}

// ============================================================================
// Clear then query
// ============================================================================

TEST_CASE("error: query after clear_all returns empty", "[error][query]") {
  ensure_clean();

  // Insert data
  uint64_t ts[] = {1000, 2000, 3000};
  timeseries_field_value_t vals[3];
  for (int i = 0; i < 3; i++) {
    vals[i].type = TIMESERIES_FIELD_TYPE_INT;
    vals[i].data.int_val = i * 10;
  }
  const char *fields[] = {"v"};
  timeseries_insert_data_t data = {
      .measurement_name = "clear_test",
      .field_names = fields,
      .field_values = vals,
      .num_fields = 1,
      .timestamps_ms = ts,
      .num_points = 3,
  };
  TEST_ASSERT_TRUE(timeseries_insert(&data));

  // Clear
  TEST_ASSERT_TRUE(timeseries_clear_all());

  // Query should return empty
  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = "clear_test";
  q.start_ms = 0;
  q.end_ms = INT64_MAX;
  timeseries_query_result_t r = {0};
  bool ok = timeseries_query(&q, &r);
  // Either returns false (measurement gone) or ok with 0 points
  if (ok) {
    TEST_ASSERT_EQUAL(0, r.num_points);
    timeseries_query_free_result(&r);
  }
}

// ============================================================================
// Compact on empty DB
// ============================================================================

TEST_CASE("error: compact on empty db does not crash", "[error][compact]") {
  ensure_clean();

  // Compact with no data — should succeed or no-op
  bool ok = timeseries_compact_sync();
  (void)ok;
}

TEST_CASE("error: force compact on empty db does not crash",
          "[error][compact]") {
  ensure_clean();

  bool ok = timeseries_compact_force_sync();
  (void)ok;
}

// ============================================================================
// Free result edge cases
// ============================================================================

TEST_CASE("error: free_result on zeroed result", "[error][memory]") {
  timeseries_query_result_t r;
  memset(&r, 0, sizeof(r));
  // Should not crash
  timeseries_query_free_result(&r);
}

TEST_CASE("error: free_result NULL pointer", "[error][memory]") {
  // Should not crash
  timeseries_query_free_result(NULL);
}

// ============================================================================
// Metadata on empty DB
// ============================================================================

TEST_CASE("error: get_measurements on empty db", "[error][metadata]") {
  ensure_clean();

  char **measurements = NULL;
  size_t count = 0;
  bool ok = timeseries_get_measurements(&measurements, &count);
  if (ok) {
    TEST_ASSERT_EQUAL(0, count);
    free(measurements);
  }
}

TEST_CASE("error: get_fields for nonexistent measurement",
          "[error][metadata]") {
  ensure_clean();

  char **fields = NULL;
  size_t count = 0;
  bool ok =
      timeseries_get_fields_for_measurement("nonexistent", &fields, &count);
  // Should either return false or ok with 0 fields
  if (ok) {
    TEST_ASSERT_EQUAL(0, count);
    free(fields);
  }
}

TEST_CASE("error: get_tags for nonexistent measurement",
          "[error][metadata]") {
  ensure_clean();

  tsdb_tag_pair_t *tags = NULL;
  size_t count = 0;
  bool ok = timeseries_get_tags_for_measurement("nonexistent", &tags, &count);
  if (ok) {
    TEST_ASSERT_EQUAL(0, count);
    free(tags);
  }
}

// ============================================================================
// Expire on empty DB
// ============================================================================

TEST_CASE("error: expire on empty db does not crash", "[error][expire]") {
  ensure_clean();

  bool ok = timeseries_expire();
  (void)ok;
}

// ============================================================================
// Flash partition full
// ============================================================================

TEST_CASE("error: partition full graceful handling", "[error][full]") {
  ensure_clean();

  // Strategy: Insert in rounds of 500 points, compacting after each round.
  // This keeps L0 page count low (avoiding OOM during compaction) and fills
  // the partition with compacted L1/L2 data.  Eventually there will be no
  // blank region left for a new L0 page and insert will return false.
  const size_t BATCH_SIZE = 100;
  const size_t PTS_PER_ROUND = 500;
  const size_t BATCHES_PER_ROUND = PTS_PER_ROUND / BATCH_SIZE;

  uint64_t *timestamps = malloc(BATCH_SIZE * sizeof(uint64_t));
  timeseries_field_value_t *values =
      malloc(BATCH_SIZE * sizeof(timeseries_field_value_t));
  TEST_ASSERT_NOT_NULL(timestamps);
  TEST_ASSERT_NOT_NULL(values);

  const char *field_names[] = {"temp"};
  size_t total_inserted = 0;
  bool partition_full = false;

  // --- Step 1: Fill the partition with insert+compact cycles ---
  while (!partition_full) {
    for (size_t b = 0; b < BATCHES_PER_ROUND; b++) {
      for (size_t i = 0; i < BATCH_SIZE; i++) {
        timestamps[i] = (total_inserted + i) * 1000ULL;
        values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        values[i].data.float_val = (double)(total_inserted + i) * 0.5;
      }

      timeseries_insert_data_t data = {
          .measurement_name = "fill_test",
          .tag_keys = NULL,
          .tag_values = NULL,
          .num_tags = 0,
          .field_names = field_names,
          .field_values = values,
          .num_fields = 1,
          .timestamps_ms = timestamps,
          .num_points = BATCH_SIZE,
      };

      bool insert_ok = timeseries_insert(&data);
      if (!insert_ok) {
        partition_full = true;
        break;
      }
      total_inserted += BATCH_SIZE;
    }

    // Compact after each round to keep L0 page count manageable
    if (!partition_full) {
      timeseries_compact_sync();
    }
  }

  free(timestamps);
  free(values);

  ESP_LOGI(TAG, "Partition full after %" PRIu32 " points inserted",
           (uint32_t)total_inserted);
  TEST_ASSERT_TRUE_MESSAGE(total_inserted > 0,
                           "Should have inserted some data before full");

  // --- Step 2: Verify insert returned false (not a crash or hang) ---
  // We reached this point, so the insert returned false gracefully.

  // --- Step 3: Verify previously inserted data is still queryable ---
  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = "fill_test";
  q.start_ms = 0;
  q.end_ms = INT64_MAX;
  q.limit = 100; // Query a small subset to keep memory usage low
  timeseries_query_result_t r = {0};
  bool query_ok = timeseries_query(&q, &r);
  TEST_ASSERT_TRUE_MESSAGE(query_ok,
                           "DB should still be queryable after partition full");
  TEST_ASSERT_TRUE_MESSAGE(r.num_points > 0,
                           "Query should return data after partition full");
  timeseries_query_free_result(&r);

  // --- Step 4: Verify compaction does not crash (may fail when truly full) ---
  // When the flash is 100% packed, compaction needs blank space for output
  // pages and may legitimately fail.  The key assertion is that it does not
  // crash or hang.
  timeseries_compact_sync(); // return value intentionally ignored

  // --- Step 5: After clearing, verify insert succeeds again ---
  TEST_ASSERT_TRUE(timeseries_clear_all());

  uint64_t new_ts = 1000ULL;
  timeseries_field_value_t new_val = {.type = TIMESERIES_FIELD_TYPE_FLOAT,
                                      .data.float_val = 99.9};
  timeseries_insert_data_t new_data = {
      .measurement_name = "fill_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = &new_val,
      .num_fields = 1,
      .timestamps_ms = &new_ts,
      .num_points = 1,
  };
  bool post_clear_insert = timeseries_insert(&new_data);
  TEST_ASSERT_TRUE_MESSAGE(post_clear_insert,
                           "Insert should succeed after clear_all frees space");

  // --- Cleanup ---
  TEST_ASSERT_TRUE(timeseries_clear_all());

  ESP_LOGI(TAG, "Partition full test passed: %" PRIu32 " points before full",
           (uint32_t)total_inserted);
}

// ============================================================================
// Corrupted page header recovery
// ============================================================================

TEST_CASE("error: corrupted page headers skipped on init",
          "[error][corrupt]") {
  ensure_clean();

  // --- Step 1: Insert valid data and compact to get L1 pages on flash ---
  const size_t NUM_POINTS = 200;
  uint64_t timestamps[NUM_POINTS];
  timeseries_field_value_t values[NUM_POINTS];
  for (size_t i = 0; i < NUM_POINTS; i++) {
    timestamps[i] = (i + 1) * 1000ULL;
    values[i].type = TIMESERIES_FIELD_TYPE_INT;
    values[i].data.int_val = (int64_t)i * 10;
  }
  const char *field_names[] = {"sensor"};
  timeseries_insert_data_t ins = {
      .measurement_name = "corrupt_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = NUM_POINTS,
  };
  TEST_ASSERT_TRUE(timeseries_insert(&ins));

  // Force-compact so the data lives on flash as L1 pages
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());

  // --- Step 2: Query to get baseline point count ---
  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = "corrupt_test";
  q.start_ms = 0;
  q.end_ms = INT64_MAX;
  timeseries_query_result_t baseline = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &baseline));
  size_t baseline_count = baseline.num_points;
  TEST_ASSERT_TRUE_MESSAGE(baseline_count > 0,
                           "Should have data after insert+compact");
  timeseries_query_free_result(&baseline);

  ESP_LOGI(TAG, "Baseline: %u points", (unsigned)baseline_count);

  // --- Step 3: Deinit to release the DB so we can write raw flash ---
  timeseries_deinit();
  s_db_initialized = false;

  // Get partition handle directly (DB is deinited but we need the partition)
  const esp_partition_t *part =
      esp_partition_find_first(0x40, ESP_PARTITION_SUBTYPE_ANY, NULL);
  TEST_ASSERT_NOT_NULL_MESSAGE(part, "Could not find timeseries partition");

  // Pick two corruption targets near the end of the partition.
  // Each occupies one 4KB sector. We use the last two sectors.
  uint32_t corrupt_offset_1 = part->size - (4096 * 2); // second-to-last sector
  uint32_t corrupt_offset_2 = part->size - (4096 * 1); // last sector

  // Erase the two target sectors so we can write to them
  TEST_ASSERT_EQUAL(ESP_OK,
                    esp_partition_erase_range(part, corrupt_offset_1, 4096));
  TEST_ASSERT_EQUAL(ESP_OK,
                    esp_partition_erase_range(part, corrupt_offset_2, 4096));

  // --- Step 4a: Write garbage magic (no valid magic at all) ---
  // The iterator should skip this with a 4096 sector advance.
  timeseries_page_header_t garbage_hdr;
  memset(&garbage_hdr, 0, sizeof(garbage_hdr));
  garbage_hdr.magic_number = 0xDEADBEEF; // not TIMESERIES_MAGIC_NUM
  garbage_hdr.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
  garbage_hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  garbage_hdr.page_size = 4096;
  TEST_ASSERT_EQUAL(ESP_OK,
                    esp_partition_write(part, corrupt_offset_1, &garbage_hdr,
                                       sizeof(garbage_hdr)));

  // --- Step 4b: Write valid magic but impossibly large page_size ---
  // The iterator validates page_size: it must be >= sizeof(header) and
  // offset + page_size must not exceed partition size.
  // We set page_size to something that would extend beyond the partition.
  timeseries_page_header_t oversized_hdr;
  memset(&oversized_hdr, 0, sizeof(oversized_hdr));
  oversized_hdr.magic_number = TIMESERIES_MAGIC_NUM;
  oversized_hdr.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
  oversized_hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  oversized_hdr.page_size = part->size; // way too large from this offset
  oversized_hdr.sequence_num = 99999;
  TEST_ASSERT_EQUAL(ESP_OK,
                    esp_partition_write(part, corrupt_offset_2, &oversized_hdr,
                                       sizeof(oversized_hdr)));

  ESP_LOGI(TAG, "Wrote corrupted headers at offsets 0x%08" PRIx32
                " and 0x%08" PRIx32,
           corrupt_offset_1, corrupt_offset_2);

  // --- Step 5: Re-initialize (triggers page cache rebuild from flash scan) ---
  TEST_ASSERT_TRUE(timeseries_init());
  s_db_initialized = true;

  // --- Step 6: Query again — valid data should still be returned ---
  timeseries_query_result_t after = {0};
  TEST_ASSERT_TRUE_MESSAGE(timeseries_query(&q, &after),
                           "Query should succeed after re-init with corrupt "
                           "pages on flash");
  ESP_LOGI(TAG, "After re-init: %u points (baseline %u)",
           (unsigned)after.num_points, (unsigned)baseline_count);
  TEST_ASSERT_EQUAL_MESSAGE(
      baseline_count, after.num_points,
      "Point count should match baseline — corrupted pages must be skipped");
  timeseries_query_free_result(&after);

  // --- Step 7: Verify the DB is still fully functional (insert + query) ---
  uint64_t extra_ts = 999999000ULL;
  timeseries_field_value_t extra_val = {.type = TIMESERIES_FIELD_TYPE_INT,
                                        .data.int_val = 42};
  timeseries_insert_data_t extra = {
      .measurement_name = "corrupt_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = &extra_val,
      .num_fields = 1,
      .timestamps_ms = &extra_ts,
      .num_points = 1,
  };
  TEST_ASSERT_TRUE_MESSAGE(timeseries_insert(&extra),
                           "Insert should work after corrupt-page recovery");

  timeseries_query_result_t final_r = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &final_r));
  TEST_ASSERT_EQUAL_MESSAGE(
      baseline_count + 1, final_r.num_points,
      "Should have baseline + 1 point after extra insert");
  timeseries_query_free_result(&final_r);

  // --- Step 8: Cleanup ---
  TEST_ASSERT_TRUE(timeseries_clear_all());
  ESP_LOGI(TAG, "Corrupted page header recovery test passed");
}

// ============================================================================
// Out-of-memory graceful degradation
// ============================================================================

TEST_CASE("error: OOM graceful degradation on insert/query/compact",
          "[error][oom]") {
  ensure_clean();

  // --- Step 1: Insert baseline data (100 points) ---
  const size_t BASELINE_POINTS = 100;
  uint64_t timestamps[BASELINE_POINTS];
  timeseries_field_value_t values[BASELINE_POINTS];
  for (size_t i = 0; i < BASELINE_POINTS; i++) {
    timestamps[i] = (i + 1) * 1000ULL;
    values[i].type = TIMESERIES_FIELD_TYPE_INT;
    values[i].data.int_val = (int64_t)i * 10;
  }
  const char *field_names[] = {"sensor"};
  timeseries_insert_data_t ins = {
      .measurement_name = "oom_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = values,
      .num_fields = 1,
      .timestamps_ms = timestamps,
      .num_points = BASELINE_POINTS,
  };
  TEST_ASSERT_TRUE(timeseries_insert(&ins));

  // Compact baseline data so it is safely on flash
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());

  // Verify baseline is queryable
  timeseries_query_t q;
  memset(&q, 0, sizeof(q));
  q.measurement_name = "oom_test";
  q.start_ms = 0;
  q.end_ms = INT64_MAX;

  timeseries_query_result_t baseline_r = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &baseline_r));
  size_t baseline_count = baseline_r.num_points;
  TEST_ASSERT_EQUAL(BASELINE_POINTS, baseline_count);
  timeseries_query_free_result(&baseline_r);

  ESP_LOGI(TAG, "Baseline: %u points inserted and compacted",
           (unsigned)baseline_count);

  // --- Step 2: Exhaust heap, keeping at least 2KB free for FreeRTOS ---
  const size_t CHUNK_SIZE = 1024;
  const size_t MIN_FREE_HEAP = 2048;
  const size_t MAX_CHUNKS = 512; // safety cap

  void **chunks = malloc(MAX_CHUNKS * sizeof(void *));
  TEST_ASSERT_NOT_NULL(chunks);
  size_t num_chunks = 0;

  size_t free_before =
      heap_caps_get_free_size(MALLOC_CAP_DEFAULT);
  ESP_LOGI(TAG, "Free heap before exhaustion: %" PRIu32 " bytes",
           (uint32_t)free_before);

  while (num_chunks < MAX_CHUNKS) {
    size_t free_now = heap_caps_get_free_size(MALLOC_CAP_DEFAULT);
    if (free_now < MIN_FREE_HEAP + CHUNK_SIZE) {
      break;
    }
    void *p = malloc(CHUNK_SIZE);
    if (!p) {
      break;
    }
    chunks[num_chunks++] = p;
  }

  size_t free_after_exhaust =
      heap_caps_get_free_size(MALLOC_CAP_DEFAULT);
  ESP_LOGI(TAG,
           "Allocated %u chunks (%" PRIu32 " bytes). Free heap: %" PRIu32,
           (unsigned)num_chunks, (uint32_t)(num_chunks * CHUNK_SIZE),
           (uint32_t)free_after_exhaust);

  // --- Step 3: Attempt insert under memory pressure (must not crash) ---
  uint64_t oom_ts = 999999000ULL;
  timeseries_field_value_t oom_val = {.type = TIMESERIES_FIELD_TYPE_INT,
                                      .data.int_val = 42};
  timeseries_insert_data_t oom_ins = {
      .measurement_name = "oom_test",
      .tag_keys = NULL,
      .tag_values = NULL,
      .num_tags = 0,
      .field_names = field_names,
      .field_values = &oom_val,
      .num_fields = 1,
      .timestamps_ms = &oom_ts,
      .num_points = 1,
  };
  bool insert_ok = timeseries_insert(&oom_ins);
  ESP_LOGI(TAG, "Insert under OOM: %s", insert_ok ? "ok" : "failed");
  // We don't assert true/false — the key assertion is reaching this line

  // --- Step 4: Attempt query under memory pressure (must not crash) ---
  timeseries_query_result_t oom_r = {0};
  bool query_ok = timeseries_query(&q, &oom_r);
  ESP_LOGI(TAG, "Query under OOM: %s", query_ok ? "ok" : "failed");
  if (query_ok) {
    timeseries_query_free_result(&oom_r);
  }

  // --- Step 5: Attempt compact under memory pressure (must not crash) ---
  bool compact_ok = timeseries_compact_sync();
  ESP_LOGI(TAG, "Compact under OOM: %s", compact_ok ? "ok" : "failed");

  // --- Step 6: Free all chunks (restore heap) ---
  for (size_t i = 0; i < num_chunks; i++) {
    free(chunks[i]);
  }
  free(chunks);

  size_t free_after_restore =
      heap_caps_get_free_size(MALLOC_CAP_DEFAULT);
  ESP_LOGI(TAG, "Free heap after restore: %" PRIu32 " bytes",
           (uint32_t)free_after_restore);

  // --- Step 7: Verify baseline data is still intact ---
  timeseries_query_result_t verify_r = {0};
  TEST_ASSERT_TRUE_MESSAGE(timeseries_query(&q, &verify_r),
                           "Query must succeed after heap is restored");
  TEST_ASSERT_TRUE_MESSAGE(
      verify_r.num_points >= baseline_count,
      "Baseline data must survive OOM — DB should not be corrupted");
  ESP_LOGI(TAG, "Post-OOM query: %u points (baseline was %u)",
           (unsigned)verify_r.num_points, (unsigned)baseline_count);
  timeseries_query_free_result(&verify_r);

  // --- Step 8: Cleanup ---
  TEST_ASSERT_TRUE(timeseries_clear_all());
  ESP_LOGI(TAG, "OOM graceful degradation test passed");
}
