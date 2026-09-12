#include "unity.h"
#include "timeseries_revision.h"
#include "timeseries_data.h"
#include <stdlib.h>
#include <string.h>

TEST_CASE("revision: scalar and mixed full-width revisions round trip", "[eighth][revision_codec]") {
  uint64_t values[257];
  for (size_t i = 0; i < 257; i++) values[i] = i % 3 ? UINT64_MAX - i : i;
  uint8_t *encoded = NULL, flags = 0xff; size_t length = 0;
  TEST_ASSERT_TRUE(tsdb_revision_encode(values, 257, 0, &encoded, &length, &flags));
  tsdb_revision_t revision;
  bool decoded = tsdb_revision_decode(flags, encoded, length, 257, &revision);
  bool equal = decoded && revision.values && !memcmp(values, revision.values, sizeof(values));
  free(revision.values);
  free(encoded);
  TEST_ASSERT_TRUE(equal);
  // Reusing the flags for a scalar must restore the vector discriminator.
  TEST_ASSERT_TRUE(tsdb_revision_encode(NULL, 0, UINT64_MAX, &encoded, &length, &flags));
  decoded = tsdb_revision_decode(flags, encoded, length, 1, &revision);
  equal = decoded && !revision.values && revision.value == UINT64_MAX;
  free(encoded); free(revision.values);
  TEST_ASSERT_TRUE(equal); TEST_ASSERT_EQUAL(sizeof(uint64_t), length);
}
TEST_CASE("revision: malformed trailers and inconsistent maxima are rejected", "[eighth][revision_codec]") {
  uint64_t values[] = {3, UINT64_MAX, 1};
  uint8_t *encoded = NULL, flags = 0xff; size_t length = 0;
  TEST_ASSERT_TRUE(tsdb_revision_encode(values, 3, 0, &encoded, &length, &flags));
  tsdb_revision_t revision;
  bool mismatched_count = tsdb_revision_decode(flags, encoded, length, 2, &revision);
  free(revision.values);
  bool truncated = tsdb_revision_decode(flags, encoded, 7, 3, &revision);
  free(revision.values);
  memset(encoded + length - sizeof(uint64_t), 0, sizeof(uint64_t));
  bool incorrect_maximum = tsdb_revision_decode(flags, encoded, length, 3, &revision);
  free(revision.values); free(encoded);
  TEST_ASSERT_FALSE(mismatched_count);
  TEST_ASSERT_FALSE(truncated); TEST_ASSERT_FALSE(incorrect_maximum);
  TEST_ASSERT_TRUE(tsdb_revision_decode(0xff, NULL, 0, 0, &revision));
  TEST_ASSERT_EQUAL_UINT64(0, revision.value);
  TEST_ASSERT_NULL(revision.values);
  TEST_ASSERT_FALSE(tsdb_revision_decode(0xf7, NULL, 8, 1, &revision));
  TEST_ASSERT_FALSE(tsdb_revision_encode(values, SIZE_MAX, 0, &encoded, &length, &flags));
  TEST_ASSERT_NULL(encoded);
}
#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_fail_malloc(size_t);
TEST_CASE("revision: decode allocation failure releases temporary storage", "[eighth][revision_fault]") {
  uint64_t values[] = {1, 2, 1};
  uint8_t *encoded = NULL, flags = 0xff; size_t length = 0;
  TEST_ASSERT_TRUE(tsdb_revision_encode(values, 3, 0, &encoded, &length, &flags));
  tsdb_revision_t revision;
  tsdb_audit_fail_malloc(sizeof(values));
  bool decoded = tsdb_revision_decode(flags, encoded, length, 3, &revision);
  tsdb_audit_fail_malloc(0);
  free(encoded); free(revision.values);
  TEST_ASSERT_FALSE(decoded);
}
#endif
extern timeseries_db_t *timeseries_get_db_handle(void);
TEST_CASE("revision: exhausted counter fails inserts without wrapping", "[eighth][revision_counter]") {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  uint64_t ts = 1;
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_INT, .data.int_val = 1};
  const char *field = "v";
  timeseries_insert_data_t insert = {.measurement_name = "revision", .field_names = &field,
      .field_values = &value, .num_fields = 1, .timestamps_ms = &ts, .num_points = 1};
  TEST_ASSERT_TRUE(timeseries_insert(&insert));
  db->write_revision = UINT64_MAX;
  ts = 2;
  bool inserted = timeseries_insert(&insert);
  bool unwrapped = db->write_revision == UINT64_MAX;
  timeseries_query_t q = {.measurement_name = "revision"};
  timeseries_query_result_t r = {0};
  bool matched = timeseries_query(&q, &r) && r.num_points == 1 && r.timestamps[0] == 1;
  timeseries_query_free_result(&r);
  bool cleared = timeseries_clear_all();
  bool restarted = timeseries_insert(&insert) && db->write_revision == 1;
  timeseries_deinit();
  TEST_ASSERT_FALSE(inserted); TEST_ASSERT_TRUE(unwrapped); TEST_ASSERT_TRUE(matched);
  TEST_ASSERT_TRUE(cleared); TEST_ASSERT_TRUE(restarted);
}
