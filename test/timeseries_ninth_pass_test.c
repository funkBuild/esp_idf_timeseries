#include "unity.h"
#include "timeseries_revision.h"
#include "timeseries_data.h"
#include "timeseries_iterator.h"
#include "alp/alp_int_codec.h"
#include "esp_timer.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_allocation_probe(size_t, bool);
size_t tsdb_audit_allocation_count(void);
size_t tsdb_audit_largest_allocation(void);
#endif

TEST_CASE("ninth: long revision runs avoid full codec scratch buffers", "[ninth][ninth_runs]") {
  const size_t count = 1024;
  uint64_t *input = malloc(count*sizeof(*input));
  TEST_ASSERT_NOT_NULL(input);
  for (size_t i = 0; i < count; i++) input[i] = i/500+1;
#if CONFIG_IDF_TARGET_LINUX
  tsdb_audit_allocation_probe(0, true);
#endif
  uint8_t *encoded = NULL, flags = 0xff; size_t length = 0;
  bool ok = tsdb_revision_encode(input, count, 0, &encoded, &length, &flags);
#if CONFIG_IDF_TARGET_LINUX
  size_t allocations = tsdb_audit_allocation_count(), largest = tsdb_audit_largest_allocation();
  tsdb_audit_allocation_probe(0, false);
#endif
  tsdb_revision_t decoded = {0};
  bool matched = ok && tsdb_revision_decode(flags, encoded, length, count, &decoded) &&
                 decoded.values && !memcmp(input, decoded.values, count*sizeof(*input));
  free(decoded.values); free(encoded); free(input);
  TEST_ASSERT_TRUE(matched);
  TEST_ASSERT_LESS_OR_EQUAL(38, length); // three (run length, revision) pairs and maximum
#if CONFIG_IDF_TARGET_LINUX
  TEST_ASSERT_EQUAL(1, allocations);
  TEST_ASSERT_LESS_OR_EQUAL(38, largest);
#endif
}

TEST_CASE("ninth: revision representation is no larger than ALP or runs", "[ninth][ninth_choice]") {
  uint64_t *input = malloc(1025*sizeof(*input));
  TEST_ASSERT_NOT_NULL(input);
  const size_t lengths[] = {2, 3, 7, 8, 31, 64, 127, 128, 129, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025};
  uint64_t random = 12345;
  bool ok = true;
  for (size_t index = 0; index < sizeof(lengths)/sizeof(lengths[0]) && ok; index++) {
    size_t n = lengths[index];
    for (unsigned pattern = 0; pattern < 6 && ok; pattern++) {
      size_t runs = 1;
      uint64_t maximum = 0;
      for (size_t i = 0; i < n; i++) {
        random ^= random << 13; random ^= random >> 7; random ^= random << 17;
        input[i] = pattern == 0 ? i : pattern == 1 ? i/16 : pattern == 2 ? UINT64_MAX-i/32 :
                   pattern == 3 ? (i%2 ? UINT64_MAX : 0) : pattern == 4 ? (i/8)*UINT64_C(123456789123) : random;
        if (input[i] > maximum) maximum = input[i];
        if (i && input[i] != input[i-1]) runs++;
      }
      uint8_t *alp = NULL, *encoded = NULL, flags = 0xff;
      size_t alp_len = alp_encode_int((int64_t *)input, n, &alp), length = 0;
      ok = alp_len && tsdb_revision_encode(input, n, 0, &encoded, &length, &flags);
      tsdb_revision_t decoded = {0};
      ok = ok && length <= alp_len+8 && length <= runs*10+8 &&
           tsdb_revision_decode(flags, encoded, length, n, &decoded) && decoded.value == maximum;
      if (ok) for (size_t i = 0; i < n; i++) ok &= tsdb_revision_at(&decoded, i) == input[i];
      free(alp); free(encoded); free(decoded.values);
    }
  }
  free(input);
  TEST_ASSERT_TRUE(ok);
}

TEST_CASE("ninth: revision codec throughput benchmark", "[ninth][ninth_bench]") {
  const size_t count = 1024;
  uint64_t *input = malloc(count*sizeof(*input)); TEST_ASSERT_NOT_NULL(input);
  for (size_t i = 0; i < count; i++) input[i] = i/500+1;
  bool ok = true; size_t length = 0;
  int64_t start = esp_timer_get_time();
  for (int repeat = 0; repeat < 400 && ok; repeat++) {
    uint8_t *encoded = NULL, flags = 0xff;
    tsdb_revision_t decoded = {0};
    ok = tsdb_revision_encode(input, count, 0, &encoded, &length, &flags) &&
         tsdb_revision_decode(flags, encoded, length, count, &decoded);
    if (ok) for (size_t i = 0; i < count; i++) ok &= tsdb_revision_at(&decoded, i) == input[i];
    free(decoded.values); free(encoded);
  }
  int64_t elapsed = esp_timer_get_time() - start;
  free(input);
  printf("NINTH_CODEC us=%lld bytes=%zu\n", (long long)elapsed, length);
  TEST_ASSERT_TRUE(ok);
}

static void run_pair(uint8_t *data, uint16_t count, uint64_t value) {
  memcpy(data, &count, 2); memcpy(data+2, &value, 8);
}
TEST_CASE("ninth: run decoder rejects malformed counts and maxima", "[ninth][ninth_run_bounds]") {
  uint8_t data[28]; uint64_t maximum = UINT64_MAX;
  run_pair(data, 2, maximum); run_pair(data+10, 2, 1); memcpy(data+20, &maximum, 8);
  tsdb_revision_t decoded = {0};
  bool valid = tsdb_revision_decode(0xc7, data, sizeof(data), 4, &decoded);
  valid = valid && decoded.values && decoded.values[0] == maximum && decoded.values[1] == maximum &&
          decoded.values[2] == 1 && decoded.values[3] == 1;
  free(decoded.values);
  bool rejected = !tsdb_revision_decode(0xc7, data, sizeof(data)-1, 4, &decoded);
  free(decoded.values);
  rejected &= !tsdb_revision_decode(0xc7, data, sizeof(data), 3, &decoded); free(decoded.values);
  run_pair(data, 0, maximum);
  rejected &= !tsdb_revision_decode(0xc7, data, sizeof(data), 4, &decoded); free(decoded.values);
  run_pair(data, UINT16_MAX, maximum);
  rejected &= !tsdb_revision_decode(0xc7, data, sizeof(data), 4, &decoded); free(decoded.values);
  run_pair(data, 2, maximum); run_pair(data+10, 1, 1);
  rejected &= !tsdb_revision_decode(0xc7, data, sizeof(data), 4, &decoded); free(decoded.values);
  run_pair(data+10, 2, 1); memset(data+20, 0, 8);
  rejected &= !tsdb_revision_decode(0xc7, data, sizeof(data), 4, &decoded); free(decoded.values);
  TEST_ASSERT_TRUE(valid); TEST_ASSERT_TRUE(rejected);
}
TEST_CASE("ninth: existing ALP revision records remain readable", "[ninth][ninth_alp_compat]") {
  uint64_t input[129];
  uint64_t maximum = 0;
  for (size_t i = 0; i < 129; i++) { input[i] = UINT64_MAX-i; if (input[i] > maximum) maximum = input[i]; }
  uint8_t *data = NULL;
  size_t length = alp_encode_int((int64_t *)input, 129, &data);
  TEST_ASSERT_GREATER_THAN(0, length);
  uint8_t *trailer = realloc(data, length+8); TEST_ASSERT_NOT_NULL(trailer);
  memcpy(trailer+length, &maximum, 8);
  tsdb_revision_t decoded = {0};
  bool ok = tsdb_revision_decode(0xe7, trailer, length+8, 129, &decoded) &&
            decoded.values && !memcmp(input, decoded.values, sizeof(input));
  free(trailer); free(decoded.values);
  TEST_ASSERT_TRUE(ok);
}

extern timeseries_db_t *timeseries_get_db_handle(void);
#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_reset_reads(void);
size_t tsdb_audit_read_calls(void);
size_t tsdb_audit_read_bytes(void);
#endif
TEST_CASE("ninth: recovery reuses scan windows for small record trailers", "[ninth][ninth_recover]") {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  atomic_store(&db->compaction_in_progress, true);
  size_t old_chunk = db->chunk_size; db->chunk_size = 1;
  uint64_t ts[128]; timeseries_field_value_t *values = calloc(128, sizeof(*values));
  TEST_ASSERT_NOT_NULL(values);
  for (size_t i = 0; i < 128; i++) { ts[i] = i+1; values[i].type = TIMESERIES_FIELD_TYPE_INT; }
  const char *field = "v";
  timeseries_insert_data_t insert = {.measurement_name = "ninth", .field_names = &field,
      .field_values = values, .num_fields = 1, .timestamps_ms = ts, .num_points = 128};
  bool inserted = timeseries_insert(&insert); db->chunk_size = old_chunk; free(values);
  uint64_t expected = db->write_revision; db->write_revision = 0;
#if CONFIG_IDF_TARGET_LINUX
  tsdb_audit_reset_reads();
#endif
  int64_t start = esp_timer_get_time();
  bool recovered = tsdb_revision_recover(db);
  int64_t elapsed = esp_timer_get_time() - start;
#if CONFIG_IDF_TARGET_LINUX
  size_t reads = tsdb_audit_read_calls(), bytes = tsdb_audit_read_bytes();
#endif
  bool matched = db->write_revision == expected;
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE(recovered); TEST_ASSERT_TRUE(matched);
#if CONFIG_IDF_TARGET_LINUX
  printf("NINTH_RECOVER us=%lld reads=%zu bytes=%zu\n", (long long)elapsed, reads, bytes);
  // The 128 record headers, columns and trailers span three 4 KiB windows.
  TEST_ASSERT_LESS_OR_EQUAL(3, reads); TEST_ASSERT_LESS_OR_EQUAL(12288, bytes);
#else
  (void)elapsed;
#endif
}

static timeseries_db_t *large_record_fixture(uint32_t *footer) {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  atomic_store(&db->compaction_in_progress, true);
  size_t old_chunk = db->chunk_size; db->chunk_size = 500;
  uint64_t *ts = malloc(500 * sizeof(*ts));
  timeseries_field_value_t *values = calloc(500, sizeof(*values));
  TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(values);
  for (size_t i = 0; i < 500; i++) { ts[i] = i+1; values[i].type = TIMESERIES_FIELD_TYPE_INT; }
  const char *field = "v";
  timeseries_insert_data_t insert = {.measurement_name = "ninth_large", .field_names = &field,
      .field_values = values, .num_fields = 1, .timestamps_ms = ts, .num_points = 500};
  bool inserted = timeseries_insert(&insert);
  uint32_t page = db->last_l0_page_offset;
  ts[0] = 501; insert.num_points = 1;
  inserted = inserted && timeseries_insert(&insert);
  db->chunk_size = old_chunk; free(ts); free(values);
  TEST_ASSERT_TRUE(inserted); TEST_ASSERT_EQUAL(page, db->last_l0_page_offset);
  timeseries_field_data_header_t header;
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(db->partition, page + sizeof(timeseries_page_header_t),
                                             &header, sizeof(header)));
  TEST_ASSERT_EQUAL(500, header.record_count);
  *footer = sizeof(timeseries_page_header_t) + sizeof(header) + header.record_length - 8;
  TEST_ASSERT_GREATER_THAN(4096, *footer);
  return db;
}

TEST_CASE("ninth: recovery skips large payload and shares next header window", "[ninth][ninth_recover_large]") {
  uint32_t footer; timeseries_db_t *db = large_record_fixture(&footer);
  uint64_t expected = db->write_revision; db->write_revision = 0;
#if CONFIG_IDF_TARGET_LINUX
  tsdb_audit_reset_reads();
#endif
  bool recovered = tsdb_revision_recover(db);
  bool matched = db->write_revision == expected;
#if CONFIG_IDF_TARGET_LINUX
  size_t reads = tsdb_audit_read_calls(), bytes = tsdb_audit_read_bytes();
#endif
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(recovered); TEST_ASSERT_TRUE(matched);
#if CONFIG_IDF_TARGET_LINUX
  TEST_ASSERT_LESS_OR_EQUAL(2, reads); TEST_ASSERT_LESS_OR_EQUAL(8192, bytes);
#endif
}

TEST_CASE("ninth: window reads preserve cursor and reject page boundary overflow", "[ninth][ninth_window]") {
  uint32_t footer; timeseries_db_t *db = large_record_fixture(&footer);
  uint8_t buffer[64]; timeseries_fielddata_iterator_t iter;
  TEST_ASSERT_TRUE(timeseries_fielddata_iterator_init_window(db, db->last_l0_page_offset,
      db->last_l0_page_size, buffer, sizeof(buffer), &iter));
  timeseries_field_data_header_t header, reread;
  bool ok = timeseries_fielddata_iterator_next(&iter, &header);
  uint32_t cursor = iter.offset, current = iter.current_record_offset;
  uint64_t revision = 0;
  ok = ok && timeseries_fielddata_iterator_read_at(&iter, footer, &revision, 8) && revision > 0 &&
       iter.offset == cursor && iter.current_record_offset == current;
  ok = ok && timeseries_fielddata_iterator_read_at(&iter, current, &reread, sizeof(reread)) &&
       !memcmp(&header, &reread, sizeof(header)) && iter.offset == cursor && iter.current_record_offset == current;
  ok = ok && timeseries_fielddata_iterator_next(&iter, &header) && header.record_count == 1;
  revision = UINT64_MAX;
  ok = ok && !timeseries_fielddata_iterator_read_at(&iter, iter.page_size - 7, &revision, 8) &&
       iter.read_error && !iter.valid && revision == UINT64_MAX;
  timeseries_fielddata_iterator_deinit(&iter);
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(ok);
}

#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_fail_read_at(size_t, bool);
void tsdb_audit_fail_malloc(size_t);
TEST_CASE("ninth: recovery errors preserve counter and permit retry", "[ninth][ninth_recover_fault]") {
  uint32_t footer; timeseries_db_t *db = large_record_fixture(&footer);
  uint64_t expected = db->write_revision;
  db->write_revision = UINT64_MAX;
  tsdb_audit_fail_read_at(db->last_l0_page_offset + footer, true);
  bool rejected = !tsdb_revision_recover(db) && db->write_revision == UINT64_MAX;
  tsdb_audit_fail_read_at(0, false);
  tsdb_audit_fail_malloc(4096);
  rejected = rejected && !tsdb_revision_recover(db) && db->write_revision == UINT64_MAX;
  tsdb_audit_fail_malloc(0);
  bool retried = tsdb_revision_recover(db) && db->write_revision == expected;
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
  TEST_ASSERT_TRUE(rejected); TEST_ASSERT_TRUE(retried);
}

TEST_CASE("ninth: run allocation failure and malformed runs leave no output", "[ninth][ninth_run_fault]") {
  uint64_t values[] = {1, 1, 2, 2};
  uint8_t *encoded = NULL, flags = 0xff; size_t length = 0;
  tsdb_audit_allocation_probe(1, true);
  bool rejected = !tsdb_revision_encode(values, 4, 0, &encoded, &length, &flags) && !encoded && !length;
  tsdb_audit_allocation_probe(0, false);
  free(encoded);
  uint8_t data[18] = {0};
  tsdb_revision_t revision = {0};
  tsdb_audit_allocation_probe(0, true);
  rejected = rejected && !tsdb_revision_decode(0xc7, data, sizeof(data), 4, &revision) && !revision.values;
  size_t allocations = tsdb_audit_allocation_count();
  tsdb_audit_allocation_probe(0, false);
  free(revision.values);
  TEST_ASSERT_TRUE(rejected); TEST_ASSERT_EQUAL(0, allocations);
}
#endif
#if CONFIG_IDF_TARGET_LINUX
TEST_CASE("ninth: empty recovery allocates no scan window", "[ninth][ninth_recover_empty]") {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  tsdb_audit_allocation_probe(0, true);
  bool recovered = tsdb_revision_recover(db);
  size_t allocations = tsdb_audit_allocation_count();
  tsdb_audit_allocation_probe(0, false);
  timeseries_deinit();
  TEST_ASSERT_TRUE(recovered); TEST_ASSERT_EQUAL(0, allocations);
}
#endif
