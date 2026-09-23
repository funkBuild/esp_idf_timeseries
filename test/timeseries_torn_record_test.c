/* TS-1: a field-data record torn by an interrupted write (reset between the
 * flash program operations of one esp_partition_write) must never fail
 * startup. Each test writes real records, tears the last one on flash the way
 * an ascending, chunked program leaves it (a programmed prefix, erased rest),
 * restarts the database and checks that init, inserts, queries and compaction
 * all proceed. */
#include "unity.h"
#include "timeseries.h"
#include "timeseries_data.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include <stdlib.h>
#include <string.h>

extern timeseries_db_t *timeseries_get_db_handle(void);

static const char *s_field = "v";

static bool torn_insert(uint64_t ts) {
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_INT, .data.int_val = (int64_t)ts * 10};
  timeseries_insert_data_t in = {.measurement_name = "torn", .field_names = &s_field, .field_values = &value,
                                 .num_fields = 1, .timestamps_ms = &ts, .num_points = 1};
  return timeseries_insert(&in);
}

/* True when the query returns exactly these timestamps, each with value ts*10. */
static bool torn_matches(const uint64_t *expected, size_t n) {
  timeseries_query_t q = {.measurement_name = "torn"};
  timeseries_query_result_t r = {0};
  bool ok = timeseries_query(&q, &r) && r.num_points == n;
  for (size_t i = 0; ok && i < n; i++)
    ok = r.timestamps[i] == expected[i] && r.columns[0].values[i].data.int_val == (int64_t)expected[i] * 10;
  timeseries_query_free_result(&r);
  return ok;
}

typedef struct {
  const esp_partition_t *part;
  uint32_t page;       // L0 page offset
  uint32_t page_size;
  uint32_t record[2];  // absolute offsets of the two records written
  uint16_t length[2];
} torn_layout_t;

/* Fresh database holding two single-point L0 records (ts 1 and 2). */
static torn_layout_t torn_prepare(void) {
  torn_layout_t l = {0};
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  TEST_ASSERT_TRUE(torn_insert(1));
  TEST_ASSERT_TRUE(torn_insert(2));
  timeseries_db_t *db = timeseries_get_db_handle();
  l.part = db->partition;
  timeseries_page_cache_iterator_t it;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &it));
  timeseries_page_header_t h;
  uint32_t off, size;
  bool found = false;
  while (!found && timeseries_page_cache_iterator_next(&it, &h, &off, &size))
    found = h.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA && h.page_state == TIMESERIES_PAGE_STATE_ACTIVE &&
            h.field_data_level == 0;
  timeseries_page_cache_iterator_deinit(&it);
  TEST_ASSERT_TRUE(found);
  l.page = off;
  l.page_size = size;
  timeseries_fielddata_iterator_t f;
  TEST_ASSERT_TRUE(timeseries_fielddata_iterator_init(db, off, size, &f));
  timeseries_field_data_header_t fh;
  size_t n = 0;
  while (timeseries_fielddata_iterator_next(&f, &fh)) {
    TEST_ASSERT_LESS_THAN(2, n);
    l.record[n] = off + f.current_record_offset;
    l.length[n] = fh.record_length;
    n++;
  }
  timeseries_fielddata_iterator_deinit(&f);
  TEST_ASSERT_EQUAL(2, n);
  timeseries_deinit();
  return l;
}

/* Replace flash bytes (bits may be set as well as cleared) by rewriting the
 * containing sector, as a tear or a foreign firmware image would leave them. */
static void torn_patch(const esp_partition_t *part, uint32_t offset, const void *bytes, int fill, size_t len) {
  uint32_t sector = offset & ~0xFFFu;
  TEST_ASSERT_LESS_OR_EQUAL(4096, offset - sector + len);
  uint8_t *buf = malloc(4096);
  TEST_ASSERT_NOT_NULL(buf);
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(part, sector, buf, 4096));
  if (bytes) memcpy(buf + offset - sector, bytes, len);
  else memset(buf + offset - sector, fill, len);
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_erase_range(part, sector, 4096));
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_write(part, sector, buf, 4096));
  free(buf);
}

static uint8_t torn_flags(const esp_partition_t *part, uint32_t record) {
  uint8_t flags = 0;
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(part, record, &flags, 1));
  return flags;
}

/* After restart: startup succeeded, the counter was not taken from a torn
 * trailer, and inserts, queries and compaction all work. */
static void torn_check_recovered(const uint64_t *survivors, size_t n) {
  timeseries_db_t *db = timeseries_get_db_handle();
  // Not the erased trailer (which would exhaust the counter): at most the
  // highest page sequence in the high word.
  TEST_ASSERT_TRUE((db->write_revision >> 32) <= (uint64_t)db->sequence_num);
  TEST_ASSERT_TRUE(torn_matches(survivors, n));
  uint64_t expected[4];
  memcpy(expected, survivors, n * sizeof(*survivors));
  expected[n] = 3;
  TEST_ASSERT_TRUE(torn_insert(3));
  TEST_ASSERT_TRUE(torn_matches(expected, n + 1));
  TEST_ASSERT_TRUE(timeseries_compact_force_sync());
  TEST_ASSERT_TRUE(torn_matches(expected, n + 1));
  expected[n + 1] = 4;
  TEST_ASSERT_TRUE(torn_insert(4));
  TEST_ASSERT_TRUE(torn_matches(expected, n + 2));
  // A second restart finds nothing further to repair and the same data.
  timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(torn_matches(expected, n + 2));
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_deinit();
}

TEST_CASE("torn: erased revision trailer is dropped, not adopted as the maximum", "[torn][ts1]") {
  torn_layout_t l = torn_prepare();
  // Cut after the header/columns: the trailing 8-byte revision stays erased.
  torn_patch(l.part, l.record[1] + sizeof(timeseries_field_data_header_t) + l.length[1] - sizeof(uint64_t), NULL,
             0xFF, sizeof(uint64_t));
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_EQUAL_HEX8(0, torn_flags(l.part, l.record[1]) & TSDB_FIELDDATA_FLAG_DELETED);
  TEST_ASSERT_NOT_EQUAL(0, torn_flags(l.part, l.record[0]) & TSDB_FIELDDATA_FLAG_DELETED);
  const uint64_t survivors[] = {1};
  torn_check_recovered(survivors, 1);
}

TEST_CASE("torn: erased column header is neutralised", "[torn][ts1]") {
  torn_layout_t l = torn_prepare();
  // Cut right after the 37-byte record header.
  uint32_t data = l.record[1] + sizeof(timeseries_field_data_header_t);
  torn_patch(l.part, data, NULL, 0xFF, l.length[1]);
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_EQUAL_HEX8(0, torn_flags(l.part, l.record[1]) & TSDB_FIELDDATA_FLAG_DELETED);
  const uint64_t survivors[] = {1};
  torn_check_recovered(survivors, 1);
}

TEST_CASE("torn: header with an erased length becomes a deleted filler", "[torn][ts1]") {
  torn_layout_t l = torn_prepare();
  // Cut inside the header: flags and count programmed, everything from
  // record_length on still erased, so the record claims 0xFFFF bytes.
  uint32_t from = l.record[1] + offsetof(timeseries_field_data_header_t, record_length);
  torn_patch(l.part, from, NULL, 0xFF, sizeof(timeseries_field_data_header_t) + l.length[1] -
                                           offsetof(timeseries_field_data_header_t, record_length));
  TEST_ASSERT_TRUE(timeseries_init());
  timeseries_field_data_header_t h;
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(l.part, l.record[1], &h, sizeof(h)));
  TEST_ASSERT_EQUAL_HEX8(0, h.flags & TSDB_FIELDDATA_FLAG_DELETED);
  TEST_ASSERT_EQUAL_UINT32(l.page + l.page_size, l.record[1] + sizeof(h) + h.record_length);
  const uint64_t survivors[] = {1};
  torn_check_recovered(survivors, 1);
}

TEST_CASE("torn: overflowing length that cannot become a filler is salvaged", "[torn][ts1]") {
  torn_layout_t l = torn_prepare();
  // A length past the page end with bits the filler length needs cleared
  // (0x2000 & fill == 0): only a copy of the intact prefix can fix the page.
  uint16_t length = 0x2000;
  uint32_t fill = l.page + l.page_size - l.record[1] - sizeof(timeseries_field_data_header_t);
  TEST_ASSERT_NOT_EQUAL(fill, length & fill);
  torn_patch(l.part, l.record[1] + offsetof(timeseries_field_data_header_t, record_length), &length, 0,
             sizeof(length));
  TEST_ASSERT_TRUE(timeseries_init());
  timeseries_page_header_t old;
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(l.part, l.page, &old, sizeof(old)));
  TEST_ASSERT_EQUAL_HEX8(TIMESERIES_PAGE_STATE_OBSOLETE, old.page_state);
  const uint64_t survivors[] = {1};
  torn_check_recovered(survivors, 1);
}

/* 1.0.19 wrote records without a revision trailer (REVISION bit erased).
 * Rewrite the records selected by legacy_mask in that layout, packed back to
 * back as 1.0.19 appended them, and update the layout. */
static void torn_make_legacy(torn_layout_t *l, unsigned legacy_mask) {
  size_t total = 0;
  uint8_t *records = malloc(2 * (sizeof(timeseries_field_data_header_t) + 0x800));
  TEST_ASSERT_NOT_NULL(records);
  uint32_t first = l->record[0];
  for (size_t i = 0; i < 2; i++) {
    timeseries_field_data_header_t h;
    size_t size = sizeof(h) + l->length[i];
    TEST_ASSERT_LESS_OR_EQUAL(sizeof(h) + 0x800, size);
    TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(l->part, l->record[i], records + total, size));
    memcpy(&h, records + total, sizeof(h));
    if (legacy_mask & (1u << i)) {
      h.flags |= TSDB_FIELDDATA_FLAG_REVISION;
      h.record_length -= sizeof(uint64_t);
      memcpy(records + total, &h, sizeof(h));
      size -= sizeof(uint64_t);
    }
    l->record[i] = first + total;
    l->length[i] = h.record_length;
    total += size;
  }
  // Erase what the beta layout occupied beyond the packed records.
  uint8_t *padded = malloc(total + 2 * sizeof(uint64_t));
  TEST_ASSERT_NOT_NULL(padded);
  memset(padded, 0xFF, total + 2 * sizeof(uint64_t));
  memcpy(padded, records, total);
  torn_patch(l->part, first, padded, 0, total + 2 * sizeof(uint64_t));
  free(padded);
  free(records);
}

TEST_CASE("torn: 1.0.19 record torn after its header is neutralised", "[torn][ts1]") {
  torn_layout_t l = torn_prepare();
  torn_make_legacy(&l, 3);
  uint32_t data = l.record[1] + sizeof(timeseries_field_data_header_t);
  // The second record's columns are erased.
  torn_patch(l.part, data, NULL, 0xFF, sizeof(timeseries_col_data_header_t));
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_NOT_EQUAL(0, torn_flags(l.part, l.record[0]) & TSDB_FIELDDATA_FLAG_DELETED);
  TEST_ASSERT_EQUAL_HEX8(0, torn_flags(l.part, l.record[1]) & TSDB_FIELDDATA_FLAG_DELETED);
  const uint64_t survivors[] = {1};
  torn_check_recovered(survivors, 1);
}

TEST_CASE("torn: 1.0.19 header overflow (appended over by 1.0.19) keeps init alive", "[torn][ts1]") {
  torn_layout_t l = torn_prepare();
  torn_make_legacy(&l, 3);
  // Record length torn to a bitwise superset past the page end.
  uint16_t length = 0xFFFF;
  torn_patch(l.part, l.record[1] + offsetof(timeseries_field_data_header_t, record_length), &length, 0,
             sizeof(length));
  TEST_ASSERT_TRUE(timeseries_init());
  const uint64_t survivors[] = {1};
  torn_check_recovered(survivors, 1);
}

TEST_CASE("torn: intact records survive the startup repair untouched", "[torn][ts1]") {
  torn_layout_t l = torn_prepare();
  torn_make_legacy(&l, 1);  // a 1.0.19 record next to a current one
  uint8_t before[2] = {torn_flags(l.part, l.record[0]), torn_flags(l.part, l.record[1])};
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_EQUAL_HEX8(before[0], torn_flags(l.part, l.record[0]));
  TEST_ASSERT_EQUAL_HEX8(before[1], torn_flags(l.part, l.record[1]));
  const uint64_t survivors[] = {1, 2};
  torn_check_recovered(survivors, 2);
}

/* TS-2 / TS-5: a compaction output page stays FREE on flash (invisible to
 * startup and to 1.0.19) until finalize has written its final size and every
 * record; only then is it flipped to ACTIVE. */
#include "timeseries_page_stream_writer.h"

TEST_CASE("torn: stream-writer output page is FREE until finalize", "[torn][ts2]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  TEST_ASSERT_TRUE(torn_insert(1));
  timeseries_db_t *db = timeseries_get_db_handle();
  const esp_partition_t *part = db->partition;
  uint8_t series_id[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  timeseries_field_value_t val = {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 1.5f};

  // A finished page: ACTIVE, size shrunk from the 16 KiB reservation.
  timeseries_page_stream_writer_t done = {0};
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &done, 1, 16384));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(&done, series_id, TIMESERIES_FIELD_TYPE_FLOAT));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&done, 1000));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&done));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&done, &val));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&done));
  timeseries_page_header_t h;
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(part, done.base_offset, &h, sizeof(h)));
  TEST_ASSERT_EQUAL_HEX8(TIMESERIES_PAGE_STATE_FREE, h.page_state);
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize(&done));
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(part, done.base_offset, &h, sizeof(h)));
  TEST_ASSERT_EQUAL_HEX8(TIMESERIES_PAGE_STATE_ACTIVE, h.page_state);
  TEST_ASSERT_EQUAL_UINT32(4096, h.page_size);
  TEST_ASSERT_EQUAL_UINT32(TIMESERIES_MAGIC_NUM, h.magic_number);

  // An unfinished page, as a reset mid-compaction leaves it.
  timeseries_page_stream_writer_t cut = {0};
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_init(db, &cut, 1, 8192));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_begin_series(&cut, series_id, TIMESERIES_FIELD_TYPE_FLOAT));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_timestamp(&cut, 2000));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_finalize_timestamp(&cut));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_write_value(&cut, &val));
  TEST_ASSERT_TRUE(timeseries_page_stream_writer_end_series(&cut));
  uint32_t cut_offset = cut.base_offset;
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(part, cut_offset, &h, sizeof(h)));
  TEST_ASSERT_EQUAL_HEX8(TIMESERIES_PAGE_STATE_FREE, h.page_state);
  timeseries_deinit();  // "reset" before finalize

  TEST_ASSERT_TRUE(timeseries_init());
  db = timeseries_get_db_handle();
  timeseries_page_cache_iterator_t it;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &it));
  uint32_t off, size;
  bool cut_live = false, done_live = false;
  while (timeseries_page_cache_iterator_next(&it, &h, &off, &size)) {
    cut_live |= off == cut_offset;
    done_live |= off == done.base_offset;
  }
  timeseries_page_cache_iterator_deinit(&it);
  TEST_ASSERT_FALSE(cut_live);
  TEST_ASSERT_TRUE(done_live);
  const uint64_t survivors[] = {1};
  TEST_ASSERT_TRUE(torn_matches(survivors, 1));
  TEST_ASSERT_TRUE(torn_insert(3));
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_deinit();
}

/* TS-3: measurement IDs must be found whatever the name length, and an ID torn
 * by an interrupted write (erased top byte) must not be adopted. */
#include "timeseries_metadata.h"

static bool torn_insert_into(const char *measurement, uint64_t ts) {
  timeseries_field_value_t value = {.type = TIMESERIES_FIELD_TYPE_INT, .data.int_val = (int64_t)ts};
  timeseries_insert_data_t in = {.measurement_name = measurement, .field_names = &s_field, .field_values = &value,
                                 .num_fields = 1, .timestamps_ms = &ts, .num_points = 1};
  return timeseries_insert(&in);
}

TEST_CASE("torn: a long measurement name keeps its ID after restart", "[torn][ts3]") {
  char long_name[101];
  memset(long_name, 'm', 100);
  long_name[100] = 0;
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  TEST_ASSERT_TRUE(torn_insert_into("short", 1));
  TEST_ASSERT_TRUE(torn_insert_into(long_name, 1));  // highest ID has a 100-byte name
  timeseries_db_t *db = timeseries_get_db_handle();
  uint32_t long_id = 0;
  TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, long_name, &long_id));
  timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_init());
  db = timeseries_get_db_handle();
  TEST_ASSERT_GREATER_THAN_UINT32(long_id, db->next_measurement_id);
  TEST_ASSERT_TRUE(torn_insert_into("after", 1));
  uint32_t after_id = 0;
  TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "after", &after_id));
  TEST_ASSERT_NOT_EQUAL(long_id, after_id);
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_deinit();
}

TEST_CASE("torn: a torn measurement ID is not adopted", "[torn][ts3]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  TEST_ASSERT_TRUE(torn_insert_into("first", 1));
  TEST_ASSERT_TRUE(torn_insert_into("torn_one", 1));
  timeseries_db_t *db = timeseries_get_db_handle();
  const esp_partition_t *part = db->partition;
  // Find the "torn_one" MEASUREMENT entry and erase its ID's top byte.
  uint32_t id_offset = 0;
  timeseries_page_cache_iterator_t it;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &it));
  timeseries_page_header_t h;
  uint32_t off, size;
  while (!id_offset && timeseries_page_cache_iterator_next(&it, &h, &off, &size)) {
    if (h.page_type != TIMESERIES_PAGE_TYPE_METADATA) continue;
    timeseries_entity_iterator_t e;
    TEST_ASSERT_TRUE(timeseries_entity_iterator_init(db, off, TIMESERIES_METADATA_PAGE_SIZE, &e));
    timeseries_entry_header_t eh;
    char key[16];
    while (timeseries_entity_iterator_next(&e, &eh)) {
      if (eh.key_type != TIMESERIES_KEYTYPE_MEASUREMENT || eh.key_len != 8) continue;
      uint32_t id;
      TEST_ASSERT_TRUE(timeseries_entity_iterator_read_data(&e, &eh, key, &id));
      if (!memcmp(key, "torn_one", 8))
        id_offset = off + e.current_entry_offset + sizeof(eh) + eh.key_len;
    }
    timeseries_entity_iterator_deinit(&e);
  }
  timeseries_page_cache_iterator_deinit(&it);
  TEST_ASSERT_NOT_EQUAL(0, id_offset);
  timeseries_deinit();
  torn_patch(part, id_offset + 3, NULL, 0xFF, 1);
  TEST_ASSERT_TRUE(timeseries_init());
  db = timeseries_get_db_handle();
  TEST_ASSERT_LESS_THAN_UINT32(1u << 24, db->next_measurement_id);
  TEST_ASSERT_NOT_EQUAL(0, db->next_measurement_id);
  uint32_t id = 0;
  TEST_ASSERT_FALSE(tsdb_find_measurement_id(db, "torn_one", &id));
  TEST_ASSERT_TRUE(torn_insert_into("torn_one", 2));  // gets a fresh ID
  TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "torn_one", &id));
  TEST_ASSERT_LESS_THAN_UINT32(1u << 24, id);
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_deinit();
}

/* TS-4: an entry header torn at the metadata tail (overflowing length) must
 * seal the page, not become the next append target. */
TEST_CASE("torn: a torn metadata tail is never written over", "[torn][ts4]") {
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  TEST_ASSERT_TRUE(torn_insert_into("before", 1));
  timeseries_db_t *db = timeseries_get_db_handle();
  const esp_partition_t *part = db->partition;
  uint32_t tail = 0, meta = 0;
  bool found = false;
  timeseries_page_cache_iterator_t it;
  TEST_ASSERT_TRUE(timeseries_page_cache_iterator_init(db, &it));
  timeseries_page_header_t h;
  uint32_t off, size;
  while (!found && timeseries_page_cache_iterator_next(&it, &h, &off, &size))
    if (h.page_type == TIMESERIES_PAGE_TYPE_METADATA) { meta = off; found = true; }
  timeseries_page_cache_iterator_deinit(&it);
  TEST_ASSERT_TRUE(found);
  timeseries_entity_iterator_t e;
  TEST_ASSERT_TRUE(timeseries_entity_iterator_init(db, meta, TIMESERIES_METADATA_PAGE_SIZE, &e));
  timeseries_entry_header_t eh;
  while (timeseries_entity_iterator_next(&e, &eh)) {}
  tail = meta + e.offset;
  timeseries_entity_iterator_deinit(&e);
  timeseries_deinit();
  // Torn header: marker + type programmed, lengths partly programmed.
  const uint8_t torn[6] = {0xFF, TIMESERIES_KEYTYPE_MEASUREMENT, 0xFF, 0x7F, 0xFF, 0xFF};
  torn_patch(part, tail, torn, 0, sizeof(torn));
  TEST_ASSERT_TRUE(timeseries_init());
  db = timeseries_get_db_handle();
  TEST_ASSERT_TRUE(torn_insert_into("after", 2));
  uint8_t now[6];
  TEST_ASSERT_EQUAL(ESP_OK, esp_partition_read(part, tail, now, sizeof(now)));
  TEST_ASSERT_EQUAL_HEX8_ARRAY(torn, now, sizeof(now));
  uint32_t id = 0;
  TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "after", &id));
  TEST_ASSERT_TRUE(tsdb_find_measurement_id(db, "before", &id));
  timeseries_query_t q = {.measurement_name = "after"};
  timeseries_query_result_t r = {0};
  TEST_ASSERT_TRUE(timeseries_query(&q, &r));
  TEST_ASSERT_EQUAL(1, r.num_points);
  timeseries_query_free_result(&r);
  timeseries_deinit();
  TEST_ASSERT_TRUE(timeseries_init());
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_deinit();
}
