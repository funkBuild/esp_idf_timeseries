#include "unity.h"
#include "timeseries.h"
#include "timeseries_compaction.h"
#include "timeseries_data.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache_snapshot.h"
#include "timeseries_page_stream_writer.h"
#include "esp_partition.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

// Wear leveling: the allocator's rotating first-fit origin (timeseries_data.h)
// and the capped compaction output pages (timeseries_page_stream_writer.h).

extern timeseries_db_t *timeseries_get_db_handle(void);

static timeseries_db_t *prepare(void) {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db = timeseries_get_db_handle();
  atomic_store(&db->compaction_in_progress, true);  // no background compaction
  return db;
}
static void cleanup(timeseries_db_t *db) {
  atomic_store(&db->compaction_in_progress, false); timeseries_deinit();
}
// Move the origin to `offset` (a sector) without moving sequence_num backwards.
static void set_origin(timeseries_db_t *db, uint32_t offset) {
  uint32_t sectors = db->partition->size / 4096, cycle = TSDB_WEAR_ROTATE_PAGES * sectors;
  uint32_t seq = (atomic_load(&db->sequence_num) / cycle + 1) * cycle + offset / 4096 * TSDB_WEAR_ROTATE_PAGES;
  atomic_store(&db->sequence_num, seq);
}
// The largest free run of the live cache: [*start, *end).
static void largest_free_run(timeseries_db_t *db, uint32_t *start, uint32_t *end) {
  tsdb_page_cache_snapshot_t *s = tsdb_snapshot_acquire_current(db);
  uint32_t at = 0; *start = *end = 0;
  for (size_t i = 0; i <= s->count; i++) {
    uint32_t next = i < s->count ? s->entries[i].offset : db->partition->size;
    if (next > at && next - at > *end - *start) { *start = at; *end = next; }
    if (i < s->count) at = s->entries[i].offset + s->entries[i].header.page_size;
  }
  tsdb_snapshot_release(s);
}
static bool insert_points(size_t count) {
  uint64_t *ts = malloc(count * sizeof(*ts));
  timeseries_field_value_t *values = calloc(count, sizeof(*values));
  if (!ts || !values) { free(ts); free(values); return false; }
  for (size_t i = 0; i < count; i++) { ts[i] = 1000+i; values[i].type = TIMESERIES_FIELD_TYPE_INT; values[i].data.int_val = ts[i]; }
  const char *field = "v";
  timeseries_insert_data_t in = {.measurement_name = "wear", .field_names = &field,
    .field_values = values, .num_fields = 1, .timestamps_ms = ts, .num_points = count};
  bool ok = timeseries_insert(&in); free(ts); free(values); return ok;
}
static size_t query_count(void) {
  timeseries_query_t q = {.measurement_name = "wear"}; timeseries_query_result_t r = {0};
  size_t n = timeseries_query(&q, &r) ? r.num_points : 0;
  timeseries_query_free_result(&r); return n;
}

TEST_CASE("wear: a region is allocated at the rotating origin, not at the lowest free offset", "[wear][wear_origin]") {
  timeseries_db_t *db = prepare();
  uint32_t start, end; largest_free_run(db, &start, &end);
  uint32_t origin = (start + (end - start) / 2) & ~0xFFFu;
  set_origin(db, origin);
  uint32_t ofs = 0, size = 0;
  bool reserved = tsdb_reserve_blank_region(db, 8192, NULL, &ofs, &size);
  if (reserved) tsdb_release_reserved_region(db, ofs, NULL);
  cleanup(db);
  TEST_ASSERT_TRUE(end - start >= 3 * 8192);
  TEST_ASSERT_TRUE(reserved); TEST_ASSERT_EQUAL_HEX32(origin, ofs); TEST_ASSERT_EQUAL_UINT32(8192, size);
}

TEST_CASE("wear: allocation wraps to first-fit from offset 0 when nothing fits after the origin", "[wear][wear_wrap]") {
  timeseries_db_t *db = prepare();
  uint32_t last = db->partition->size - 4096;
  tsdb_page_cache_snapshot_t *s = tsdb_snapshot_acquire_current(db);
  bool last_free = true;  // the origin's sector is free but 8 KiB does not fit after it
  for (size_t i = 0; i < s->count; i++)
    if (s->entries[i].offset + s->entries[i].header.page_size > last) last_free = false;
  tsdb_snapshot_release(s);
  uint32_t start, end; largest_free_run(db, &start, &end);
  set_origin(db, last);
  uint32_t ofs = 0, size = 0;
  bool reserved = tsdb_reserve_blank_region(db, 8192, NULL, &ofs, &size);
  if (reserved) tsdb_release_reserved_region(db, ofs, NULL);
  uint32_t first_fit = 0;  // the lowest free offset that fits 8 KiB
  s = tsdb_snapshot_acquire_current(db);
  for (size_t i = 0, at = 0; i <= s->count; i++) {
    uint32_t next = i < s->count ? s->entries[i].offset : db->partition->size;
    if (next >= at + 8192) { first_fit = at; break; }
    if (i < s->count) at = s->entries[i].offset + s->entries[i].header.page_size;
  }
  tsdb_snapshot_release(s);
  cleanup(db);
  TEST_ASSERT_TRUE(last_free); TEST_ASSERT_TRUE(reserved);
  TEST_ASSERT_EQUAL_HEX32(first_fit, ofs); TEST_ASSERT_TRUE(ofs < last);
}

// A page allocated inside the extent of an old OBSOLETE header (left on flash
// by a page freed earlier) must still be found by the next startup, which
// skips an OBSOLETE page by its page_size.
TEST_CASE("wear: a page inside a stale OBSOLETE page's extent survives a restart", "[wear][wear_unshadow]") {
  timeseries_db_t *db = prepare();
  uint32_t start, end; largest_free_run(db, &start, &end);
  uint32_t stale = start + 4096, origin = stale + 16384;
  timeseries_page_header_t h;
  memset(&h, 0xFF, sizeof(h));
  h.magic_number = TIMESERIES_MAGIC_NUM; h.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
  h.page_state = TIMESERIES_PAGE_STATE_OBSOLETE; h.sequence_num = 1; h.field_data_level = 0; h.page_size = 65536;
  bool planted = end - start >= 4096 + 65536 &&
      esp_partition_erase_range(db->partition, stale, 4096) == ESP_OK &&
      esp_partition_write(db->partition, stale, &h, sizeof(h)) == ESP_OK;
  db->l0_cursor = origin;
  bool inserted = insert_points(50);  // the new L0 page goes to the cursor, inside [stale, stale+64K)
  tsdb_page_cache_snapshot_t *s = tsdb_snapshot_acquire_current(db);
  bool at_origin = false;
  for (size_t i = 0; i < s->count; i++) at_origin |= s->entries[i].offset == origin;
  tsdb_snapshot_release(s);
  uint32_t magic = 0;
  bool read = esp_partition_read(db->partition, stale, &magic, sizeof(magic)) == ESP_OK;
  cleanup(db);
  bool restarted = timeseries_init();
  size_t after = restarted ? query_count() : 0;
  timeseries_deinit();
  TEST_ASSERT_TRUE(planted); TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE(at_origin);
  TEST_ASSERT_TRUE(read); TEST_ASSERT_EQUAL_HEX32(0, magic);
  TEST_ASSERT_TRUE(restarted); TEST_ASSERT_EQUAL_UINT32(50, after);
}

TEST_CASE("wear: L0 pages are allocated next-fit and the cursor resumes after a restart", "[wear][wear_l0_cursor]") {
  timeseries_db_t *db = prepare();
  uint32_t start, end; largest_free_run(db, &start, &end);
  uint32_t cursor = (start + (end - start) / 2) & ~0xFFFu;
  db->l0_cursor = cursor;
  uint32_t first = 0, second = 0;
  // Two L0 pages: the second follows the first instead of reusing lower space.
  bool inserted = insert_points(1);
  tsdb_page_cache_snapshot_t *s = tsdb_snapshot_acquire_current(db);
  for (size_t i = 0; i < s->count; i++)
    if (s->entries[i].header.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA) first = s->entries[i].offset;
  tsdb_snapshot_release(s);
  uint32_t after_first = db->l0_cursor;
  cleanup(db);
  bool restarted = timeseries_init();
  db = timeseries_get_db_handle();
  uint32_t resumed = db->l0_cursor;
  atomic_store(&db->compaction_in_progress, true);
  // A full first page forces a second.
  for (int i = 0; restarted && i < 40 && !second; i++) {
    insert_points(50);
    s = tsdb_snapshot_acquire_current(db);
    for (size_t k = 0; k < s->count; k++)
      if (s->entries[k].header.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA && s->entries[k].offset != first)
        second = s->entries[k].offset;
    tsdb_snapshot_release(s);
  }
  cleanup(db);
  TEST_ASSERT_TRUE(inserted); TEST_ASSERT_EQUAL_HEX32(cursor, first);
  TEST_ASSERT_EQUAL_HEX32(first + TIMESERIES_FIELD_DATA_PAGE_SIZE, after_first);
  TEST_ASSERT_TRUE(restarted); TEST_ASSERT_EQUAL_HEX32(after_first, resumed);
  TEST_ASSERT_EQUAL_HEX32(first + TIMESERIES_FIELD_DATA_PAGE_SIZE, second);
}

// Field-data pages at `level` in the live cache, and the largest of them.
static size_t level_pages(timeseries_db_t *db, uint8_t level, uint32_t *largest) {
  tsdb_page_cache_snapshot_t *s = tsdb_snapshot_acquire_current(db);
  size_t n = 0; *largest = 0;
  for (size_t i = 0; i < s->count; i++) {
    const timeseries_page_header_t *h = &s->entries[i].header;
    if (h->page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA || h->page_state != TIMESERIES_PAGE_STATE_ACTIVE ||
        h->field_data_level != level) continue;
    n++;
    if (h->page_size > *largest) *largest = h->page_size;
  }
  tsdb_snapshot_release(s);
  return n;
}
// Incompressible float points (ALP keeps them near 8 bytes each) at ts 1..n.
static bool insert_floats(size_t n, uint64_t *sum) {
  uint64_t *ts = malloc(n * sizeof(*ts));
  timeseries_field_value_t *values = calloc(n, sizeof(*values));
  if (!ts || !values) { free(ts); free(values); return false; }
  uint32_t x = 2463534242u; *sum = 0;
  for (size_t i = 0; i < n; i++) {
    x ^= x << 13; x ^= x >> 17; x ^= x << 5;
    ts[i] = i + 1; values[i].type = TIMESERIES_FIELD_TYPE_FLOAT; values[i].data.float_val = (double)x / 3.0;
    *sum += x;
  }
  const char *field = "f";
  timeseries_insert_data_t in = {.measurement_name = "wear", .field_names = &field,
    .field_values = values, .num_fields = 1, .timestamps_ms = ts, .num_points = n};
  bool ok = timeseries_insert(&in); free(ts); free(values); return ok;
}
static bool floats_intact(size_t n, uint64_t sum) {
  const char *field = "f";
  timeseries_query_t q = {.measurement_name = "wear", .field_names = &field, .num_fields = 1};
  timeseries_query_result_t r = {0};
  bool ok = timeseries_query(&q, &r) && r.num_points == n && r.num_columns == 1;
  uint64_t got = 0;
  for (size_t i = 0; ok && i < n; i++) {
    ok = r.timestamps[i] == i + 1;
    got += (uint64_t)(r.columns[0].values[i].data.float_val * 3.0 + 0.5);
  }
  timeseries_query_free_result(&r);
  return ok && got == sum;
}

TEST_CASE("wear: a compaction output larger than the page cap continues in further pages", "[wear][wear_page_cap]") {
  timeseries_db_t *db = prepare();
  const size_t n = 10000;  // ~160 KiB of L0 pages, ~80 KiB compacted
  uint64_t sum = 0;
  bool inserted = insert_floats(n, &sum);
  bool compacted = timeseries_compact_level0_pages(db);
  uint32_t l0_largest = 0, l1_largest = 0;
  size_t l0 = level_pages(db, 0, &l0_largest), l1 = level_pages(db, 1, &l1_largest);
  bool intact = floats_intact(n, sum);
  cleanup(db);
  bool restarted = timeseries_init();
  bool intact_after = restarted && floats_intact(n, sum);
  timeseries_deinit();
  TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE(compacted);
  TEST_ASSERT_EQUAL(0, l0); TEST_ASSERT_GREATER_OR_EQUAL(3, l1);
  TEST_ASSERT_LESS_OR_EQUAL_UINT32(TSDB_STREAM_PAGE_MAX_SIZE, l1_largest);
  TEST_ASSERT_TRUE(intact); TEST_ASSERT_TRUE(restarted); TEST_ASSERT_TRUE(intact_after);
}

// Strings take the Gorilla streaming path, whose record size is unknown until
// it ends: the writer starts a new page between records instead.
TEST_CASE("wear: streamed string records split across capped pages", "[wear][wear_page_cap_strings]") {
  timeseries_db_t *db = prepare();
  const size_t n = 600, len = 200;  // ~120 KiB of strings
  uint64_t *ts = malloc(n * sizeof(*ts));
  timeseries_field_value_t *values = calloc(n, sizeof(*values));
  char *text = malloc(n * len);
  TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(values); TEST_ASSERT_NOT_NULL(text);
  uint32_t x = 88172645u;
  for (size_t i = 0; i < n; i++) {
    for (size_t k = 0; k < len; k++) { x ^= x << 13; x ^= x >> 17; x ^= x << 5; text[i * len + k] = 'a' + x % 26; }
    ts[i] = i + 1; values[i].type = TIMESERIES_FIELD_TYPE_STRING;
    values[i].data.string_val.str = text + i * len; values[i].data.string_val.length = len;
  }
  const char *field = "s";
  timeseries_insert_data_t in = {.measurement_name = "wear", .field_names = &field,
    .field_values = values, .num_fields = 1, .timestamps_ms = ts, .num_points = n};
  bool inserted = timeseries_insert(&in);
  bool compacted = timeseries_compact_level0_pages(db);
  uint32_t l1_largest = 0;
  size_t l1 = level_pages(db, 1, &l1_largest);
  cleanup(db);
  bool restarted = timeseries_init();
  timeseries_query_t q = {.measurement_name = "wear", .field_names = &field, .num_fields = 1};
  timeseries_query_result_t r = {0};
  bool intact = restarted && timeseries_query(&q, &r) && r.num_points == n;
  for (size_t i = 0; intact && i < n; i++)
    intact = r.timestamps[i] == i + 1 && r.columns[0].values[i].data.string_val.length == len &&
             !memcmp(r.columns[0].values[i].data.string_val.str, text + i * len, len);
  timeseries_query_free_result(&r);
  timeseries_deinit();
  free(ts); free(values); free(text);
  TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE(compacted);
  TEST_ASSERT_GREATER_OR_EQUAL(2, l1);
  // One streamed record (a string chunk, ~8 KiB) may grow a page past the cap.
  TEST_ASSERT_LESS_OR_EQUAL_UINT32(TSDB_STREAM_PAGE_MAX_SIZE + 16384, l1_largest);
  TEST_ASSERT_TRUE(restarted); TEST_ASSERT_TRUE(intact);
}

// The startup scan helper the allocator relies on agrees with the page iterator.
TEST_CASE("wear: page scan step honours a page header and skips other sectors", "[wear][wear_scan_step]") {
  timeseries_db_t *db = prepare();
  uint32_t start, end; largest_free_run(db, &start, &end);
  timeseries_page_header_t h, out;
  memset(&h, 0xFF, sizeof(h));
  h.magic_number = TIMESERIES_MAGIC_NUM; h.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
  h.page_state = TIMESERIES_PAGE_STATE_OBSOLETE; h.page_size = 12288;
  bool planted = esp_partition_erase_range(db->partition, start, 4096) == ESP_OK &&
      esp_partition_write(db->partition, start, &h, sizeof(h)) == ESP_OK;
  uint32_t next_page = 0, next_blank = 0;
  tsdb_page_scan_step_t page = tsdb_page_scan_step(db->partition, start, &out, &next_page);
  tsdb_page_scan_step_t blank = tsdb_page_scan_step(db->partition, start + 4096, NULL, &next_blank);
  uint32_t clear = 0;
  esp_partition_write(db->partition, start, &clear, sizeof(clear));
  cleanup(db);
  TEST_ASSERT_TRUE(planted);
  TEST_ASSERT_EQUAL(TSDB_PAGE_SCAN_PAGE, page); TEST_ASSERT_EQUAL_HEX32(start + 12288, next_page);
  TEST_ASSERT_EQUAL(TIMESERIES_PAGE_STATE_OBSOLETE, out.page_state);
  TEST_ASSERT_EQUAL(TSDB_PAGE_SCAN_NONE, blank); TEST_ASSERT_EQUAL_HEX32(start + 8192, next_blank);
}
