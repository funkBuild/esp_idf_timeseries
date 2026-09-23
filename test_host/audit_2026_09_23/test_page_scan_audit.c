// Audit 2026-09-23: boot-time page scan vs. a torn / corrupted page header.
//
// Compiles the REAL src/timeseries_page_cache.c and
// src/timeseries_page_cache_snapshot.c, plus the real page/blank iterator
// functions extracted verbatim from src/timeseries_iterator.c (run_tests.sh).
// The partition is a RAM array behind a stub esp_partition_read.
//
// A page header is written in one esp_partition_write (create_level0_field_page,
// metadata page creation, compaction). Power loss mid-program (or later bit
// decay) can leave bits of the header still erased (1). A torn page_size is
// therefore any bit-superset of the intended value, e.g. 0x00002000 ->
// 0xFFFFFFFF or 0xFFFFF000, while magic/type/state are already programmed.
#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include "esp_log.h"

#define PART_SIZE (256u * 1024u)
static uint8_t flash[PART_SIZE];
static esp_partition_t part = { .address = 0x100000, .size = PART_SIZE, .label = "tsdb" };
static long read_budget;
static jmp_buf hang_escape;

esp_err_t esp_partition_read(const esp_partition_t* p, size_t off, void* dst, size_t len) {
    (void)p;
    if (--read_budget < 0) longjmp(hang_escape, 1);  // scan did not terminate
    if (off > PART_SIZE || len > PART_SIZE - off) return ESP_ERR_INVALID_SIZE;
    memcpy(dst, flash + off, len);
    return ESP_OK;
}
esp_err_t esp_partition_write(const esp_partition_t* p, size_t off, const void* src, size_t len) {
    (void)p; memcpy(flash + off, src, len); return ESP_OK;
}
esp_err_t esp_partition_erase_range(const esp_partition_t* p, size_t off, size_t len) {
    (void)p; memset(flash + off, 0xFF, len); return ESP_OK;
}
void esp_partition_munmap(esp_partition_mmap_handle_t h) { (void)h; }

// Real iterator functions, extracted verbatim by run_tests.sh.
static const char* TAG = "TimeseriesIterator";
#include "iterator_funcs.inc"

static int failures;
#define CHECK(cond, ...) do { if (!(cond)) { failures++; \
    printf("FAIL %s:%d: %s -- ", __FILE__, __LINE__, #cond); printf(__VA_ARGS__); printf("\n"); } } while (0)

static void put_page(uint32_t off, uint8_t type, uint8_t state, uint32_t seq, uint32_t size) {
    timeseries_page_header_t h;
    memset(&h, 0xFF, sizeof h);
    h.magic_number = TIMESERIES_MAGIC_NUM;
    h.page_type = type;
    h.page_state = state;
    h.sequence_num = seq;
    h.field_data_level = 0;
    h.page_size = size;
    memcpy(flash + off, &h, sizeof h);
    // Some non-blank payload so the region is obviously in use.
    memset(flash + off + sizeof h, 0x5A, 64);
}

static timeseries_db_t db;

static int overlaps(uint32_t lo, uint32_t hi, uint32_t a, uint32_t b) { return lo < b && a < hi; }

static void fresh(void) {
    memset(flash, 0xFF, sizeof flash);
    if (db.current_snapshot) tsdb_snapshot_release(db.current_snapshot);
    memset(&db, 0, sizeof db);
    db.partition = &part;
    db.snapshot_mutex = (SemaphoreHandle_t)1;
}

static const timeseries_cached_page_t* find(const tsdb_page_cache_snapshot_t* s, uint32_t off) {
    for (size_t i = 0; i < s->count; i++) if (s->entries[i].offset == off) return &s->entries[i];
    return NULL;
}

// LIBS-2 (a): torn page_size 0xFFFFFFFF on an ACTIVE page. offset + page_size
// wraps to offset - 1, passes the "> partition size" check, the page is
// accepted with a 4 GiB size, and the scan cursor moves BACKWARDS to an
// unaligned offset: every later page (here D) is never seen. The blank
// allocator then treats D's region as free and erases it on the next page
// allocation -> silent loss of all data stored after the torn page.
static void test_LIBS_2_torn_page_size_wraps_scan(void) {
    fresh();
    put_page(0x0000, TIMESERIES_PAGE_TYPE_METADATA,   TIMESERIES_PAGE_STATE_ACTIVE, 1, 0x2000);
    put_page(0x2000, TIMESERIES_PAGE_TYPE_FIELD_DATA, TIMESERIES_PAGE_STATE_ACTIVE, 2, 0x2000);
    put_page(0x4000, TIMESERIES_PAGE_TYPE_FIELD_DATA, TIMESERIES_PAGE_STATE_ACTIVE, 3, 0xFFFFFFFFu); // torn
    put_page(0x6000, TIMESERIES_PAGE_TYPE_FIELD_DATA, TIMESERIES_PAGE_STATE_ACTIVE, 4, 0x2000);

    read_budget = 100000;
    if (setjmp(hang_escape)) { CHECK(0, "tsdb_build_page_cache did not terminate"); return; }
    bool ok = tsdb_build_page_cache(&db);
    CHECK(ok, "build failed");
    const tsdb_page_cache_snapshot_t* s = db.current_snapshot;
    for (size_t i = 0; i < s->count; i++) {
        uint64_t end = (uint64_t)s->entries[i].offset + s->entries[i].header.page_size;
        CHECK(end <= PART_SIZE, "cached page @0x%05x has size 0x%08x (ends past the partition)",
              (unsigned)s->entries[i].offset, (unsigned)s->entries[i].header.page_size);
    }
    CHECK(find(s, 0x6000) != NULL, "valid ACTIVE page @0x6000 (after the torn header) is missing from the page cache");

    // Replay what tsdb_reserve_blank_region does for the next two 8 KiB page
    // allocations: take the first blank run, erase reserve_size bytes at its
    // start, register the new page in the live snapshot. No allocation may
    // ever erase a live page (A @0x0000, B @0x2000, D @0x6000).
    for (int n = 0; n < 2; n++) {
        timeseries_blank_iterator_t bi;
        uint32_t boff = 0, bsize = 0;
        timeseries_blank_iterator_init(&db, &bi, 0x2000);
        bool got = timeseries_blank_iterator_next(&bi, &boff, &bsize);
        timeseries_blank_iterator_deinit(&bi);
        if (!got) break;
        uint32_t lo = boff, hi = boff + 0x2000;
        CHECK(!overlaps(lo, hi, 0x0000, 0x4000) && !overlaps(lo, hi, 0x6000, 0x8000),
              "allocation #%d reserves [0x%05x,0x%05x) which overlaps a live page "
              "(A/B @0x0000-0x4000, D @0x6000-0x8000) -> tsdb_reserve_blank_region erases it",
              n + 1, (unsigned)lo, (unsigned)hi);
        timeseries_page_header_t h;
        memset(&h, 0xFF, sizeof h);
        h.magic_number = TIMESERIES_MAGIC_NUM;
        h.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
        h.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
        h.sequence_num = 10 + n;
        h.page_size = 0x2000;
        tsdb_pagecache_add_entry(&db, boff, &h);
    }
}

// LIBS-2 (b): torn page_size 0xFFFFF000 (bit-superset of 0x00002000) with the
// state byte still erased (FREE, a bit-superset of ACTIVE 0x01) on a
// non-ACTIVE page: offset + size wraps to offset - 0x1000, the cursor steps
// back one sector, rescans forward onto the same header, and loops forever.
// tsdb_build_page_cache (called from timeseries_init) never returns.
static void test_LIBS_2_torn_page_size_infinite_scan(void) {
    fresh();
    put_page(0x0000, TIMESERIES_PAGE_TYPE_METADATA,   TIMESERIES_PAGE_STATE_ACTIVE,   1, 0x2000);
    put_page(0x2000, TIMESERIES_PAGE_TYPE_FIELD_DATA, TIMESERIES_PAGE_STATE_FREE,     2, 0xFFFFF000u); // torn: state 0x01 -> 0xFF, size 0x2000 -> 0xFFFFF000
    read_budget = 100000;
    if (setjmp(hang_escape)) {
        CHECK(0, "tsdb_build_page_cache did not terminate (>100000 flash reads for a 64-sector partition)");
        return;
    }
    bool ok = tsdb_build_page_cache(&db);
    CHECK(ok, "build failed");
}

// LIBS-2 (sibling): timeseries_entity_iterator_init had the same wrapping
// `page_offset + page_size > partition size` bound. A size near 2^32 must be
// rejected, and an exact fit to the partition end still accepted.
static void test_LIBS_2_entity_iterator_bound(void) {
    fresh();
    timeseries_entity_iterator_t it;
    CHECK(!timeseries_entity_iterator_init(&db, 0x4000, 0xFFFFFFFFu, &it),
          "entity iterator accepted page @0x4000 with size 0xffffffff");
    CHECK(!timeseries_entity_iterator_init(&db, 0x4000, 0xFFFFF000u, &it),
          "entity iterator accepted page @0x4000 with size 0xfffff000");
    CHECK(!timeseries_entity_iterator_init(&db, PART_SIZE + 0x1000, 0x1000, &it),
          "entity iterator accepted an offset past the partition");
    CHECK(timeseries_entity_iterator_init(&db, PART_SIZE - 0x2000, 0x2000, &it),
          "entity iterator rejected a page ending exactly at the partition end");
}

// LIBS-2: a torn header in the middle, followed by more valid pages, with the
// state already ACTIVE and a size that is a bit-superset of 0x2000 but still
// past the partition end (0x7FFFF000): the scan must skip it and keep every
// valid page after it, and never report a page extending past the end.
static void test_LIBS_2_torn_header_pages_after_kept(void) {
    fresh();
    put_page(0x0000, TIMESERIES_PAGE_TYPE_METADATA,   TIMESERIES_PAGE_STATE_ACTIVE, 1, 0x2000);
    put_page(0x2000, TIMESERIES_PAGE_TYPE_METADATA,   TIMESERIES_PAGE_STATE_ACTIVE, 2, 0x7FFFF000u); // torn
    put_page(0x4000, TIMESERIES_PAGE_TYPE_FIELD_DATA, TIMESERIES_PAGE_STATE_OBSOLETE, 3, 0x2000);
    put_page(0x6000, TIMESERIES_PAGE_TYPE_FIELD_DATA, TIMESERIES_PAGE_STATE_ACTIVE, 4, 0x2000);
    put_page(PART_SIZE - 0x2000, TIMESERIES_PAGE_TYPE_FIELD_DATA, TIMESERIES_PAGE_STATE_ACTIVE, 5, 0x2000);
    read_budget = 100000;
    if (setjmp(hang_escape)) { CHECK(0, "tsdb_build_page_cache did not terminate"); return; }
    CHECK(tsdb_build_page_cache(&db), "build failed");
    const tsdb_page_cache_snapshot_t* s = db.current_snapshot;
    CHECK(s->count == 3, "expected 3 ACTIVE pages (0x0, 0x6000, end-0x2000), got %zu", s->count);
    CHECK(find(s, 0x0000) && find(s, 0x6000) && find(s, PART_SIZE - 0x2000), "a valid page is missing");
    CHECK(!find(s, 0x2000), "torn page @0x2000 was cached");
}

int main(void) {
    test_LIBS_2_torn_page_size_wraps_scan();
    test_LIBS_2_torn_page_size_infinite_scan();
    test_LIBS_2_entity_iterator_bound();
    test_LIBS_2_torn_header_pages_after_kept();
    printf("%s: %d failure(s)\n", failures ? "FAILED" : "PASSED", failures);
    return failures ? 1 : 0;
}
