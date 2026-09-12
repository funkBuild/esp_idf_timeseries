#include "unity.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include "esp_timer.h"
#include "esp_log.h"
#include <stdlib.h>
#include <string.h>

#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_allocation_probe(size_t, bool);
bool tsdb_audit_allocation_hit(void);
size_t tsdb_audit_allocation_count(void);
size_t tsdb_audit_allocation_bytes(void);
size_t tsdb_audit_largest_allocation(void);
void tsdb_audit_fail_malloc(size_t);
void tsdb_audit_on_allocation(void (*)(void));
#endif

static timeseries_page_header_t header(uint32_t sequence) {
    timeseries_page_header_t h;
    // Verify reserved/padding bytes survive growth as well as named fields.
    memset(&h, 0xa5, sizeof(h));
    h.magic_number = TIMESERIES_MAGIC_NUM;
    h.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
    h.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
    h.page_size = 4096;
    h.sequence_num = sequence;
    return h;
}

static timeseries_db_t *cache_db(size_t count) {
    timeseries_db_t *db = calloc(1, sizeof(*db));
    TEST_ASSERT_NOT_NULL(db);
    db->snapshot_mutex = xSemaphoreCreateMutex();
    TEST_ASSERT_NOT_NULL(db->snapshot_mutex);
    db->current_snapshot = tsdb_snapshot_create(8);
    TEST_ASSERT_NOT_NULL(db->current_snapshot);
    for (size_t i = 0; i < count; ++i) {
        timeseries_page_header_t h = header(i + 1);
        TEST_ASSERT_TRUE(tsdb_pagecache_add_entry(db, (i + 1) * 4096, &h));
    }
    return db;
}

static bool valid_entries(const tsdb_page_cache_snapshot_t *snap, size_t count) {
    if (snap->count != count) return false;
    for (size_t i = 0; i < count; ++i) {
        timeseries_page_header_t h = header(i + 1);
        if (snap->entries[i].offset != (i + 1) * 4096 ||
            memcmp(&snap->entries[i].header, &h, sizeof(h))) return false;
    }
    return true;
}

static void free_cache_db(timeseries_db_t *db) {
    tsdb_snapshot_release(db->current_snapshot);
    vSemaphoreDelete(db->snapshot_mutex);
    free(db);
}

static void growth_benchmark(bool shuffled) {
    timeseries_db_t *db = cache_db(0);
    esp_log_level_set("*", ESP_LOG_NONE);
#if CONFIG_IDF_TARGET_LINUX
    tsdb_audit_allocation_probe(0, true);
#endif
    bool ok = true;
    int64_t start = esp_timer_get_time();
    for (size_t i = 0; i < 1024 && ok; ++i) {
        // Odd multiplication permutes all 1024 entries without allocating indices.
        uint32_t index = shuffled ? (i * 613) % 1024 : i;
        timeseries_page_header_t h = header(index + 1);
        ok = tsdb_pagecache_add_entry(db, (index + 1) * 4096, &h);
    }
    int64_t elapsed = esp_timer_get_time() - start;
#if CONFIG_IDF_TARGET_LINUX
    size_t calls = tsdb_audit_allocation_count();
    size_t bytes = tsdb_audit_allocation_bytes();
    size_t largest = tsdb_audit_largest_allocation();
    tsdb_audit_allocation_probe(0, false);
    printf("THIRTEENTH_GROW shuffled=%u us=%lld allocations=%zu bytes=%zu largest=%zu capacity=%zu\n",
           (unsigned)shuffled, (long long)elapsed, calls, bytes, largest, db->current_snapshot->capacity);
#else
    printf("THIRTEENTH_GROW shuffled=%u us=%lld capacity=%zu\n",
           (unsigned)shuffled, (long long)elapsed, db->current_snapshot->capacity);
#endif
    esp_log_level_set("*", ESP_LOG_INFO);
    bool valid = valid_entries(db->current_snapshot, 1024);
    size_t capacity = db->current_snapshot->capacity;
    free_cache_db(db);
    TEST_ASSERT_TRUE(ok);
    TEST_ASSERT_TRUE(valid);
    TEST_ASSERT_LESS_OR_EQUAL(1280, capacity);
#if CONFIG_IDF_TARGET_LINUX
    TEST_ASSERT_LESS_OR_EQUAL(48, calls);
    TEST_ASSERT_LESS_OR_EQUAL(160 * 1024, bytes);
    TEST_ASSERT_LESS_OR_EQUAL(1280 * sizeof(timeseries_cached_page_t), largest);
#endif
}

TEST_CASE("thirteenth: growing cache amortizes ascending additions", "[thirteenth][thirteenth_growth]") {
    growth_benchmark(false);
}
TEST_CASE("thirteenth: growing cache preserves shuffled entries", "[thirteenth][thirteenth_growth]") {
    growth_benchmark(true);
}

TEST_CASE("thirteenth: shared growth stays tight and preserves its reader", "[thirteenth]") {
    timeseries_db_t *db = cache_db(8);
    tsdb_page_cache_snapshot_t *reader = tsdb_snapshot_acquire_current(db);
    timeseries_page_header_t h = header(9);
    bool ok = tsdb_pagecache_add_entry(db, 9 * 4096, &h);
    bool valid = valid_entries(reader, 8) && valid_entries(db->current_snapshot, 9);
    size_t capacity = db->current_snapshot->capacity;
    tsdb_snapshot_release(reader);
    free_cache_db(db);
    TEST_ASSERT_TRUE(ok);
    TEST_ASSERT_TRUE(valid);
    TEST_ASSERT_EQUAL(9, capacity);
}

TEST_CASE("thirteenth: shared header updates need no additional entry", "[thirteenth]") {
    timeseries_db_t *db = cache_db(8);
    tsdb_page_cache_snapshot_t *reader = tsdb_snapshot_acquire_current(db);
    timeseries_page_header_t h = header(99);
    bool ok = tsdb_pagecache_add_entry(db, 4 * 4096, &h);
    bool valid = valid_entries(reader, 8) && db->current_snapshot->count == 8 &&
        !memcmp(&db->current_snapshot->entries[3].header, &h, sizeof(h));
    size_t capacity = db->current_snapshot->capacity;
    tsdb_snapshot_release(reader);
    free_cache_db(db);
    TEST_ASSERT_TRUE(ok);
    TEST_ASSERT_TRUE(valid);
    TEST_ASSERT_EQUAL(8, capacity);
}

#if CONFIG_IDF_TARGET_LINUX
static timeseries_db_t *allocating_db;
static tsdb_page_cache_snapshot_t *late_reader;
static void acquire_during_growth(void) {
    // This checkpoint runs after the growth decision, before the private clone
    // exists. A reader must be able to acquire the snapshot mutex here.
    if (xSemaphoreTake(allocating_db->snapshot_mutex, 0) == pdTRUE) {
        late_reader = tsdb_snapshot_acquire(allocating_db->current_snapshot);
        xSemaphoreGive(allocating_db->snapshot_mutex);
    }
}

TEST_CASE("thirteenth: reader can arrive during growth without blocking or changing", "[thirteenth]") {
    timeseries_db_t *db = cache_db(8);
    timeseries_page_header_t h = header(9);
    allocating_db = db;
    late_reader = NULL;
    tsdb_audit_on_allocation(acquire_during_growth);
    bool ok = tsdb_pagecache_add_entry(db, 9 * 4096, &h);
    tsdb_audit_on_allocation(NULL);
    bool valid = late_reader && valid_entries(late_reader, 8) && valid_entries(db->current_snapshot, 9);
    // Subsequent in-place mutation of the new current cache cannot alter reader.
    h = header(10);
    ok = ok && tsdb_pagecache_add_entry(db, 10 * 4096, &h);
    valid = valid && valid_entries(late_reader, 8) && valid_entries(db->current_snapshot, 10);
    tsdb_snapshot_release(late_reader);
    late_reader = NULL;
    allocating_db = NULL;
    free_cache_db(db);
    TEST_ASSERT_TRUE(ok);
    TEST_ASSERT_TRUE(valid);
}

TEST_CASE("thirteenth: failed optional growth retries the minimum allocation", "[thirteenth]") {
    // Fail the speculative snapshot header, then its entries allocation.
    for (size_t failure = 1; failure <= 2; ++failure) {
        timeseries_db_t *db = cache_db(8);
        timeseries_page_header_t h = header(9);
        esp_log_level_set("*", ESP_LOG_NONE);
        tsdb_audit_allocation_probe(failure, false);
        bool ok = tsdb_pagecache_add_entry(db, 9 * 4096, &h);
        bool hit = tsdb_audit_allocation_hit();
        tsdb_audit_allocation_probe(0, false);
        esp_log_level_set("*", ESP_LOG_INFO);
        bool valid = valid_entries(db->current_snapshot, 9);
        size_t capacity = db->current_snapshot->capacity;
        free_cache_db(db);
        TEST_ASSERT_TRUE(hit);
        TEST_ASSERT_TRUE(ok);
        TEST_ASSERT_TRUE(valid);
        TEST_ASSERT_EQUAL(9, capacity);
    }
}

TEST_CASE("thirteenth: failed growth and retry preserve cache and release lock", "[thirteenth]") {
    timeseries_db_t *db = cache_db(8);
    timeseries_page_header_t h = header(9);
    tsdb_page_cache_snapshot_t *original = db->current_snapshot; // deliberately unpinned
    esp_log_level_set("*", ESP_LOG_NONE);
    tsdb_audit_allocation_probe(1, false);
    tsdb_audit_fail_malloc(9 * sizeof(timeseries_cached_page_t));
    bool rejected = !tsdb_pagecache_add_entry(db, 9 * 4096, &h);
    bool hit = tsdb_audit_allocation_hit();
    tsdb_audit_allocation_probe(0, false);
    tsdb_audit_fail_malloc(0);
    esp_log_level_set("*", ESP_LOG_INFO);
    bool preserved = db->current_snapshot == original && valid_entries(original, 8);
    bool retried = tsdb_pagecache_add_entry(db, 9 * 4096, &h) && valid_entries(db->current_snapshot, 9);
    free_cache_db(db);
    TEST_ASSERT_TRUE(hit);
    TEST_ASSERT_TRUE(rejected);
    TEST_ASSERT_TRUE(preserved);
    TEST_ASSERT_TRUE(retried);
}
#endif
