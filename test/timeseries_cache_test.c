/**
 * @file timeseries_cache_test.c
 * @brief Comprehensive unit tests for timeseries cache implementation
 *
 * Tests cover:
 * - Cache initialization and cleanup
 * - Cache hit/miss behavior
 * - Cache eviction (LRU policy)
 * - Cache overflow when full
 * - Hash collision handling
 * - Cache clearing
 * - Cache statistics accuracy
 * - Memory management
 * - Edge cases and error handling
 */

#include "esp_log.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_cache.h"
#include "unity.h"
#include <string.h>

static const char *TAG = "CacheTest";

// Helper function to get direct access to DB context
extern timeseries_db_t *timeseries_get_db_handle(void);

// Helper function to create a series ID from a string
static void create_series_id(const char *str, unsigned char series_id[16]) {
    memset(series_id, 0, 16);
    size_t len = strlen(str);
    if (len > 16) len = 16;
    memcpy(series_id, str, len);
}

// Helper function to compare series IDs
static bool series_id_equal(const unsigned char a[16], const unsigned char b[16]) {
    return memcmp(a, b, 16) == 0;
}

// Helper function to generate a unique key string
static void generate_key(char *buffer, size_t buffer_size, int index) {
    snprintf(buffer, buffer_size, "measurement_tag%d_field%d", index, index);
}

// Helper function to count valid entries in cache
static size_t count_valid_cache_entries(timeseries_db_t *db) {
    size_t count = 0;
    if (db && db->series_cache) {
        for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
            if (db->series_cache[i].valid) {
                count++;
            }
        }
    }
    return count;
}

// Helper function to find a cache entry by key
// Note: Truncates key to 127 chars to match cache storage behavior
static series_id_cache_entry_t* find_cache_entry(timeseries_db_t *db, const char *key) {
    if (!db || !db->series_cache || !key) {
        return NULL;
    }

    // Truncate key to match cache storage size (128 bytes including null terminator)
    char truncated_key[128];
    strncpy(truncated_key, key, sizeof(truncated_key) - 1);
    truncated_key[sizeof(truncated_key) - 1] = '\0';

    for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
        if (db->series_cache[i].valid && strcmp(db->series_cache[i].key, truncated_key) == 0) {
            return &db->series_cache[i];
        }
    }
    return NULL;
}

// Helper function to ensure clean cache state for tests
static void setup_clean_cache(void) {
    TEST_ASSERT_TRUE(timeseries_init());
    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

    // If cache was freed by a previous test, reinitialize it
    if (db->series_cache == NULL) {
        TEST_ASSERT_TRUE(tsdb_cache_init(db));
    }

    // Clear the cache to ensure clean state between tests
    tsdb_cache_clear(db);
}

// ============================================================================
// Cache Initialization Tests
// ============================================================================

TEST_CASE("cache initialization allocates memory correctly", "[cache][init]") {
    TEST_ASSERT_TRUE(timeseries_init());
    // Clear to ensure clean state
    timeseries_db_t *db_clean = timeseries_get_db_handle();
    tsdb_cache_clear(db_clean);

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);
    TEST_ASSERT_NOT_NULL(db->series_cache);
    TEST_ASSERT_EQUAL(0, db->cache_access_counter);

    // Verify all entries are initialized as invalid
    for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
        TEST_ASSERT_FALSE(db->series_cache[i].valid);
        TEST_ASSERT_EQUAL(0, db->series_cache[i].last_access);
    }
}

TEST_CASE("cache init with NULL db returns false", "[cache][init][error]") {
    TEST_ASSERT_FALSE(tsdb_cache_init(NULL));
}

TEST_CASE("cache statistics are initialized to zero", "[cache][init][stats]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    TEST_ASSERT_EQUAL(0, db->cache_stats.hits);
    TEST_ASSERT_EQUAL(0, db->cache_stats.misses);
    TEST_ASSERT_EQUAL(0, db->cache_stats.evictions);
    TEST_ASSERT_EQUAL(0, db->cache_stats.insertions);
#endif
}

// ============================================================================
// Cache Lookup Tests (Hit/Miss Behavior)
// ============================================================================

TEST_CASE("cache lookup on empty cache returns miss", "[cache][lookup]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];

    bool found = tsdb_cache_lookup_series_id(db, "nonexistent_key", series_id);
    TEST_ASSERT_FALSE(found);

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    TEST_ASSERT_EQUAL(0, db->cache_stats.hits);
    TEST_ASSERT_EQUAL(1, db->cache_stats.misses);
#endif
}

TEST_CASE("cache lookup with NULL parameters returns false", "[cache][lookup][error]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];

    // NULL db
    TEST_ASSERT_FALSE(tsdb_cache_lookup_series_id(NULL, "key", series_id));

    // NULL key
    TEST_ASSERT_FALSE(tsdb_cache_lookup_series_id(db, NULL, series_id));

    // NULL series_id
    TEST_ASSERT_FALSE(tsdb_cache_lookup_series_id(db, "key", NULL));
}

TEST_CASE("cache lookup finds inserted entry", "[cache][lookup][insert]") {
    setup_clean_cache();  // Ensure clean state for stats verification

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char original_id[16];
    unsigned char retrieved_id[16];

    create_series_id("test_series_1", original_id);
    tsdb_cache_insert_series_id(db, "test_key", original_id);

    bool found = tsdb_cache_lookup_series_id(db, "test_key", retrieved_id);
    TEST_ASSERT_TRUE(found);
    TEST_ASSERT_TRUE(series_id_equal(original_id, retrieved_id));

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    TEST_ASSERT_EQUAL(1, db->cache_stats.hits);
    TEST_ASSERT_EQUAL(0, db->cache_stats.misses);
    TEST_ASSERT_EQUAL(1, db->cache_stats.insertions);
#endif
}

TEST_CASE("cache lookup multiple different keys", "[cache][lookup]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();

    const int num_keys = 10;
    char keys[num_keys][128];
    unsigned char series_ids[num_keys][16];

    // Insert multiple entries
    for (int i = 0; i < num_keys; i++) {
        generate_key(keys[i], sizeof(keys[i]), i);
        create_series_id(keys[i], series_ids[i]);
        tsdb_cache_insert_series_id(db, keys[i], series_ids[i]);
    }

    // Verify all can be retrieved
    for (int i = 0; i < num_keys; i++) {
        unsigned char retrieved_id[16];
        bool found = tsdb_cache_lookup_series_id(db, keys[i], retrieved_id);
        TEST_ASSERT_TRUE_MESSAGE(found, keys[i]);
        TEST_ASSERT_TRUE(series_id_equal(series_ids[i], retrieved_id));
    }
}

TEST_CASE("cache lookup updates LRU access counter", "[cache][lookup][lru]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];
    unsigned char retrieved_id[16];

    create_series_id("test_series", series_id);
    tsdb_cache_insert_series_id(db, "test_key", series_id);

#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
    uint32_t initial_counter = db->cache_access_counter;

    // First lookup
    tsdb_cache_lookup_series_id(db, "test_key", retrieved_id);
    uint32_t after_first = db->cache_access_counter;
    TEST_ASSERT_GREATER_THAN(initial_counter, after_first);

    // Second lookup
    tsdb_cache_lookup_series_id(db, "test_key", retrieved_id);
    uint32_t after_second = db->cache_access_counter;
    TEST_ASSERT_GREATER_THAN(after_first, after_second);
#endif
}

// ============================================================================
// Cache Insertion Tests
// ============================================================================

TEST_CASE("cache insert with NULL parameters is safe", "[cache][insert][error]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];
    create_series_id("test", series_id);

    // These should not crash
    tsdb_cache_insert_series_id(NULL, "key", series_id);
    tsdb_cache_insert_series_id(db, NULL, series_id);
    tsdb_cache_insert_series_id(db, "key", NULL);
}

TEST_CASE("cache insert adds new entry", "[cache][insert]") {
    setup_clean_cache();  // Ensure clean state

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];

    create_series_id("test_series", series_id);

    size_t before = count_valid_cache_entries(db);
    tsdb_cache_insert_series_id(db, "test_key", series_id);
    size_t after = count_valid_cache_entries(db);

    TEST_ASSERT_EQUAL(before + 1, after);

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    TEST_ASSERT_EQUAL(1, db->cache_stats.insertions);
#endif
}

TEST_CASE("cache insert updates existing entry", "[cache][insert][update]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id1[16];
    unsigned char series_id2[16];
    unsigned char retrieved_id[16];

    create_series_id("original_series", series_id1);
    create_series_id("updated_series", series_id2);

    // Insert original
    tsdb_cache_insert_series_id(db, "test_key", series_id1);
    size_t count_after_first = count_valid_cache_entries(db);

    // Update with new series ID
    tsdb_cache_insert_series_id(db, "test_key", series_id2);
    size_t count_after_update = count_valid_cache_entries(db);

    // Count should not increase (update, not insert)
    TEST_ASSERT_EQUAL(count_after_first, count_after_update);

    // Verify the updated value
    bool found = tsdb_cache_lookup_series_id(db, "test_key", retrieved_id);
    TEST_ASSERT_TRUE(found);
    TEST_ASSERT_TRUE(series_id_equal(series_id2, retrieved_id));
    TEST_ASSERT_FALSE(series_id_equal(series_id1, retrieved_id));
}

TEST_CASE("cache insert truncates long keys", "[cache][insert][boundary]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];

    // Create a key longer than 128 bytes
    char long_key[200];
    memset(long_key, 'A', sizeof(long_key) - 1);
    long_key[sizeof(long_key) - 1] = '\0';

    create_series_id("test", series_id);
    tsdb_cache_insert_series_id(db, long_key, series_id);

    // Verify the entry exists
    series_id_cache_entry_t *entry = find_cache_entry(db, long_key);
    TEST_ASSERT_NOT_NULL(entry);

    // Key should be truncated to 127 chars + null terminator
    TEST_ASSERT_EQUAL(127, strlen(entry->key));
}

// ============================================================================
// Hash Collision Tests
// ============================================================================

TEST_CASE("cache handles hash collisions with linear probing", "[cache][collision]") {
    setup_clean_cache();  // Ensure clean state

    timeseries_db_t *db = timeseries_get_db_handle();

    // Insert many entries to increase likelihood of collisions
    // Note: Use single key/series_id buffers instead of large arrays to avoid stack overflow
    // (ESP32 stack is typically 4KB, and large arrays would overflow it)
    const int num_entries = SERIES_ID_CACHE_SIZE / 2;
    char key[128];
    unsigned char series_id[16];

    for (int i = 0; i < num_entries; i++) {
        snprintf(key, sizeof(key), "collision_key_%d", i);
        create_series_id(key, series_id);
        tsdb_cache_insert_series_id(db, key, series_id);
    }

    // Verify all entries can be retrieved (collision handling works)
    for (int i = 0; i < num_entries; i++) {
        unsigned char expected_id[16];
        unsigned char retrieved_id[16];
        snprintf(key, sizeof(key), "collision_key_%d", i);
        create_series_id(key, expected_id);
        bool found = tsdb_cache_lookup_series_id(db, key, retrieved_id);
        TEST_ASSERT_TRUE_MESSAGE(found, key);
        TEST_ASSERT_TRUE(series_id_equal(expected_id, retrieved_id));
    }
}

TEST_CASE("cache lookup stops at first empty slot during probing", "[cache][collision]") {
    setup_clean_cache();  // Ensure clean state

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];
    unsigned char retrieved_id[16];

    // Insert a few entries
    create_series_id("series1", series_id);
    tsdb_cache_insert_series_id(db, "key1", series_id);
    tsdb_cache_insert_series_id(db, "key2", series_id);

    // Lookup a non-existent key should return miss, not loop forever
    bool found = tsdb_cache_lookup_series_id(db, "nonexistent", retrieved_id);
    TEST_ASSERT_FALSE(found);
}

// ============================================================================
// Cache Eviction Tests (LRU Policy)
// ============================================================================

TEST_CASE("cache evicts entries when full", "[cache][eviction]") {
    setup_clean_cache();  // Ensure clean state

    timeseries_db_t *db = timeseries_get_db_handle();

    // Fill the cache completely
    for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
        char key[128];
        unsigned char series_id[16];
        generate_key(key, sizeof(key), i);
        create_series_id(key, series_id);
        tsdb_cache_insert_series_id(db, key, series_id);
    }

    TEST_ASSERT_EQUAL(SERIES_ID_CACHE_SIZE, count_valid_cache_entries(db));

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    uint32_t insertions_before_eviction = db->cache_stats.insertions;
#endif

    // Insert one more entry - should trigger eviction
    char new_key[128];
    unsigned char new_series_id[16];
    generate_key(new_key, sizeof(new_key), SERIES_ID_CACHE_SIZE + 100);
    create_series_id(new_key, new_series_id);
    tsdb_cache_insert_series_id(db, new_key, new_series_id);

    // Cache should still be full
    TEST_ASSERT_EQUAL(SERIES_ID_CACHE_SIZE, count_valid_cache_entries(db));

    // New entry should be findable
    unsigned char retrieved_id[16];
    bool found = tsdb_cache_lookup_series_id(db, new_key, retrieved_id);
    TEST_ASSERT_TRUE(found);
    TEST_ASSERT_TRUE(series_id_equal(new_series_id, retrieved_id));

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    TEST_ASSERT_EQUAL(1, db->cache_stats.evictions);
    TEST_ASSERT_EQUAL(insertions_before_eviction + 1, db->cache_stats.insertions);
#endif
}

TEST_CASE("cache evicts least recently used entry", "[cache][eviction][lru]") {
    setup_clean_cache();  // Ensure clean state

    timeseries_db_t *db = timeseries_get_db_handle();

#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
    // Fill the cache using individual insertions (avoid large stack arrays)
    for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
        char key[128];
        unsigned char series_id[16];
        generate_key(key, sizeof(key), i);
        create_series_id(key, series_id);
        tsdb_cache_insert_series_id(db, key, series_id);
    }

    // Access entries 8+ to make them recently used (entries 0-7 become LRU candidates)
    unsigned char temp[16];
    for (size_t i = 8; i < SERIES_ID_CACHE_SIZE; i++) {
        char key[128];
        generate_key(key, sizeof(key), i);
        tsdb_cache_lookup_series_id(db, key, temp);
    }

    // Insert a new entry - should evict one of the first few entries (0-7)
    char new_key[128];
    unsigned char new_series_id[16];
    generate_key(new_key, sizeof(new_key), SERIES_ID_CACHE_SIZE + 200);
    create_series_id(new_key, new_series_id);
    tsdb_cache_insert_series_id(db, new_key, new_series_id);

    // The recently accessed entries (8+) should still be present
    int recent_found = 0;
    for (size_t i = 8; i < SERIES_ID_CACHE_SIZE; i++) {
        char key[128];
        generate_key(key, sizeof(key), i);
        if (tsdb_cache_lookup_series_id(db, key, temp)) {
            recent_found++;
        }
    }

    // Most recent entries should still be in cache
    TEST_ASSERT_GREATER_THAN(SERIES_ID_CACHE_SIZE - 16, recent_found);
#else
    TEST_IGNORE_MESSAGE("LRU not enabled");
#endif
}

TEST_CASE("cache eviction with multiple consecutive inserts", "[cache][eviction]") {
    setup_clean_cache();  // Ensure clean state

    timeseries_db_t *db = timeseries_get_db_handle();

    // Fill cache
    for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
        char key[128];
        unsigned char series_id[16];
        generate_key(key, sizeof(key), i);
        create_series_id(key, series_id);
        tsdb_cache_insert_series_id(db, key, series_id);
    }

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    TEST_ASSERT_EQUAL(0, db->cache_stats.evictions);
#endif

    // Insert 10 more entries
    const int extra_inserts = 10;
    for (int i = 0; i < extra_inserts; i++) {
        char key[128];
        unsigned char series_id[16];
        generate_key(key, sizeof(key), SERIES_ID_CACHE_SIZE + i);
        create_series_id(key, series_id);
        tsdb_cache_insert_series_id(db, key, series_id);
    }

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    TEST_ASSERT_EQUAL(extra_inserts, db->cache_stats.evictions);
    TEST_ASSERT_EQUAL(SERIES_ID_CACHE_SIZE + extra_inserts, db->cache_stats.insertions);
#endif

    // Cache should still be full
    TEST_ASSERT_EQUAL(SERIES_ID_CACHE_SIZE, count_valid_cache_entries(db));
}

// ============================================================================
// Cache Clear Tests
// ============================================================================

TEST_CASE("cache clear removes all entries", "[cache][clear]") {
    setup_clean_cache();  // Ensure clean state first

    timeseries_db_t *db = timeseries_get_db_handle();

    // Add some entries
    for (int i = 0; i < 10; i++) {
        char key[128];
        unsigned char series_id[16];
        generate_key(key, sizeof(key), i);
        create_series_id(key, series_id);
        tsdb_cache_insert_series_id(db, key, series_id);
    }

    TEST_ASSERT_EQUAL(10, count_valid_cache_entries(db));

    // Clear the cache
    tsdb_cache_clear(db);

    // Verify all entries are invalid
    TEST_ASSERT_EQUAL(0, count_valid_cache_entries(db));
    TEST_ASSERT_EQUAL(0, db->cache_access_counter);

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    TEST_ASSERT_EQUAL(0, db->cache_stats.hits);
    TEST_ASSERT_EQUAL(0, db->cache_stats.misses);
    TEST_ASSERT_EQUAL(0, db->cache_stats.evictions);
    TEST_ASSERT_EQUAL(0, db->cache_stats.insertions);
#endif
}

TEST_CASE("cache clear with NULL db is safe", "[cache][clear][error]") {
    // Should not crash
    tsdb_cache_clear(NULL);
}

TEST_CASE("cache clear and re-insert works correctly", "[cache][clear]") {
    setup_clean_cache();  // Ensure clean state

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id1[16];
    unsigned char series_id2[16];
    unsigned char retrieved_id[16];

    create_series_id("original", series_id1);
    create_series_id("new", series_id2);

    // Insert, clear, then re-insert with different series ID
    tsdb_cache_insert_series_id(db, "test_key", series_id1);
    tsdb_cache_clear(db);
    tsdb_cache_insert_series_id(db, "test_key", series_id2);

    // Should find the new series ID, not the old one
    bool found = tsdb_cache_lookup_series_id(db, "test_key", retrieved_id);
    TEST_ASSERT_TRUE(found);
    TEST_ASSERT_TRUE(series_id_equal(series_id2, retrieved_id));
    TEST_ASSERT_FALSE(series_id_equal(series_id1, retrieved_id));
}

// ============================================================================
// Cache Free Tests (Memory Management)
// ============================================================================

TEST_CASE("cache free releases memory", "[cache][free]") {
    setup_clean_cache();  // Ensure clean state

    timeseries_db_t *db = timeseries_get_db_handle();
    TEST_ASSERT_NOT_NULL(db);
    TEST_ASSERT_NOT_NULL(db->series_cache);

    // Add some entries
    for (int i = 0; i < 5; i++) {
        char key[128];
        unsigned char series_id[16];
        generate_key(key, sizeof(key), i);
        create_series_id(key, series_id);
        tsdb_cache_insert_series_id(db, key, series_id);
    }

    TEST_ASSERT_EQUAL(5, count_valid_cache_entries(db));

    // Free the cache
    tsdb_cache_free(db);
    TEST_ASSERT_NULL(db->series_cache);

    // Reinitialize for other tests
    TEST_ASSERT_TRUE(tsdb_cache_init(db));
}

TEST_CASE("cache free with NULL db is safe", "[cache][free][error]") {
    // Should not crash
    tsdb_cache_free(NULL);
}

// ============================================================================
// Cache Statistics Tests
// ============================================================================

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
TEST_CASE("cache statistics track hits and misses correctly", "[cache][stats]") {
    setup_clean_cache();  // Ensure clean state for accurate stats

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];
    unsigned char retrieved_id[16];

    create_series_id("test", series_id);

    // Initial state (should be zero after clean cache)
    TEST_ASSERT_EQUAL(0, db->cache_stats.hits);
    TEST_ASSERT_EQUAL(0, db->cache_stats.misses);

    // Miss
    tsdb_cache_lookup_series_id(db, "key1", retrieved_id);
    TEST_ASSERT_EQUAL(0, db->cache_stats.hits);
    TEST_ASSERT_EQUAL(1, db->cache_stats.misses);

    // Insert
    tsdb_cache_insert_series_id(db, "key1", series_id);
    TEST_ASSERT_EQUAL(1, db->cache_stats.insertions);

    // Hit
    tsdb_cache_lookup_series_id(db, "key1", retrieved_id);
    TEST_ASSERT_EQUAL(1, db->cache_stats.hits);
    TEST_ASSERT_EQUAL(1, db->cache_stats.misses);

    // Another hit
    tsdb_cache_lookup_series_id(db, "key1", retrieved_id);
    TEST_ASSERT_EQUAL(2, db->cache_stats.hits);
    TEST_ASSERT_EQUAL(1, db->cache_stats.misses);

    // Another miss
    tsdb_cache_lookup_series_id(db, "key2", retrieved_id);
    TEST_ASSERT_EQUAL(2, db->cache_stats.hits);
    TEST_ASSERT_EQUAL(2, db->cache_stats.misses);
}

TEST_CASE("cache statistics track insertions correctly", "[cache][stats]") {
    setup_clean_cache();  // Ensure clean state for stats

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];

    create_series_id("test", series_id);

    const int num_inserts = 20;
    for (int i = 0; i < num_inserts; i++) {
        char key[128];
        generate_key(key, sizeof(key), i);
        tsdb_cache_insert_series_id(db, key, series_id);
    }

    TEST_ASSERT_EQUAL(num_inserts, db->cache_stats.insertions);
}

TEST_CASE("cache statistics track evictions correctly", "[cache][stats]") {
    setup_clean_cache();  // Ensure clean state for stats

    timeseries_db_t *db = timeseries_get_db_handle();

    // Fill cache
    for (size_t i = 0; i < SERIES_ID_CACHE_SIZE; i++) {
        char key[128];
        unsigned char series_id[16];
        generate_key(key, sizeof(key), i);
        create_series_id(key, series_id);
        tsdb_cache_insert_series_id(db, key, series_id);
    }

    TEST_ASSERT_EQUAL(0, db->cache_stats.evictions);

    // Trigger evictions
    const int extra = 5;
    for (int i = 0; i < extra; i++) {
        char key[128];
        unsigned char series_id[16];
        generate_key(key, sizeof(key), SERIES_ID_CACHE_SIZE + i);
        create_series_id(key, series_id);
        tsdb_cache_insert_series_id(db, key, series_id);
    }

    TEST_ASSERT_EQUAL(extra, db->cache_stats.evictions);
}

TEST_CASE("cache log stats does not crash", "[cache][stats]") {
    setup_clean_cache();

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];

    // Fill cache partially
    for (int i = 0; i < SERIES_ID_CACHE_SIZE / 2; i++) {
        char key[128];
        generate_key(key, sizeof(key), i);
        create_series_id(key, series_id);
        tsdb_cache_insert_series_id(db, key, series_id);
    }

    // This should not crash
    tsdb_cache_log_stats(db);
}

TEST_CASE("cache log stats with NULL db is safe", "[cache][stats][error]") {
    // Should not crash
    tsdb_cache_log_stats(NULL);
}
#endif // CONFIG_TIMESERIES_ENABLE_CACHE_STATS

// ============================================================================
// Boundary and Edge Case Tests
// ============================================================================

TEST_CASE("cache handles empty string key", "[cache][boundary]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];
    unsigned char retrieved_id[16];

    create_series_id("empty_key_test", series_id);

    // Insert with empty key
    tsdb_cache_insert_series_id(db, "", series_id);

    // Should be able to retrieve
    bool found = tsdb_cache_lookup_series_id(db, "", retrieved_id);
    TEST_ASSERT_TRUE(found);
    TEST_ASSERT_TRUE(series_id_equal(series_id, retrieved_id));
}

TEST_CASE("cache handles single character key", "[cache][boundary]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];
    unsigned char retrieved_id[16];

    create_series_id("single_char", series_id);

    tsdb_cache_insert_series_id(db, "x", series_id);

    bool found = tsdb_cache_lookup_series_id(db, "x", retrieved_id);
    TEST_ASSERT_TRUE(found);
    TEST_ASSERT_TRUE(series_id_equal(series_id, retrieved_id));
}

TEST_CASE("cache handles maximum length key", "[cache][boundary]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];
    unsigned char retrieved_id[16];

    // Create a key that's exactly 127 characters (max that fits in 128 with null terminator)
    char max_key[128];
    memset(max_key, 'A', 127);
    max_key[127] = '\0';

    create_series_id("max_key", series_id);
    tsdb_cache_insert_series_id(db, max_key, series_id);

    bool found = tsdb_cache_lookup_series_id(db, max_key, retrieved_id);
    TEST_ASSERT_TRUE(found);
    TEST_ASSERT_TRUE(series_id_equal(series_id, retrieved_id));
}

TEST_CASE("cache handles keys with special characters", "[cache][boundary]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];
    unsigned char retrieved_id[16];

    const char *special_keys[] = {
        "key with spaces",
        "key\twith\ttabs",
        "key=with=equals",
        "key,with,commas",
        "key:with:colons",
        "key|with|pipes",
        "key@with@at",
        "key#with#hash"
    };

    for (size_t i = 0; i < sizeof(special_keys) / sizeof(special_keys[0]); i++) {
        create_series_id(special_keys[i], series_id);
        tsdb_cache_insert_series_id(db, special_keys[i], series_id);

        bool found = tsdb_cache_lookup_series_id(db, special_keys[i], retrieved_id);
        TEST_ASSERT_TRUE_MESSAGE(found, special_keys[i]);
        TEST_ASSERT_TRUE(series_id_equal(series_id, retrieved_id));
    }
}

TEST_CASE("cache handles all-zero series ID", "[cache][boundary]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];
    unsigned char retrieved_id[16];

    memset(series_id, 0, 16);

    tsdb_cache_insert_series_id(db, "zero_id_key", series_id);

    bool found = tsdb_cache_lookup_series_id(db, "zero_id_key", retrieved_id);
    TEST_ASSERT_TRUE(found);
    TEST_ASSERT_TRUE(series_id_equal(series_id, retrieved_id));
}

TEST_CASE("cache handles all-ones series ID", "[cache][boundary]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];
    unsigned char retrieved_id[16];

    memset(series_id, 0xFF, 16);

    tsdb_cache_insert_series_id(db, "ones_id_key", series_id);

    bool found = tsdb_cache_lookup_series_id(db, "ones_id_key", retrieved_id);
    TEST_ASSERT_TRUE(found);
    TEST_ASSERT_TRUE(series_id_equal(series_id, retrieved_id));
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST_CASE("cache integrates with real data insertion", "[cache][integration]") {
    setup_clean_cache();  // Ensure clean cache state
    TEST_ASSERT_TRUE(timeseries_clear_all());

    timeseries_db_t *db = timeseries_get_db_handle();

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    // Record initial stats
    uint32_t initial_hits = db->cache_stats.hits;
    uint32_t initial_misses = db->cache_stats.misses;
    uint32_t initial_insertions = db->cache_stats.insertions;
#endif

    // Insert some real data
    const char *tag_keys[] = {"location"};
    const char *tag_values[] = {"test_site"};
    const char *field_names[] = {"temperature"};

    const size_t num_points = 5;
    uint64_t timestamps[num_points];
    timeseries_field_value_t field_values[num_points];

    for (size_t i = 0; i < num_points; i++) {
        timestamps[i] = 1000 * i;
        field_values[i].type = TIMESERIES_FIELD_TYPE_FLOAT;
        field_values[i].data.float_val = 20.0f + i;
    }

    timeseries_insert_data_t insert_data = {
        .measurement_name = "sensors",
        .tag_keys = tag_keys,
        .tag_values = tag_values,
        .num_tags = 1,
        .field_names = field_names,
        .field_values = field_values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = num_points,
    };

    // First insert should miss cache then insert
    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    // Cache should have been used during insertion
    TEST_ASSERT_GREATER_THAN(initial_hits + initial_misses,
                             db->cache_stats.hits + db->cache_stats.misses);
#endif

    // Second insert of same series should benefit from cache
    for (size_t i = 0; i < num_points; i++) {
        timestamps[i] = 1000 * (i + num_points);
    }

    TEST_ASSERT_TRUE(timeseries_insert(&insert_data));

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    // Should have more hits now
    TEST_ASSERT_GREATER_THAN(initial_hits, db->cache_stats.hits);
#endif
}

TEST_CASE("cache behavior under stress test", "[cache][stress]") {
    setup_clean_cache();  // Ensure clean state

    timeseries_db_t *db = timeseries_get_db_handle();

    // Note: Use single buffers to avoid stack overflow on ESP32 (4KB stack)
    // Previously used keys[100][128] + series_ids[100][16] = 14,400 bytes which overflows
    const int num_operations = 500;
    const int num_unique_keys = 100;
    char key[128];
    unsigned char series_id[16];
    unsigned char retrieved_id[16];

    // Perform random operations - generate key/series_id on the fly
    for (int op = 0; op < num_operations; op++) {
        int key_idx = op % num_unique_keys;
        generate_key(key, sizeof(key), key_idx);
        create_series_id(key, series_id);

        if (op % 3 == 0) {
            // Insert
            tsdb_cache_insert_series_id(db, key, series_id);
        } else {
            // Lookup
            tsdb_cache_lookup_series_id(db, key, retrieved_id);
        }
    }

    // Verify cache is still consistent
    TEST_ASSERT_LESS_OR_EQUAL(SERIES_ID_CACHE_SIZE, count_valid_cache_entries(db));

#ifdef CONFIG_TIMESERIES_ENABLE_CACHE_STATS
    ESP_LOGI(TAG, "Stress test stats - Hits: %u, Misses: %u, Insertions: %u, Evictions: %u",
             db->cache_stats.hits, db->cache_stats.misses,
             db->cache_stats.insertions, db->cache_stats.evictions);
#endif
}

// ============================================================================
// Access Counter Overflow Test
// ============================================================================

TEST_CASE("cache handles access counter near overflow", "[cache][overflow]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();

#ifdef CONFIG_TIMESERIES_CACHE_USE_LRU
    // Set counter near max value
    db->cache_access_counter = UINT32_MAX - 10;

    unsigned char series_id[16];
    unsigned char retrieved_id[16];
    create_series_id("test", series_id);

    tsdb_cache_insert_series_id(db, "key1", series_id);

    // Perform several lookups to increment counter past overflow
    for (int i = 0; i < 20; i++) {
        tsdb_cache_lookup_series_id(db, "key1", retrieved_id);
    }

    // Should still work correctly after overflow
    bool found = tsdb_cache_lookup_series_id(db, "key1", retrieved_id);
    TEST_ASSERT_TRUE(found);
    TEST_ASSERT_TRUE(series_id_equal(series_id, retrieved_id));
#endif
}

// ============================================================================
// Concurrency-Related Tests (Single-threaded validation)
// ============================================================================

TEST_CASE("cache maintains consistency with repeated insert-lookup cycles", "[cache][consistency]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();

    const int num_cycles = 100;
    const int num_keys = 10;

    for (int cycle = 0; cycle < num_cycles; cycle++) {
        for (int i = 0; i < num_keys; i++) {
            char key[128];
            unsigned char series_id[16];
            unsigned char retrieved_id[16];

            generate_key(key, sizeof(key), i);
            create_series_id(key, series_id);

            // Insert
            tsdb_cache_insert_series_id(db, key, series_id);

            // Immediately lookup
            bool found = tsdb_cache_lookup_series_id(db, key, retrieved_id);
            TEST_ASSERT_TRUE(found);
            TEST_ASSERT_TRUE(series_id_equal(series_id, retrieved_id));
        }
    }
}

TEST_CASE("cache handles alternating insert and clear operations", "[cache][consistency]") {
    TEST_ASSERT_TRUE(timeseries_init());

    timeseries_db_t *db = timeseries_get_db_handle();
    unsigned char series_id[16];
    unsigned char retrieved_id[16];

    create_series_id("test", series_id);

    for (int i = 0; i < 20; i++) {
        // Insert
        tsdb_cache_insert_series_id(db, "test_key", series_id);

        // Verify
        bool found = tsdb_cache_lookup_series_id(db, "test_key", retrieved_id);
        TEST_ASSERT_TRUE(found);

        // Clear
        tsdb_cache_clear(db);

        // Verify cleared
        found = tsdb_cache_lookup_series_id(db, "test_key", retrieved_id);
        TEST_ASSERT_FALSE(found);
        TEST_ASSERT_EQUAL(0, count_valid_cache_entries(db));
    }
}
