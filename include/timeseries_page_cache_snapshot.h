#ifndef TIMESERIES_PAGE_CACHE_SNAPSHOT_H
#define TIMESERIES_PAGE_CACHE_SNAPSHOT_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "timeseries_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forward declaration
typedef struct timeseries_db_t_fwd timeseries_db_t_fwd;

typedef struct tsdb_page_cache_snapshot {
    timeseries_cached_page_t *entries;  // sorted array (owned)
    size_t count;
    size_t capacity;
    _Atomic uint32_t refcount;
    // Batch tracking (only used during batch mode)
    uint32_t *removed_offsets;   // offsets removed during batch
    size_t removed_count;
    size_t removed_capacity;
} tsdb_page_cache_snapshot_t;

/**
 * @brief Create a new empty snapshot with the given initial capacity.
 * @param initial_capacity Initial capacity for the entries array.
 * @return New snapshot with refcount=1, or NULL on failure.
 */
tsdb_page_cache_snapshot_t *tsdb_snapshot_create(size_t initial_capacity);

/**
 * @brief Deep-copy a snapshot. The new snapshot has refcount=1.
 * @param src Source snapshot to clone.
 * @return New snapshot, or NULL on failure.
 */
tsdb_page_cache_snapshot_t *tsdb_snapshot_clone(const tsdb_page_cache_snapshot_t *src);

/**
 * @brief Atomically increment the refcount and return the snapshot.
 * @param snap Snapshot to acquire.
 * @return The same snapshot pointer.
 */
tsdb_page_cache_snapshot_t *tsdb_snapshot_acquire(tsdb_page_cache_snapshot_t *snap);

/**
 * @brief Atomically decrement the refcount. If it reaches 0, free the snapshot.
 * @param snap Snapshot to release.
 */
void tsdb_snapshot_release(tsdb_page_cache_snapshot_t *snap);

/**
 * @brief Acquire the current snapshot from the database (thread-safe).
 * Locks snapshot_mutex, acquires, unlocks.
 * @param db Database handle.
 * @return Acquired snapshot, or NULL if none exists.
 */
tsdb_page_cache_snapshot_t *tsdb_snapshot_acquire_current(timeseries_db_t *db);

/**
 * @brief Swap in a new snapshot atomically (thread-safe).
 * Locks snapshot_mutex, replaces current_snapshot, unlocks, releases old.
 * @param db Database handle.
 * @param new_snap New snapshot to install (caller transfers ownership).
 */
void tsdb_snapshot_swap(timeseries_db_t *db, tsdb_page_cache_snapshot_t *new_snap);

/**
 * @brief Compare-and-swap for snapshots.
 * If db->current_snapshot == expected, replace with new_snap and release old.
 * Otherwise return false (caller should retry).
 * @param db Database handle.
 * @param expected Expected current snapshot.
 * @param new_snap New snapshot to install if CAS succeeds.
 * @return true if swap succeeded, false if current_snapshot changed.
 */
bool tsdb_snapshot_compare_and_swap(timeseries_db_t *db,
                                     tsdb_page_cache_snapshot_t *expected,
                                     tsdb_page_cache_snapshot_t *new_snap);

#ifdef __cplusplus
}
#endif

#endif // TIMESERIES_PAGE_CACHE_SNAPSHOT_H
