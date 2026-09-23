#ifndef TIMESERIES_EPOCH_H
#define TIMESERIES_EPOCH_H

#include <stdbool.h>
#include <stdint.h>

#include "timeseries_internal.h"
#include "timeseries_page_cache_snapshot.h"

/*
 * Epoch-based reclamation of flash regions.
 *
 * A reader may hold a page offset (a page-cache snapshot, a metadata page
 * list, an iterator) for as long as its public operation runs, without any
 * lock. A region that leaves the live page cache is "retired" with the
 * current epoch instead of becoming free. The region allocator treats it as
 * occupied until no operation that entered at or before that epoch is still
 * active, so no reader ever sees a region it could have seen erased or
 * reused.
 *
 * Every public operation owns an epoch slot (TSDB_OPERATION_GUARD); the
 * background compaction task owns one per run. A slot is idle while its task
 * waits for flash_write_mutex, the maintenance slot or a compaction, holding
 * no page offsets; leaving idle, or tsdb_epoch_quiesce(), moves the slot to
 * the current epoch.
 *
 * Retired regions are kept in a table allocated at init (PSRAM via malloc),
 * guarded by snapshot_mutex. Retired regions are disjoint from each other and
 * from live pages and start on distinct 4 KiB boundaries, so partition_size /
 * 4 KiB entries always suffice. A database without the table (unit tests that
 * build one by hand) reuses regions immediately, as before.
 */

bool tsdb_epoch_init(timeseries_db_t *db);
void tsdb_epoch_deinit(timeseries_db_t *db);

int tsdb_epoch_enter(timeseries_db_t *db);
void tsdb_epoch_exit(timeseries_db_t *db, int slot);
/* The caller holds no page offsets obtained before this call. */
void tsdb_epoch_quiesce(timeseries_db_t *db);
/* Mark the caller's slots idle (it waits holding no page offsets) or active
 * again (the slots move to the current epoch). */
void tsdb_epoch_idle(timeseries_db_t *db, bool idle);
/* Take flash_write_mutex; the caller's slots are idle while it blocks. Every
 * caller derives page offsets only after it holds the mutex. */
void tsdb_write_lock(timeseries_db_t *db);
/* Wait until no other active slot entered before this call; false after
 * timeout_ms. */
bool tsdb_epoch_synchronize(timeseries_db_t *db, uint32_t timeout_ms);

/* snapshot_mutex held: retire the regions old_snap lists and new_snap does not. */
void tsdb_retire_diff_locked(timeseries_db_t *db, const tsdb_page_cache_snapshot_t *old_snap,
                             const tsdb_page_cache_snapshot_t *new_snap);
/* snapshot_mutex held: retire one region leaving the live cache. */
void tsdb_retire_region_locked(timeseries_db_t *db, uint32_t offset, uint32_t size);
/* Forget retired regions that are live again (cache rebuilt from flash). */
void tsdb_retired_forget_live(timeseries_db_t *db);
/* Forget every retired region (the partition was erased after a grace period). */
void tsdb_retired_clear(timeseries_db_t *db);

/* A snapshot of every region the allocator must not hand out: the live pages
 * plus retired regions some active operation may still read. Release it with
 * tsdb_snapshot_release(). NULL on OOM. */
tsdb_page_cache_snapshot_t *tsdb_occupied_snapshot(timeseries_db_t *db);
/* True when a retired region is still pinned by an active operation of
 * another task (the allocator may wait for it instead of failing). */
bool tsdb_retired_pinned_by_others(timeseries_db_t *db);

#endif  // TIMESERIES_EPOCH_H
