#ifndef TIMESERIES_EXPIRATION_H
#define TIMESERIES_EXPIRATION_H

#include "timeseries_internal.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Checks database usage and if above threshold, expires (deletes)
 *        oldest data and triggers compaction.
 *
 * On a full, fragmented partition, where the records straddling the horizon
 * cannot be rewritten for lack of room, the oldest page is dropped first
 * (records of the same series overlapping it go with it), then the run
 * retries; that drop removes whole records rather than at one horizon.
 *
 * @param db                Pointer to your timeseries database handle.
 * @param usage_threshold   Fraction of total partition size at which to
 *                          trigger expiration (e.g. 0.90 = 90%).
 * @return true on success or when usage is below threshold; false on error or
 *         when compaction/another maintenance operation owns the slot. A busy
 *         result leaves records unchanged and can be retried.
 */
bool timeseries_expiration_run(timeseries_db_t *db, float usage_threshold,
                               float reduction_threshold);

/*
 * The functions below run with the maintenance slot (compaction_in_progress)
 * already held by the caller, and may use the free-space reserves
 * (timeseries_data.h).
 */

/* Expire the oldest data until about bytes_to_free is freed (one horizon).
 * When the trims find no room (a full, fragmented partition), the oldest
 * page is dropped first (tsdb_drop_oldest_page_held) and the run retried. */
bool tsdb_expire_bytes_held(timeseries_db_t *db, uint32_t bytes_to_free);

/* Reclaim the space of deleted records: pages without live records are
 * retired (no allocation); with `rewrite`, pages whose rewrite frees at least
 * one sector are rewritten without them, most freed first, while room
 * allows. *reclaimed: bytes of the pages retired. */
bool tsdb_reclaim_deleted_held(timeseries_db_t *db, bool rewrite, uint32_t *reclaimed);

/* Free space without allocating: retire the pages left without live
 * records if there are any, else delete every record of the field-data page
 * whose newest point is oldest, with the records of the same series that
 * overlap them in time, and retire the pages left without live records.
 * *dropped: whether any space was freed (false: nothing left to drop). */
bool tsdb_drop_oldest_page_held(timeseries_db_t *db, bool *dropped);

/* Background clearing (db->space.drop_oldest): whether usable free space is
 * below db->space.clear_free or usage above max_usage_percent, as of the last
 * allocation. */
bool tsdb_clear_space_needed(timeseries_db_t *db);
/* Make usable free space at least clear_free and usage at most
 * max_usage_percent, plus a step: reclaim deleted records first, then expire
 * the oldest data, and drop the oldest page if that expiration frees
 * nothing. */
bool tsdb_clear_space_held(timeseries_db_t *db);

#ifdef __cplusplus
}
#endif

#endif /* TIMESERIES_EXPIRATION_H */
