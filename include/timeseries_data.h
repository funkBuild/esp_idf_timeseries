#ifndef TIMESERIES_DATA_H
#define TIMESERIES_DATA_H

#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_page_cache_snapshot.h"

typedef struct {
  uint32_t ts_len;  // number of bytes in compressed timestamps
  uint32_t val_len; // number of bytes in compressed values
} timeseries_col_data_header_t;

_Static_assert(sizeof(timeseries_col_data_header_t) == 8, "unexpected padding in col_data_header");

/* Reject values that cannot fit in one L0 record, before reserving flash. */
bool tsdb_validate_point_values(const timeseries_field_value_t *values, size_t count);

/* A borrowed-input ordering plan. NULL indices denotes an already ordered,
 * unique batch. Otherwise indices selects the last input at each timestamp. */
typedef struct {
  size_t *indices;
  size_t count;
  size_t source_count;
} tsdb_point_order_t;
bool tsdb_point_order_init(const uint64_t *timestamps, size_t count, tsdb_point_order_t *order);
void tsdb_point_order_deinit(tsdb_point_order_t *order);
bool tsdb_append_ordered_points(timeseries_db_t *db, const unsigned char series_id[16],
                                const uint64_t *timestamps, const timeseries_field_value_t *values,
                                const tsdb_point_order_t *order);

/* As tsdb_append_ordered_points, but when schema_gen is non-NULL, every time
 * flash_write_mutex is taken the database schema generation must still equal
 * *schema_gen; otherwise nothing more is written, *stale is set and false is
 * returned (the caller restarts from its metadata lookups). */
bool tsdb_append_ordered_points_checked(timeseries_db_t *db, const unsigned char series_id[16],
                                        const uint64_t *timestamps, const timeseries_field_value_t *values,
                                        const tsdb_point_order_t *order, const uint32_t *schema_gen,
                                        bool *stale);
bool tsdb_append_multiple_points(timeseries_db_t *db,
                                 const unsigned char series_id[16],
                                 const uint64_t *timestamps,
                                 const timeseries_field_value_t *values,
                                 size_t count);

/*
 * Wear leveling. Plain first-fit from offset 0 reused the lowest free space
 * first, so the start of the partition took most erases (in the host B1
 * benchmark the hottest sector took ~6.5x the mean erases; flash wears out at
 * its hottest sector). Two policies now spread them:
 *
 * - Every region except an L0 page: first-fit from an origin that moves one
 *   sector every TSDB_WEAR_ROTATE_PAGES page allocations and wraps at the end.
 *   Long-lived pages still pack densely after the origin, which keeps large
 *   free runs (a faster-moving origin fragments free space), while the busy
 *   zone drifts round the whole partition. The origin derives from
 *   sequence_num, which startup recovers from the page headers.
 * - L0 pages: next-fit from db->l0_cursor (just past the previous L0 page;
 *   startup resumes after the newest page). They live only until compaction,
 *   so scattering them costs little contiguity, whereas with first-fit each
 *   one reused the first hole ahead of the origin as soon as it was freed.
 *
 * B1 at the product's 50% usage threshold: hottest sector 1.26x the mean.
 * A region may start inside a free run; see unshadow_region().
 */
#ifndef TSDB_WEAR_ROTATE_PAGES
#define TSDB_WEAR_ROTATE_PAGES 16u
#endif

/*
 * Free space and reserves (db->space, set from Kconfig by
 * tsdb_space_config_default()).
 *
 * Compaction and expiration write before they free: a compaction writes its
 * outputs before it retires its sources, and an expiration trims the records
 * that straddle its horizon before it deletes. Allocations therefore leave
 * free space for them in tiers:
 *
 * - an L0 page (insert) leaves db->space.insert_reserve;
 * - any other region (compaction, metadata) leaves maintenance_reserve;
 * - a task that frees space (expiration, reserve_exempt_task) may use all.
 *
 * Space is counted in free runs of at least TSDB_USABLE_RUN_MIN: smaller
 * holes rarely hold the pages maintenance writes. The count is a sum, not
 * the largest run, because compaction and expiration outputs continue in
 * further pages (TSDB_STREAM_PAGE_MAX_SIZE), and a page that fits no free
 * run is split between records (timeseries_page_stream_writer.c).
 *
 * A compaction run takes only the oldest pages whose outputs fit in the
 * usable free space above the insert reserve (above L0, outputs can be as
 * large as their sources), so it completes alongside inserts.
 *
 * Reserves alone cannot keep a full partition writable: free space
 * fragments until an expiration cannot place its trims. Dropping the oldest
 * page allocates nothing (tsdb_drop_oldest_page_held(),
 * timeseries_expiration.h): an expiration whose trims find no room drops
 * one first, and with db->space.drop_oldest an insert that finds no room
 * drops pages until an L0 page fits, while the background task clears the
 * oldest data before free space gets that low (tsdb_clear_space_held()).
 * Deleted records are reclaimed before any live data is cleared.
 */
#define TSDB_USABLE_RUN_MIN (16 * 1024u)

/* From Kconfig, capped for a small partition (reserves at most 1/8 and 1/16
 * of it, clearing below at most 1/4). db->partition must be set. */
void tsdb_space_config_default(timeseries_db_t *db);
/* Usable free space now (free runs of at least TSDB_USABLE_RUN_MIN), also
 * stored in db->usable_free. Takes snapshot_mutex. */
uint32_t tsdb_usable_free(timeseries_db_t *db);
/* Whether a new L0 page would be granted now, leaving the insert reserve.
 * Waits (bounded) for space another operation may still read, as the
 * allocator does. */
bool tsdb_l0_page_fits(timeseries_db_t *db);

/**
 * @brief Reserve a blank region under the region_alloc_mutex.
 *
 * region_alloc_mutex is a leaf lock that serializes only the allocator (blank
 * scan, erase, placeholder publication); callers may or may not hold
 * flash_write_mutex. A region is blank only if it is neither live nor retired
 * and still readable by an active operation (timeseries_epoch.h).
 *
 * Finds a blank region, erases it, and registers a placeholder header
 * in both the live and batch snapshots to prevent double-allocation.
 *
 * @param db              Database handle
 * @param min_size        Minimum contiguous size required
 * @param batch_snapshot  If non-NULL, the compaction batch snapshot
 * @param out_offset      Returns the start offset of the allocated region
 * @param out_size        Returns the size of the allocated region
 * @return true on success, false if no region found or erase failed
 */
bool tsdb_reserve_blank_region(timeseries_db_t *db, uint32_t min_size,
                               tsdb_page_cache_snapshot_t *batch_snapshot,
                               uint32_t *out_offset, uint32_t *out_size);
bool tsdb_finalize_reserved_region(timeseries_db_t *db, uint32_t offset,
                                   const timeseries_page_header_t *hdr,
                                   tsdb_page_cache_snapshot_t *batch_snapshot);
void tsdb_release_reserved_region(timeseries_db_t *db, uint32_t offset,
                                  tsdb_page_cache_snapshot_t *batch_snapshot);

#endif // TIMESERIES_DATA_H
