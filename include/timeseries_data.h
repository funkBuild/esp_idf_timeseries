#ifndef TIMESERIES_DATA_H
#define TIMESERIES_DATA_H

#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_page_cache_snapshot.h"

typedef struct {
  uint32_t ts_len;  // number of bytes in compressed timestamps
  uint32_t val_len; // number of bytes in compressed values
} timeseries_col_data_header_t;

bool tsdb_append_field_data(timeseries_db_t *db,
                            const unsigned char series_id[16],
                            uint64_t timestamp_ms,
                            const timeseries_field_value_t *field_val);

bool tsdb_create_level1_field_page(timeseries_db_t *db, uint32_t *out_offset);
bool tsdb_append_multiple_points(timeseries_db_t *db,
                                 const unsigned char series_id[16],
                                 const uint64_t *timestamps,
                                 const timeseries_field_value_t *values,
                                 size_t count);

/**
 * @brief Reserve a blank region under the region_alloc_mutex.
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

#endif // TIMESERIES_DATA_H