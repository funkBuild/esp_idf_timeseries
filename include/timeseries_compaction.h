#ifndef TIMESERIES_COMPACTION_H
#define TIMESERIES_COMPACTION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "timeseries.h"
#include "timeseries_internal.h"
#include <stdbool.h>

/**
 * Minimal data structure for storing a single data point in memory:
 * timestamp + entire field_value_t
 */
typedef struct {
  uint64_t timestamp;
  timeseries_field_value_t field_val;
  uint32_t page_seq;  // page sequence number; higher = newer
} tsdb_compact_data_point_t;

/**
 * @brief In-memory buffer for a single series, used during compaction.
 *
 * If the same timestamp appears multiple times, we override the earlier value.
 */
typedef struct {
  unsigned char series_id[16];
  uint64_t start_ts;
  uint64_t end_ts;
  tsdb_compact_data_point_t *points;
  size_t count;
  size_t capacity;
} tsdb_series_buffer_t;

typedef struct {
  uint32_t offset;
  uint32_t size;
} tsdb_level_page_t;

#define MIN_PAGES_FOR_COMPACTION 4

/* Maximum points per ALP record written during compaction.
 * Bounded by the uint16_t record_count field in field_data_header_t.
 * Configurable via Kconfig (CONFIG_TIMESERIES_ALP_CHUNK_MAX_POINTS) so test
 * builds can use a small value to exercise the chunking code path without
 * needing 65535+ data points. */
#define TSDB_ALP_CHUNK_MAX_POINTS CONFIG_TIMESERIES_ALP_CHUNK_MAX_POINTS

typedef struct {
  uint32_t data_offset; // absolute offset to the record data (NOT including
                        // field_data_header_t)
  uint16_t record_length;
  uint16_t record_count;
  timeseries_field_type_e field_type;
  uint8_t data_flags; // raw fd_hdr.flags byte (replaces bool is_compressed)
  uint32_t page_seq; // for multi-iterator merging
} tsdb_record_descriptor_t;

typedef struct {
  unsigned char series_id[16];
  tsdb_record_descriptor_t *records;
  size_t record_used;
  size_t record_capacity;
} tsdb_series_descriptors_t;

/**
 * @brief Perform level-0 compaction of field data pages.
 *        Merges multiple single-value entries (seriesID + timestamp + fieldVal)
 *        into a single compressed entry in a new level-1 page.
 *
 * @param db Pointer to the timeseries_db_t context.
 * @return true if compaction succeeded, false otherwise.
 */
bool timeseries_compact_level0_pages(timeseries_db_t *db);

size_t
timeseries_field_value_serialize_size(const timeseries_field_value_t *val);

size_t timeseries_field_value_serialize(const timeseries_field_value_t *val,
                                        unsigned char *out_buf);

bool timeseries_compact_all_levels(timeseries_db_t *db);

/**
 * @brief Force compaction of all levels, ignoring MIN_PAGES_FOR_COMPACTION.
 *
 * Compacts any level that has >= 1 page. Useful for benchmarking where all
 * L0 pages must be compacted regardless of how many remain after auto-compaction.
 */
bool timeseries_compact_all_levels_force(timeseries_db_t *db);

bool timeseries_compact_level_pages(timeseries_db_t *db, uint8_t from_level,
                                    uint8_t to_level);

/**
 * @brief Compact a specific list of pages, re-writing each page without
 *        deleted records, and then marking the old page as obsolete.
 *        This is a "page-level" compaction method, used for expiration.
 *
 * @param db            Database handle
 * @param page_offsets  Array of page offsets
 * @param page_count    Number of pages in the array
 *
 * @return true on success (all pages processed), false on any error
 */
bool timeseries_compact_page_list(timeseries_db_t *db,
                                  const uint32_t *page_offsets,
                                  size_t page_count);

#ifdef __cplusplus
}
#endif

#endif // TIMESERIES_COMPACTION_H
