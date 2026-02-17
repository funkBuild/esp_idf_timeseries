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

typedef struct {
  uint32_t data_offset; // absolute offset to the record data (NOT including
                        // field_data_header_t)
  uint16_t record_length;
  uint16_t record_count;
  timeseries_field_type_e field_type;
  bool is_compressed;
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

/**
 * @brief Create a new level-1 field data page. For demonstration,
 *        this might be 64KB or 128KB in size, with field_data_level=1.
 *
 * @param db          Pointer to the timeseries_db_t context.
 * @param out_offset  If successful, returns the absolute offset of the new page
 * in flash.
 * @return true if page creation succeeded, false otherwise.
 *
 * @note This function is a placeholder in the skeleton. You need to implement
 *       the actual logic to find free space, erase, and write the page header
 *       with field_data_level=1, etc.
 */
bool tsdb_create_level1_field_page(timeseries_db_t *db, uint32_t *out_offset);

size_t
timeseries_field_value_serialize_size(const timeseries_field_value_t *val);

size_t timeseries_field_value_serialize(const timeseries_field_value_t *val,
                                        unsigned char *out_buf);

bool timeseries_compact_all_levels(timeseries_db_t *db);

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
