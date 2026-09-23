#pragma once
#include "timeseries_compaction.h"

/* Caller owns source-page claims and the private cache batch. Failure leaves
 * source retirement to the caller; partial output is aborted here. */
bool tsdb_compact_stream_records(timeseries_db_t *db, const tsdb_level_page_t *pages,
                                size_t page_count, uint8_t to_level,
                                tsdb_page_cache_snapshot_t *batch);

/* One live field-data record, as expiration found it. */
typedef struct {
  uint32_t page_offset;    // page start in the partition
  uint32_t page_sequence;  // detects page replacement at the same offset
  uint32_t record_offset;  // record header, relative to the page start
  timeseries_field_data_header_t header;
} tsdb_record_ref_t;

/* Write the points at or after `horizon` of every record in refs into a new
 * page of `to_level` through the private cache batch (the caller publishes or
 * abandons it). Records of a series without metadata are skipped. The source
 * records are not modified. Partial output is aborted here on failure. */
bool tsdb_trim_stream_records(timeseries_db_t *db, const tsdb_record_ref_t *refs, size_t n, uint64_t horizon,
                              uint8_t to_level, tsdb_page_cache_snapshot_t *batch);
