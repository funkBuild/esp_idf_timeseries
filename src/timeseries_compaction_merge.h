#pragma once
#include "timeseries_compaction.h"

/* Caller owns source-page claims and the private cache batch. Failure leaves
 * source retirement to the caller; partial output is aborted here. */
bool tsdb_compact_stream_records(timeseries_db_t *db, const tsdb_level_page_t *pages,
                                size_t page_count, uint8_t to_level,
                                tsdb_page_cache_snapshot_t *batch);
