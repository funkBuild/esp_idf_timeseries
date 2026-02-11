#ifndef TIMESERIES_PAGE_CACHE_H
#define TIMESERIES_PAGE_CACHE_H

#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_page_cache_snapshot.h"

bool tsdb_build_page_cache(timeseries_db_t *db);
void tsdb_pagecache_add_entry(timeseries_db_t *db, uint32_t offset,
                              const timeseries_page_header_t *hdr);
bool tsdb_pagecache_remove_entry(timeseries_db_t *db, uint32_t offset);
uint32_t tsdb_pagecache_get_total_active_size(timeseries_db_t *db);
uint32_t tsdb_pagecache_get_page_size(timeseries_db_t *db,
                                      uint32_t page_offset);
void tsdb_pagecache_clear(timeseries_db_t *db);

// Batch API for compaction (operates on a private mutable snapshot)
tsdb_page_cache_snapshot_t *tsdb_pagecache_begin_batch(timeseries_db_t *db);
void tsdb_pagecache_batch_add(tsdb_page_cache_snapshot_t *snap, uint32_t offset,
                              const timeseries_page_header_t *hdr);
bool tsdb_pagecache_batch_remove(tsdb_page_cache_snapshot_t *snap, uint32_t offset);
void tsdb_pagecache_batch_sort(tsdb_page_cache_snapshot_t *snap);
void tsdb_pagecache_commit_batch(timeseries_db_t *db, tsdb_page_cache_snapshot_t *batch);

#endif // TIMESERIES_PAGE_CACHE_H
