#ifndef TIMESERIES_PAGE_CACHE_H
#define TIMESERIES_PAGE_CACHE_H

#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_page_cache_snapshot.h"

bool tsdb_build_page_cache(timeseries_db_t *db);
bool tsdb_pagecache_add_entry(timeseries_db_t *db, uint32_t offset,
                              const timeseries_page_header_t *hdr);
bool tsdb_pagecache_remove_entry(timeseries_db_t *db, uint32_t offset);
uint32_t tsdb_pagecache_get_total_active_size(timeseries_db_t *db);
uint32_t tsdb_pagecache_get_page_size(timeseries_db_t *db,
                                      uint32_t page_offset);
void tsdb_pagecache_clear(timeseries_db_t *db);

// Batch API for compaction: a journal of the compaction's own cache changes
// (entries = pages it sets, removed_offsets = pages it removes; the last
// operation per offset wins), replayed onto the live cache at publication.
tsdb_page_cache_snapshot_t *tsdb_pagecache_begin_batch(timeseries_db_t *db);
// Prepare storage without changing entries or removal history.
bool tsdb_pagecache_batch_reserve(tsdb_page_cache_snapshot_t *snap, size_t additional);
bool tsdb_pagecache_batch_add(tsdb_page_cache_snapshot_t *snap, uint32_t offset,
                              const timeseries_page_header_t *hdr);
bool tsdb_pagecache_batch_remove(tsdb_page_cache_snapshot_t *snap, uint32_t offset);
void tsdb_pagecache_batch_sort(tsdb_page_cache_snapshot_t *snap);
// Reservations are published live with this page_type (all ones) until the
// real header replaces them; readers skip them.
#define TSDB_PLACEHOLDER_PAGE_TYPE 0xFF
bool tsdb_pagecache_is_placeholder(const timeseries_page_header_t *hdr);
// Remove offset from the live cache only if it is still a placeholder there.
// Caller holds flash_write_mutex (no other writer is mid-reservation).
bool tsdb_pagecache_remove_placeholder(timeseries_db_t *db, uint32_t offset);
// base (may be NULL) + the batch's journal - also_remove, with room for
// `spare` more entries. Allocates; does not touch the database. NULL on OOM.
tsdb_page_cache_snapshot_t *tsdb_pagecache_prepare_batch(const tsdb_page_cache_snapshot_t *base,
                                                         const tsdb_page_cache_snapshot_t *batch,
                                                         const uint32_t *also_remove, size_t also_remove_count,
                                                         size_t spare);
// Put base's entry for offset back into a prepared snapshot (spare room).
bool tsdb_pagecache_restore_entry(tsdb_page_cache_snapshot_t *next, const tsdb_page_cache_snapshot_t *base,
                                  uint32_t offset);
// Replay the journal onto the live snapshot and publish it. Consumes batch;
// on failure (OOM) the live snapshot is unchanged.
bool tsdb_pagecache_commit_batch(timeseries_db_t *db, tsdb_page_cache_snapshot_t *batch);

#endif // TIMESERIES_PAGE_CACHE_H
