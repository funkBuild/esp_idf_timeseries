#ifndef TIMESERIES_PAGE_CACHE_H
#define TIMESERIES_PAGE_CACHE_H

#include "timeseries.h"
#include "timeseries_internal.h"

bool tsdb_build_page_cache(timeseries_db_t *db);
void tsdb_pagecache_add_entry(timeseries_db_t *db, uint32_t offset,
                              const timeseries_page_header_t *hdr);
bool tsdb_pagecache_remove_entry(timeseries_db_t *db, uint32_t offset);
uint32_t tsdb_pagecache_get_total_active_size(const timeseries_db_t *db);
uint32_t tsdb_pagecache_get_page_size(const timeseries_db_t *db,
                                      uint32_t page_offset);
void tsdb_pagecache_clear(timeseries_db_t *db);

#endif // TIMESERIES_PAGE_CACHE_H