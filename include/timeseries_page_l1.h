#ifndef TIMESERIES_PAGE_L1_H
#define TIMESERIES_PAGE_L1_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

bool tsdb_write_page_dynamic(timeseries_db_t *db,
                             const tsdb_series_buffer_t *series_bufs,
                             size_t series_count, unsigned int level);

#ifdef __cplusplus
}

#endif
#endif // TIMESERIES_PAGE_L1_H