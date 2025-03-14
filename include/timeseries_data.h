#ifndef TIMESERIES_DATA_H
#define TIMESERIES_DATA_H

#include "timeseries.h"
#include "timeseries_internal.h"

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

#endif // TIMESERIES_DATA_H