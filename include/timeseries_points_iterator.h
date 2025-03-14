// timeseries_points_iterator.h
#pragma once

#include "esp_err.h"
#include "gorilla/gorilla_stream_decoder.h"
#include "timeseries_data.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  timeseries_db_t *db;
  bool valid;

  // Basic info
  uint16_t ts_count;
  timeseries_field_type_e series_type;
  bool is_compressed;
  bool is_column_format;

  // Gorilla decoder streams
  gorilla_decoder_stream_t ts_decoder;
  gorilla_decoder_stream_t val_decoder;

  // Compressed buffers (if compressed)
  uint8_t *ts_comp_buf;
  size_t ts_comp_len;
  uint8_t *val_comp_buf;
  size_t val_comp_len;

  // Index/counter
  uint16_t current_idx;

  // Uncompressed arrays
  uint64_t *ts_array;
  union {
    double *float_array;
    int64_t *int_array;
    bool *bool_array;
  };

  // ********** ADD THESE FIELDS **********
  // These store the decoding context so that incremental
  // gorilla_decoder_get_* calls are valid across function calls.
  DecoderContext ts_decoder_context;
  DecoderContext val_decoder_context;
  CompressedBuffer ts_cb_storage;
  CompressedBuffer val_cb_storage;

} timeseries_points_iterator_t;

/**
 * @brief Initialize the points iterator for a column-formatted record.
 *
 * This function reads and decodes the timestamp and value blocks from flash.
 */
bool timeseries_points_iterator_init(
    timeseries_db_t *db, uint32_t record_data_offset, uint32_t record_length,
    uint16_t record_count, timeseries_field_type_e series_type, bool compressed,
    timeseries_points_iterator_t *iter);

/**
 * @brief Get the next timestamp from the iterator.
 *
 * Returns true if a timestamp was read successfully, false if no more points.
 */
bool timeseries_points_iterator_next_timestamp(
    timeseries_points_iterator_t *iter, uint64_t *out_timestamp);

/**
 * @brief Get the next value from the iterator.
 *
 * The returned value is placed in out_value based on the field type.
 */
bool timeseries_points_iterator_next_value(timeseries_points_iterator_t *iter,
                                           timeseries_field_value_t *out_value);

/**
 * @brief Deinitialize the points iterator and free allocated memory.
 */
void timeseries_points_iterator_deinit(timeseries_points_iterator_t *iter);

#ifdef __cplusplus
}
#endif
