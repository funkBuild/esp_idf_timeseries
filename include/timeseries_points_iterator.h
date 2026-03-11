// timeseries_points_iterator.h
#pragma once

#include "esp_err.h"
#include "gorilla/gorilla_stream_decoder.h"
#include "alp/alp_stream_decoder.h"
#include "timeseries_data.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  uint32_t length;
  char *str;
} string_array_t;

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
    string_array_t *string_array;
  };

  // ALP decode state (non-NULL when encoding=ALP)
  int64_t *alp_ts;
  union {
    double  *alp_float;
    int64_t *alp_int;
  };

  // ALP streaming decoder (for records with > 128 values, decodes on demand)
  // When non-NULL, alp_float/alp_int are NOT used; values come from this decoder.
  uint8_t *alp_comp_buf;              // retained compressed buffer
  alp_stream_decoder_t *alp_val_dec;  // streaming value decoder

  // Gorilla batch-decoded timestamps (non-NULL when Gorilla ts are pre-decoded)
  // Values are still streamed via Gorilla decoders.
  int64_t *gorilla_batch_ts;

  // Gorilla decoder context (stores state across incremental decode calls)
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
    uint16_t record_count, timeseries_field_type_e series_type,
    uint8_t data_flags,       // raw fd_hdr.flags byte (replaces bool compressed)
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
 * @brief Trim the iterator to only yield points within [start_ms, end_ms].
 *
 * Must be called after init, before any next_timestamp/next_value calls.
 * Uses binary search on the decoded timestamp array (ALP and uncompressed
 * paths only; Gorilla streaming path is unaffected).
 *
 * @param iter      Initialized iterator
 * @param start_ms  Lower bound (0 = no lower bound)
 * @param end_ms    Upper bound (0 = no upper bound)
 */
void timeseries_points_iterator_set_time_range(
    timeseries_points_iterator_t *iter, uint64_t start_ms, uint64_t end_ms);

/**
 * @brief Deinitialize the points iterator and free allocated memory.
 */
void timeseries_points_iterator_deinit(timeseries_points_iterator_t *iter);

#ifdef __cplusplus
}
#endif
