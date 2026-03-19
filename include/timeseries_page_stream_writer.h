#ifndef TIMESERIES_PAGE_STREAM_WRITER_H
#define TIMESERIES_PAGE_STREAM_WRITER_H

#include "timeseries.h"
#include "timeseries_internal.h" // for timeseries_db_t, timeseries_field_value_t, etc.
#include <stdbool.h>
#include <stdint.h>

#include "gorilla/gorilla_stream_encoder.h" // for gorilla_stream_t

// 1) Define your flush context struct
typedef struct timeseries_page_stream_writer timeseries_page_stream_writer_t;

typedef struct {
  timeseries_page_stream_writer_t *writer;
  size_t *accum_bytes;
} gorilla_flush_ctx_t;

/**
 * A structure for streaming a new page, one (or more) series at a time,
 * using Gorilla compression for timestamps and values.
 */
typedef struct timeseries_page_stream_writer {
  // The database reference and current region info
  timeseries_db_t *db;
  uint32_t base_offset;
  uint32_t ts_end_offset;
  uint32_t write_ptr;
  uint32_t capacity;
  bool finalized;

  // Bookkeeping for the current series:
  timeseries_field_data_header_t fd_hdr;
  uint32_t fd_hdr_offset;
  uint32_t col_hdr_offset;
  timeseries_field_type_e series_field_type;

  // Gorilla streaming objects
  gorilla_stream_t ts_stream;
  gorilla_stream_t val_stream;
  size_t ts_bytes;
  size_t val_bytes;

  // Flush contexts used by your on-the-fly Gorilla compression
  gorilla_flush_ctx_t ts_flush_ctx;
  gorilla_flush_ctx_t val_flush_ctx;

  // Batch snapshot for compaction (when non-NULL, cache ops go through batch)
  tsdb_page_cache_snapshot_t *batch_snapshot;

} timeseries_page_stream_writer_t;

/**
 * Initializes a new page for streaming.
 * ...
 */
bool timeseries_page_stream_writer_init(timeseries_db_t *db,
                                        timeseries_page_stream_writer_t *writer,
                                        uint8_t to_level,
                                        uint32_t prev_data_size);

/**
 * Begins writing a new series within the current page.
 * ...
 */
bool timeseries_page_stream_writer_begin_series(
    timeseries_page_stream_writer_t *writer, const unsigned char series_id[16],
    timeseries_field_type_e ftype);

bool timeseries_page_stream_writer_write_timestamp(
    timeseries_page_stream_writer_t *writer, uint64_t ts);

bool timeseries_page_stream_writer_write_value(
    timeseries_page_stream_writer_t *writer,
    const timeseries_field_value_t *fv);

/**
 * Ends the current series.
 * ...
 */
bool timeseries_page_stream_writer_end_series(
    timeseries_page_stream_writer_t *writer);

/**
 * Finalizes the entire page.
 * ...
 */
bool timeseries_page_stream_writer_finalize(
    timeseries_page_stream_writer_t *writer);

/**
 * Abort the stream writer, cleaning up any in-progress state.
 *
 * Marks the partially-written page as OBSOLETE on flash so it will not
 * be re-scanned on reboot, and removes it from the page cache (or batch).
 * Must be called instead of finalize() when the write sequence fails
 * partway through.
 */
void timeseries_page_stream_writer_abort(
    timeseries_page_stream_writer_t *writer);

bool timeseries_page_stream_writer_finalize_timestamp(
    timeseries_page_stream_writer_t *writer);

/**
 * Write a pre-encoded ALP series directly to the page.
 * Sets COMPRESSED flag and clears ENCODING_ALP flag bit in the field_data_header.
 * Call instead of begin_series/write_timestamp/finalize_timestamp/write_value/end_series.
 */
bool timeseries_page_stream_writer_write_alp_series(
    timeseries_page_stream_writer_t *sw,
    const uint8_t series_id[16],
    const uint8_t *ts_buf,  size_t ts_len,   // alp_encode_int output
    const uint8_t *val_buf, size_t val_len,  // alp_encode / alp_encode_int output
    uint16_t record_count,
    uint64_t start_time,
    uint64_t end_time);

#endif // TIMESERIES_PAGE_STREAM_WRITER_H
