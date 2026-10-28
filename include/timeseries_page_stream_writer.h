#ifndef TIMESERIES_PAGE_STREAM_WRITER_H
#define TIMESERIES_PAGE_STREAM_WRITER_H

#include "timeseries.h"
#include "timeseries_internal.h" // for timeseries_db_t, timeseries_field_value_t, etc.
#include <stdbool.h>
#include <stdint.h>

#include "gorilla/gorilla_stream_encoder.h" // for gorilla_stream_t

/*
 * Largest page a stream writer opens (compaction and expiration outputs). A
 * larger output continues in further pages, split between records, so the
 * allocator is never asked for one large contiguous region. A writer that
 * needed its whole source size in one piece could not compact once free space
 * was fragmented (by a rotating allocation origin, timeseries_data.h, or by
 * fill alone), leaving inserts without the space the compaction would have
 * freed. A single record (under 64 KiB) may still grow one page past this.
 */
#ifndef TSDB_STREAM_PAGE_MAX_SIZE
#define TSDB_STREAM_PAGE_MAX_SIZE (32 * 1024U)
#endif

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
  // The database reference and the page under construction. The page is
  // built in page_buf and written to flash at finalize, into a region of its
  // exact size; offsets below are relative to the page start.
  timeseries_db_t *db;
  uint8_t *page_buf;
  uint32_t base_offset;   // flash offset of the last page finalized
  uint32_t ts_end_offset;
  uint32_t write_ptr;
  uint32_t capacity;      // bytes the page may use before the next one starts
  bool reserved;          // base_offset is reserved for a page not yet finalized
  bool finalized;
  uint8_t level;         // level of every page this writer opens
  uint32_t source_left;  // source bytes not yet covered by finished pages (sizes the next page)

  // Optional borrowed revisions for the next record, supplied by compaction.
  // Count 1 broadcasts a single revision to every point.
  const uint64_t *point_revisions;
  size_t point_revision_count;

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
 * Frees the page under construction (nothing of it is on flash), and gives
 * back the region of a page whose finalize failed. Pages already finalized
 * stay with the caller. Must be called instead of finalize() when the write
 * sequence fails partway through.
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
