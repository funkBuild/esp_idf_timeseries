// timeseries_page_stream_writer.c

#include "timeseries_epoch.h"
#include "timeseries_page_stream_writer.h"
#include "timeseries_revision.h"
#include "timeseries_flash_utils.h"

#include "esp_log.h"
#include "esp_partition.h"
#include <inttypes.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "esp_timer.h"
#include "gorilla/gorilla_stream_encoder.h"  // Gorilla streaming interface
#include "timeseries_data.h"                 // for tsdb_reserve_blank_region, timeseries_blank_iterator_*
#include "timeseries_flash_utils.h"          // for tsdb_round_up_4k, tsdb_flash_write_byte
#include "timeseries_internal.h"             // definition of timeseries_db_t, etc.
#include "timeseries_iterator.h"             // for timeseries_blank_iterator_t

#include "timeseries_page_cache.h"  // for tsdb_pagecache_add_entry()
#include "timeseries_page_cache_snapshot.h"

static const char* TAG = "PAGE_STREAM_WRITER";

// ----------------------------------------------------------------------
// The page under construction
// ----------------------------------------------------------------------
//
// A page is built in RAM (writer->page_buf, PSRAM via malloc) and written to
// flash once, at finalize, into a region reserved at exactly its final size.
// Nothing is reserved while records are encoded, a page never moves, and its
// header is written once: shrinking a reserved page at finalize needed a
// second erase of its first sector (page_size bits 0 -> 1) and left the
// unused tail as a small hole other pages rarely fit. Offsets in the writer
// (write_ptr, fd_hdr_offset, col_hdr_offset, ts_end_offset) are relative to
// the page start.

/**
 * Ensures that `writer->write_ptr + needed_bytes` fits within the page. If it
 * does not, the page grows to fit, rounded up to 4096.
 */
static bool ensure_capacity(timeseries_page_stream_writer_t* writer, size_t needed_bytes) {
  // Subtraction, never write_ptr + needed_bytes: the addition can wrap
  // uint32 for a corrupt/huge needed_bytes and falsely report "fits".
  uint32_t avail = writer->write_ptr <= writer->capacity ? writer->capacity - writer->write_ptr : 0;
  if (needed_bytes <= avail) {
    return true;  // enough space already
  }
  if (needed_bytes > writer->db->partition->size - writer->write_ptr) {
    ESP_LOGE(TAG, "Page cannot hold %zu more bytes", needed_bytes);
    return false;
  }
  uint32_t new_size = tsdb_round_up_4k(writer->write_ptr + (uint32_t)needed_bytes);
  uint8_t* grown = realloc(writer->page_buf, new_size);
  if (!grown) {
    ESP_LOGE(TAG, "OOM growing page buffer to %" PRIu32 " bytes", new_size);
    return false;
  }
  memset(grown + writer->capacity, 0xFF, new_size - writer->capacity);
  ESP_LOGV(TAG, "Page grows from %" PRIu32 " to %" PRIu32 " bytes", writer->capacity, new_size);
  writer->page_buf = grown;
  writer->capacity = new_size;
  return true;
}

// Copies `len` bytes to page offset `at`, inside the capacity already ensured.
static void page_put(timeseries_page_stream_writer_t* writer, uint32_t at, const void* data, size_t len) {
  if (len) memcpy(writer->page_buf + at, data, len);
}

// Appends `len` bytes at write_ptr, growing the page as needed.
static bool page_append(timeseries_page_stream_writer_t* writer, const void* data, size_t len) {
  if (!ensure_capacity(writer, len)) return false;
  page_put(writer, writer->write_ptr, data, len);
  writer->write_ptr += len;
  return true;
}

/**
 * Gorilla flush callback: appends compressed bytes to the page.
 */
static bool gorilla_flush_to_page(void* context, const uint8_t* data, size_t len) {
  gorilla_flush_ctx_t* ctx = (gorilla_flush_ctx_t*)context;
  if (!page_append(ctx->writer, data, len)) return false;
  *(ctx->accum_bytes) += len;
  return true;
}

// ----------------------------------------------------------------------
//  Implementation of the streaming writer interface
// ----------------------------------------------------------------------

static bool prepare_revisions(timeseries_page_stream_writer_t *writer, size_t count,
                              uint8_t **encoded, size_t *length, uint8_t *flags) {
  uint64_t revision = 0;
  if (writer->point_revisions) {
    if (writer->point_revision_count != count && writer->point_revision_count != 1) return false;
  } else {
    if (writer->db->flash_write_mutex) tsdb_write_lock(writer->db);
    bool ok = tsdb_revision_next_locked(writer->db, &revision);
    if (writer->db->flash_write_mutex) xSemaphoreGive(writer->db->flash_write_mutex);
    if (!ok) return false;
  }
  if (writer->point_revisions && writer->point_revision_count == 1)
    return tsdb_revision_encode(NULL, 0, writer->point_revisions[0], encoded, length, flags);
  return tsdb_revision_encode(writer->point_revisions, count, revision, encoded, length, flags);
}

// Starts a new output page at writer->level with room for `needed` bytes
// after its header: sized to the source bytes still expected, at most
// TSDB_STREAM_PAGE_MAX_SIZE unless `needed` itself is larger. The header is
// completed at finalize. On failure the writer is unchanged.
static bool open_page(timeseries_page_stream_writer_t* writer, uint32_t needed) {
  timeseries_db_t* db = writer->db;
  uint32_t want = writer->source_left < TSDB_STREAM_PAGE_MAX_SIZE ? writer->source_left : TSDB_STREAM_PAGE_MAX_SIZE;
  if (needed > db->partition->size - sizeof(timeseries_page_header_t)) return false;
  uint32_t fit = needed + (uint32_t)sizeof(timeseries_page_header_t);
  uint32_t initial_size = tsdb_round_up_4k(want > fit ? want : fit);
  uint8_t* buf = malloc(initial_size);
  if (!buf) {
    ESP_LOGE(TAG, "OOM allocating a %" PRIu32 "-byte page buffer", initial_size);
    return false;
  }
  memset(buf, 0xFF, initial_size);
  free(writer->page_buf);
  writer->page_buf = buf;
  writer->base_offset = 0;
  writer->write_ptr = sizeof(timeseries_page_header_t);
  writer->capacity = initial_size;
  writer->finalized = false;
  return true;
}

bool timeseries_page_stream_writer_init(timeseries_db_t* db, timeseries_page_stream_writer_t* writer, uint8_t to_level,
                                        uint32_t prev_data_size) {
  if (!db || !writer || !db->partition) {
    return false;
  }
  // Preserve batch_snapshot that may have been set before init
  tsdb_page_cache_snapshot_t *saved_batch = writer->batch_snapshot;
  memset(writer, 0, sizeof(*writer));
  writer->batch_snapshot = saved_batch;
  writer->db = db;
  writer->level = to_level;
  writer->source_left = prev_data_size;
  return open_page(writer, 0);
}

// Finishes the current page and continues the output in a new one. Called
// only between records: a record never spans pages. The finished page is
// published like a single output (batch or live cache); if the output then
// fails, the caller gives back the finished pages (a compaction batch is
// abandoned whole).
static bool rollover(timeseries_page_stream_writer_t* writer, uint32_t needed) {
  uint32_t used = writer->write_ptr;
  if (!timeseries_page_stream_writer_finalize(writer)) return false;
  writer->source_left = used < writer->source_left ? writer->source_left - used : 0;
  writer->finalized = false;  // the writer continues; abort now refers to the finished page
  ESP_LOGD(TAG, "Output page @0x%08" PRIx32 " full (%" PRIu32 " B); continuing in a new page",
           writer->base_offset, used);
  return open_page(writer, needed);
}

static bool page_has_records(const timeseries_page_stream_writer_t* writer) {
  return writer->write_ptr > sizeof(timeseries_page_header_t);
}

// Room for a whole record of `needed` bytes at a record boundary: a new page
// once this one holds records, else (one oversized record) a larger page.
static bool make_room_for_record(timeseries_page_stream_writer_t* writer, uint32_t needed) {
  uint32_t avail = writer->write_ptr <= writer->capacity ? writer->capacity - writer->write_ptr : 0;
  if (needed <= avail) return true;
  if (page_has_records(writer)) return rollover(writer, needed);
  return ensure_capacity(writer, needed);
}

// Space left in a page below which a Gorilla-streamed record (size unknown
// until it ends) starts a new page rather than grow this one.
#define SERIES_HEADROOM 4096u

bool timeseries_page_stream_writer_begin_series(timeseries_page_stream_writer_t* writer,
                                                const unsigned char series_id[16], timeseries_field_type_e ftype) {
  if (!writer || writer->finalized) {
    return false;
  }
  uint32_t avail = writer->write_ptr <= writer->capacity ? writer->capacity - writer->write_ptr : 0;
  if (avail < SERIES_HEADROOM && page_has_records(writer) && !rollover(writer, SERIES_HEADROOM)) {
    return false;
  }

  // Prepare field_data_header but don't write it until the end
  memset(&writer->fd_hdr, 0xff, sizeof(writer->fd_hdr));
  memcpy(writer->fd_hdr.series_id, series_id, 16);
  writer->fd_hdr.flags &= ~TSDB_FIELDDATA_FLAG_COMPRESSED;
  if (!ensure_capacity(writer, sizeof(writer->fd_hdr) + sizeof(timeseries_col_data_header_t))) return false;
  writer->fd_hdr_offset = writer->write_ptr;
  writer->fd_hdr.record_count = 0;
  writer->fd_hdr.start_time = UINT64_MAX;
  writer->fd_hdr.end_time = 0;

  writer->write_ptr += sizeof(writer->fd_hdr);
  writer->col_hdr_offset = writer->write_ptr;
  writer->write_ptr += sizeof(timeseries_col_data_header_t);

  // Prepare Gorilla streams (timestamps & values)
  memset(&writer->ts_stream, 0, sizeof(writer->ts_stream));
  memset(&writer->val_stream, 0, sizeof(writer->val_stream));
  writer->ts_bytes = 0;
  writer->val_bytes = 0;
  writer->ts_end_offset = 0;  // <--- NEW field to track TS end offset
  writer->series_field_type = ftype;

  // Set up flush contexts
  writer->ts_flush_ctx.writer = writer;
  writer->ts_flush_ctx.accum_bytes = &writer->ts_bytes;

  writer->val_flush_ctx.writer = writer;
  writer->val_flush_ctx.accum_bytes = &writer->val_bytes;

  // Initialize Gorilla streams
  // For timestamps we use GORILLA_STREAM_INT (64-bit integer deltas)
  if (!gorilla_stream_init(&writer->ts_stream, GORILLA_STREAM_INT,
                           /*initial_xor=*/NULL, 0 /*leading_zeros*/, gorilla_flush_to_page, &writer->ts_flush_ctx)) {
    ESP_LOGE(TAG, "Failed to init Gorilla ts_stream");
    return false;
  }

  // For values, pick an appropriate Gorilla mode
  gorilla_stream_type_t val_mode = GORILLA_STREAM_INT;  // default
  switch (ftype) {
    case TIMESERIES_FIELD_TYPE_FLOAT:
      val_mode = GORILLA_STREAM_FLOAT;
      break;
    case TIMESERIES_FIELD_TYPE_BOOL:
      val_mode = GORILLA_STREAM_BOOL;
      break;
    case TIMESERIES_FIELD_TYPE_STRING:
      ESP_LOGI(TAG, "Setting val_mode to GORILLA_STREAM_STRING");
      val_mode = GORILLA_STREAM_STRING;
      break;
    default:
      val_mode = GORILLA_STREAM_INT;
      break;
  }

  if (!gorilla_stream_init(&writer->val_stream, val_mode,
                           /*initial_xor=*/NULL, 0 /*leading_zeros*/, gorilla_flush_to_page, &writer->val_flush_ctx)) {
    ESP_LOGE(TAG, "Failed to init Gorilla val_stream");
    // best-effort cleanup
    gorilla_stream_deinit(&writer->ts_stream);
    return false;
  }

  return true;
}

/*
 * Write only the timestamp portion for a new data point.
 * Updates min/max timestamps and increments record_count.
 */
bool timeseries_page_stream_writer_write_timestamp(timeseries_page_stream_writer_t* writer, uint64_t ts) {
  if (!writer || writer->finalized) {
    return false;
  }

  // Update min/max timestamps
  if ((writer->fd_hdr.record_count == 0) || (ts < writer->fd_hdr.start_time)) {
    writer->fd_hdr.start_time = ts;
  }

  if (ts > writer->fd_hdr.end_time) {
    writer->fd_hdr.end_time = ts;
  }

  // Add timestamp to the Gorilla TS stream
  if (!gorilla_stream_add_timestamp(&writer->ts_stream, ts)) {
    ESP_LOGE(TAG, "Failed adding timestamp to gorilla ts_stream");
    return false;
  }

  // Increment record count AFTER successful write
  writer->fd_hdr.record_count++;

  return true;
}

/*
 * ----------------------------------------------------------------------------
 * IMPLEMENTATION of timeseries_page_stream_writer_finalize_timestamp()
 * ----------------------------------------------------------------------------
 * Finishes the Gorilla TS stream and records the offset at which timestamps
 * end. DOES NOT finalize or write the column header yet; that’s done in
 * end_series along with the value data.
 */
bool timeseries_page_stream_writer_finalize_timestamp(timeseries_page_stream_writer_t* writer) {
  if (!writer) {
    ESP_LOGE(TAG, "Invalid writer");
    return false;
  }

  // Finalize the Gorilla TS stream (flush final bits to flash)
  if (!gorilla_stream_finish(&writer->ts_stream)) {
    ESP_LOGE(TAG, "Failed finishing gorilla ts_stream");
    gorilla_stream_deinit(&writer->ts_stream);  // avoid leaking the encoder on failure
    return false;
  }
  // Capture the offset where TS data ends
  writer->ts_end_offset = writer->write_ptr;
  gorilla_stream_deinit(&writer->ts_stream);

  // We can log or check for overflow here if desired.
  // For example, the user might want to confirm that `ts_bytes <= 65535`.
  if (writer->ts_bytes > 0xFFFF) {
    ESP_LOGE(TAG, "TS bytes overflowed 16-bit limit in finalize_timestamp()");
    return false;
  }

  ESP_LOGV(TAG,
           "Series %02X%02X%02X%02X: finalize_timestamp => ts=%u bytes "
           "(offset=0x%08" PRIx32 ")",
           writer->fd_hdr.series_id[0], writer->fd_hdr.series_id[1], writer->fd_hdr.series_id[2],
           writer->fd_hdr.series_id[3], (unsigned int)writer->ts_bytes, writer->ts_end_offset);

  // Make sure the compressed buffer now

  return true;
}

/**
 * Write only the value portion for the previously added timestamp.
 * (Assumes a 1:1 mapping between timestamps and values.)
 */
bool timeseries_page_stream_writer_write_value(timeseries_page_stream_writer_t* writer,
                                               const timeseries_field_value_t* fv) {
  if (!writer || !fv || writer->finalized) {
    return false;
  }

  // Add value to the Gorilla VAL stream
  switch (writer->series_field_type) {
    case TIMESERIES_FIELD_TYPE_INT: {
      int64_t i64 = fv->data.int_val;
      // Reusing add_timestamp() for storing the integer,
      // or you might have a dedicated gorilla_stream_add_int64() call.
      if (!gorilla_stream_add_timestamp(&writer->val_stream, (uint64_t)i64)) {
        ESP_LOGE(TAG, "Failed adding INT value to val_stream");
        return false;
      }
      break;
    }
    case TIMESERIES_FIELD_TYPE_FLOAT: {
      double d = fv->data.float_val;
      if (!gorilla_stream_add_float(&writer->val_stream, d)) {
        ESP_LOGE(TAG, "Failed adding FLOAT value to val_stream");
        return false;
      }
      break;
    }
    case TIMESERIES_FIELD_TYPE_BOOL: {
      bool b = fv->data.bool_val;
      if (!gorilla_stream_add_boolean(&writer->val_stream, b)) {
        ESP_LOGE(TAG, "Failed adding BOOL value to val_stream");
        return false;
      }
      break;
    }
    case TIMESERIES_FIELD_TYPE_STRING: {
      if (!gorilla_stream_add_string(&writer->val_stream, fv->data.string_val.str, fv->data.string_val.length)) {
        ESP_LOGE(TAG, "Failed adding STRING value to val_stream");
        return false;
      }

      break;
    }
    default:
      // Not supported in this compression path
      ESP_LOGV(TAG, "Unsupported field type %d for streaming compression", (int)writer->series_field_type);
      return false;
  }

  return true;
}

/*
 * ----------------------------------------------------------------------------
 * timeseries_page_stream_writer_end_series()
 * NOW does NOT finalize timestamps. That is done by finalize_timestamp().
 * We *only* finalize the VALUE stream here.
 * ----------------------------------------------------------------------------
 */
bool timeseries_page_stream_writer_end_series(timeseries_page_stream_writer_t* writer) {
  if (!writer) {
    ESP_LOGE(TAG, "Invalid writer");
    return false;
  }

  /*
   * 1) We have removed the timestamp finalization code.
   *    finalize_timestamp() should have been called earlier.
   */

  // 2) Finalize the Gorilla VAL stream (flush final bits to flash)
  if (!gorilla_stream_finish(&writer->val_stream)) {
    ESP_LOGE(TAG, "Failed finishing gorilla val_stream");
    gorilla_stream_deinit(&writer->val_stream);  // avoid leaking the encoder on failure
    return false;
  }
  // Capture the end offset for values
  uint32_t val_end = writer->write_ptr;
  gorilla_stream_deinit(&writer->val_stream);

  uint32_t ts_len = writer->ts_bytes;       // as recorded
  uint32_t ts_end = writer->ts_end_offset;  // offset where TS ended
  uint32_t val_len = val_end - ts_end;      // compute difference

  // 4) Patch the col_data_header with the correct lengths
  timeseries_col_data_header_t col_hdr;
  col_hdr.ts_len = ts_len;
  col_hdr.val_len = val_len;

  ESP_LOGV(TAG, "Series %02X%02X%02X%02X: end_series => ts=%u, val=%u bytes", writer->fd_hdr.series_id[0],
           writer->fd_hdr.series_id[1], writer->fd_hdr.series_id[2], writer->fd_hdr.series_id[3],
           (unsigned int)col_hdr.ts_len, (unsigned int)col_hdr.val_len);

  page_put(writer, writer->col_hdr_offset, &col_hdr, sizeof(col_hdr));

  // 5) If no points were added, fix the header's timestamps
  if (writer->fd_hdr.record_count == 0) {
    ESP_LOGV(TAG, "Series %02X%02X%02X%02X... has no points", writer->fd_hdr.series_id[0], writer->fd_hdr.series_id[1],
             writer->fd_hdr.series_id[2], writer->fd_hdr.series_id[3]);
    writer->fd_hdr.start_time = 0;
    writer->fd_hdr.end_time = 0;
  }

  // 6) The "record_length" is total space for col_hdr + TS + VAL
  uint32_t total_col_bytes = sizeof(col_hdr) + ts_len + val_len;
  if (total_col_bytes > 0xFFFF) {
    ESP_LOGE(TAG, "Series chunk = %u bytes (ts=%u, val=%u), exceeds 16-bit limit!",
             (unsigned int)total_col_bytes, (unsigned int)ts_len, (unsigned int)val_len);
    return false;
  }
  uint8_t *revisions = NULL;
  size_t revisions_len = 0;
  if (!prepare_revisions(writer, writer->fd_hdr.record_count, &revisions, &revisions_len, &writer->fd_hdr.flags))
    return false;
  bool revisions_ok = revisions_len <= UINT16_MAX - total_col_bytes && page_append(writer, revisions, revisions_len);
  free(revisions);
  if (!revisions_ok) return false;
  writer->fd_hdr.record_length = (uint16_t)(total_col_bytes + revisions_len);

  ESP_LOGI(TAG, "Series %02X%02X%02X%02X: record_count=%u, record_length=%u bytes (ts=%u, val=%u), times=[%" PRIu64 "-%" PRIu64 "]",
           writer->fd_hdr.series_id[0], writer->fd_hdr.series_id[1],
           writer->fd_hdr.series_id[2], writer->fd_hdr.series_id[3],
           (unsigned int)writer->fd_hdr.record_count, (unsigned int)writer->fd_hdr.record_length,
           (unsigned int)ts_len, (unsigned int)val_len,
           writer->fd_hdr.start_time, writer->fd_hdr.end_time);

  ESP_LOGV(TAG, "Header offset=0x%08" PRIx32 ", col_hdr=0x%08" PRIx32, writer->fd_hdr_offset, writer->col_hdr_offset);

  // 7) Patch the field_data_header with the updated metadata
  page_put(writer, writer->fd_hdr_offset, &writer->fd_hdr, sizeof(writer->fd_hdr));

  return true;
}

static bool write_alp_with_revisions(
    timeseries_page_stream_writer_t *sw,
    const uint8_t series_id[16],
    const uint8_t *ts_buf,  size_t ts_len,
    const uint8_t *val_buf, size_t val_len,
    uint16_t record_count,
    uint64_t start_time,
    uint64_t end_time, const uint8_t *revisions, size_t revisions_len, uint8_t revision_flags) {
  if (!sw || sw->finalized) {
    return false;
  }

  // Validate record_length fits in uint16_t before touching flash.
  if (ts_len > UINT16_MAX || val_len > UINT16_MAX || revisions_len > UINT16_MAX) return false;
  size_t total_col = sizeof(timeseries_col_data_header_t) + ts_len + val_len + revisions_len;
  if (total_col > UINT16_MAX) {
    ESP_LOGE(TAG, "ALP series record_length %u exceeds 16-bit limit", (unsigned)total_col);
    return false;
  }

  size_t total = sizeof(timeseries_field_data_header_t) +
                 sizeof(timeseries_col_data_header_t) + ts_len + val_len + revisions_len;

  if (!make_room_for_record(sw, (uint32_t)total)) {
    ESP_LOGE(TAG, "No room for ALP series (need %zu bytes)", total);
    return false;
  }

  // Prepare field_data_header
  timeseries_field_data_header_t fd_hdr;
  memset(&fd_hdr, 0xff, sizeof(fd_hdr));
  memcpy(fd_hdr.series_id, series_id, 16);
  fd_hdr.record_count  = record_count;
  fd_hdr.start_time    = start_time;
  fd_hdr.end_time      = end_time;
  fd_hdr.record_length = (uint16_t)total_col;
  // Clear COMPRESSED bit (0 = compressed) and ENCODING_ALP bit (0 = ALP)
  fd_hdr.flags = revision_flags & ~(TSDB_FIELDDATA_FLAG_COMPRESSED | TSDB_FIELDDATA_FLAG_ENCODING_ALP);

  // make_room_for_record ensured room for the whole record.
  timeseries_col_data_header_t col_hdr;
  col_hdr.ts_len  = (uint32_t)ts_len;
  col_hdr.val_len = (uint32_t)val_len;
  page_put(sw, sw->write_ptr, &fd_hdr, sizeof(fd_hdr));
  sw->write_ptr += sizeof(fd_hdr);
  page_put(sw, sw->write_ptr, &col_hdr, sizeof(col_hdr));
  sw->write_ptr += sizeof(col_hdr);
  page_put(sw, sw->write_ptr, ts_buf, ts_len);
  sw->write_ptr += ts_len;
  page_put(sw, sw->write_ptr, val_buf, val_len);
  sw->write_ptr += val_len;
  page_put(sw, sw->write_ptr, revisions, revisions_len);
  sw->write_ptr += revisions_len;

  ESP_LOGD(TAG, "ALP series %02X%02X%02X%02X: count=%u, ts_len=%zu, val_len=%zu, times=[%" PRIu64 "-%" PRIu64 "]",
           series_id[0], series_id[1], series_id[2], series_id[3],
           record_count, ts_len, val_len, start_time, end_time);

  return true;
}

bool timeseries_page_stream_writer_write_alp_series(
    timeseries_page_stream_writer_t *writer, const uint8_t series_id[16],
    const uint8_t *ts_buf, size_t ts_len, const uint8_t *val_buf, size_t val_len,
    uint16_t count, uint64_t first, uint64_t last) {
  if (!writer || !writer->db || writer->finalized) return false;
  uint8_t *revisions = NULL, flags = 0xff;
  size_t revisions_len = 0;
  if (!prepare_revisions(writer, count, &revisions, &revisions_len, &flags)) return false;
  bool ok = write_alp_with_revisions(writer, series_id, ts_buf, ts_len, val_buf, val_len,
                                     count, first, last, revisions, revisions_len, flags);
  free(revisions);
  return ok;
}

// Writes the records in page_buf[from, to) as one page, into a region of
// exactly its size, and publishes it (batch or live cache). *placed is false
// when no free region holds it: then nothing was written.
static bool publish_span(timeseries_page_stream_writer_t* writer, uint32_t from, uint32_t to, bool* placed) {
  timeseries_db_t* db = writer->db;
  uint32_t used = (uint32_t)sizeof(timeseries_page_header_t) + (to - from);
  uint32_t size = tsdb_round_up_4k(used);
  uint32_t region_ofs = 0, region_size = 0;
  *placed = false;
  if (!tsdb_reserve_blank_region(db, size, writer->batch_snapshot, &region_ofs, &region_size)) return false;
  *placed = true;
  writer->base_offset = region_ofs;
  writer->reserved = true;

  // FREE until every byte is on flash: a reset in between leaves no partly
  // written page for startup (or 1.0.19, which also skips FREE pages) to
  // read. The live cache keeps the reservation's ACTIVE placeholder, which
  // excludes other allocators.
  timeseries_page_header_t hdr;
  memset(&hdr, 0xFF, sizeof(hdr));
  hdr.magic_number = TIMESERIES_MAGIC_NUM;
  hdr.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
  hdr.page_state = TIMESERIES_PAGE_STATE_FREE;
  // Reserve a unique physical page sequence across concurrent allocations.
  hdr.sequence_num = atomic_fetch_add(&db->sequence_num, 1) + 1;
  hdr.field_data_level = writer->level;
  hdr.page_size = size;
  if (esp_partition_write(db->partition, region_ofs, &hdr, sizeof(hdr)) != ESP_OK ||
      (to > from &&
       esp_partition_write(db->partition, region_ofs + sizeof(hdr), writer->page_buf + from, to - from) != ESP_OK)) {
    ESP_LOGE(TAG, "Failed writing page @0x%08" PRIx32, region_ofs);
    return false;  // the caller aborts, releasing the region
  }

  // Publish on flash. FREE (0xFF) -> ACTIVE (0x01) only clears bits.
  if (tsdb_flash_write_byte(db->partition, region_ofs + offsetof(timeseries_page_header_t, page_state),
                            TIMESERIES_PAGE_STATE_ACTIVE) != ESP_OK) {
    ESP_LOGE(TAG, "Failed activating page @0x%08" PRIx32, region_ofs);
    return false;
  }
  hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  ESP_LOGI(TAG, "Finalize: Page @0x%08" PRIx32 " size=%" PRIu32 " used=%" PRIu32 " level=%u", region_ofs, size,
           used, (unsigned int)hdr.field_data_level);

  // Update page cache entry with final header (batch_add updates in-place if offset exists)
  if (writer->batch_snapshot) {
    if (!tsdb_pagecache_batch_add(writer->batch_snapshot, region_ofs, &hdr)) {
      ESP_LOGE(TAG, "OOM updating batch cache in finalize");
      return false;
    }
    tsdb_pagecache_batch_sort(writer->batch_snapshot);
  } else if (!tsdb_pagecache_add_entry(db, region_ofs, &hdr)) {
    ESP_LOGE(TAG, "OOM updating page cache in finalize @0x%08" PRIx32, region_ofs);
    return false;
  }
  writer->reserved = false;
  return true;
}

// The record boundary in (from, to) nearest its middle, or `from` when the
// span holds a single record.
static uint32_t split_point(const timeseries_page_stream_writer_t* writer, uint32_t from, uint32_t to) {
  uint32_t mid = from + (to - from) / 2, best = from;
  for (uint32_t at = from; at < to;) {
    timeseries_field_data_header_t rec;
    memcpy(&rec, writer->page_buf + at, sizeof(rec));
    uint32_t next = at + (uint32_t)sizeof(rec) + rec.record_length;
    if (next >= to) break;
    uint32_t d = next > mid ? next - mid : mid - next, best_d = best > mid ? best - mid : mid - best;
    if (best == from || d < best_d) best = next;
    at = next;
  }
  return best;
}

// Publishes the records in page_buf[from, to): as one page if a free region
// holds it, else split between records into smaller pages. Free space may be
// plentiful but fragmented; a record is never split.
static bool publish_records(timeseries_page_stream_writer_t* writer, uint32_t from, uint32_t to) {
  bool placed = false;
  if (publish_span(writer, from, to, &placed)) return true;
  if (placed) return false;  // a write failed
  uint32_t mid = split_point(writer, from, to);
  if (mid == from) {
    ESP_LOGE(TAG, "Failed to find blank region (size=%" PRIu32 ")",
             tsdb_round_up_4k((uint32_t)sizeof(timeseries_page_header_t) + (to - from)));
    return false;
  }
  ESP_LOGI(TAG, "No free region holds %" PRIu32 " bytes; splitting the page", to - from);
  return publish_records(writer, from, mid) && publish_records(writer, mid, to);
}

bool timeseries_page_stream_writer_finalize(timeseries_page_stream_writer_t* writer) {
  if (!writer || writer->finalized || !writer->page_buf) {
    return false;
  }
  if (!publish_records(writer, sizeof(timeseries_page_header_t), writer->write_ptr)) return false;
  free(writer->page_buf);
  writer->page_buf = NULL;
  writer->finalized = true;
  return true;
}

void timeseries_page_stream_writer_abort(timeseries_page_stream_writer_t* writer) {
  if (!writer || !writer->db) {
    return;
  }
  ESP_LOGW(TAG, "Aborting stream writer (level %u)", (unsigned int)writer->level);
  writer->finalized = true;

  // Release any in-progress Gorilla encoders.  If a series was begun but never
  // ended (the common abort case) both streams hold heap-allocated encoders
  // that end_series() would otherwise have freed.  gorilla_stream_deinit() is
  // NULL-safe and idempotent: the writer is fully zeroed in init() and each
  // deinit clears encoder_impl, so this is safe whether begin_series() ran or
  // not, and whether the streams were already finished.
  gorilla_stream_deinit(&writer->ts_stream);
  gorilla_stream_deinit(&writer->val_stream);

  // A page that failed during finalize gives its region back. Pages already
  // finished belong to the caller (a compaction batch abandons them).
  if (writer->reserved) tsdb_release_reserved_region(writer->db, writer->base_offset, writer->batch_snapshot);
  writer->reserved = false;
  free(writer->page_buf);
  writer->page_buf = NULL;
}
