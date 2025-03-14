#include "timeseries_compaction.h"

#include "esp_err.h"
#include "esp_log.h"
#include "esp_partition.h"

#include "timeseries_compression.h"
#include "timeseries_data.h" // for tsdb_write_levelX_page_dynamic()
#include "timeseries_internal.h"
#include "timeseries_iterator.h" // for fielddata_iterator, points_iterator
#include "timeseries_metadata.h"
#include "timeseries_multi_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_l1.h" // example, your L1 logic
#include "timeseries_page_rewriter.h"
#include "timeseries_page_stream_writer.h"
#include "timeseries_points_iterator.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "TimeseriesCompaction";

/**
 * @brief Minimum number of pages needed at a level before compaction occurs.
 */
#define MIN_PAGES_FOR_COMPACTION 4

/**
 * @brief Maximum level we support in ascending compaction. Adjust as needed.
 */
#define TSDB_MAX_LEVEL 4

// -----------------------------------------------------------------------------
// Forward Declarations
// -----------------------------------------------------------------------------

static bool tsdb_find_level_pages(timeseries_db_t *db, uint8_t source_level,
                                  tsdb_level_page_t **out_pages,
                                  size_t *out_count);

static bool tsdb_build_in_memory_map(timeseries_db_t *db,
                                     const tsdb_level_page_t *pages,
                                     size_t page_count,
                                     tsdb_series_buffer_t **out_series_bufs,
                                     size_t *out_series_count,
                                     bool is_compressed);

static bool tsdb_mark_old_level_pages_obsolete(timeseries_db_t *db,
                                               uint8_t source_level,
                                               const tsdb_level_page_t *pages,
                                               size_t page_count);

static bool
tsdb_write_levelX_page_dynamic(timeseries_db_t *db, uint8_t to_level,
                               const tsdb_series_buffer_t *series_bufs,
                               size_t series_count);

/**
 * @brief Helper to find or create a series buffer in a dynamic array.
 */
static tsdb_series_buffer_t *
find_or_create_series_buf(tsdb_series_buffer_t **series_bufs,
                          size_t *series_used, size_t *series_capacity,
                          const unsigned char *series_id);

static bool tsdb_compact_multi_iterator(timeseries_db_t *db,
                                        const tsdb_level_page_t *pages,
                                        size_t page_count, uint8_t to_level);

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

bool timeseries_compact_all_levels(timeseries_db_t *db) {
  if (!db) {
    return false;
  }

  /**
   * For each level from 0 up to TSDB_MAX_LEVEL-1, attempt compaction
   * from that level to the next (level+1).
   *
   * We always do them in ascending order so that if compaction of L0→L1
   * creates multiple L1 pages, the L1→L2 step can then run, and so on.
   */
  for (uint8_t from_level = 0; from_level < TSDB_MAX_LEVEL; from_level++) {
    uint8_t to_level = (uint8_t)(from_level + 1);

    // 1) Figure out how many pages are at `from_level`
    tsdb_level_page_t *pages = NULL;
    size_t page_count = 0;
    if (!tsdb_find_level_pages(db, from_level, &pages, &page_count)) {
      // If we fail to even list pages, treat as an error
      free(pages);
      ESP_LOGE(TAG, "Failed listing pages at level %u", from_level);
      return false;
    }

    // If no pages or fewer than MIN_PAGES_FOR_COMPACTION, skip compaction
    if (page_count < MIN_PAGES_FOR_COMPACTION) {
      free(pages);
      ESP_LOGV(TAG, "Level-%u has %zu page(s), below threshold=%d => skip",
               from_level, page_count, MIN_PAGES_FOR_COMPACTION);
      continue;
    }
    free(pages);

    // 2) We have enough pages => do the actual compaction
    if (!timeseries_compact_level_pages(db, from_level, to_level)) {
      ESP_LOGE(TAG, "Compaction from level %u to %u failed.", from_level,
               to_level);
      // Decide if you want to continue or break on failure
      return false;
    }
  }

  // Count the highest level for debug
  tsdb_level_page_t *pages = NULL;
  size_t page_count = 0;

  if (!tsdb_find_level_pages(db, 4, &pages, &page_count)) {
    // If we fail to even list pages, treat as an error
    free(pages);
    ESP_LOGE(TAG, "Failed listing pages at level %u", 4);
    return false;
  }

  ESP_LOGV(TAG, "Highest level has %zu page(s)", page_count);

  uint32_t partition_size = db->partition->size;
  uint32_t used_space = tsdb_pagecache_get_total_active_size(db);
  ESP_LOGE(TAG, "Total used space: %u, %f", (unsigned int)(used_space / 1024),
           100.0f * used_space / partition_size);

  free(pages);

  return true;
}

bool timeseries_compact_level_pages(timeseries_db_t *db, uint8_t from_level,
                                    uint8_t to_level) {
  if (!db) {
    return false;
  }

  ESP_LOGV(TAG, "Starting compaction from level=%u to level=%u ...", from_level,
           to_level);

  // 1) Find all active pages at `from_level`.
  tsdb_level_page_t *pages = NULL;
  size_t page_count = 0;
  if (!tsdb_find_level_pages(db, from_level, &pages, &page_count)) {
    return false;
  }

  // Also skip if below threshold
  if (page_count < MIN_PAGES_FOR_COMPACTION) {
    ESP_LOGV(TAG, "Level-%u has only %zu page(s), below threshold=%d => skip",
             from_level, page_count, MIN_PAGES_FOR_COMPACTION);
    free(pages);
    return true; // Return "true" meaning "no error", just no compaction
  }

  ESP_LOGV(TAG, "Found %zu level-%u pages for compaction.", page_count,
           from_level);

  // 2) Collect data from these pages
  bool success = false;
  tsdb_series_buffer_t *series_bufs = NULL;
  size_t series_count = 0;

  if (from_level == 0) {
    ESP_LOGI(TAG, "Compacting from level-0 to level-1...");
    // ----------------------------------------------------------------
    // L0 => L1: OLD APPROACH: in-memory consolidation
    // ----------------------------------------------------------------
    bool is_compressed = false; // Typically L0 pages are uncompressed
    if (!tsdb_build_in_memory_map(db, pages, page_count, &series_bufs,
                                  &series_count, is_compressed)) {
      ESP_LOGE(TAG, "Failed building in-memory map for level-0 compaction.");
      free(pages);
      return false;
    }
    success = true;

    if (!series_bufs || series_count == 0) {
      // Edge case: no data found => just mark the pages obsolete
      ESP_LOGV(TAG, "No valid series found; marking old pages obsolete...");
      tsdb_mark_old_level_pages_obsolete(db, from_level, pages, page_count);
      free(series_bufs);
      free(pages);
      return true;
    }

    ESP_LOGV(TAG, "Collected series data. Found %zu distinct series.",
             series_count);

    // 3) Write them out to a new page at `to_level`.
    if (!tsdb_write_page_dynamic(db, series_bufs, series_count, to_level)) {
      ESP_LOGE(TAG, "Failed writing new level-%u page.", to_level);

      // Cleanup
      for (size_t i = 0; i < series_count; i++) {
        free(series_bufs[i].points);
      }
      free(series_bufs);
      free(pages);
      return false;
    }
    ESP_LOGV(TAG, "Level-%u page(s) written with %zu series.", to_level,
             series_count);

  } else {
    // ----------------------------------------------------------------
    // L1 (or higher) => L2: NEW APPROACH: multi-iterator streaming
    // ----------------------------------------------------------------
    ESP_LOGI(TAG, "Compacting from level-%u to level-%u...", from_level,
             to_level);
    success = tsdb_compact_multi_iterator(db, pages, page_count, to_level);
  }

  if (!success) {
    // If we failed to gather data either way
    ESP_LOGE(TAG, "Failed to gather data for compaction (level %u).",
             from_level);
    free(pages);
    return false;
  }

  // 4) Mark old pages at `from_level` as obsolete
  if (!tsdb_mark_old_level_pages_obsolete(db, from_level, pages, page_count)) {
    ESP_LOGW(TAG, "Failed to mark old level-%u pages as obsolete.", from_level);
  }

  // Cleanup
  for (size_t i = 0; i < series_count; i++) {
    free(series_bufs[i].points);
  }
  free(series_bufs);
  free(pages);

  ESP_LOGE(TAG, "Compaction from level=%u to level=%u complete.", from_level,
           to_level);
  return true;
}

bool timeseries_compact_level0_pages(timeseries_db_t *db) {
  // Just call the generic function with from_level=0, to_level=1
  return timeseries_compact_level_pages(db, 0, 1);
}

// -----------------------------------------------------------------------------
// Step 1) Find pages at the source level
// -----------------------------------------------------------------------------

static bool tsdb_find_level_pages(timeseries_db_t *db, uint8_t source_level,
                                  tsdb_level_page_t **out_pages,
                                  size_t *out_count) {
  if (!db || !out_pages || !out_count) {
    return false;
  }
  *out_pages = NULL;
  *out_count = 0;

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t offset = 0, size = 0;

// For demo, limit to 32 pages
#define MAX_PAGES_PER_LEVEL 32
  tsdb_level_page_t temp[MAX_PAGES_PER_LEVEL];
  size_t found = 0;

  while (
      timeseries_page_cache_iterator_next(&page_iter, &hdr, &offset, &size)) {
    // Is it an active field-data page at the desired level?
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM &&
        hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE &&
        hdr.field_data_level == source_level) {
      if (found < MAX_PAGES_PER_LEVEL) {
        temp[found].offset = offset;
        temp[found].size = size;
        found++;
      } else {
        ESP_LOGW(TAG, "Too many pages found at level=%u (> %d).", source_level,
                 MAX_PAGES_PER_LEVEL);
        break;
      }
    }
  }

  if (found == 0) {
    return true; // no pages => nothing to do
  }

  tsdb_level_page_t *arr =
      (tsdb_level_page_t *)malloc(found * sizeof(tsdb_level_page_t));
  if (!arr) {
    return false;
  }
  memcpy(arr, temp, found * sizeof(tsdb_level_page_t));
  *out_pages = arr;
  *out_count = found;
  return true;
}

// -----------------------------------------------------------------------------
// Step 2) Build in-memory map by scanning field-data records
// -----------------------------------------------------------------------------

static bool tsdb_build_in_memory_map(timeseries_db_t *db,
                                     const tsdb_level_page_t *pages,
                                     size_t page_count,
                                     tsdb_series_buffer_t **out_series_bufs,
                                     size_t *out_series_count,
                                     bool is_compressed) {
  tsdb_series_buffer_t *series_bufs = NULL;
  size_t series_used = 0, series_capacity = 0;

  for (size_t p = 0; p < page_count; p++) {
    uint32_t offset = pages[p].offset;
    uint32_t page_size = pages[p].size;

    ESP_LOGV(TAG, "Reading page=0x%08" PRIx32 ", page_size=%" PRIu32, offset,
             page_size);

    // Field-data iterator over this page
    timeseries_fielddata_iterator_t f_iter;
    if (!timeseries_fielddata_iterator_init(db, offset, page_size, &f_iter)) {
      ESP_LOGW(TAG, "Failed to init fielddata iterator @0x%08" PRIx32, offset);
      continue;
    }

    timeseries_field_data_header_t fd_hdr;
    unsigned int pages_read = 0;

    while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
      pages_read++;

      if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
        // ESP_LOGW(TAG, "Skipping deleted record in page=0x%08" PRIx32,
        // offset);
        continue;
      }

      // Lookup series type (int, float, string, etc.)
      // TODO: Cache this so we don't look it up every time
      timeseries_field_type_e series_type;
      if (!tsdb_lookup_series_type_in_metadata(db, fd_hdr.series_id,
                                               &series_type)) {
        ESP_LOGW(
            TAG,
            "No series type found for series=%.2X%.2X%.2X%.2X..., skipping",
            fd_hdr.series_id[0], fd_hdr.series_id[1], fd_hdr.series_id[2],
            fd_hdr.series_id[3]);
        continue;
      }

      size_t npoints = fd_hdr.record_count;
      uint32_t header_abs_offset = offset + f_iter.current_record_offset;
      uint32_t data_abs_offset =
          header_abs_offset + sizeof(timeseries_field_data_header_t);

      ESP_LOGV(
          TAG,
          "Record in page=0x%08" PRIx32 ": record_count=%zu, record_length=%u, "
          "header_abs_offset=0x%08" PRIx32,
          offset, npoints, (unsigned)fd_hdr.record_length, header_abs_offset);

      // A points-iterator over this record
      timeseries_points_iterator_t pts_iter;
      if (!timeseries_points_iterator_init(
              db, data_abs_offset, fd_hdr.record_length, fd_hdr.record_count,
              series_type, is_compressed, &pts_iter)) {
        ESP_LOGW(TAG,
                 "Failed init points iterator for series=%.2X%.2X%.2X%.2X...",
                 fd_hdr.series_id[0], fd_hdr.series_id[1], fd_hdr.series_id[2],
                 fd_hdr.series_id[3]);
        continue;
      }

      // Allocate temporary array to hold the points from this record
      tsdb_compact_data_point_t *tmp_pts = (tsdb_compact_data_point_t *)calloc(
          npoints, sizeof(tsdb_compact_data_point_t));
      if (!tmp_pts) {
        ESP_LOGW(TAG, "OOM reading record in page=0x%08" PRIx32, offset);
        timeseries_points_iterator_deinit(&pts_iter);
        return false;
      }

      size_t actual_read = 0;
      uint64_t ts;
      timeseries_field_value_t fv;

      // Iterate using the new separate timestamp and value functions
      while (actual_read < npoints &&
             timeseries_points_iterator_next_timestamp(&pts_iter, &ts) &&
             timeseries_points_iterator_next_value(&pts_iter, &fv)) {
        tmp_pts[actual_read].timestamp = ts;
        tmp_pts[actual_read].field_val = fv;
        actual_read++;
      }

      timeseries_points_iterator_deinit(&pts_iter);

      if (actual_read < npoints) {
        ESP_LOGW(TAG, "Record_count mismatch: read only %zu < %zu", actual_read,
                 npoints);
      }

      // Merge these points into the series buffer
      tsdb_series_buffer_t *sbuf = find_or_create_series_buf(
          &series_bufs, &series_used, &series_capacity, fd_hdr.series_id);
      if (!sbuf) {
        ESP_LOGW(TAG, "OOM merging points into series buffer.");
        free(tmp_pts);
        return false;
      }

      // Insert or overwrite points (de-duplicate by timestamp)
      for (size_t i = 0; i < actual_read; i++) {
        uint64_t t = tmp_pts[i].timestamp;
        timeseries_field_value_t *v = &tmp_pts[i].field_val;

        bool found_dup = false;
        for (size_t d = 0; d < sbuf->count; d++) {
          if (sbuf->points[d].timestamp == t) {
            // Overwrite duplicate
            sbuf->points[d].field_val = *v;
            found_dup = true;
            break;
          }
        }
        if (!found_dup) {
          // Append new point and update series boundaries
          if (t < sbuf->start_ts) {
            sbuf->start_ts = t;
          }
          if (t > sbuf->end_ts) {
            sbuf->end_ts = t;
          }
          if (sbuf->count == sbuf->capacity) {
            size_t newCap = (sbuf->capacity == 0) ? 8 : (sbuf->capacity * 2);
            tsdb_compact_data_point_t *grow =
                (tsdb_compact_data_point_t *)realloc(
                    sbuf->points, newCap * sizeof(tsdb_compact_data_point_t));
            if (!grow) {
              ESP_LOGW(TAG, "OOM growing series buffer.");
              free(tmp_pts);
              return false;
            }
            sbuf->points = grow;
            sbuf->capacity = newCap;
          }
          sbuf->points[sbuf->count].timestamp = t;
          sbuf->points[sbuf->count].field_val = *v;
          sbuf->count++;
        }
      }
      free(tmp_pts);
    }

    ESP_LOGV(TAG, "Read %u records from page=0x%08" PRIx32, pages_read, offset);
  }

  *out_series_bufs = series_bufs;
  *out_series_count = series_used;
  return true;
}

// -----------------------------------------------------------------------------
// Step 4) Mark old pages as obsolete
// -----------------------------------------------------------------------------

static bool tsdb_mark_old_level_pages_obsolete(timeseries_db_t *db,
                                               uint8_t source_level,
                                               const tsdb_level_page_t *pages,
                                               size_t page_count) {
  if (!db || !pages) {
    return false;
  }

  for (size_t i = 0; i < page_count; i++) {
    uint32_t pofs = pages[i].offset;
    ESP_LOGV(TAG, "Marking level-%u page @0x%08" PRIx32 " as obsolete.",
             source_level, pofs);

    // 1) Read the existing page header
    timeseries_page_header_t hdr;
    esp_err_t err = esp_partition_read(db->partition, pofs, &hdr, sizeof(hdr));
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed to read page header @0x%08" PRIx32 " (err=0x%x)",
               pofs, err);
      continue;
    }

    // 2) Verify it's the correct level
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM &&
        hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE &&
        hdr.field_data_level == source_level) {
      // 3) Mark obsolete
      hdr.page_state = TIMESERIES_PAGE_STATE_OBSOLETE;

      // 4) Write updated header
      err = esp_partition_write(db->partition, pofs, &hdr, sizeof(hdr));
      if (err != ESP_OK) {
        ESP_LOGE(TAG,
                 "Failed marking page @0x%08" PRIx32 " obsolete (err=0x%x)",
                 pofs, err);
        continue;
      }

      // Remove from cache
      tsdb_pagecache_remove_entry(db, pofs);

      ESP_LOGV(TAG, "Obsolete mark success for page @0x%08" PRIx32, pofs);
    } else {
      ESP_LOGW(TAG,
               "Skipping page @0x%08" PRIx32
               " (not an active level-%u field-data page).",
               pofs, source_level);
    }
  }

  return true;
}

// -----------------------------------------------------------------------------
// Helper: find_or_create_series_buf
// -----------------------------------------------------------------------------

static tsdb_series_buffer_t *
find_or_create_series_buf(tsdb_series_buffer_t **series_bufs,
                          size_t *series_used, size_t *series_capacity,
                          const unsigned char *series_id) {
  // Check if we already have a buffer for this series
  for (size_t i = 0; i < *series_used; i++) {
    if (memcmp((*series_bufs)[i].series_id, series_id, 16) == 0) {
      return &(*series_bufs)[i];
    }
  }

  // Not found; grow array if needed
  if (*series_used == *series_capacity) {
    size_t newCap = (*series_capacity == 0) ? 8 : (*series_capacity * 2);
    tsdb_series_buffer_t *grow = (tsdb_series_buffer_t *)realloc(
        *series_bufs, newCap * sizeof(tsdb_series_buffer_t));
    if (!grow) {
      return NULL;
    }
    *series_bufs = grow;
    *series_capacity = newCap;
  }

  // Create a new entry
  tsdb_series_buffer_t *buf = &(*series_bufs)[(*series_used)++];
  memset(buf, 0, sizeof(*buf));
  memcpy(buf->series_id, series_id, 16);
  buf->start_ts = UINT64_MAX;
  buf->end_ts = 0;
  return buf;
}

size_t
timeseries_field_value_serialize_size(const timeseries_field_value_t *val) {
  switch (val->type) {
  case TIMESERIES_FIELD_TYPE_STRING:
    // 4 + actual bytes
    return 4 + val->data.string_val.length;
  case TIMESERIES_FIELD_TYPE_FLOAT:
  case TIMESERIES_FIELD_TYPE_INT:
    return 8;
  case TIMESERIES_FIELD_TYPE_BOOL:
    return 1;
  }
  return 0;
}

size_t timeseries_field_value_serialize(const timeseries_field_value_t *val,
                                        unsigned char *out_buf) {
  size_t written = 0;
  switch (val->type) {
  case TIMESERIES_FIELD_TYPE_STRING: {
    uint32_t len = (uint32_t)val->data.string_val.length;
    memcpy(out_buf, &len, 4);
    written += 4;
    if (len > 0) {
      memcpy(out_buf + written, val->data.string_val.str, len);
      written += len;
    }
    break;
  }
  case TIMESERIES_FIELD_TYPE_FLOAT: {
    double d = (double)val->data.float_val;
    memcpy(out_buf, &d, 8);
    written += 8;
    break;
  }
  case TIMESERIES_FIELD_TYPE_INT: {
    int64_t i64 = (int64_t)val->data.int_val;
    memcpy(out_buf, &i64, 8);
    written += 8;
    break;
  }
  case TIMESERIES_FIELD_TYPE_BOOL: {
    out_buf[0] = val->data.bool_val ? 1 : 0;
    written++;
    break;
  }
  }
  return written;
}

// Example qsort compare function:
static int sort_by_seq_and_offset(const void *a, const void *b) {
  const tsdb_record_descriptor_t *A = (const tsdb_record_descriptor_t *)a;
  const tsdb_record_descriptor_t *B = (const tsdb_record_descriptor_t *)b;
  if (A->page_seq < B->page_seq)
    return -1;
  else if (A->page_seq > B->page_seq)
    return +1;

  // tie-break by data_offset:
  if (A->data_offset < B->data_offset)
    return -1;
  else if (A->data_offset > B->data_offset)
    return +1;
  return 0;
}

static bool tsdb_compact_multi_iterator(timeseries_db_t *db,
                                        const tsdb_level_page_t *pages,
                                        size_t page_count, uint8_t to_level) {
  if (!db || !pages || page_count == 0) {
    return false;
  }

  // ----------------------------------------------------------------
  // 1) Build a map of series => record descriptors (unchanged)
  // ----------------------------------------------------------------
  tsdb_series_descriptors_t *series_map = NULL;
  size_t series_map_used = 0;
  size_t series_map_cap = 0;
  size_t total_record_bytes =
      0; // Holds total bytes of all records minus headers

  for (size_t i = 0; i < page_count; i++) {
    const tsdb_level_page_t *pg = &pages[i];
    timeseries_page_header_t hdr;
    if (esp_partition_read(db->partition, pg->offset, &hdr, sizeof(hdr)) !=
        ESP_OK) {
      ESP_LOGW(TAG, "Failed reading header @0x%08" PRIx32, pg->offset);
      continue;
    }
    // We need the page_seq for sorting merges in multi-iterator:
    uint32_t page_seq = hdr.sequence_num;

    // FieldData iterator on this page:
    timeseries_fielddata_iterator_t f_iter;
    if (!timeseries_fielddata_iterator_init(db, pg->offset, pg->size,
                                            &f_iter)) {
      ESP_LOGW(TAG, "Failed init fielddata iterator @0x%08" PRIx32, pg->offset);
      continue;
    }

    timeseries_field_data_header_t fd_hdr;
    unsigned int records_read = 0;

    while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
      if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
        // skip deleted records
        continue;
      }

      records_read++;
      total_record_bytes += fd_hdr.record_length;

      // Build descriptor
      tsdb_record_descriptor_t desc;
      memset(&desc, 0, sizeof(desc));
      desc.record_count = fd_hdr.record_count;
      desc.record_length = fd_hdr.record_length;
      desc.is_compressed = (fd_hdr.flags & TSDB_FIELDDATA_FLAG_COMPRESSED) == 0;
      // Figure out field_type from metadata
      timeseries_field_type_e ftype;
      if (!tsdb_lookup_series_type_in_metadata(db, fd_hdr.series_id, &ftype)) {
        // skip unknown series
        continue;
      }
      desc.field_type = ftype;
      // data_offset = page_offset + <this record offset> + sizeof(fd_hdr)
      desc.data_offset = pg->offset + f_iter.current_record_offset +
                         sizeof(timeseries_field_data_header_t);
      desc.page_seq = page_seq;

      // Insert into series_map
      bool found = false;
      for (size_t s = 0; s < series_map_used; s++) {
        if (memcmp(series_map[s].series_id, fd_hdr.series_id, 16) == 0) {
          // Add to that vector
          tsdb_series_descriptors_t *S = &series_map[s];
          if (S->record_used == S->record_capacity) {
            size_t newcap =
                (S->record_capacity == 0) ? 4 : (S->record_capacity * 2);
            tsdb_record_descriptor_t *grow =
                realloc(S->records, newcap * sizeof(tsdb_record_descriptor_t));
            if (!grow) {
              return false;
            }
            S->records = grow;
            S->record_capacity = newcap;
          }
          S->records[S->record_used++] = desc;
          found = true;
          break;
        }
      }

      if (!found) {
        // Create a new entry in the series_map
        if (series_map_used == series_map_cap) {
          size_t newcap = (series_map_cap == 0) ? 4 : (series_map_cap * 2);
          tsdb_series_descriptors_t *grow =
              realloc(series_map, newcap * sizeof(tsdb_series_descriptors_t));
          if (!grow) {
            return false;
          }
          series_map = grow;
          series_map_cap = newcap;
        }
        memset(&series_map[series_map_used], 0,
               sizeof(series_map[series_map_used]));
        memcpy(series_map[series_map_used].series_id, fd_hdr.series_id, 16);
        series_map[series_map_used].record_used = 1;
        series_map[series_map_used].record_capacity = 4;
        series_map[series_map_used].records =
            calloc(4, sizeof(tsdb_record_descriptor_t));
        if (!series_map[series_map_used].records) {
          return false;
        }
        series_map[series_map_used].records[0] = desc;
        series_map_used++;
      }

      ESP_LOGV(TAG, "Added seriesId=%.2X%.2X%.2X%.2X... to map",
               fd_hdr.series_id[0], fd_hdr.series_id[1], fd_hdr.series_id[2],
               fd_hdr.series_id[3]);
    } // end while

    ESP_LOGV(TAG, "Read %u records from page", records_read);
  }

  if (series_map_used == 0) {
    ESP_LOGV(TAG, "No data found among these pages; nothing to compact.");
    free(series_map);
    return true; // no error
  }

  // ----------------------------------------------------------------
  // 2) Create one stream writer for a new page that will hold multiple series
  // ----------------------------------------------------------------
  timeseries_page_stream_writer_t writer;
  if (!timeseries_page_stream_writer_init(db, &writer, to_level,
                                          total_record_bytes)) {
    ESP_LOGE(TAG, "Failed to initialize stream-writer for multi-series page");
    for (size_t s = 0; s < series_map_used; s++) {
      free(series_map[s].records);
    }
    free(series_map);
    return false;
  }

  // ----------------------------------------------------------------
  // 3) For each series, stream its data into the same page
  //    But now, we do two passes:
  //      - Pass 1: Write timestamps
  //      - Reset multi-iterator
  //      - Pass 2: Write values
  // ----------------------------------------------------------------
  for (size_t s = 0; s < series_map_used; s++) {
    tsdb_series_descriptors_t *S = &series_map[s];
    if (S->record_used == 0) {
      continue;
    }

    // Sort records by page_seq (and data_offset as tie-breaker)
    qsort(S->records, S->record_used, sizeof(tsdb_record_descriptor_t),
          sort_by_seq_and_offset);

    // Build sub-iterators for each record
    timeseries_points_iterator_t **sub_iters =
        calloc(S->record_used, sizeof(*sub_iters));
    if (!sub_iters) {
      ESP_LOGE(TAG, "OOM allocating sub-iterators for series s=%zu", s);
      continue;
    }

    uint32_t *seqs = calloc(S->record_used, sizeof(uint32_t));
    if (!seqs) {
      free(sub_iters);
      ESP_LOGE(TAG, "OOM allocating seqs for series s=%zu", s);
      continue;
    }

    for (size_t r = 0; r < S->record_used; r++) {
      const tsdb_record_descriptor_t *RD = &S->records[r];
      sub_iters[r] = calloc(1, sizeof(timeseries_points_iterator_t));
      if (!sub_iters[r]) {
        ESP_LOGW(TAG, "OOM creating sub-iter for series s=%zu, record r=%zu", s,
                 r);
        continue;
      }
      if (!timeseries_points_iterator_init(
              db, RD->data_offset, RD->record_length, RD->record_count,
              RD->field_type, RD->is_compressed, sub_iters[r])) {
        ESP_LOGW(TAG, "Failed init sub-iter offset=0x%08" PRIx32,
                 RD->data_offset);
        free(sub_iters[r]);
        sub_iters[r] = NULL;
        continue;
      }
      seqs[r] = RD->page_seq;
    }

    ESP_LOGV(TAG, "Built %zu sub-iterators for series s=%zu", S->record_used,
             s);

    // Initialize the multi-iterator for pass #1 (timestamps)
    timeseries_multi_points_iterator_t multi_iter;
    if (!timeseries_multi_points_iterator_init(sub_iters, seqs, S->record_used,
                                               &multi_iter)) {
      ESP_LOGW(TAG, "Failed init multi-iter for series s=%zu (pass 1)", s);
      // Cleanup sub-iters
      for (size_t r = 0; r < S->record_used; r++) {
        if (sub_iters[r]) {
          timeseries_points_iterator_deinit(sub_iters[r]);
          free(sub_iters[r]);
        }
      }
      free(sub_iters);
      free(seqs);
      continue;
    }

    // Begin a new series in the already-open page
    if (!timeseries_page_stream_writer_begin_series(&writer, S->series_id,
                                                    S->records[0].field_type)) {
      ESP_LOGE(TAG, "Failed to begin series for s=%zu", s);
      timeseries_multi_points_iterator_deinit(&multi_iter);
      for (size_t r = 0; r < S->record_used; r++) {
        if (sub_iters[r]) {
          timeseries_points_iterator_deinit(sub_iters[r]);
          free(sub_iters[r]);
        }
      }
      free(sub_iters);
      free(seqs);
      continue;
    }

    // --- PASS #1: Write timestamps only ---
    {
      uint64_t ts;
      timeseries_field_value_t fv;
      while (timeseries_multi_points_iterator_next(&multi_iter, &ts, &fv)) {
        // Timestamps get written first
        if (!timeseries_page_stream_writer_write_timestamp(&writer, ts)) {
          ESP_LOGE(TAG, "Failed writing timestamp for s=%zu", s);
          break;
        }
      }
    }

    timeseries_page_stream_writer_finalize_timestamp(&writer);
    timeseries_multi_points_iterator_deinit(&multi_iter);

    // Re-init the multi-iterator for pass #2 (values)
    // We must re-init all sub-iterators so that they start from the beginning.
    for (size_t r = 0; r < S->record_used; r++) {
      if (sub_iters[r]) {
        timeseries_points_iterator_deinit(sub_iters[r]);
        free(sub_iters[r]);
      }
    }

    // Build sub-iterators again:
    for (size_t r = 0; r < S->record_used; r++) {
      const tsdb_record_descriptor_t *RD = &S->records[r];
      sub_iters[r] = calloc(1, sizeof(timeseries_points_iterator_t));
      if (!sub_iters[r]) {
        ESP_LOGW(TAG, "OOM re-creating sub-iter for series s=%zu, record r=%zu",
                 s, r);
        continue;
      }
      if (!timeseries_points_iterator_init(
              db, RD->data_offset, RD->record_length, RD->record_count,
              RD->field_type, RD->is_compressed, sub_iters[r])) {
        ESP_LOGW(TAG, "Failed re-init sub-iter offset=0x%08" PRIx32,
                 RD->data_offset);
        free(sub_iters[r]);
        sub_iters[r] = NULL;
        continue;
      }
    }

    if (!timeseries_multi_points_iterator_init(sub_iters, seqs, S->record_used,
                                               &multi_iter)) {
      ESP_LOGW(TAG, "Failed init multi-iter for series s=%zu (pass 2)", s);
      // Cleanup sub-iters
      for (size_t r = 0; r < S->record_used; r++) {
        if (sub_iters[r]) {
          timeseries_points_iterator_deinit(sub_iters[r]);
          free(sub_iters[r]);
        }
      }
      free(sub_iters);
      free(seqs);
      // Try to end the series anyway
      timeseries_page_stream_writer_end_series(&writer);
      continue;
    }

    // --- PASS #2: Write values only ---
    {
      uint64_t ts;
      timeseries_field_value_t fv;
      while (timeseries_multi_points_iterator_next(&multi_iter, &ts, &fv)) {
        if (!timeseries_page_stream_writer_write_value(&writer, &fv)) {
          ESP_LOGE(TAG, "Failed writing value for s=%zu", s);
          break;
        }
      }
    }
    timeseries_multi_points_iterator_deinit(&multi_iter);

    // Cleanup sub-iterators
    for (size_t r = 0; r < S->record_used; r++) {
      if (sub_iters[r]) {
        timeseries_points_iterator_deinit(sub_iters[r]);
        free(sub_iters[r]);
      }
    }
    free(sub_iters);
    free(seqs);

    // End the current series in the page
    if (!timeseries_page_stream_writer_end_series(&writer)) {
      ESP_LOGE(TAG, "Failed to end series in stream-writer for s=%zu", s);
      continue;
    }

    ESP_LOGV(TAG, "Finished writing series s=%zu in multi-series page", s);
  }

  // ----------------------------------------------------------------
  // 4) Finalize the page once all series have been written
  // ----------------------------------------------------------------
  if (!timeseries_page_stream_writer_finalize(&writer)) {
    ESP_LOGE(TAG, "Failed to finalize multi-series page");
    for (size_t s = 0; s < series_map_used; s++) {
      free(series_map[s].records);
    }
    free(series_map);
    return false;
  }

  // Cleanup series descriptors
  for (size_t s = 0; s < series_map_used; s++) {
    free(series_map[s].records);
  }
  free(series_map);

  return true;
}

/**
 * @brief Rewrite a single old page (if it's an active field-data page)
 *        into a new page, omitting any deleted records.
 *
 * This function uses a two-pass approach. First it computes the total size
 * of all non-deleted field data records, then it allocates a new page using
 * that size. Finally, it copies each non-deleted record into the new page.
 *
 * @param db             Database handle
 * @param old_page_ofs   The offset of the old page
 * @return true on success, or if the page was skipped (not active field-data);
 *         false if there's a fatal error.
 */
static bool tsdb_rewrite_page_without_deleted(timeseries_db_t *db,
                                              uint32_t old_page_ofs) {
  // 1) Read the old page header.
  timeseries_page_header_t hdr;
  esp_err_t err =
      esp_partition_read(db->partition, old_page_ofs, &hdr, sizeof(hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to read old page header @0x%08X (err=0x%x)",
             (unsigned int)old_page_ofs, err);
    return false;
  }

  // 2) Check if it's an active field-data page.
  if (hdr.magic_number != TIMESERIES_MAGIC_NUM ||
      hdr.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA ||
      hdr.page_state != TIMESERIES_PAGE_STATE_ACTIVE) {
    ESP_LOGW(TAG, "Page @0x%08X is not an active field-data page; skipping",
             (unsigned int)old_page_ofs);
    return true;
  }

  // Retrieve the old page size from the page cache (or header).
  uint32_t old_page_size = hdr.page_size;
  uint8_t level = hdr.field_data_level;

  ESP_LOGW(TAG, "Compacting page @0x%08X (level=%u) size=%u",
           (unsigned int)old_page_ofs, (unsigned int)level,
           (unsigned int)old_page_size);

  // 3) First pass: iterate over the field data records to compute the total
  // size of non-deleted records.
  uint32_t total_data_size = 0;
  uint32_t total_records = 0;
  timeseries_fielddata_iterator_t f_iter;
  if (!timeseries_fielddata_iterator_init(db, old_page_ofs, old_page_size,
                                          &f_iter)) {
    ESP_LOGE(TAG, "Failed to init fielddata iterator @0x%08X",
             (unsigned int)old_page_ofs);
    return false;
  }

  timeseries_field_data_header_t fd_hdr;
  while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
    // If DELETED bit is 0, the record is deleted: skip.
    if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
      ESP_LOGE(
          TAG,
          "Skipping deleted record in page=0x%08X header_abs_offset=0x%08X",
          (unsigned int)old_page_ofs,
          (unsigned int)f_iter.current_record_offset);
      continue;
    }
    total_data_size +=
        sizeof(timeseries_field_data_header_t) + fd_hdr.record_length;
    total_records++;

    ESP_LOGW(TAG,
             "Record in page=0x%08X: record_count=%zu, record_length=%u, "
             "header_abs_offset=0x%08X",
             (unsigned int)old_page_ofs, (unsigned int)fd_hdr.record_count,
             (unsigned int)fd_hdr.record_length,
             (unsigned int)f_iter.current_record_offset);
  }

  ESP_LOGI(TAG, "Page @0x%08X: total_records=%u, total_data_size=%u",
           (unsigned int)old_page_ofs, (unsigned int)total_records,
           (unsigned int)total_data_size);

  // 4) Allocate a new page using the rewriter, passing the computed total size.
  if (total_records > 0) {
    timeseries_page_rewriter_t rewriter;
    if (!timeseries_page_rewriter_start(db, level, total_data_size,
                                        &rewriter)) {
      ESP_LOGE(TAG, "Failed to initialize page rewriter");
      return false;
    }
    // The new page offset is now rewriter.base_offset.

    // 5) Second pass: reinitialize the iterator to copy over records.
    if (!timeseries_fielddata_iterator_init(db, old_page_ofs, old_page_size,
                                            &f_iter)) {
      ESP_LOGE(TAG, "Failed to re-init fielddata iterator @0x%08X",
               (unsigned int)old_page_ofs);
      timeseries_page_rewriter_abort(&rewriter);
      return false;
    }
    while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
      // Skip deleted records.
      if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
        continue;
      }
      // Write the record into the new page.
      if (!timeseries_page_rewriter_write_field_data(
              &rewriter, old_page_ofs, f_iter.current_record_offset, &fd_hdr)) {
        ESP_LOGE(TAG, "Failed writing field data to new page @0x%08X",
                 (unsigned int)rewriter.base_offset);
        timeseries_page_rewriter_abort(&rewriter);
        return false;
      }
    }

    // 6) Finalize the new page.
    if (!timeseries_page_rewriter_finalize(&rewriter)) {
      ESP_LOGE(TAG, "Failed to finalize new page @0x%08X",
               (unsigned int)rewriter.base_offset);
      return false;
    }
  }

  // 7) Mark the OLD page as obsolete.
  hdr.page_state = TIMESERIES_PAGE_STATE_OBSOLETE;
  err = esp_partition_write(db->partition, old_page_ofs, &hdr, sizeof(hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed marking old page @0x%08X obsolete (err=0x%x)",
             (unsigned int)old_page_ofs, err);
    // Not strictly fatal, so continue.
  }

  // Remove the old page from the cache.
  tsdb_pagecache_remove_entry(db, old_page_ofs);

  ESP_LOGI(TAG, "Page @0x%08X compacted into new page",
           (unsigned int)old_page_ofs);
  return true;
}

bool timeseries_compact_page_list(timeseries_db_t *db,
                                  const uint32_t *page_offsets,
                                  size_t page_count) {
  if (!db || !page_offsets || page_count == 0) {
    // Nothing to do
    return true;
  }

  for (size_t i = 0; i < page_count; i++) {
    uint32_t old_page_ofs = page_offsets[i];
    // Use our new helper function
    if (!tsdb_rewrite_page_without_deleted(db, old_page_ofs)) {
      // If there's a fatal error, stop
      return false;
    }
  }

  return true;
}