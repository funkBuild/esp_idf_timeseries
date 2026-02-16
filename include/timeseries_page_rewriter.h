#ifndef TIMESERIES_PAGE_REWRITER_H
#define TIMESERIES_PAGE_REWRITER_H

#include "esp_partition.h"
#include "timeseries_data.h"     // For timeseries_field_data_header_t
#include "timeseries_internal.h" // For timeseries_db_t, etc.
#include <stdbool.h>
#include <stdint.h>

/**
 * Structure holding page rewriter state.
 */
typedef struct {
  timeseries_db_t *db;
  uint32_t base_offset; // Starting offset of the new page in flash.
  uint32_t write_ptr;   // Current write pointer within the new page.
  uint32_t capacity;    // Total allocated capacity (in bytes).
  uint8_t level;        // Field data level (from header).
  bool finalized;
  // Batch snapshot for compaction (when non-NULL, cache ops go through batch)
  tsdb_page_cache_snapshot_t *batch_snapshot;
} timeseries_page_rewriter_t;

/**
 * @brief Allocate a new page and initialize a page rewriter.
 *
 * This function uses the blank iterator to find a blank region sized to hold
 * the previous page's compressed data (rounded up to 4K). It erases that
 * region, writes a placeholder page header, and initializes the rewriter state.
 *
 * @param db             Pointer to the database structure.
 * @param level          Field data level to use in the new page header.
 * @param prev_data_size The size (in bytes) of the previous compressed data.
 * @param rewriter       Pointer to the rewriter structure to initialize.
 *
 * @return true on success, false on failure.
 */
bool timeseries_page_rewriter_start(timeseries_db_t *db, uint8_t level,
                                    uint32_t prev_data_size,
                                    timeseries_page_rewriter_t *rewriter);

/**
 * @brief Write one field data record from an old page into the new page.
 *
 * This function reads the entire record (header + payload) from flash using the
 * old page offset plus the record’s relative offset, and writes it to the new
 * page.
 *
 * @param rewriter       Pointer to an initialized rewriter.
 * @param old_page_ofs   Base offset of the old page.
 * @param record_offset  Offset (relative to old_page_ofs) where the record
 * starts.
 * @param fd_hdr         Pointer to the field data header for the record.
 *
 * @return true on success, false on failure.
 */
bool timeseries_page_rewriter_write_field_data(
    timeseries_page_rewriter_t *rewriter, uint32_t old_page_ofs,
    uint32_t record_offset, const timeseries_field_data_header_t *fd_hdr);

/**
 * @brief Finalize the new page.
 *
 * This function updates the page header (for example, its size) based on the
 * amount of data written, and optionally adds the new page to the page cache.
 *
 * @param rewriter Pointer to the rewriter.
 *
 * @return true on success, false on failure.
 */
bool timeseries_page_rewriter_finalize(timeseries_page_rewriter_t *rewriter);

/**
 * @brief Abort the page rewriter, cleaning up any in-progress state.
 *
 * @param rewriter Pointer to the rewriter.
 */
void timeseries_page_rewriter_abort(timeseries_page_rewriter_t *rewriter);

#endif // TIMESERIES_PAGE_REWRITER_H
