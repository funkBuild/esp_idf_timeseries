#ifndef TIMESERIES_ITERATOR_H
#define TIMESERIES_ITERATOR_H

#include "timeseries.h"
#include "timeseries_internal.h"
#include "esp_partition.h"

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// -----------------------------------------------------------------------------
// Page Iterator
// -----------------------------------------------------------------------------
typedef struct {
  timeseries_db_t* db;
  uint32_t current_offset;  // Current byte offset in the partition
  bool valid;
} timeseries_page_iterator_t;

bool timeseries_page_iterator_init(timeseries_db_t* db, timeseries_page_iterator_t* iter);

bool timeseries_page_iterator_next(timeseries_page_iterator_t* iter, timeseries_page_header_t* out_header,
                                   uint32_t* out_offset, uint32_t* out_size);

// -----------------------------------------------------------------------------
// Metadata (key-value) Entry Iterator
// -----------------------------------------------------------------------------
typedef struct {
  timeseries_db_t* db;
  uint32_t page_offset;
  uint32_t page_size;
  uint32_t offset;
  uint32_t current_entry_offset;
  bool valid;
  const uint8_t* page_ptr; /* mmap’d view, NULL if mmap failed   */
  esp_partition_mmap_handle_t mmap_hdl;
} timeseries_entity_iterator_t;

bool timeseries_entity_iterator_init(timeseries_db_t* db, uint32_t page_offset, uint32_t page_size,
                                     timeseries_entity_iterator_t* ent_iter);

void timeseries_entity_iterator_deinit(timeseries_entity_iterator_t* ent_iter);

bool timeseries_entity_iterator_next(timeseries_entity_iterator_t* ent_iter, timeseries_entry_header_t* out_header);

bool timeseries_entity_iterator_read_data(timeseries_entity_iterator_t* ent_iter,
                                          const timeseries_entry_header_t* header, void* key_buf, void* value_buf);

// -----------------------------------------------------------------------------
// Field Data Iterator (using timeseries_field_data_header_t)
// -----------------------------------------------------------------------------
/**
 * @brief Iterator struct for field-data records in a field-data page.
 */
typedef struct {
  timeseries_db_t* db;
  uint32_t page_offset;
  uint32_t page_size;
  uint32_t offset;  // next read offset relative to page_offset
  uint32_t current_record_offset;
  bool valid;
} timeseries_fielddata_iterator_t;

/**
 * @brief An iterator that finds contiguous blank (all-0xFF) regions in
 *        4096-byte aligned sectors.
 */
typedef struct {
  bool valid;
  uint32_t min_size;
  uint32_t current_offset;  // where we are scanning in the partition
  size_t current_index;     // which entry in the page_cache we're examining

  // We'll accumulate a "free region" start + length as we go
  uint32_t run_start;
  uint32_t run_length;
  bool in_blank_run;

  // We still store a pointer to db->partition for partition->size
  const esp_partition_t* partition;
  timeseries_db_t* db;

  // Snapshot reference held during iteration
  tsdb_page_cache_snapshot_t* snapshot;
  bool owns_snapshot;  // true if we acquired it (and must release)
} timeseries_blank_iterator_t;

typedef struct {
  size_t index;
  bool valid;
  // Snapshot reference held during iteration
  tsdb_page_cache_snapshot_t* snapshot;
} timeseries_page_cache_iterator_t;

/**
 * @brief Initialize an iterator over a field data page.
 *        The first record is read after the page header.
 *
 * @param db           The DB context.
 * @param page_offset  The byte offset of this field-data page.
 * @param page_size    The size of this page.
 * @param f_iter       The field-data iterator to initialize.
 * @return true if success, false if invalid parameters.
 */
bool timeseries_fielddata_iterator_init(timeseries_db_t* db, uint32_t page_offset, uint32_t page_size,
                                        timeseries_fielddata_iterator_t* f_iter);

/**
 * @brief Move to the next field-data record in the page, returning its header.
 *
 * @param f_iter   The field-data iterator.
 * @param out_hdr  If non-null, the timeseries_field_data_header_t is written
 * here.
 *
 * @return true if a valid record was found, false if end of page or invalid
 * data.
 */
bool timeseries_fielddata_iterator_next(timeseries_fielddata_iterator_t* f_iter,
                                        timeseries_field_data_header_t* out_hdr);

/**
 * @brief Optionally read the raw data after the header if your format stores
 *        additional data points behind the header. This is up to your format:
 *        you can store a block of data of length = header->record_count * ...
 *
 * For example, if you have "n" data points after the header, you can read them
 * with timeseries_fielddata_iterator_read_data(...) into a buffer.
 */
bool timeseries_fielddata_iterator_read_data(timeseries_fielddata_iterator_t* f_iter,
                                             const timeseries_field_data_header_t* hdr, void* out_buf, size_t buf_len);

/**
 * @brief Initialize the blank-region iterator.
 *
 * @param db             Database context (must have a valid partition)
 * @param iter           Iterator object
 * @param min_size       Minimum contiguous size (in bytes) required
 * @return true if success, false if invalid args
 */
bool timeseries_blank_iterator_init(timeseries_db_t* db, timeseries_blank_iterator_t* iter, uint32_t min_size);

/**
 * @brief Find the next blank region that has at least `min_size` contiguous
 * bytes.
 *
 * @param iter       Iterator object
 * @param out_offset If found, the start offset of the blank region
 * @param out_size   The contiguous blank size (>= min_size)
 * @return true if a blank region was found; false if no more found or error
 */
bool timeseries_blank_iterator_next(timeseries_blank_iterator_t* iter, uint32_t* out_offset, uint32_t* out_size);

bool timeseries_page_cache_iterator_init(timeseries_db_t* db, timeseries_page_cache_iterator_t* iter);

bool timeseries_page_cache_iterator_next(timeseries_page_cache_iterator_t* iter, timeseries_page_header_t* out_header,
                                         uint32_t* out_offset, uint32_t* out_size);

void timeseries_page_cache_iterator_deinit(timeseries_page_cache_iterator_t* iter);

/**
 * @brief Initialize a blank iterator using an externally-provided snapshot.
 * The snapshot is NOT acquired (caller owns the reference).
 */
bool timeseries_blank_iterator_init_with_snapshot(timeseries_db_t* db, timeseries_blank_iterator_t* iter,
                                                   uint32_t min_size, tsdb_page_cache_snapshot_t* snapshot);

void timeseries_blank_iterator_deinit(timeseries_blank_iterator_t* iter);

#ifdef __cplusplus
}
#endif

#endif  // TIMESERIES_ITERATOR_H
