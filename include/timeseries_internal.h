#ifndef TIMESERIES_INTERNAL_H
#define TIMESERIES_INTERNAL_H

#include "esp_partition.h"
#include <stdbool.h>
#include <stdint.h>

// -----------------------------------------------------------------------------
// Page State Constants
// -----------------------------------------------------------------------------
#define TIMESERIES_PAGE_STATE_FREE 0xFF
#define TIMESERIES_PAGE_STATE_ACTIVE 0x01
#define TIMESERIES_PAGE_STATE_OBSOLETE 0x02

// -----------------------------------------------------------------------------
// Page Type Constants
// -----------------------------------------------------------------------------
#define TIMESERIES_PAGE_TYPE_METADATA 0x01
#define TIMESERIES_PAGE_TYPE_FIELD_DATA 0x02

// -----------------------------------------------------------------------------
// Entry (Metadata) Key Types
// -----------------------------------------------------------------------------
typedef enum {
  TIMESERIES_KEYTYPE_MEASUREMENT = 0x01,
  TIMESERIES_KEYTYPE_FIELDINDEX = 0x02,
  TIMESERIES_KEYTYPE_TAGINDEX = 0x03,
  TIMESERIES_KEYTYPE_FIELDLISTINDEX = 0x04,
  TIMESERIES_KEYTYPE_FIELDDATA = 0x10,
} timeseries_keytype_t;

// -----------------------------------------------------------------------------
// Delete Marker (for metadata entries)
// -----------------------------------------------------------------------------
#define TIMESERIES_DELETE_MARKER_VALID 0xFF
#define TIMESERIES_DELETE_MARKER_DELETED 0x00

// -----------------------------------------------------------------------------
// Page Size Definitions
// -----------------------------------------------------------------------------

/**
 * For metadata pages, we only need 8KB each.
 * (Be sure to handle partition writes/erases in multiples of 8KB.)
 */
#define TIMESERIES_METADATA_PAGE_SIZE (8 * 1024U)

/**
 * Field data pages are multiples of 32KB or 64KB, etc.
 * Here we define 32KB for demonstration.
 */
#define TIMESERIES_FIELD_DATA_PAGE_SIZE (8 * 1024U)

/**
 * Magic number used to identify TSDB pages: "TSDB" => 0x54534442
 */
#define TIMESERIES_MAGIC_NUM 0x54534442

// -----------------------------------------------------------------------------
// Page Header
// -----------------------------------------------------------------------------

/**
 * @brief Header stored at the start of each page.
 *
 * If page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA,
 * then field_data_level indicates the "level" (0=uncompressed, 1+=compressed).
 */
#pragma pack(push, 1)
typedef struct {
  uint32_t magic_number;  // e.g. TIMESERIES_MAGIC_NUM = 0x54534442
  uint8_t page_type;      // METADATA, FIELD_DATA
  uint8_t page_state;     // FREE, ACTIVE, OBSOLETE
  uint16_t reserved;      // alignment/future use
  uint32_t sequence_num;  // for compaction ordering

  // If page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA,
  // this indicates level (0=uncompressed, 1+=compressed).
  // Otherwise unused (0).
  uint8_t field_data_level;

  // Add a dynamic page size so we know how large this page is.
  // This will replace or augment your old static approach.
  uint32_t page_size;

  // Keep the rest of the reserved space for future expansions.
  uint8_t reserved2[3];

} timeseries_page_header_t;

/**
 * @brief Header stored before each *metadata* entry (key-value style).
 *
 * The layout is: [entry_header][key bytes][value bytes].
 * Typically used for measurement definitions, tags, indexes.
 */
typedef struct {
  uint8_t delete_marker;  // 0xFF = valid, 0x00 = soft-deleted
  uint8_t key_type;       // e.g. MEASUREMENT, TAGINDEX, etc.
  uint16_t key_len;       // key length in bytes
  uint16_t value_len;     // value length in bytes
                          // Followed by: key[key_len] + value[value_len]
} timeseries_entry_header_t;

/**
 * Bitmask flags for field data header.
 * Example usage:
 *   #define TSDB_FIELDDATA_FLAG_DELETED    (1 << 0)
 *   #define TSDB_FIELDDATA_FLAG_COMPRESSED (1 << 1)
 */
#define TSDB_FIELDDATA_FLAG_DELETED 0x01
#define TSDB_FIELDDATA_FLAG_COMPRESSED 0x02

/**
 * @brief Header stored at the start of each *field data* record
 *        (not key-value-based).
 *
 * The layout is:
 * [timeseries_field_data_header_t] + [raw data of points].
 *
 * This replaces the old approach of "key_len" + "value_len" for field data.
 */
typedef struct {
  /**
   * flags bitfield:
   *   bit0 => deleted
   *   bit1 => compressed
   *   etc.
   */
  uint8_t flags;

  /**
   * The number of records in this entry (like the # of points).
   */
  uint16_t record_count;
  uint16_t record_length;

  /**
   * Start and end timestamps for these points.
   * Could be used for quick scanning or indexing.
   */
  uint64_t start_time;
  uint64_t end_time;

  /**
   * The series ID (16 bytes) referencing measurement + tags + field name.
   */
  unsigned char series_id[16];

} timeseries_field_data_header_t;
#pragma pack(pop)

// -----------------------------------------------------------------------------
// DB Context
// -----------------------------------------------------------------------------

typedef struct {
  uint32_t offset;
  timeseries_page_header_t header;
} timeseries_cached_page_t;

// Cache entry structure for measurement name -> ID mapping
typedef struct {
  char* name;          // dynamically allocated measurement name
  uint32_t id;         // measurement ID
  uint32_t last_used;  // for LRU eviction
} measurement_cache_entry_t;

typedef struct {
  bool initialized;
  uint32_t next_measurement_id;
  uint32_t sequence_num;
  const esp_partition_t* partition;  // pointer to the 'storage' partition

  // Page cache
  timeseries_cached_page_t* page_cache;  // dynamic array
  size_t page_cache_count;
  size_t page_cache_capacity;

  // Last used L0 page/offset
  bool last_l0_cache_valid;
  uint32_t last_l0_page_offset;
  uint32_t last_l0_used_offset;

  // Measurement name->ID cache
  measurement_cache_entry_t* measurement_cache;
  size_t measurement_cache_count;
  size_t measurement_cache_capacity;
  uint32_t cache_access_counter;  // for LRU
} timeseries_db_t;

typedef struct {
  uint8_t* data;
  size_t size;
  size_t capacity;
} CompressedBuffer;

typedef struct {
  CompressedBuffer* cb;
  size_t offset;
} DecoderContext;

#endif  // TIMESERIES_INTERNAL_H
