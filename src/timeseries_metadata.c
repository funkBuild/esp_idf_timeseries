/**
 * @file timeseries_metadata.c
 * @brief Implementation of metadata-related logic (measurements, tags, series
 * types).
 */

#include "timeseries_metadata.h"

#include "esp_err.h"
#include "esp_log.h"
#include "esp_partition.h"
#include "timeseries_page_cache_snapshot.h"
#include "mbedtls/md.h"

#include <inttypes.h>
#include <stddef.h>
#include <string.h>

#include "timeseries_data.h"
#include "timeseries_flash_utils.h"
#include "timeseries_id_list.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_repair.h"
#include "timeseries_string_list.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

static const char* TAG = "TimeseriesMetadata";

// Measurement IDs count up from 1; 2^24 is far beyond any real database. An
// entry whose ID write was cut short keeps an erased (0xFF) top byte.
#define TSDB_MEASUREMENT_ID_LIMIT (1u << 24)
static inline bool tsdb_measurement_id_plausible(uint32_t id) { return id < TSDB_MEASUREMENT_ID_LIMIT; }

// Max linear probe distance for type cache collision resolution
#define TYPE_CACHE_MAX_PROBE 4

// -----------------------------------------------------------------------------
// Forward-declared static helpers
// -----------------------------------------------------------------------------

static bool tsdb_create_empty_metadata_page(timeseries_db_t* db, uint32_t page_offset);
static uint32_t tsdb_compact_metadata_page(timeseries_db_t* db, uint32_t page_offset, uint32_t *out_new_offset);

/* Open a page for a READ-ONLY scan: the page is copied to RAM with one flash
 * read and entries are served from the copy (one read per page instead of one
 * to three per entry). Falls back to per-entry flash reads if the copy cannot
 * be made. Callers that write to the page while iterating must use
 * timeseries_entity_iterator_init() directly so they see their own writes. */
static bool tsdb_meta_scan_open(timeseries_db_t* db, uint32_t page_offset, uint32_t page_size,
                                timeseries_entity_iterator_t* it) {
  if (!timeseries_entity_iterator_init(db, page_offset, page_size, it)) return false;
  (void)timeseries_entity_iterator_buffer(it);
  return true;
}

/* Where the next metadata entry of `need` bytes goes on this page. The entry
 * iterator stops without advancing both at the blank tail and at a header
 * whose extent overflows the page (a torn entry header); the second is not
 * free space, and writing there would AND a new entry into programmed bytes.
 * So a non-blank stop means the page is sealed (treated as full), and the
 * target range must read back erased before it is used. Returns false when
 * the entry cannot be appended to this page. */
static bool tsdb_meta_append_offset(timeseries_db_t* db, uint32_t page_offset, uint32_t page_size, uint32_t need,
                                    uint32_t* out_entry_offset) {
  timeseries_entity_iterator_t it;
  if (!timeseries_entity_iterator_init(db, page_offset, page_size, &it)) return false;
  uint32_t last = it.offset;
  timeseries_entry_header_t hdr;
  while (timeseries_entity_iterator_next(&it, &hdr)) last = it.offset;
  bool read_error = it.read_error;
  timeseries_entity_iterator_deinit(&it);
  if (read_error || last > page_size || need > page_size - last) return false;
  // Verify the tail from `last` (including the stop header) is erased.
  uint8_t buf[64];
  for (uint32_t done = 0; done < need;) {
    uint32_t n = need - done < sizeof(buf) ? need - done : (uint32_t)sizeof(buf);
    if (esp_partition_read(db->partition, page_offset + last + done, buf, n) != ESP_OK) return false;
    for (uint32_t i = 0; i < n; i++) {
      if (buf[i] != 0xFF) {
        ESP_LOGW(TAG, "Metadata page @0x%08" PRIx32 " has a non-blank tail @+0x%" PRIx32 "; treating it as full",
                 page_offset, last);
        return false;
      }
    }
    done += n;
  }
  *out_entry_offset = page_offset + last;
  return true;
}
static bool tsdb_insert_measurement_entry(timeseries_db_t* db, uint32_t page_offset, const char* measurement_name,
                                          uint32_t id);
// Every ACTIVE metadata page, in offset order. The list keeps up to four
// offsets inline (no allocation, the common case) and grows on the heap
// (PSRAM) beyond that; TSDB_META_PAGES frees it when it goes out of scope.
// *out_offsets/*found_count are set to the list's contents.
// tsdb_find_metadata_pages returns false only when the list could not be
// built (bad arguments, snapshot unavailable, OOM growing the list); no
// metadata pages is success with *found_count == 0. Callers that act on
// "no metadata" must check the count and must not treat a false return as
// an empty metadata set, or an OOM becomes a silent empty answer.
typedef struct {
  uint32_t *heap;
  size_t heap_capacity;
  uint32_t inline_offsets[4];
} tsdb_meta_pages_t;
static void tsdb_meta_pages_release(tsdb_meta_pages_t* list) {
  free(list->heap);
  list->heap = NULL;
  list->heap_capacity = 0;
}
#define TSDB_META_PAGES(name) tsdb_meta_pages_t name __attribute__((cleanup(tsdb_meta_pages_release))) = {0}
static bool tsdb_find_metadata_pages(timeseries_db_t* db, tsdb_meta_pages_t* list, const uint32_t** out_offsets,
                                     size_t* found_count);

typedef enum {
  TSDB_META_WALK_NO_PAGES, /* page lookup failed, or no metadata pages */
  TSDB_META_WALK_DONE,     /* every page was visited */
  TSDB_META_WALK_STOPPED,  /* the visitor stopped the walk */
} tsdb_meta_walk_result_t;

/* Called for each VALID entry of the walked key type. Returning false stops
 * the walk; the visitor records why in its context. */
typedef bool (*tsdb_meta_visit_fn)(timeseries_entity_iterator_t* it, const timeseries_entry_header_t* hdr,
                                   void* ctx);

/* Read-only walk over every active metadata page, in page order, handing
 * the visitor each valid entry of one key type. A page whose iterator
 * cannot be opened is skipped. */
static tsdb_meta_walk_result_t tsdb_meta_walk(timeseries_db_t* db, timeseries_keytype_t key_type,
                                              tsdb_meta_visit_fn visit, void* ctx);

static bool tsdb_append_series_ids(timeseries_series_id_list_t* list, const unsigned char* ids, size_t n) {
  for (size_t i = 0; i < n; i++) {
    if (!tsdb_series_id_list_append(list, ids + i * 16)) {
      return false;
    }
  }
  return true;
}

// tsdb_soft_delete_entry is declared in timeseries_metadata.h (non-static)
static bool tsdb_retire_replaced_entry(timeseries_db_t* db, uint32_t gen_before, uint32_t page_offset,
                                       uint32_t entry_offset, timeseries_keytype_t key_type, const char* key_str,
                                       const unsigned char* old_value, size_t old_value_len);

static bool tsdb_create_new_index_entry(timeseries_db_t* db, uint32_t page_offset, timeseries_keytype_t key_type,
                                        const char* key_str, const unsigned char* series_ids, size_t series_ids_len);

static bool tsdb_upsert_seriesid_list_entry(timeseries_db_t* db, timeseries_keytype_t target_key_type,
                                            const uint32_t* meta_offsets, size_t meta_count, const char* key_str,
                                            const unsigned char series_id[16]);

static inline size_t hash_series_id(const unsigned char series_id[16], size_t cache_size);

// Type-cache helpers (defined near the type-cache code below). Both take
// db->type_cache_mutex internally, so callers must NOT hold it.
static bool tsdb_type_cache_get(timeseries_db_t* db, const unsigned char series_id[16],
                                timeseries_field_type_e* out_type);
static void tsdb_type_cache_store(timeseries_db_t* db, const unsigned char series_id[16],
                                  timeseries_field_type_e type);

static bool tsdb_upsert_tagindex_entry(timeseries_db_t* db, const uint32_t* meta_offsets, size_t meta_count,
                                       const char* key_str, const unsigned char series_id[16]);

static bool tsdb_upsert_fieldlist_entry(timeseries_db_t* db, const uint32_t* meta_offsets, size_t meta_count,
                                        const char* key_str, const unsigned char series_id[16]);

// -----------------------------------------------------------------------------
// Public API Implementations
// -----------------------------------------------------------------------------

bool timeseries_metadata_create_page(timeseries_db_t* db) {
  if (!db) {
    return false;
  }

  uint32_t blank_offset = 0, blank_length = 0;
  if (!tsdb_reserve_blank_region(db, TIMESERIES_METADATA_PAGE_SIZE, NULL,
                                 &blank_offset, &blank_length)) {
    ESP_LOGE(TAG, "No blank region found to create metadata page!");
    return false;
  }

  ESP_LOGV(TAG, "Found blank region @0x%08" PRIx32 " (length=%" PRIu32 " bytes).", blank_offset, blank_length);

  // 3) Create the empty metadata page here
  if (!tsdb_create_empty_metadata_page(db, blank_offset)) {
    ESP_LOGE(TAG, "Failed to create metadata page @0x%08" PRIx32, blank_offset);
    return false;
  }

  return true;
}

bool tsdb_load_pages_into_memory(timeseries_db_t* db) {
  if (!db) {
    return false;
  }
  ESP_LOGV(TAG, "Scanning 'storage' partition for existing TSDB pages...");

  if (!tsdb_build_page_cache(db)) {
    ESP_LOGE(TAG, "Failed to build page cache");
    return false;
  }

  ESP_LOGV(TAG, "Page cache built => %u total entries",
           (unsigned)(db->current_snapshot ? db->current_snapshot->count : 0));

  // 1) Use the page iterator to check if a metadata page exists
  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    return false;
  }

  bool found_metadata_page = false;
  unsigned int pages_scanned = 0;
  uint32_t newest_seq = 0, newest_end = 0;

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0;
  uint32_t page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset, &page_size)) {
    pages_scanned++;

    // If we find an ACTIVE metadata page, note it
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM) {
      ESP_LOGV(TAG,
               "Found TSDB page at offset=0x%08" PRIx32 " (type=%" PRIu8 ", state=%" PRIu8 ", seq=%" PRIu32
               ", size=%" PRIu32 ")",
               page_offset, hdr.page_type, hdr.page_state, hdr.sequence_num, hdr.page_size);

      if (hdr.page_type == TIMESERIES_PAGE_TYPE_METADATA && hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
        found_metadata_page = true;
      }

      // Update sequence number
      if (hdr.sequence_num > db->sequence_num) {
        db->sequence_num = hdr.sequence_num;
      }
      if (hdr.sequence_num >= newest_seq) {
        newest_seq = hdr.sequence_num;
        newest_end = page_offset + page_size;
      }
    }
  }
  timeseries_page_cache_iterator_deinit(&page_iter);
  // L0 pages continue next-fit after the newest page (timeseries_data.h).
  db->l0_cursor = newest_end < db->partition->size ? newest_end : 0;

  ESP_LOGV(TAG, "Scanned %u pages in 'storage' partition.", pages_scanned);

  // 2) If no metadata page found, create one
  if (!found_metadata_page) {
    ESP_LOGV(TAG, "No active metadata page found; creating one...");
    found_metadata_page = timeseries_metadata_create_page(db);
  }

  return true;
}

bool tsdb_find_measurement_id(timeseries_db_t* db, const char* measurement_name, uint32_t* out_id) {
  if (!db || !measurement_name || !out_id) {
    return false;
  }

  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count) || meta_count == 0) {
    ESP_LOGW(TAG, "No metadata pages found to search for measurement '%s'.", measurement_name);
    return false;
  }

  // For each active metadata page offset
  for (size_t i = 0; i < meta_count; i++) {
    uint32_t page_offset = meta_offsets[i];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;  // 8 KB

    // Initialize an entity iterator for this page
    timeseries_entity_iterator_t ent_iter;
    if (!tsdb_meta_scan_open(db, page_offset, page_size, &ent_iter)) {
      ESP_LOGW(TAG, "Failed to init entity iterator for meta offset=0x%08" PRIx32, page_offset);
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      ESP_LOGV(TAG, "Reading entry at offset=0x%08" PRIx32, ent_iter.offset);

      // We only care about measurement entries
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_MEASUREMENT) {
        continue;
      }
      if (e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }

      if (e_hdr.value_len != sizeof(uint32_t)) {
        ESP_LOGW(TAG,
                 "Skipping measurement entry with bad value_len=%u (key_type=%u key_len=%u)",
                 (unsigned)e_hdr.value_len, (unsigned)e_hdr.key_type, (unsigned)e_hdr.key_len);
        continue;
      }

      if (e_hdr.key_len == strlen(measurement_name)) {
        if (e_hdr.key_len < 256) {
          char *key_buf = (char *)malloc(256);
          if (!key_buf) {
            ESP_LOGE(TAG, "OOM allocating key_buf in tsdb_find_measurement_id");
            continue;
          }
          // read the key + the value
          uint32_t val = 0;
          if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, key_buf, &val)) {
            ESP_LOGW(TAG, "Failed reading measurement entry data.");
            free(key_buf);
            continue;
          }

          key_buf[e_hdr.key_len] = '\0';

          ESP_LOGV(TAG, "Read measurement entry: key='%s' val=%" PRIu32, key_buf, val);

          // Compare. A torn ID is skipped, so the name gets a fresh one.
          if (strcmp(key_buf, measurement_name) == 0 && tsdb_measurement_id_plausible(val)) {
            *out_id = val;
            free(key_buf);
            timeseries_entity_iterator_deinit(&ent_iter);
            return true;
          }
          free(key_buf);
        }
      }
    }

    timeseries_entity_iterator_deinit(&ent_iter);
  }

  // Not found in any metadata page
  return false;
}

bool tsdb_create_measurement_id(timeseries_db_t* db, const char* measurement_name, uint32_t* out_id) {
  if (!db || !measurement_name || !out_id) {
    return false;
  }

  // Grab next ID from db
  uint32_t new_id = db->next_measurement_id++;
  *out_id = new_id;

  // We pick the first active metadata page offset to store the measurement
  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count) || meta_count == 0) {
    ESP_LOGE(TAG, "No active metadata page to insert measurement '%s'.", measurement_name);
    return false;
  }

  // Try each metadata page, create new one if all are full
  bool inserted = false;
  for (int attempt = 0; attempt < 2 && !inserted; attempt++) {
    for (size_t p = 0; p < meta_count && !inserted; p++) {
      inserted = tsdb_insert_measurement_entry(db, meta_offsets[p], measurement_name, new_id);
    }
    if (!inserted && attempt == 0) {
      if (!timeseries_metadata_create_page(db)) {
        break;
      }
      meta_count = 0;
      if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count) || meta_count == 0) {
        break;
      }
    }
  }
  if (!inserted) {
    ESP_LOGE(TAG, "Failed to insert measurement entry for '%s'!", measurement_name);
    return false;
  }

  ESP_LOGV(TAG, "Created measurement '%s' with ID=%" PRIu32, measurement_name, new_id);
  return true;
}

/* The measurement ID an index key ("<id>:field" or "<id>:tag:value") names,
 * or false when the key does not start that way. */
static bool index_key_measurement_id(const char *key, size_t len, uint32_t *out) {
  uint32_t id = 0;
  size_t i = 0;
  for (; i < len && key[i] >= '0' && key[i] <= '9'; i++) {
    id = id * 10u + (uint32_t)(key[i] - '0');
    if (!tsdb_measurement_id_plausible(id)) return false;
  }
  if (i == 0 || i == len || key[i] != ':') return false;
  *out = id;
  return true;
}

bool tsdb_find_max_measurement_id(timeseries_db_t *db, uint32_t *out_max) {
  if (!db || !out_max) {
    return false;
  }
  *out_max = 0;

  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  // No metadata pages => max is 0 (meta_count == 0). A failed lookup is not
  // "no measurements": reporting 0 would let new measurements reuse IDs.
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count)) {
    return false;
  }
  if (meta_count == 0) return true;

  // Index keys carry their measurement's ID too. Normally none exceeds the
  // measurement entries' maximum; after a metadata page is lost (erased by
  // init recovery) the index entries of its measurements can outlive them,
  // and a new measurement given one of their IDs would inherit their series.
  char *key = malloc(256);
  if (!key) {
    tsdb_init_fault_transient(db);
    return false;
  }
  bool ok = true;
  for (size_t i = 0; i < meta_count; i++) {
    uint32_t page_offset = meta_offsets[i];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

    timeseries_entity_iterator_t ent_iter;
    if (!tsdb_meta_scan_open(db, page_offset, page_size, &ent_iter)) {
      continue;
    }

    // A read failure here (entity header, key or value) leaves the maximum
    // unknown: record the page for init's recovery and fail.
    bool unreadable = false;
    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      if (e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }
      uint32_t val = 0;
      if (e_hdr.key_type == TIMESERIES_KEYTYPE_MEASUREMENT) {
        // Read only the measurement ID (the value); the name may be up to 255
        // bytes, and skipping long names made their IDs invisible here, so
        // the next measurement reused them. An ID torn by an interrupted write
        // (0xFF top byte) is not a real ID and must not wrap next_id to 0.
        if (e_hdr.value_len != sizeof(uint32_t)) continue;
        if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, NULL, &val)) {
          unreadable = true;
          break;
        }
      } else if (e_hdr.key_type == TIMESERIES_KEYTYPE_TAGINDEX ||
                 e_hdr.key_type == TIMESERIES_KEYTYPE_FIELDLISTINDEX) {
        if (e_hdr.key_len == 0 || e_hdr.key_len > 255) continue;
        if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, key, NULL)) {
          unreadable = true;
          break;
        }
        if (!index_key_measurement_id(key, e_hdr.key_len, &val)) continue;
      } else {
        continue;
      }
      if (tsdb_measurement_id_plausible(val) && val > *out_max) {
        *out_max = val;
      }
    }
    if (unreadable || ent_iter.read_error) {
      ESP_LOGE(TAG, "Metadata page @0x%08" PRIx32 " could not be read while restoring the measurement ID",
               page_offset);
      tsdb_init_fault_bad_page(db, page_offset, page_size);
      ok = false;
    }
    timeseries_entity_iterator_deinit(&ent_iter);
  }
  free(key);
  return ok;
}


/**
 * @brief Lookup an existing field type in metadata for the given series_id.
 *
 * @param db          The DB context
 * @param series_id   16-byte unique identifier for the series
 * @param out_type    If found, store the existing type here
 * @return true if found a match, false if not found or error
 */
static tsdb_series_type_status_t scan_series_type(timeseries_db_t* db, const unsigned char series_id[16],
                                                  timeseries_field_type_e* out_type) {
  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count) || meta_count == 0) {
    ESP_LOGE(TAG, "No active metadata pages for series ID lookup");
    return TSDB_SERIES_TYPE_UNKNOWN;
  }

  bool complete = true;
  for (size_t i = 0; i < meta_count; i++) {
    timeseries_entity_iterator_t ent_iter;
    if (!tsdb_meta_scan_open(db, meta_offsets[i], TIMESERIES_METADATA_PAGE_SIZE, &ent_iter)) {
      complete = false;
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_FIELDINDEX || e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }
      // must be 16-byte key, 1-byte value
      if (e_hdr.key_len == 16 && e_hdr.value_len == 1) {
        unsigned char key_buf[16];
        uint8_t stored_type = 0;
        if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, key_buf, &stored_type)) {
          complete = false;
          continue;
        }
        if (memcmp(key_buf, series_id, 16) == 0) {
          *out_type = (timeseries_field_type_e)stored_type;
          timeseries_entity_iterator_deinit(&ent_iter);
          return TSDB_SERIES_TYPE_FOUND;
        }
      }
    }
    if (ent_iter.read_error) complete = false;
    timeseries_entity_iterator_deinit(&ent_iter);
  }
  return complete ? TSDB_SERIES_TYPE_ABSENT : TSDB_SERIES_TYPE_UNKNOWN;
}

/**
 * @brief Lookup an existing field type in metadata for the given series_id.
 *
 * @param db          The DB context
 * @param series_id   16-byte unique identifier for the series
 * @param out_type    If found, store the existing type here
 * @return true if found a match, false if not found or error
 */
bool tsdb_lookup_series_type_in_metadata(timeseries_db_t* db, const unsigned char series_id[16],
                                         timeseries_field_type_e* out_type) {
  if (!db || !series_id || !out_type) {
    return false;
  }
  return scan_series_type(db, series_id, out_type) == TSDB_SERIES_TYPE_FOUND;
}

/**
 * @brief Ensure the given series_id has a matching field_type in metadata,
 *        or create a new entry if it doesn't exist. Returns false on conflict.
 */
bool tsdb_ensure_series_type_in_metadata(timeseries_db_t* db, const unsigned char series_id[16],
                                         timeseries_field_type_e field_type) {
  if (!db || !series_id) {
    return false;
  }

  // 1) Check RAM cache first, then fall back to flash scan
  timeseries_field_type_e existing_type;
  if (tsdb_lookup_series_type_cached(db, series_id, &existing_type)) {
    // found existing
    if (existing_type != field_type) {
      ESP_LOGE(TAG, "SeriesID conflict: existing type=%d, new=%d", (int)existing_type, (int)field_type);
      if (db->log_hook) {
        char msg[96];
        snprintf(msg, sizeof(msg), "Timeseries type conflict: existing=%d, new=%d", (int)existing_type,
                 (int)field_type);
        db->log_hook(ESP_LOG_ERROR, msg);
      }
      return false;
    }
    // else it matches => done
    ESP_LOGV(TAG, "SeriesID already has matching type=%d", (int)field_type);
    return true;
  }

  // 2) Not found => create new entry, trying each metadata page
  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count) || meta_count == 0) {
    ESP_LOGE(TAG, "No active metadata page to create new field index");
    return false;
  }

  // Try each metadata page until one has space
  for (int attempt = 0; attempt < 2; attempt++) {
    for (size_t p = 0; p < meta_count; p++) {
      uint32_t page_offset = meta_offsets[p];
      uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

      timeseries_entry_header_t new_hdr;
      memset(&new_hdr, 0, sizeof(new_hdr));
      new_hdr.delete_marker = TIMESERIES_DELETE_MARKER_VALID;
      new_hdr.key_type = TIMESERIES_KEYTYPE_FIELDINDEX;
      new_hdr.key_len = 16;
      new_hdr.value_len = 1;

      uint32_t total_entry_size = sizeof(new_hdr) + new_hdr.key_len + new_hdr.value_len;
      uint32_t entry_offset = 0;
      if (!tsdb_meta_append_offset(db, page_offset, page_size, total_entry_size, &entry_offset)) {
        uint32_t reclaimed = tsdb_compact_metadata_page(db, page_offset, &page_offset);
        if (reclaimed == 0 ||
            !tsdb_meta_append_offset(db, page_offset, page_size, total_entry_size, &entry_offset)) {
          continue;  // Still full, try next page
        }
      }

      // Build the complete entry in one buffer: a single write (not atomic: the
      // flash driver programs it in chunks; tsdb_meta_append_offset seals a torn tail)
      uint8_t entry_buf[sizeof(new_hdr) + 16 + 1];
      uint8_t stored_type = (uint8_t)field_type;
      memcpy(entry_buf, &new_hdr, sizeof(new_hdr));
      memcpy(entry_buf + sizeof(new_hdr), series_id, 16);
      memcpy(entry_buf + sizeof(new_hdr) + 16, &stored_type, 1);

      esp_err_t err = esp_partition_write(db->partition, entry_offset, entry_buf, sizeof(entry_buf));
      if (err != ESP_OK) {
        return false;
      }

      // Populate type cache so subsequent inserts skip flash lookup
      tsdb_type_cache_store(db, series_id, field_type);

      ESP_LOGV(TAG, "Created new fieldindex for series=%.2X%.2X%.2X%.2X..., type=%d", series_id[0], series_id[1],
               series_id[2], series_id[3], (int)field_type);
      return true;
    }

    // All pages full on first attempt - create a new metadata page and retry
    if (attempt == 0) {
      if (!timeseries_metadata_create_page(db)) {
        ESP_LOGE(TAG, "Failed to create new metadata page for field index");
        return false;
      }
      meta_count = 0;
      if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count) || meta_count == 0) {
        return false;
      }
    }
  }

  ESP_LOGE(TAG, "All metadata pages full for field index entry");
  return false;
}

bool tsdb_index_tags_for_series(timeseries_db_t* db, uint32_t measurement_id, const char** tag_keys,
                                const char** tag_values, size_t num_tags, const unsigned char series_id[16]) {
  if (!db || !series_id) {
    return false;
  }
  if (num_tags == 0) {
    return true;
  }

  // Limit batch size to keep stack usage bounded
  if (num_tags > 16) {
    // Fall back to per-tag upsert for unusually large tag sets. An upsert can
    // relocate a metadata page, so the page list is re-read for every tag.
    TSDB_META_PAGES(meta_list);
    const uint32_t* meta_offsets = NULL;
    size_t meta_count = 0;
    for (size_t i = 0; i < num_tags; i++) {
      if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count) || meta_count == 0) {
        return false;
      }
      char key_buf[128];
      int n = snprintf(key_buf, sizeof(key_buf), "%" PRIu32 ":%s:%s", measurement_id, tag_keys[i], tag_values[i]);
      if (n < 0 || (size_t)n >= sizeof(key_buf)) return false;
      if (!tsdb_upsert_tagindex_entry(db, meta_offsets, meta_count, key_buf, series_id)) return false;
    }
    return true;
  }

  // Build all tag key strings upfront (heap-allocated to avoid 2KB stack pressure)
  char* tag_key_bufs = malloc(num_tags * 128);
  if (!tag_key_bufs) {
    return false;
  }
  size_t tag_key_lens[16];
  bool tag_done[16];  // true once found/updated
  for (size_t i = 0; i < num_tags; i++) {
    int n = snprintf(tag_key_bufs + i * 128, 128, "%" PRIu32 ":%s:%s",
                     measurement_id, tag_keys[i], tag_values[i]);
    if (n < 0 || (size_t)n >= 128) {
      free(tag_key_bufs);
      return false;
    }
    tag_key_lens[i] = (size_t)n;
    tag_done[i] = false;
  }

  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  // Single scan: iterate each metadata page once, checking all pending tags.
  // A replacement that relocates a metadata page retires meta_offsets[] and
  // the page ent_iter walks, so the scan restarts from a fresh page list; the
  // tag just replaced is already marked done, so at most num_tags restarts.
rescan:
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count) || meta_count == 0) {
    ESP_LOGE(TAG, "No active metadata pages for tag indexing");
    free(tag_key_bufs);
    return false;
  }

  for (size_t p = 0; p < meta_count; p++) {
    // Check if all tags are done
    bool all_done = true;
    for (size_t t = 0; t < num_tags; t++) {
      if (!tag_done[t]) { all_done = false; break; }
    }
    if (all_done) break;

    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, meta_offsets[p], TIMESERIES_METADATA_PAGE_SIZE, &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_TAGINDEX ||
          e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID ||
          (e_hdr.value_len % 16 != 0)) {
        continue;
      }

      // Check this entry against all pending tags
      for (size_t t = 0; t < num_tags; t++) {
        if (tag_done[t] || e_hdr.key_len != tag_key_lens[t]) continue;

        if (e_hdr.value_len == 0) continue;

        char stored_key[128];
        if (e_hdr.key_len >= sizeof(stored_key)) continue;
        size_t series_count = e_hdr.value_len / 16;
        unsigned char* all_series_ids = malloc(e_hdr.value_len);
        if (!all_series_ids) {
          timeseries_entity_iterator_deinit(&ent_iter);
          free(tag_key_bufs);
          return false;
        }

        if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, stored_key, all_series_ids)) {
          free(all_series_ids);
          continue;
        }
        stored_key[tag_key_lens[t]] = '\0';

        if (strcmp(stored_key, tag_key_bufs + t * 128) != 0) {
          free(all_series_ids);
          continue;
        }

        // Found matching entry for tag t
        tag_done[t] = true;

        bool already_present = false;
        for (size_t s = 0; s < series_count; s++) {
          if (memcmp(all_series_ids + s * 16, series_id, 16) == 0) {
            already_present = true;
            break;
          }
        }

        if (!already_present) {
          size_t new_size = (series_count + 1) * 16;
          unsigned char* new_ids = malloc(new_size);
          if (!new_ids) {
            free(all_series_ids);
            timeseries_entity_iterator_deinit(&ent_iter);
            free(tag_key_bufs);
            return false;
          }
          memcpy(new_ids, all_series_ids, series_count * 16);
          memcpy(new_ids + series_count * 16, series_id, 16);

          const uint32_t gen_before = db->metadata_page_gen;
          bool entry_created = tsdb_create_new_index_entry(
              db, meta_offsets[p], TIMESERIES_KEYTYPE_TAGINDEX,
              tag_key_bufs + t * 128, new_ids, new_size);
          if (!entry_created) {
            for (size_t q = 0; q < meta_count && !entry_created; q++) {
              if (meta_offsets[q] != meta_offsets[p]) {
                entry_created = tsdb_create_new_index_entry(
                    db, meta_offsets[q], TIMESERIES_KEYTYPE_TAGINDEX,
                    tag_key_bufs + t * 128, new_ids, new_size);
              }
            }
          }
          if (!entry_created && timeseries_metadata_create_page(db)) {
            TSDB_META_PAGES(new_list);
            const uint32_t* new_offsets = NULL;
            size_t new_count = 0;
            if (tsdb_find_metadata_pages(db, &new_list, &new_offsets, &new_count)) {
              for (size_t q = 0; q < new_count && !entry_created; q++) {
                entry_created = tsdb_create_new_index_entry(
                    db, new_offsets[q], TIMESERIES_KEYTYPE_TAGINDEX,
                    tag_key_bufs + t * 128, new_ids, new_size);
              }
            }
          }
          free(new_ids);

          if (!entry_created) {
            free(all_series_ids);
            timeseries_entity_iterator_deinit(&ent_iter);
            free(tag_key_bufs);
            return false;
          }

          if (!tsdb_retire_replaced_entry(db, gen_before, meta_offsets[p], ent_iter.current_entry_offset,
                                          TIMESERIES_KEYTYPE_TAGINDEX, tag_key_bufs + t * 128,
                                          all_series_ids, series_count * 16)) {
            ESP_LOGE(TAG, "Failed to retire old tag-index entry after replacement");
            free(all_series_ids);
            timeseries_entity_iterator_deinit(&ent_iter);
            free(tag_key_bufs);
            return false;
          }
          if (db->metadata_page_gen != gen_before) {
            free(all_series_ids);
            timeseries_entity_iterator_deinit(&ent_iter);
            goto rescan;
          }
        }

        free(all_series_ids);
        break;  // This entry matched one tag; move to next entry
      }  // for each pending tag
    }  // while next entity

    timeseries_entity_iterator_deinit(&ent_iter);
  }  // for each metadata page

  // Create new entries for tags not found in any page. Creating one can
  // relocate a metadata page, so the page list is re-read for every tag.
  for (size_t t = 0; t < num_tags; t++) {
    if (tag_done[t]) continue;

    bool created = false;
    if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count)) {
      ESP_LOGE(TAG, "Metadata page lookup failed while indexing tag '%s'", tag_key_bufs + t * 128);
      free(tag_key_bufs);
      return false;
    }
    for (size_t p = 0; p < meta_count && !created; p++) {
      created = tsdb_create_new_index_entry(
          db, meta_offsets[p], TIMESERIES_KEYTYPE_TAGINDEX,
          tag_key_bufs + t * 128, series_id, 16);
    }
    if (!created && timeseries_metadata_create_page(db)) {
      TSDB_META_PAGES(new_list);
      const uint32_t* new_offsets = NULL;
      size_t new_count = 0;
      if (tsdb_find_metadata_pages(db, &new_list, &new_offsets, &new_count)) {
        for (size_t p = 0; p < new_count && !created; p++) {
          created = tsdb_create_new_index_entry(
              db, new_offsets[p], TIMESERIES_KEYTYPE_TAGINDEX,
              tag_key_bufs + t * 128, series_id, 16);
        }
      }
    }
    if (!created) {
      ESP_LOGE(TAG, "Failed to create tag index entry for '%s'", tag_key_bufs + t * 128);
      free(tag_key_bufs);
      return false;
    }
  }

  free(tag_key_bufs);
  return true;
}

// -----------------------------------------------------------------------------
// Private (static) Helpers
// -----------------------------------------------------------------------------

/**
 * @brief Compact a metadata page by removing soft-deleted entries.
 *
 * Uses a two-phase commit to avoid data loss on power failure:
 *   1. Write live entries to a NEW metadata page (allocated from a blank region).
 *   2. Mark the old page OBSOLETE (NOR-safe bit-clear: 0x01 -> 0x00).
 *   3. Update the page cache (remove old, add new).
 *
 * If any step fails, both pages remain valid and no data is lost.
 * Returns the amount of free space reclaimed, or 0 if compaction is not
 * possible or not worthwhile.
 */
static uint32_t tsdb_compact_metadata_page(timeseries_db_t* db, uint32_t page_offset, uint32_t *out_new_offset) {
  if (!db || !db->partition) {
    return 0;
  }

  uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

  // First pass: count live vs deleted entries and total live data size
  timeseries_entity_iterator_t ent_iter;
  if (!timeseries_entity_iterator_init(db, page_offset, page_size, &ent_iter)) {
    return 0;
  }

  size_t live_data_size = 0;
  size_t deleted_data_size = 0;
  timeseries_entry_header_t e_hdr;
  while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
    size_t entry_size = sizeof(e_hdr) + e_hdr.key_len + e_hdr.value_len;
    if (e_hdr.delete_marker == TIMESERIES_DELETE_MARKER_VALID) {
      live_data_size += entry_size;
    } else {
      deleted_data_size += entry_size;
    }
  }
  timeseries_entity_iterator_deinit(&ent_iter);

  if (deleted_data_size == 0) {
    return 0;  // Nothing to compact
  }

  ESP_LOGI(TAG, "Compacting metadata page @0x%08" PRIx32 ": %zu bytes live, %zu bytes deleted",
           page_offset, live_data_size, deleted_data_size);

  // Allocate buffer for all live entries
  uint8_t *live_buf = NULL;
  if (live_data_size > 0) {
    live_buf = malloc(live_data_size);
    if (!live_buf) {
      ESP_LOGE(TAG, "OOM allocating buffer for metadata compaction");
      return 0;
    }
  }

  // Second pass: copy live entries into buffer
  if (!timeseries_entity_iterator_init(db, page_offset, page_size, &ent_iter)) {
    free(live_buf);
    return 0;
  }

  size_t buf_offset = 0;
  while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
    if (e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
      continue;
    }
    size_t entry_size = sizeof(e_hdr) + e_hdr.key_len + e_hdr.value_len;

    // Read the full entry (header + key + value) from flash
    uint32_t entry_flash_offset = page_offset + ent_iter.current_entry_offset;
    if (esp_partition_read(db->partition, entry_flash_offset, live_buf + buf_offset, entry_size) != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading entry during metadata compaction");
      free(live_buf);
      timeseries_entity_iterator_deinit(&ent_iter);
      return 0;
    }
    buf_offset += entry_size;
  }
  timeseries_entity_iterator_deinit(&ent_iter);

  // --- Two-phase commit: write new page, then mark old page obsolete ---

  // Phase 1a: Atomically reserve and erase a region for the new page.
  uint32_t new_page_offset = 0, blank_length = 0;
  if (!tsdb_reserve_blank_region(db, page_size, NULL, &new_page_offset, &blank_length)) {
    ESP_LOGE(TAG, "No blank region available for metadata compaction — skipping");
    free(live_buf);
    return 0;
  }

  // Phase 1c: Build and write the new page header, FREE until every entry is
  // on flash: a reset before activation leaves a page startup (and 1.0.19)
  // ignores, never a second, partially written ACTIVE metadata page.
  timeseries_page_header_t new_hdr;
  memset(&new_hdr, 0xFF, sizeof(new_hdr));
  new_hdr.magic_number = TIMESERIES_MAGIC_NUM;
  new_hdr.page_type = TIMESERIES_PAGE_TYPE_METADATA;
  new_hdr.page_state = TIMESERIES_PAGE_STATE_FREE;
  // Atomic increment to avoid duplicate sequence numbers across concurrent
  // page allocations (the field is _Atomic but ++ is a non-atomic RMW).
  new_hdr.sequence_num = atomic_fetch_add(&db->sequence_num, 1) + 1;
  new_hdr.field_data_level = 0;
  new_hdr.page_size = page_size;

  if (esp_partition_write(db->partition, new_page_offset, &new_hdr, sizeof(new_hdr)) != ESP_OK) {
    ESP_LOGE(TAG, "Failed writing header to new metadata page @0x%08" PRIx32, new_page_offset);
    tsdb_release_reserved_region(db, new_page_offset, NULL);
    free(live_buf);
    return 0;
  }

  // Phase 1d: Write all live entries after the header
  if (buf_offset > 0) {
    uint32_t write_offset = new_page_offset + sizeof(new_hdr);
    if (esp_partition_write(db->partition, write_offset, live_buf, buf_offset) != ESP_OK) {
      ESP_LOGE(TAG, "Failed writing entries to new metadata page @0x%08" PRIx32, new_page_offset);
      tsdb_release_reserved_region(db, new_page_offset, NULL);
      free(live_buf);
      return 0;
    }
  }

  free(live_buf);
  live_buf = NULL;

  // Phase 1e: Activate (FREE 0xFF -> ACTIVE 0x01 only clears bits).
  if (tsdb_flash_write_byte(db->partition, new_page_offset + offsetof(timeseries_page_header_t, page_state),
                            TIMESERIES_PAGE_STATE_ACTIVE) != ESP_OK) {
    ESP_LOGE(TAG, "Failed activating new metadata page @0x%08" PRIx32, new_page_offset);
    tsdb_release_reserved_region(db, new_page_offset, NULL);
    return 0;
  }
  new_hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;

  if (!tsdb_finalize_reserved_region(db, new_page_offset, &new_hdr, NULL)) {
    ESP_LOGE(TAG, "Failed finalizing metadata reservation @0x%08" PRIx32,
             new_page_offset);
    tsdb_release_reserved_region(db, new_page_offset, NULL);
    return 0;
  }

  // New page is now fully written and ACTIVE on flash.
  // Even if power is lost after this point, both old and new pages are valid
  // ACTIVE pages with correct data — recovery will see duplicates but no loss.

  // Phase 2: Mark the old page OBSOLETE (NOR-safe: clear bit 0 of page_state,
  // transitioning 0x01 -> 0x00). Read-modify-write the header.
  timeseries_page_header_t old_hdr;
  if (esp_partition_read(db->partition, page_offset, &old_hdr, sizeof(old_hdr)) != ESP_OK) {
    // The old page's state is unknown: keep both copies rather than risk
    // losing the metadata (both are ACTIVE and identical at this point).
    ESP_LOGW(TAG, "Failed reading old header for obsolete marking — both pages remain active");
    db->metadata_page_gen++;
    if (out_new_offset) *out_new_offset = new_page_offset;
    return (uint32_t)deleted_data_size;
  }

  old_hdr.page_state = TIMESERIES_PAGE_STATE_OBSOLETE;
  if (esp_partition_write(db->partition, page_offset, &old_hdr, sizeof(old_hdr)) != ESP_OK) {
    // Two ACTIVE copies of every entry would diverge: later updates and
    // soft-deletes reach only one of them. If the old page is still ACTIVE on
    // flash, keep it and give the new page back.
    timeseries_page_header_t check;
    if (esp_partition_read(db->partition, page_offset, &check, sizeof(check)) == ESP_OK &&
        check.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
      ESP_LOGW(TAG, "Failed marking old page @0x%08" PRIx32 " obsolete; keeping the uncompacted page",
               page_offset);
      tsdb_release_reserved_region(db, new_page_offset, NULL);
      return 0;
    }
    ESP_LOGW(TAG, "Marking old page @0x%08" PRIx32 " obsolete reported failure but took effect", page_offset);
  }

  // Phase 3: Update the page cache — add new page, remove old page
  tsdb_pagecache_remove_entry(db, page_offset);

  // Invalidate every page/entry offset a caller captured before this call
  // (see tsdb_retire_replaced_entry).
  db->metadata_page_gen++;

  ESP_LOGI(TAG, "Metadata page compaction: @0x%08" PRIx32 " -> @0x%08" PRIx32 ", reclaimed %zu bytes",
           page_offset, new_page_offset, deleted_data_size);
  if (out_new_offset) *out_new_offset = new_page_offset;
  return (uint32_t)deleted_data_size;
}

static bool tsdb_create_empty_metadata_page(timeseries_db_t* db, uint32_t page_offset) {
  if (!db) {
    return false;
  }

  ESP_LOGV(TAG, "Creating empty metadata page at offset=0x%08" PRIx32 "...", page_offset);

  timeseries_page_header_t new_hdr;
  memset(&new_hdr, 0xFF, sizeof(new_hdr));
  new_hdr.magic_number = TIMESERIES_MAGIC_NUM;
  new_hdr.page_type = TIMESERIES_PAGE_TYPE_METADATA;
  new_hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  // Atomic increment to avoid duplicate sequence numbers across concurrent
  // page allocations (the field is _Atomic but ++ is a non-atomic RMW).
  new_hdr.sequence_num = atomic_fetch_add(&db->sequence_num, 1) + 1;
  new_hdr.field_data_level = 0;  // not used for metadata
  new_hdr.page_size = TIMESERIES_METADATA_PAGE_SIZE;

  esp_err_t err = esp_partition_write(db->partition, page_offset, &new_hdr, sizeof(new_hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to write new metadata header (err=0x%x)", err);
    tsdb_release_reserved_region(db, page_offset, NULL);
    return false;
  }

  if (!tsdb_finalize_reserved_region(db, page_offset, &new_hdr, NULL)) {
    ESP_LOGE(TAG, "OOM adding metadata page @0x%08" PRIx32 " to cache", page_offset);
    tsdb_release_reserved_region(db, page_offset, NULL);
    return false;
  }

  ESP_LOGV(TAG, "Empty metadata page created at offset=0x%08" PRIx32 " size = %d", page_offset,
           TIMESERIES_METADATA_PAGE_SIZE);
  return true;
}

static bool tsdb_insert_measurement_entry(timeseries_db_t* db, uint32_t page_offset, const char* measurement_name,
                                          uint32_t id) {
  if (!db || !measurement_name) {
    return false;
  }

  uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

  // build header
  timeseries_entry_header_t new_hdr;
  memset(&new_hdr, 0, sizeof(new_hdr));
  new_hdr.delete_marker = TIMESERIES_DELETE_MARKER_VALID;
  new_hdr.key_type = TIMESERIES_KEYTYPE_MEASUREMENT;
  new_hdr.key_len = (uint16_t)strlen(measurement_name);
  new_hdr.value_len = sizeof(uint32_t);

  // Bounds check: ensure entry fits within the metadata page
  uint32_t total_entry_size = sizeof(new_hdr) + new_hdr.key_len + new_hdr.value_len;
  uint32_t entry_offset = 0;
  if (!tsdb_meta_append_offset(db, page_offset, page_size, total_entry_size, &entry_offset)) {
    uint32_t reclaimed = tsdb_compact_metadata_page(db, page_offset, &page_offset);
    if (reclaimed == 0 || !tsdb_meta_append_offset(db, page_offset, page_size, total_entry_size, &entry_offset)) {
      ESP_LOGW(TAG, "Metadata page full: cannot insert measurement '%s'", measurement_name);
      return false;
    }
  }

  // Build the complete entry in one buffer: a single write (not atomic: the
  // flash driver programs it in chunks; tsdb_meta_append_offset seals a torn tail)
  uint8_t *entry_buf = malloc(total_entry_size);
  if (!entry_buf) {
    ESP_LOGE(TAG, "Failed to allocate %u bytes for measurement entry", (unsigned)total_entry_size);
    return false;
  }
  size_t pos = 0;
  memcpy(entry_buf + pos, &new_hdr, sizeof(new_hdr));
  pos += sizeof(new_hdr);
  memcpy(entry_buf + pos, measurement_name, new_hdr.key_len);
  pos += new_hdr.key_len;
  memcpy(entry_buf + pos, &id, sizeof(id));

  esp_err_t err = esp_partition_write(db->partition, entry_offset, entry_buf, total_entry_size);
  free(entry_buf);
  if (err != ESP_OK) {
    return false;
  }

  ESP_LOGV(TAG, "Inserted measurement='%s' ID=%" PRIu32 " into metadata page @ offset=0x%08" PRIx32, measurement_name,
           id, page_offset);
  return true;
}

/**
 * @brief Finds all offsets in the partition that are active METADATA pages.
 *
 * @return true when the list is complete, including an empty list (no
 *         metadata pages); false when it could not be built (bad arguments,
 *         no page-cache snapshot, or OOM growing the list).
 */
static bool tsdb_find_metadata_pages(timeseries_db_t* db, tsdb_meta_pages_t* list, const uint32_t** out_offsets,
                                     size_t* found_count) {
  if (!db || !list || !out_offsets || !found_count) {
    return false;
  }
  *out_offsets = NULL;
  *found_count = 0;

  // Derived from the current page-cache snapshot on every call (a RAM scan);
  // there is no separate cached list that could go stale. A caller may still
  // hold the result after a metadata page is retired, but the region stays
  // intact until its operation ends (timeseries_epoch.h). Every page is
  // listed: entries in a page left out would be invisible to lookups.
  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    return false;
  }

  uint32_t* offsets = list->heap ? list->heap : list->inline_offsets;
  size_t capacity = list->heap ? list->heap_capacity : sizeof(list->inline_offsets) / sizeof(list->inline_offsets[0]);
  size_t count = 0;
  bool ok = true;
  timeseries_page_header_t hdr;
  uint32_t page_offset = 0;
  uint32_t page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset, &page_size)) {
    if (hdr.magic_number != TIMESERIES_MAGIC_NUM || hdr.page_type != TIMESERIES_PAGE_TYPE_METADATA ||
        hdr.page_state != TIMESERIES_PAGE_STATE_ACTIVE) {
      continue;
    }
    if (count == capacity) {
      size_t grown = capacity * 2;
      uint32_t* heap = realloc(list->heap, grown * sizeof(*heap));
      if (!heap) {
        ESP_LOGE(TAG, "OOM listing %zu metadata pages", count + 1);
        ok = false;
        break;
      }
      if (!list->heap) memcpy(heap, list->inline_offsets, count * sizeof(*heap));
      list->heap = heap;
      list->heap_capacity = grown;
      offsets = heap;
      capacity = grown;
    }
    offsets[count++] = page_offset;
  }
  timeseries_page_cache_iterator_deinit(&page_iter);

  // A partial list would hide metadata; report failure instead.
  if (!ok) return false;
  *out_offsets = offsets;
  *found_count = count;
  return true;
}

bool tsdb_soft_delete_entry(timeseries_db_t* db, uint32_t page_offset, uint32_t current_entry_offset) {
  if (!db) {
    return false;
  }

  uint32_t entry_addr = page_offset + current_entry_offset;

  // Overwrite delete_marker=0x00
  uint8_t marker = TIMESERIES_DELETE_MARKER_DELETED;
  esp_err_t err = tsdb_flash_write_byte(db->partition, entry_addr, marker);
  if (err != ESP_OK) {
    return false;
  }

  ESP_LOGD(TAG, "Soft-deleted old tagindex entry at offset=0x%08" PRIx32, entry_addr);
  return true;
}

/**
 * @brief Soft-delete the entry that a new index entry has just replaced.
 *
 * page_offset/entry_offset were captured before the replacement was written.
 * If no metadata page was relocated since then (metadata_page_gen still equals
 * gen_before), they are still valid and the entry is deleted in place.
 * Otherwise the page may have been retired, and its region reused, by
 * tsdb_compact_metadata_page. Compaction copied the old entry (still live at
 * the time) into the new page, so every live entry with the same key_type,
 * key and exact old value is looked up in the current metadata pages and
 * soft-deleted there instead. The replacement has a different value, so it is
 * never matched.
 */
static bool tsdb_retire_replaced_entry(timeseries_db_t* db, uint32_t gen_before, uint32_t page_offset,
                                       uint32_t entry_offset, timeseries_keytype_t key_type, const char* key_str,
                                       const unsigned char* old_value, size_t old_value_len) {
  if (db->metadata_page_gen == gen_before) {
    return tsdb_soft_delete_entry(db, page_offset, entry_offset);
  }

  size_t key_len = strlen(key_str);
  if (key_len >= 128 || old_value_len > UINT16_MAX) {
    return false;
  }
  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count) || meta_count == 0) {
    return false;
  }
  unsigned char* value_buf = malloc(old_value_len > 0 ? old_value_len : 1);
  if (!value_buf) {
    return false;
  }

  bool ok = true;
  size_t retired = 0;
  for (size_t p = 0; p < meta_count && ok; p++) {
    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, meta_offsets[p], TIMESERIES_METADATA_PAGE_SIZE, &ent_iter)) {
      continue;
    }
    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      if (e_hdr.key_type != key_type || e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID ||
          e_hdr.key_len != key_len || e_hdr.value_len != old_value_len) {
        continue;
      }
      char stored_key[128];
      if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, stored_key, value_buf)) {
        continue;
      }
      if (memcmp(stored_key, key_str, key_len) != 0 || memcmp(value_buf, old_value, old_value_len) != 0) {
        continue;
      }
      if (!tsdb_soft_delete_entry(db, meta_offsets[p], ent_iter.current_entry_offset)) {
        ok = false;
        break;
      }
      retired++;
    }
    timeseries_entity_iterator_deinit(&ent_iter);
  }
  free(value_buf);

  if (ok && retired == 0) {
    ESP_LOGW(TAG, "Replaced entry '%s' not found after metadata page relocation", key_str);
  }
  return ok;
}

bool tsdb_replace_seriesid_list_entry(timeseries_db_t* db,
                                      uint32_t old_page_offset,
                                      uint32_t old_entry_offset,
                                      timeseries_keytype_t key_type,
                                      const char* key,
                                      const unsigned char* series_ids,
                                      size_t series_id_count) {
  if (!db || !key ||
      (key_type != TIMESERIES_KEYTYPE_TAGINDEX &&
       key_type != TIMESERIES_KEYTYPE_FIELDLISTINDEX) ||
      (series_id_count > 0 && !series_ids) ||
      series_id_count > UINT16_MAX / 16U) {
    return false;
  }

  if (series_id_count > 0) {
    bool created = false;
    TSDB_META_PAGES(meta_list);
    const uint32_t* meta_offsets = NULL;
    size_t meta_count = 0;
    if (tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count)) {
      // Avoid the old page: if it fills, compaction would copy the entry being
      // replaced and leave another live copy behind.
      for (size_t i = 0; i < meta_count && !created; i++) {
        if (meta_offsets[i] != old_page_offset) {
          created = tsdb_create_new_index_entry(db, meta_offsets[i], key_type,
                                                key, series_ids,
                                                series_id_count * 16U);
        }
      }
    }

    if (!created && timeseries_metadata_create_page(db)) {
      meta_count = 0;
      if (tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count)) {
        for (size_t i = 0; i < meta_count && !created; i++) {
          if (meta_offsets[i] != old_page_offset) {
            created = tsdb_create_new_index_entry(db, meta_offsets[i], key_type,
                                                  key, series_ids,
                                                  series_id_count * 16U);
          }
        }
      }
    }
    if (!created) return false;
  }

  if (!tsdb_soft_delete_entry(db, old_page_offset, old_entry_offset)) {
    ESP_LOGE(TAG, "Failed to retire replaced metadata entry '%s'", key);
    return false;
  }
  return true;
}

static bool tsdb_create_new_index_entry(timeseries_db_t* db, uint32_t page_offset, timeseries_keytype_t key_type,
                                        const char* key_str, const unsigned char* series_ids, size_t series_ids_len) {
  if (!db || !key_str || !series_ids) {
    return false;
  }

  uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

  // Build header
  timeseries_entry_header_t new_hdr;
  memset(&new_hdr, 0, sizeof(new_hdr));
  new_hdr.delete_marker = TIMESERIES_DELETE_MARKER_VALID;
  new_hdr.key_type = key_type;
  new_hdr.key_len = (uint16_t)strlen(key_str);

  if (series_ids_len > UINT16_MAX) {
    ESP_LOGE(TAG, "Series IDs list too large for metadata entry: %zu bytes", series_ids_len);
    return false;
  }
  new_hdr.value_len = (uint16_t)series_ids_len;

  // Bounds check: ensure entry fits within the metadata page
  uint32_t total_entry_size = sizeof(new_hdr) + new_hdr.key_len + new_hdr.value_len;
  uint32_t entry_offset = 0;
  if (!tsdb_meta_append_offset(db, page_offset, page_size, total_entry_size, &entry_offset)) {
    uint32_t reclaimed = tsdb_compact_metadata_page(db, page_offset, &page_offset);
    if (reclaimed == 0 || !tsdb_meta_append_offset(db, page_offset, page_size, total_entry_size, &entry_offset)) {
      ESP_LOGW(TAG, "Metadata page full (even after compaction): cannot insert index entry for '%s'", key_str);
      return false;
    }
  }

  // Build the complete entry in one buffer: a single write (not atomic: the
  // flash driver programs it in chunks; tsdb_meta_append_offset seals a torn tail)
  uint8_t *entry_buf = malloc(total_entry_size);
  if (!entry_buf) {
    ESP_LOGE(TAG, "Failed to allocate %u bytes for index entry", (unsigned)total_entry_size);
    return false;
  }
  size_t pos = 0;
  memcpy(entry_buf + pos, &new_hdr, sizeof(new_hdr));
  pos += sizeof(new_hdr);
  memcpy(entry_buf + pos, key_str, new_hdr.key_len);
  pos += new_hdr.key_len;
  memcpy(entry_buf + pos, series_ids, series_ids_len);

  esp_err_t err = esp_partition_write(db->partition, entry_offset, entry_buf, total_entry_size);
  free(entry_buf);
  if (err != ESP_OK) {
    return false;
  }

  ESP_LOGV(TAG, "Created new index entry (type=%u) for '%s', value_len=%u", (unsigned)key_type, key_str,
           (unsigned)new_hdr.value_len);
  return true;
}

/**
 * @brief Upserts an entry in the metadata so that
 *        measurementId:fieldName => [... seriesIds ...].
 *
 * This is called during insert to record that a given seriesId
 * belongs to (measurementId, fieldName).
 *
 * @param db              The DB context
 * @param measurement_id  The measurement ID
 * @param field_name      The field name
 * @param series_id       16-byte Series ID
 * @return true on success, false otherwise
 */
bool tsdb_index_field_for_series(timeseries_db_t* db, uint32_t measurement_id, const char* field_name,
                                 const unsigned char series_id[16]) {
  if (!db || !field_name || !series_id) {
    return false;
  }

  // Build the key string "measurementId:fieldName"
  char key_buf[128];
  int n = snprintf(key_buf, sizeof(key_buf), "%" PRIu32 ":%s", measurement_id, field_name);
  if (n < 0 || (size_t)n >= sizeof(key_buf)) {
    ESP_LOGE(TAG, "tsdb_index_field_for_series: key_buf overflow");
    return false;
  }

  // Find all active metadata pages
  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count) || meta_count == 0) {
    ESP_LOGE(TAG, "No active metadata page found for field index");
    return false;
  }

  // Upsert the key => append the seriesId if it doesn't exist.
  return tsdb_upsert_fieldlist_entry(db, meta_offsets, meta_count, key_buf, series_id);
}

static tsdb_meta_walk_result_t tsdb_meta_walk(timeseries_db_t* db, timeseries_keytype_t key_type,
                                              tsdb_meta_visit_fn visit, void* ctx) {
  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count) || meta_count == 0) {
    return TSDB_META_WALK_NO_PAGES;
  }
  for (size_t p = 0; p < meta_count; p++) {
    timeseries_entity_iterator_t it;
    if (!tsdb_meta_scan_open(db, meta_offsets[p], TIMESERIES_METADATA_PAGE_SIZE, &it)) {
      ESP_LOGW(TAG, "Failed to initialize entity iterator for page offset=0x%08" PRIx32, meta_offsets[p]);
      continue;
    }
    timeseries_entry_header_t hdr;
    while (timeseries_entity_iterator_next(&it, &hdr)) {
      if (hdr.key_type != key_type || hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }
      if (!visit(&it, &hdr, ctx)) {
        timeseries_entity_iterator_deinit(&it);
        return TSDB_META_WALK_STOPPED;
      }
    }
    timeseries_entity_iterator_deinit(&it);
  }
  return TSDB_META_WALK_DONE;
}

/* Context for the walks that collect the series IDs stored under one exact
 * index key ("<meas_id>:<tag>:<value>" or "<meas_id>:<field>"). */
typedef struct {
  const char* key;
  size_t key_len;
  timeseries_series_id_list_t* out;
  bool found_any;
  bool oom;
  unsigned char* scratch; /* field walk: one buffer grown to the largest entry */
  size_t scratch_cap;
} tsdb_key_ids_walk_t;

static bool tsdb_visit_tag_ids(timeseries_entity_iterator_t* it, const timeseries_entry_header_t* hdr, void* ctx) {
  tsdb_key_ids_walk_t* w = ctx;
  // The key_len should match length of our "measurementId:tagKey:tagValue"
  if (hdr->key_len != w->key_len) return true;
  // The value_len should be a multiple of 16 (each series ID is 16 bytes)
  if (hdr->value_len % 16 != 0) {
    ESP_LOGW(TAG, "Unexpected tagindex value_len=%" PRIu16 " (not multiple of 16).", hdr->value_len);
    return true;
  }
  if (hdr->value_len == 0) return true;

  char stored_key[128];
  if (w->key_len >= sizeof(stored_key)) {
    ESP_LOGW(TAG, "tagindex key too large to read");
    return true;
  }
  unsigned char* series_ids = malloc(hdr->value_len);
  if (!series_ids) {
    ESP_LOGE(TAG, "Out of memory reading tagindex series IDs");
    return true;
  }
  memset(stored_key, 0, sizeof(stored_key));
  if (!timeseries_entity_iterator_read_data(it, hdr, stored_key, series_ids)) {
    ESP_LOGW(TAG, "Failed reading tagindex entry data");
    free(series_ids);
    return true;
  }
  stored_key[w->key_len] = '\0';

  if (strcmp(stored_key, w->key) == 0) {
    w->found_any = true;
    if (!tsdb_append_series_ids(w->out, series_ids, hdr->value_len / 16)) {
      ESP_LOGE(TAG, "Failed appending a series ID (OOM?)");
      w->oom = true;
    }
  }
  free(series_ids);
  return !w->oom;
}

static bool tsdb_visit_field_ids(timeseries_entity_iterator_t* it, const timeseries_entry_header_t* hdr,
                                 void* ctx) {
  tsdb_key_ids_walk_t* w = ctx;
  if (hdr->key_len != w->key_len) return true;
  // The value_len should be a multiple of 16 (each series ID is 16 bytes)
  if (hdr->value_len % 16 != 0) {
    ESP_LOGW(TAG, "Unexpected fieldlist value_len=%" PRIu16 " (not multiple of 16).", hdr->value_len);
    return true;
  }
  /* grow scratch buffer if this entry is larger than previous ones */
  if (hdr->value_len > w->scratch_cap) {
    unsigned char* nb = realloc(w->scratch, hdr->value_len);
    if (!nb) {
      ESP_LOGE(TAG, "OOM enlarging scratch buffer");
      return true;
    }
    w->scratch = nb;
    w->scratch_cap = hdr->value_len;
  }
  char stored_key[128];
  memset(stored_key, 0, w->key_len + 1);
  if (!timeseries_entity_iterator_read_data(it, hdr, stored_key, w->scratch)) {
    ESP_LOGW(TAG, "Failed reading fieldlist entry data");
    return true;
  }
  /* fixed-length compare avoids second strlen() */
  if (memcmp(stored_key, w->key, w->key_len) == 0) {
    w->found_any = true;
    if (!tsdb_append_series_ids(w->out, w->scratch, hdr->value_len / 16)) {
      ESP_LOGE(TAG, "Failed appending a series ID (OOM?)");
      w->oom = true;
      return false;
    }
  }
  return true;
}

bool tsdb_find_series_ids_for_tag(timeseries_db_t* db, uint32_t measurement_id, const char* tag_key,
                                  const char* tag_value, timeseries_series_id_list_t* out_series_list) {
  if (!db || !tag_key || !tag_value || !out_series_list) {
    return false;
  }

  // The key that identifies the tag index entry, e.g. "1234:myTag:someValue"
  char key_buf[128];
  int n = snprintf(key_buf, sizeof(key_buf), "%" PRIu32 ":%s:%s", measurement_id, tag_key, tag_value);
  if (n < 0 || (size_t)n >= sizeof(key_buf)) {
    ESP_LOGE(TAG, "tsdb_find_series_ids_for_tag: key_buf overflow");
    return false;
  }

  tsdb_key_ids_walk_t w = {.key = key_buf, .key_len = (size_t)n, .out = out_series_list};
  if (tsdb_meta_walk(db, TIMESERIES_KEYTYPE_TAGINDEX, tsdb_visit_tag_ids, &w) == TSDB_META_WALK_NO_PAGES) {
    ESP_LOGD(TAG, "No active metadata pages for tag '%s'.", key_buf);
    return false;
  }
  // true means at least one entry was found
  return w.found_any && !w.oom;
}

/**
 * @brief Insert or update an index entry for either TAGINDEX or FIELDLISTINDEX,
 *        storing a list of 16-byte series IDs.
 *
 * This unifies the logic from tsdb_upsert_tagindex_entry and
 * tsdb_upsert_fieldlist_entry.
 */
static bool tsdb_upsert_seriesid_list_entry(timeseries_db_t* db, timeseries_keytype_t target_key_type,
                                            const uint32_t* meta_offsets, size_t meta_count, const char* key_str,
                                            const unsigned char series_id[16]) {
  if (!db || !key_str || !series_id) {
    return false;
  }

  bool found_existing = false;
  bool updated = false;
  size_t key_len = strlen(key_str);

  for (size_t p = 0; p < meta_count && !updated; p++) {
    uint32_t page_offset = meta_offsets[p];

    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, page_offset, TIMESERIES_METADATA_PAGE_SIZE, &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      // Skip if not the right key_type or if deleted
      if (e_hdr.key_type != target_key_type || e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }

      // The key must match exactly in length
      if (e_hdr.key_len == key_len && (e_hdr.value_len % 16 == 0)) {
        // Attempt to read the existing key + series IDs
        char stored_key[128];
        if (key_len >= sizeof(stored_key)) {
          // Key is too large for our buffer => skip
          continue;
        }
        memset(stored_key, 0, sizeof(stored_key));

        size_t series_count = e_hdr.value_len / 16;
        // Allocate for existing IDs + 1 extra slot so we can append in-place
        size_t new_size = (series_count + 1) * 16;
        unsigned char* all_series_ids = malloc(new_size);
        if (!all_series_ids) {
          return false;
        }

        if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, stored_key, all_series_ids)) {
          free(all_series_ids);
          continue;
        }

        stored_key[key_len] = '\0';
        if (strcmp(stored_key, key_str) == 0) {
          found_existing = true;

          // Check if this series_id is already present
          bool already_present = false;
          for (size_t s = 0; s < series_count; s++) {
            if (memcmp(all_series_ids + s * 16, series_id, 16) == 0) {
              already_present = true;
              break;
            }
          }

          // If not present, append in-place (buffer already has room)
          if (!already_present) {
            memcpy(all_series_ids + series_count * 16, series_id, 16);

            // Create new entry with new_size - try same page first, then others
            const uint32_t gen_before = db->metadata_page_gen;
            bool entry_created = tsdb_create_new_index_entry(db, page_offset, target_key_type, key_str, all_series_ids, new_size);
            if (!entry_created) {
              // Same page is full - try other pages
              for (size_t q = 0; q < meta_count && !entry_created; q++) {
                if (meta_offsets[q] != page_offset) {
                  entry_created = tsdb_create_new_index_entry(db, meta_offsets[q], target_key_type, key_str, all_series_ids, new_size);
                }
              }
              if (!entry_created) {
                // All pages full - create a new metadata page
                if (timeseries_metadata_create_page(db)) {
                  TSDB_META_PAGES(new_list);
                  const uint32_t* new_offsets = NULL;
                  size_t new_count = 0;
                  if (tsdb_find_metadata_pages(db, &new_list, &new_offsets, &new_count)) {
                    for (size_t q = 0; q < new_count && !entry_created; q++) {
                      entry_created = tsdb_create_new_index_entry(db, new_offsets[q], target_key_type, key_str, all_series_ids, new_size);
                    }
                  }
                }
              }
              if (!entry_created) {
                free(all_series_ids);
                timeseries_entity_iterator_deinit(&ent_iter);
                return false;
              }
            }

            // Soft-delete the old entry (the first series_count IDs are its value)
            if (!tsdb_retire_replaced_entry(db, gen_before, page_offset, ent_iter.current_entry_offset,
                                            target_key_type, key_str, all_series_ids, series_count * 16)) {
              ESP_LOGW(TAG, "Failed to soft-delete old entry: '%s'", key_str);
            }
            updated = true;
            ESP_LOGD(TAG, "Appended series_id to %s entry '%s'",
                     (target_key_type == TIMESERIES_KEYTYPE_TAGINDEX) ? "tag" : "fieldlist", key_str);
          } else {
            ESP_LOGD(TAG, "Series ID already present for '%s'; no rewrite", key_str);
          }

          free(all_series_ids);
          break;  // done in this page
        }  // if matching stored_key

        free(all_series_ids);
      }  // if key_len etc
    }  // while next entity

    timeseries_entity_iterator_deinit(&ent_iter);

  }  // for meta_count

  // If we never found an existing entry, create a brand-new one
  if (!found_existing && meta_count > 0) {
    bool created = false;
    for (size_t p = 0; p < meta_count && !created; p++) {
      created = tsdb_create_new_index_entry(db, meta_offsets[p], target_key_type, key_str, series_id, 16);
    }
    if (!created) {
      // All existing pages full - create a new metadata page and retry
      if (timeseries_metadata_create_page(db)) {
        TSDB_META_PAGES(new_list);
        const uint32_t* new_offsets = NULL;
        size_t new_count = 0;
        if (tsdb_find_metadata_pages(db, &new_list, &new_offsets, &new_count)) {
          for (size_t p = 0; p < new_count && !created; p++) {
            created = tsdb_create_new_index_entry(db, new_offsets[p], target_key_type, key_str, series_id, 16);
          }
        }
      }
      if (!created) {
        ESP_LOGE(TAG, "Failed to create index entry for '%s' - all metadata pages full", key_str);
        return false;
      }
    }
    ESP_LOGD(TAG, "Created new index entry (type=%u) for '%s' w/ single series_id", (unsigned)target_key_type, key_str);
  }

  return true;
}

static bool tsdb_upsert_tagindex_entry(timeseries_db_t* db, const uint32_t* meta_offsets, size_t meta_count,
                                       const char* key_str, const unsigned char series_id[16]) {
  return tsdb_upsert_seriesid_list_entry(db, TIMESERIES_KEYTYPE_TAGINDEX, meta_offsets, meta_count, key_str, series_id);
}

static bool tsdb_upsert_fieldlist_entry(timeseries_db_t* db, const uint32_t* meta_offsets, size_t meta_count,
                                        const char* key_str, const unsigned char series_id[16]) {
  return tsdb_upsert_seriesid_list_entry(db, TIMESERIES_KEYTYPE_FIELDLISTINDEX, meta_offsets, meta_count, key_str,
                                         series_id);
}

/**
 * @brief Look up all series IDs matching a given (measurement_id, field_name).
 *
 * @param db                Pointer to the database context
 * @param measurement_id    Numeric measurement ID
 * @param field_name        The field name to match
 * @param out_series_list   A timeseries_series_id_list_t object to which
 *                          matching series IDs will be appended
 * @return true if one or more series IDs were found, false otherwise
 */
bool tsdb_find_series_ids_for_field(timeseries_db_t* db, uint32_t measurement_id, const char* field_name,
                                    timeseries_series_id_list_t* out_series_list) {
  if (!db || !field_name || !out_series_list) {
    return false;
  }

  // The key that identifies the field list entry, e.g. "1234:fieldName"
  char key_buf[128];
  int n = snprintf(key_buf, sizeof(key_buf), "%" PRIu32 ":%s", measurement_id, field_name);
  if (n < 0 || (size_t)n >= sizeof(key_buf)) {
    ESP_LOGE(TAG, "tsdb_find_series_ids_for_field: key_buf overflow");
    return false;
  }

  tsdb_key_ids_walk_t w = {.key = key_buf, .key_len = (size_t)n, .out = out_series_list};
  tsdb_meta_walk_result_t r = tsdb_meta_walk(db, TIMESERIES_KEYTYPE_FIELDLISTINDEX, tsdb_visit_field_ids, &w);
  free(w.scratch);
  if (r == TSDB_META_WALK_NO_PAGES) {
    ESP_LOGD(TAG, "No active metadata pages for field '%s'.", key_buf);
    return false;
  }
  // true means at least one entry was found
  return w.found_any && !w.oom;
}

/**
 * @brief Retrieves all measurement names from the metadata pages (ignoring
 * duplicates).
 *
 * @param[in]  db              Pointer to the database context
 * @param[out] out_measurements A string list where measurement names are
 * appended
 * @return true if at least one measurement is found, false if none or error
 */
bool tsdb_list_all_measurements(timeseries_db_t* db, timeseries_string_list_t* out_measurements) {
  if (!db || !out_measurements) {
    return false;
  }

  // 1) Find active METADATA pages
  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  // false means the lookup failed; an empty list (a fresh DB has no metadata
  // pages yet) is a normal answer, not an error.
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count)) {
    ESP_LOGW(TAG, "Metadata page lookup failed while listing measurements");
    return false;
  }
  if (meta_count == 0) {
    ESP_LOGD(TAG, "No active metadata pages => no measurements");
    return true;
  }

  bool ok = true;

  // 2) Scan each page for measurement entries
  for (size_t p = 0; p < meta_count; p++) {
    uint32_t page_offset = meta_offsets[p];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

    timeseries_entity_iterator_t ent_iter;
    if (!tsdb_meta_scan_open(db, page_offset, page_size, &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_MEASUREMENT || e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }

      // The measurement name is e_hdr.key_len long
      // We'll read that plus the ID, but we only need the name for listing.
      if (e_hdr.value_len == sizeof(uint32_t) && e_hdr.key_len > 0 && e_hdr.key_len < 256) {
        char *name_buf = (char *)malloc(256);
        if (!name_buf) {
          ESP_LOGE(TAG, "OOM allocating name_buf in tsdb_list_all_measurements");
          ok = false;
          continue;
        }
        uint32_t measurement_id;
        memset(name_buf, 0, 256);

        if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, name_buf, &measurement_id)) {
          ESP_LOGW(TAG, "Failed reading measurement entry data.");
          free(name_buf);
          continue;
        }
        // name_buf is now the measurement name
        name_buf[e_hdr.key_len] = '\0';

        // Append unique to the output list (false only on OOM)
        if (!tsdb_string_list_append_unique(out_measurements, name_buf)) {
          ok = false;
        }
        free(name_buf);
      }
    }

    timeseries_entity_iterator_deinit(&ent_iter);
  }

  return ok;
}

/**
 * @brief Lists all field names for the given measurement ID by parsing all
 *        FIELDLISTINDEX entries and extracting the field portion of
 *        "<meas_id>:<field_name>".
 *
 * @param[in]  db          Pointer to the database context
 * @param[in]  measurement_id The numeric measurement ID
 * @param[out] out_fields  A string list to which unique field names are
 * appended
 * @return true if at least one field name was found, false if none or error
 */
typedef struct {
  uint32_t measurement_id;
  timeseries_string_list_t* out;
} tsdb_fields_walk_t;

static bool tsdb_visit_field_names(timeseries_entity_iterator_t* it, const timeseries_entry_header_t* hdr,
                                   void* ctx) {
  tsdb_fields_walk_t* w = ctx;
  // The key looks like "<meas_id>:<field_name>"
  if (hdr->key_len == 0 || hdr->key_len >= 128) return true;
  char key_buf[128];
  memset(key_buf, 0, sizeof(key_buf));
  // The series IDs in the value are not needed for this listing.
  unsigned char* dummy_series = NULL;
  if (hdr->value_len > 0) {
    dummy_series = malloc(hdr->value_len);
    if (!dummy_series) {
      // OOM: cannot safely call read_data which would write to NULL
      ESP_LOGW(TAG, "OOM allocating dummy_series (%u bytes), skipping entry", (unsigned)hdr->value_len);
      return true;
    }
  }
  bool read_ok = timeseries_entity_iterator_read_data(it, hdr, key_buf, dummy_series);
  free(dummy_series);
  if (!read_ok) {
    ESP_LOGW(TAG, "Failed reading fieldlist entry data");
    return true;
  }
  key_buf[hdr->key_len] = '\0';

  char* colon_ptr = strchr(key_buf, ':');
  if (!colon_ptr) return true;  // malformed key
  *colon_ptr = '\0';
  if ((uint32_t)strtoul(key_buf, NULL, 10) == w->measurement_id) {
    tsdb_string_list_append_unique(w->out, colon_ptr + 1);
  }
  return true;
}

bool tsdb_list_fields_for_measurement(timeseries_db_t* db, uint32_t measurement_id,
                                      timeseries_string_list_t* out_fields) {
  if (!db || !out_fields) {
    return false;
  }
  tsdb_fields_walk_t w = {.measurement_id = measurement_id, .out = out_fields};
  if (tsdb_meta_walk(db, TIMESERIES_KEYTYPE_FIELDLISTINDEX, tsdb_visit_field_names, &w) ==
      TSDB_META_WALK_NO_PAGES) {
    ESP_LOGD(TAG, "No active metadata pages => no fields");
    return false;
  }
  /* A measurement whose fields have all been deleted lists empty; that is a
   * successful lookup, not a failure. Callers that need the distinction check
   * out_fields->count. */
  return true;
}

typedef struct {
  uint32_t measurement_id;
  timeseries_series_id_list_t* out;
  bool found_any;
  bool oom;
} tsdb_meas_ids_walk_t;

static bool tsdb_visit_meas_series_ids(timeseries_entity_iterator_t* it, const timeseries_entry_header_t* hdr,
                                       void* ctx) {
  tsdb_meas_ids_walk_t* w = ctx;
  // The key should look like "1234:someField"
  if (hdr->key_len == 0 || hdr->key_len >= 128) return true;
  // Value must be multiple of 16 (each series ID = 16 bytes)
  if (hdr->value_len % 16 != 0) {
    ESP_LOGW(TAG, "Unexpected fieldlist value_len=%" PRIu16 " (not multiple of 16).", hdr->value_len);
    return true;
  }
  if (hdr->value_len == 0) return true;

  char key_buf[128];
  memset(key_buf, 0, sizeof(key_buf));
  unsigned char* series_ids = malloc(hdr->value_len);
  if (!series_ids) {
    ESP_LOGE(TAG, "Out of memory reading fieldlist series IDs");
    return true;
  }
  if (!timeseries_entity_iterator_read_data(it, hdr, key_buf, series_ids)) {
    ESP_LOGW(TAG, "Failed reading fieldlist entry data");
    free(series_ids);
    return true;
  }
  key_buf[hdr->key_len] = '\0';

  char* colon_ptr = strchr(key_buf, ':');
  if (colon_ptr) {
    *colon_ptr = '\0';
    if ((uint32_t)strtoul(key_buf, NULL, 10) == w->measurement_id) {
      w->found_any = true;
      size_t num_ids = hdr->value_len / 16;
      for (size_t i = 0; i < num_ids && !w->oom; i++) {
        unsigned char* sid = series_ids + (i * 16);
        bool is_duplicate = false;
        for (size_t j = 0; j < w->out->count; j++) {
          if (memcmp(w->out->ids[j].bytes, sid, 16) == 0) {
            is_duplicate = true;
            break;
          }
        }
        if (!is_duplicate && !tsdb_series_id_list_append(w->out, sid)) {
          ESP_LOGE(TAG, "Failed appending a series ID (OOM?)");
          w->oom = true;
        }
      }
    }
  }
  free(series_ids);
  return !w->oom;
}

bool tsdb_find_all_series_ids_for_measurement(timeseries_db_t* db, uint32_t measurement_id,
                                              timeseries_series_id_list_t* out_series_list) {
  if (!db || !out_series_list) {
    return false;
  }
  tsdb_meas_ids_walk_t w = {.measurement_id = measurement_id, .out = out_series_list};
  if (tsdb_meta_walk(db, TIMESERIES_KEYTYPE_FIELDLISTINDEX, tsdb_visit_meas_series_ids, &w) ==
      TSDB_META_WALK_NO_PAGES) {
    ESP_LOGD(TAG, "No active metadata pages => no series to find.");
    return false;
  }
  return w.found_any && !w.oom;
}

// =============================================================================
// Series Type Cache (Phase 2 Optimization)
// =============================================================================

/**
 * @brief Simple hash function for series_id (use first 4 bytes)
 */
static inline size_t hash_series_id(const unsigned char series_id[16], size_t cache_size) {
  if (cache_size == 0) return 0;
  uint32_t hash = 0;
  // Use first 4 bytes of series_id for hash
  memcpy(&hash, series_id, sizeof(uint32_t));
  return hash % cache_size;
}

// Probe the type cache for series_id. Takes type_cache_mutex internally.
static bool tsdb_type_cache_get(timeseries_db_t* db, const unsigned char series_id[16],
                                timeseries_field_type_e* out_type) {
  if (!db->type_cache || db->type_cache_size == 0) {
    return false;
  }
  bool hit = false;
  if (db->type_cache_mutex) xSemaphoreTake(db->type_cache_mutex, portMAX_DELAY);
  size_t idx = hash_series_id(series_id, db->type_cache_size);
  for (size_t p = 0; p < TYPE_CACHE_MAX_PROBE; p++) {
    size_t slot = (idx + p) % db->type_cache_size;
    series_type_cache_entry_t* entry = &db->type_cache[slot];
    if (!entry->valid) break;  // empty slot = not cached
    if (memcmp(entry->series_id, series_id, 16) == 0) {
      *out_type = (timeseries_field_type_e)entry->field_type;
      hit = true;
      break;
    }
  }
  if (db->type_cache_mutex) xSemaphoreGive(db->type_cache_mutex);
  return hit;
}

// Insert/update a (series_id -> type) mapping. Takes type_cache_mutex
// internally. A series' type is immutable, so on a full probe window we simply
// evict the last probe slot rather than silently dropping the entry (which
// would force a full metadata scan on every future lookup of that series).
static void tsdb_type_cache_store(timeseries_db_t* db, const unsigned char series_id[16],
                                  timeseries_field_type_e type) {
  if (!db->type_cache || db->type_cache_size == 0) {
    return;
  }
  if (db->type_cache_mutex) xSemaphoreTake(db->type_cache_mutex, portMAX_DELAY);
  size_t idx = hash_series_id(series_id, db->type_cache_size);
  size_t target = (idx + TYPE_CACHE_MAX_PROBE - 1) % db->type_cache_size;  // eviction victim if window full
  for (size_t p = 0; p < TYPE_CACHE_MAX_PROBE; p++) {
    size_t slot = (idx + p) % db->type_cache_size;
    series_type_cache_entry_t* entry = &db->type_cache[slot];
    if (!entry->valid || memcmp(entry->series_id, series_id, 16) == 0) {
      target = slot;
      break;
    }
  }
  series_type_cache_entry_t* entry = &db->type_cache[target];
  memcpy(entry->series_id, series_id, 16);
  entry->field_type = (uint8_t)type;
  entry->valid = true;
  if (db->type_cache_mutex) xSemaphoreGive(db->type_cache_mutex);
}

/**
 * @brief Look up series type in cache, with fallback to metadata scan
 * @note This replaces direct calls to tsdb_lookup_series_type_in_metadata
 */
bool tsdb_lookup_series_type_cached(timeseries_db_t* db, const unsigned char series_id[16],
                                     timeseries_field_type_e* out_type) {
  if (!db || !series_id || !out_type) {
    return false;
  }

  // Check cache first (locked, short critical section)
  if (tsdb_type_cache_get(db, series_id, out_type)) {
    return true;
  }

  // Cache miss - do the metadata scan unlocked (it is slow and takes no cache
  // lock), then publish the result.
  bool found = tsdb_lookup_series_type_in_metadata(db, series_id, out_type);
  if (found) {
    tsdb_type_cache_store(db, series_id, *out_type);
  }
  return found;
}

tsdb_series_type_status_t tsdb_lookup_series_type_status(timeseries_db_t* db, const unsigned char series_id[16],
                                                         timeseries_field_type_e* out_type) {
  if (!db || !series_id || !out_type) return TSDB_SERIES_TYPE_UNKNOWN;
  if (tsdb_type_cache_get(db, series_id, out_type)) return TSDB_SERIES_TYPE_FOUND;
  tsdb_series_type_status_t status = scan_series_type(db, series_id, out_type);
  if (status == TSDB_SERIES_TYPE_FOUND) tsdb_type_cache_store(db, series_id, *out_type);
  return status;
}

/**
 * @brief Clear the type cache (called when metadata changes)
 */
void tsdb_clear_type_cache(timeseries_db_t* db) {
  if (db && db->type_cache && db->type_cache_size > 0) {
    if (db->type_cache_mutex) xSemaphoreTake(db->type_cache_mutex, portMAX_DELAY);
    memset(db->type_cache, 0, db->type_cache_size * sizeof(series_type_cache_entry_t));
    if (db->type_cache_mutex) xSemaphoreGive(db->type_cache_mutex);
  }
}

// =============================================================================
// Delete helpers (used by timeseries_delete.c)
// =============================================================================

bool tsdb_remove_measurement_from_metadata(timeseries_db_t* db, const char* measurement_name) {
  if (!db || !measurement_name) {
    return false;
  }

  // 1) Find measurement_id
  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(db, measurement_name, &measurement_id)) {
    ESP_LOGW(TAG, "Measurement '%s' not found in metadata", measurement_name);
    return true;  // nothing to remove is not an error
  }

  // Build prefix string for TAGINDEX/FIELDLISTINDEX matching: "<meas_id>:"
  char meas_prefix[16];
  int plen = snprintf(meas_prefix, sizeof(meas_prefix), "%" PRIu32 ":", measurement_id);
  if (plen < 0 || (size_t)plen >= sizeof(meas_prefix)) {
    return false;
  }

  // 2) Find all metadata pages
  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  // No metadata pages => nothing to delete; a failed lookup is not success.
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count)) {
    return false;
  }
  if (meta_count == 0) {
    return true;
  }

  // 3) Scan each page and soft-delete matching entries
  for (size_t p = 0; p < meta_count; p++) {
    uint32_t page_offset = meta_offsets[p];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, page_offset, page_size, &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      if (e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }

      bool should_delete = false;

      if (e_hdr.key_type == TIMESERIES_KEYTYPE_MEASUREMENT) {
        // Read the key and check if it matches measurement_name
        if (e_hdr.value_len == sizeof(uint32_t) && e_hdr.key_len > 0 && e_hdr.key_len < 256) {
          char *name_buf = (char *)malloc(256);
          if (!name_buf) {
            ESP_LOGE(TAG, "OOM allocating name_buf in tsdb_remove_measurement_from_metadata");
            continue;
          }
          memset(name_buf, 0, 256);
          uint32_t dummy_id;
          if (timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, name_buf, &dummy_id)) {
            name_buf[e_hdr.key_len] = '\0';
            if (strcmp(name_buf, measurement_name) == 0) {
              should_delete = true;
            }
          }
          free(name_buf);
        }
      } else if (e_hdr.key_type == TIMESERIES_KEYTYPE_TAGINDEX ||
                 e_hdr.key_type == TIMESERIES_KEYTYPE_FIELDLISTINDEX) {
        // Read the key and check if it starts with "<meas_id>:"
        if (e_hdr.key_len > 0 && e_hdr.key_len < 128) {
          char key_buf[128];
          memset(key_buf, 0, sizeof(key_buf));
          // We don't need the value, but must provide a buffer
          unsigned char* dummy_val = NULL;
          if (e_hdr.value_len > 0) {
            dummy_val = malloc(e_hdr.value_len);
          }
          if (timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, key_buf, dummy_val)) {
            key_buf[e_hdr.key_len] = '\0';
            if (strncmp(key_buf, meas_prefix, (size_t)plen) == 0) {
              should_delete = true;
            }
          }
          free(dummy_val);
        }
      }

      if (should_delete) {
        tsdb_soft_delete_entry(db, page_offset, ent_iter.current_entry_offset);
      }
    }

    timeseries_entity_iterator_deinit(&ent_iter);
  }

  return true;
}

bool tsdb_soft_delete_fieldlistindex_entry(timeseries_db_t* db, uint32_t measurement_id, const char* field_name) {
  if (!db || !field_name) {
    return false;
  }

  // Build the exact key: "<meas_id>:<field_name>"
  char target_key[128];
  int n = snprintf(target_key, sizeof(target_key), "%" PRIu32 ":%s", measurement_id, field_name);
  if (n < 0 || (size_t)n >= sizeof(target_key)) {
    return false;
  }

  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;
  // No metadata pages => nothing to delete; a failed lookup is not success.
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count)) {
    return false;
  }
  if (meta_count == 0) {
    return true;
  }

  bool deleted_any = false;

  for (size_t p = 0; p < meta_count; p++) {
    uint32_t page_offset = meta_offsets[p];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, page_offset, page_size, &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_FIELDLISTINDEX ||
          e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }
      if (e_hdr.key_len == 0 || e_hdr.key_len >= 128) {
        continue;
      }

      char key_buf[128];
      memset(key_buf, 0, sizeof(key_buf));
      unsigned char* dummy_val = NULL;
      if (e_hdr.value_len > 0) {
        dummy_val = malloc(e_hdr.value_len);
      }
      bool read_ok = timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, key_buf, dummy_val);
      free(dummy_val);
      if (!read_ok) {
        continue;
      }
      key_buf[e_hdr.key_len] = '\0';

      if (strcmp(key_buf, target_key) == 0) {
        tsdb_soft_delete_entry(db, page_offset, ent_iter.current_entry_offset);
        deleted_any = true;
      }
    }

    timeseries_entity_iterator_deinit(&ent_iter);
  }

  return deleted_any;
}

typedef struct {
  const char* meas_prefix; /* "<meas_id>:" */
  size_t plen;
  tsdb_tag_pair_t* tags;
  size_t count;
  size_t capacity;
  bool oom;
} tsdb_tags_walk_t;

static bool tsdb_visit_tag_pairs(timeseries_entity_iterator_t* it, const timeseries_entry_header_t* hdr, void* ctx) {
  tsdb_tags_walk_t* w = ctx;
  if (hdr->key_len == 0 || hdr->key_len >= 128) return true;

  char key_buf[128];
  memset(key_buf, 0, sizeof(key_buf));
  unsigned char* dummy_val = NULL;
  if (hdr->value_len > 0) {
    dummy_val = malloc(hdr->value_len);
  }
  bool read_ok = timeseries_entity_iterator_read_data(it, hdr, key_buf, dummy_val);
  free(dummy_val);
  if (!read_ok) return true;
  key_buf[hdr->key_len] = '\0';

  // TAGINDEX key format: "<meas_id>:<tag_key>:<tag_value>"
  if (strncmp(key_buf, w->meas_prefix, w->plen) != 0) return true;
  const char* remainder = key_buf + w->plen;
  const char* second_colon = strchr(remainder, ':');
  if (!second_colon) return true;  // malformed
  size_t tag_key_len = (size_t)(second_colon - remainder);
  const char* tag_value = second_colon + 1;

  for (size_t i = 0; i < w->count; i++) {
    if (strlen(w->tags[i].key) == tag_key_len && strncmp(w->tags[i].key, remainder, tag_key_len) == 0 &&
        strcmp(w->tags[i].val, tag_value) == 0) {
      return true;  // duplicate key:value pair
    }
  }

  if (w->count >= w->capacity) {
    size_t new_cap = w->capacity == 0 ? 8 : w->capacity * 2;
    tsdb_tag_pair_t* new_tags = realloc(w->tags, new_cap * sizeof(tsdb_tag_pair_t));
    if (!new_tags) {
      ESP_LOGE(TAG, "OOM growing tag pair array");
      w->oom = true;
      return false;
    }
    w->tags = new_tags;
    w->capacity = new_cap;
  }
  tsdb_tag_pair_t* pair = &w->tags[w->count];
  pair->key = strndup(remainder, tag_key_len);
  pair->val = strdup(tag_value);
  if (!pair->key || !pair->val) {
    free(pair->key);
    free(pair->val);
    ESP_LOGE(TAG, "OOM duplicating tag strings");
    w->oom = true;
    return false;
  }
  w->count++;
  return true;
}

bool timeseries_metadata_get_tags_for_measurement(timeseries_db_t* db, uint32_t measurement_id,
                                                   tsdb_tag_pair_t** out_tags, size_t* out_num_tags) {
  if (!db || !out_tags || !out_num_tags) {
    return false;
  }

  *out_tags = NULL;
  *out_num_tags = 0;

  char meas_prefix[16];
  int plen = snprintf(meas_prefix, sizeof(meas_prefix), "%" PRIu32 ":", measurement_id);
  if (plen < 0 || (size_t)plen >= sizeof(meas_prefix)) {
    return false;
  }

  tsdb_tags_walk_t w = {.meas_prefix = meas_prefix, .plen = (size_t)plen};
  if (tsdb_meta_walk(db, TIMESERIES_KEYTYPE_TAGINDEX, tsdb_visit_tag_pairs, &w) == TSDB_META_WALK_NO_PAGES) {
    return false;
  }
  if (w.oom) {
    /* A partial list would silently drop tags, so report the failure. */
    for (size_t i = 0; i < w.count; i++) {
      free(w.tags[i].key);
      free(w.tags[i].val);
    }
    free(w.tags);
    return false;
  }

  /* A measurement with no tags is an empty list, not an error. */
  *out_tags = w.tags;
  *out_num_tags = w.count;
  return true;
}

// -----------------------------------------------------------------------------
// Measurement-ID cache
// -----------------------------------------------------------------------------

bool tsdb_meas_cache_lookup(timeseries_db_t* db, const char* measurement_name, uint32_t* out_id) {
  if (!db || !db->meas_cache || !measurement_name || !out_id) {
    return false;
  }
  for (int mc = 0; mc < MEAS_ID_CACHE_SIZE; mc++) {
    if (db->meas_cache[mc].valid && strcmp(measurement_name, db->meas_cache[mc].name) == 0) {
      *out_id = db->meas_cache[mc].id;
      return true;
    }
  }
  return false;
}

void tsdb_meas_cache_store(timeseries_db_t* db, const char* measurement_name, uint32_t id) {
  if (!db || !db->meas_cache || !measurement_name ||
      strlen(measurement_name) >= sizeof(db->meas_cache[0].name)) {
    return;
  }
  // Round-robin eviction
  uint8_t slot = db->meas_cache_next;
  strncpy(db->meas_cache[slot].name, measurement_name, sizeof(db->meas_cache[slot].name) - 1);
  db->meas_cache[slot].name[sizeof(db->meas_cache[slot].name) - 1] = '\0';
  db->meas_cache[slot].id = id;
  db->meas_cache[slot].valid = true;
  db->meas_cache_next = (slot + 1) % MEAS_ID_CACHE_SIZE;
}

// -----------------------------------------------------------------------------
// Query series collection (one metadata scan per query)
// -----------------------------------------------------------------------------

bool tsdb_collect_query_series(timeseries_db_t* db, uint32_t measurement_id, const char** tag_keys,
                               const char** tag_values, size_t num_tags, timeseries_series_id_list_t* tag_series,
                               timeseries_string_list_t* fields, bool discover_fields,
                               timeseries_series_id_list_t** field_series) {
  if (!db || !fields || !field_series || (num_tags && (!tag_keys || !tag_values || !tag_series))) {
    return false;
  }
  *field_series = NULL;
  TSDB_META_PAGES(meta_list);
  const uint32_t* meta_offsets = NULL;
  size_t meta_count = 0;

  // Field list keys are "<meas_id>:<field>", tag index keys "<meas_id>:<key>:<value>".
  char prefix[16];
  int plen = snprintf(prefix, sizeof(prefix), "%" PRIu32 ":", measurement_id);
  if (plen < 0 || (size_t)plen >= sizeof(prefix)) {
    return false;
  }

  bool ok = true;
  size_t fcap = fields->count > 4 ? fields->count : 4;
  timeseries_series_id_list_t* fs = calloc(fcap, sizeof(*fs));
  // tag_want[i] is NULL when the key exceeds the 128-byte key limit: it can
  // match nothing (tsdb_find_series_ids_for_tag rejects it the same way).
  char** tag_want = num_tags ? calloc(num_tags, sizeof(char*)) : NULL;
  unsigned char* val = NULL;
  size_t val_cap = 0;
  if (!fs || (num_tags && !tag_want)) {
    ok = false;
    goto out;
  }
  for (size_t i = 0; i < fcap; i++) tsdb_series_id_list_init(&fs[i]);
  for (size_t t = 0; t < num_tags; t++) {
    if (!tag_keys[t] || !tag_values[t]) continue;
    char kb[128];
    int n = snprintf(kb, sizeof(kb), "%" PRIu32 ":%s:%s", measurement_id, tag_keys[t], tag_values[t]);
    if (n < 0 || (size_t)n >= sizeof(kb)) continue;
    tag_want[t] = strdup(kb);
    if (!tag_want[t]) {
      ok = false;
      goto out;
    }
  }

  // No metadata pages (meta_count == 0) => nothing matches. A failed lookup
  // (OOM growing the page list) is not "no metadata": fail the query rather
  // than return an empty result for a measurement that has series.
  if (!tsdb_find_metadata_pages(db, &meta_list, &meta_offsets, &meta_count)) {
    ok = false;
    goto out;
  }

  for (size_t p = 0; p < meta_count && ok; p++) {
    timeseries_entity_iterator_t ent_iter;
    if (!tsdb_meta_scan_open(db, meta_offsets[p], TIMESERIES_METADATA_PAGE_SIZE, &ent_iter)) {
      continue;
    }
    timeseries_entry_header_t e_hdr;
    while (ok && timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      if (e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID || e_hdr.key_len >= 128) {
        continue;
      }
      bool is_field = e_hdr.key_type == TIMESERIES_KEYTYPE_FIELDLISTINDEX;
      bool is_tag = e_hdr.key_type == TIMESERIES_KEYTYPE_TAGINDEX && num_tags > 0;
      if ((!is_field && !is_tag) || e_hdr.key_len < (uint16_t)plen) {
        continue;
      }
      char key[128];
      if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, key, NULL)) {
        continue;
      }
      key[e_hdr.key_len] = '\0';
      if (memcmp(key, prefix, (size_t)plen) != 0) {
        continue;
      }

      size_t field_idx = SIZE_MAX;
      bool tag_hit = false;
      if (is_field) {
        const char* fname = key + plen;
        for (size_t f = 0; f < fields->count; f++) {
          if (strcmp(fields->items[f], fname) == 0) {
            field_idx = f;
            break;
          }
        }
        if (field_idx == SIZE_MAX) {
          if (!discover_fields) continue;
          // Discovered in scan order, before the value is validated, as
          // tsdb_list_fields_for_measurement() lists it.
          if (fields->count == fcap) {
            size_t ncap = fcap * 2;
            timeseries_series_id_list_t* nfs = realloc(fs, ncap * sizeof(*nfs));
            if (!nfs) {
              ok = false;
              break;
            }
            fs = nfs;
            for (size_t i = fcap; i < ncap; i++) tsdb_series_id_list_init(&fs[i]);
            fcap = ncap;
          }
          if (!tsdb_string_list_append_unique(fields, fname)) {
            ok = false;
            break;
          }
          field_idx = fields->count - 1;
        }
      } else {
        for (size_t t = 0; t < num_tags && !tag_hit; t++) {
          tag_hit = tag_want[t] && strlen(tag_want[t]) == e_hdr.key_len && strcmp(tag_want[t], key) == 0;
        }
        if (!tag_hit) continue;
      }

      // Each series ID is 16 bytes.
      if (e_hdr.value_len == 0 || e_hdr.value_len % 16 != 0) {
        if (e_hdr.value_len % 16 != 0) {
          ESP_LOGW(TAG, "Unexpected %s value_len=%" PRIu16 " (not multiple of 16).", is_field ? "fieldlist" : "tagindex",
                   e_hdr.value_len);
        }
        continue;
      }
      if (e_hdr.value_len > val_cap) {
        unsigned char* nb = realloc(val, e_hdr.value_len);
        if (!nb) {
          ok = false;
          break;
        }
        val = nb;
        val_cap = e_hdr.value_len;
      }
      if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, NULL, val)) {
        continue;
      }
      size_t n_ids = e_hdr.value_len / 16;
      if (is_field) {
        ok = tsdb_append_series_ids(&fs[field_idx], val, n_ids);
      } else {
        for (size_t t = 0; t < num_tags && ok; t++) {
          if (tag_want[t] && strlen(tag_want[t]) == e_hdr.key_len && strcmp(tag_want[t], key) == 0) {
            ok = tsdb_append_series_ids(&tag_series[t], val, n_ids);
          }
        }
      }
    }
    timeseries_entity_iterator_deinit(&ent_iter);
  }
  if (!ok) {
    ESP_LOGE(TAG, "OOM collecting query series for measurement %" PRIu32, measurement_id);
  }

out:
  if (tag_want) {
    for (size_t t = 0; t < num_tags; t++) free(tag_want[t]);
    free(tag_want);
  }
  free(val);
  if (fs && !ok) {
    for (size_t i = 0; i < fcap; i++) tsdb_series_id_list_free(&fs[i]);
    free(fs);
    fs = NULL;
  }
  *field_series = fs;
  return ok;
}
