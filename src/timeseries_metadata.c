/**
 * @file timeseries_metadata.c
 * @brief Implementation of metadata-related logic (measurements, tags, series
 * types).
 */

#include "timeseries_metadata.h"

#include "esp_err.h"
#include "esp_log.h"
#include "esp_partition.h"
#include "mbedtls/md5.h"

#include <inttypes.h>
#include <string.h>

#include "timeseries_data.h"
#include "timeseries_id_list.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_string_list.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

static const char *TAG = "TimeseriesMetadata";

// -----------------------------------------------------------------------------
// Forward-declared static helpers
// -----------------------------------------------------------------------------

static bool tsdb_create_empty_metadata_page(timeseries_db_t *db,
                                            uint32_t page_offset);
static bool tsdb_insert_measurement_entry(timeseries_db_t *db,
                                          uint32_t page_offset,
                                          const char *measurement_name,
                                          uint32_t id);
static bool
tsdb_find_metadata_pages(timeseries_db_t *db,
                         uint32_t *out_offsets, // store offsets not indices
                         size_t max_pages, size_t *found_count);

static bool tsdb_soft_delete_entry(timeseries_db_t *db, uint32_t page_offset,
                                   uint32_t current_entry_offset);

static bool tsdb_create_new_index_entry(timeseries_db_t *db,
                                        uint32_t page_offset,
                                        timeseries_keytype_t key_type,
                                        const char *key_str,
                                        const unsigned char *series_ids,
                                        size_t series_ids_len);

static bool tsdb_upsert_seriesid_list_entry(
    timeseries_db_t *db, timeseries_keytype_t target_key_type,
    const uint32_t *meta_offsets, size_t meta_count, const char *key_str,
    const unsigned char series_id[16]);

static bool tsdb_upsert_tagindex_entry(timeseries_db_t *db,
                                       const uint32_t *meta_offsets,
                                       size_t meta_count, const char *key_str,
                                       const unsigned char series_id[16]);

static bool tsdb_upsert_fieldlist_entry(timeseries_db_t *db,
                                        const uint32_t *meta_offsets,
                                        size_t meta_count, const char *key_str,
                                        const unsigned char series_id[16]);

// -----------------------------------------------------------------------------
// Public API Implementations
// -----------------------------------------------------------------------------

bool timeseries_metadata_create_page(timeseries_db_t *db) {
  if (!db) {
    return false;
  }

  timeseries_blank_iterator_t blank_iter;
  if (!timeseries_blank_iterator_init(db, &blank_iter,
                                      TIMESERIES_METADATA_PAGE_SIZE)) {
    ESP_LOGE(TAG, "Failed to init blank iterator");
    return false;
  }

  uint32_t blank_offset = 0, blank_length = 0;
  bool found_blank =
      timeseries_blank_iterator_next(&blank_iter, &blank_offset, &blank_length);

  if (!found_blank) {
    ESP_LOGE(TAG, "No blank region found to create metadata page!");
    return false;
  }

  ESP_LOGV(TAG,
           "Found blank region @0x%08" PRIx32 " (length=%" PRIu32 " bytes).",
           blank_offset, blank_length);

  // 3) Create the empty metadata page here
  if (!tsdb_create_empty_metadata_page(db, blank_offset)) {
    ESP_LOGE(TAG, "Failed to create metadata page @0x%08" PRIx32, blank_offset);
    return false;
  }

  return true;
}

bool tsdb_load_pages_into_memory(timeseries_db_t *db) {
  if (!db) {
    return false;
  }
  ESP_LOGV(TAG, "Scanning 'storage' partition for existing TSDB pages...");

  if (!tsdb_build_page_cache(db)) {
    ESP_LOGE(TAG, "Failed to build page cache");
    return false;
  }

  ESP_LOGV(TAG, "Page cache built => %u total entries",
           (unsigned)db->page_cache_count);

  // 1) Use the page iterator to check if a metadata page exists
  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    return false;
  }

  bool found_metadata_page = false;
  unsigned int pages_scanned = 0;

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0;
  uint32_t page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    pages_scanned++;

    // If we find an ACTIVE metadata page, note it
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM) {
      ESP_LOGV(TAG,
               "Found TSDB page at offset=0x%08" PRIx32 " (type=%" PRIu8
               ", state=%" PRIu8 ", seq=%" PRIu32 ", size=%" PRIu32 ")",
               page_offset, hdr.page_type, hdr.page_state, hdr.sequence_num,
               hdr.page_size);

      if (hdr.page_type == TIMESERIES_PAGE_TYPE_METADATA &&
          hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
        found_metadata_page = true;
      }

      // Update sequence number
      if (hdr.sequence_num > db->sequence_num) {
        db->sequence_num = hdr.sequence_num;
      }
    }
  }

  ESP_LOGV(TAG, "Scanned %u pages in 'storage' partition.", pages_scanned);

  // 2) If no metadata page found, create one
  if (!found_metadata_page) {
    ESP_LOGV(TAG, "No active metadata page found; creating one...");
    found_metadata_page = timeseries_metadata_create_page(db);
  }

  return true;
}

bool tsdb_find_measurement_id(timeseries_db_t *db, const char *measurement_name,
                              uint32_t *out_id) {
  if (!db || !measurement_name || !out_id) {
    return false;
  }

  // We'll scan all active METADATA pages to look for a matching measurement
  uint32_t meta_offsets[4];
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, meta_offsets, 4, &meta_count)) {
    ESP_LOGW(TAG, "No metadata pages found to search for measurement '%s'.",
             measurement_name);
    return false;
  }

  // For each active metadata page offset
  for (size_t i = 0; i < meta_count; i++) {
    uint32_t page_offset = meta_offsets[i];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE; // 8 KB

    // Initialize an entity iterator for this page
    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, page_offset, page_size,
                                         &ent_iter)) {
      ESP_LOGW(TAG,
               "Failed to init entity iterator for meta offset=0x%08" PRIx32,
               page_offset);
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

      if (e_hdr.key_len == strlen(measurement_name)) {
        char key_buf[64];
        if (e_hdr.key_len < sizeof(key_buf)) {
          // read the key + the value
          uint32_t val = 0;
          if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, key_buf,
                                                    &val)) {
            ESP_LOGW(TAG, "Failed reading measurement entry data.");
            continue;
          }

          key_buf[e_hdr.key_len] = '\0';

          ESP_LOGV(TAG, "Read measurement entry: key='%s' val=%" PRIu32,
                   key_buf, val);

          // Compare
          if (strcmp(key_buf, measurement_name) == 0) {
            *out_id = val;
            return true;
          }
        }
      }
    }
  }

  // Not found in any metadata page
  return false;
}

bool tsdb_create_measurement_id(timeseries_db_t *db,
                                const char *measurement_name,
                                uint32_t *out_id) {
  if (!db || !measurement_name || !out_id) {
    return false;
  }

  // Grab next ID from db
  uint32_t new_id = db->next_measurement_id++;
  *out_id = new_id;

  // We pick the first active metadata page offset to store the measurement
  uint32_t meta_offsets[4];
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, meta_offsets, 4, &meta_count) ||
      meta_count == 0) {
    ESP_LOGE(TAG, "No active metadata page to insert measurement '%s'.",
             measurement_name);
    return false;
  }

  // Insert the measurement into the first page offset
  if (!tsdb_insert_measurement_entry(db, meta_offsets[0], measurement_name,
                                     new_id)) {
    ESP_LOGE(TAG, "Failed to insert measurement entry for '%s'!",
             measurement_name);
    return false;
  }

  ESP_LOGV(TAG, "Created measurement '%s' with ID=%" PRIu32, measurement_name,
           new_id);
  return true;
}

bool tsdb_insert_single_field(timeseries_db_t *db, uint32_t measurement_id,
                              const char *measurement_name,
                              const char *field_name,
                              const timeseries_field_value_t *field_val,
                              const char **tag_keys, const char **tag_values,
                              size_t num_tags, uint64_t timestamp_ms) {
  if (!db || !measurement_name || !field_name || !field_val) {
    return false;
  }

  // 1) Build MD5 input
  char buffer[256];
  memset(buffer, 0, sizeof(buffer));

  size_t offset = 0;
  offset += snprintf(buffer + offset, sizeof(buffer) - offset, "%s",
                     measurement_name);
  for (size_t i = 0; i < num_tags; i++) {
    offset += snprintf(buffer + offset, sizeof(buffer) - offset, ":%s:%s",
                       tag_keys[i], tag_values[i]);
    if (offset >= sizeof(buffer)) {
      ESP_LOGE(TAG, "MD5 input buffer overflow");
      return false;
    }
  }
  offset +=
      snprintf(buffer + offset, sizeof(buffer) - offset, ":%s", field_name);
  if (offset >= sizeof(buffer)) {
    ESP_LOGE(TAG, "MD5 input buffer overflow (field_name too long?)");
    return false;
  }

  ESP_LOGV(TAG, "Series ID calc string: '%s'", buffer);

  // 2) MD5 => series_id
  unsigned char series_id[16];
  mbedtls_md5((const unsigned char *)buffer, strlen(buffer), series_id);

  // 3) Ensure field type in metadata
  if (!tsdb_ensure_series_type_in_metadata(db, series_id, field_val->type)) {
    return false;
  }

  // 4) Index tags
  if (!tsdb_index_tags_for_series(db, measurement_id, tag_keys, tag_values,
                                  num_tags, series_id)) {
    return false;
  }

  // 5) Append data to field data page
  if (!tsdb_append_field_data(db, series_id, timestamp_ms, field_val)) {
    return false;
  }

  ESP_LOGV(TAG,
           "Inserted field='%s' meas_id=%" PRIu32 " ts=%" PRIu64
           " (SeriesID=%.2X%.2X%.2X%.2X...)",
           field_name, measurement_id, timestamp_ms, series_id[0], series_id[1],
           series_id[2], series_id[3]);
  return true;
}

/**
 * @brief Lookup an existing field type in metadata for the given series_id.
 *
 * @param db          The DB context
 * @param series_id   16-byte unique identifier for the series
 * @param out_type    If found, store the existing type here
 * @return true if found a match, false if not found or error
 */
bool tsdb_lookup_series_type_in_metadata(timeseries_db_t *db,
                                         const unsigned char series_id[16],
                                         timeseries_field_type_e *out_type) {
  if (!db || !series_id || !out_type) {
    return false;
  }

  // find active metadata pages
  uint32_t meta_offsets[4];
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, meta_offsets, 4, &meta_count)) {
    ESP_LOGE(TAG, "No active metadata pages for series ID lookup");
    return false;
  }

  // scan each page
  for (size_t i = 0; i < meta_count; i++) {
    uint32_t page_offset = meta_offsets[i];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, page_offset, page_size,
                                         &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_FIELDINDEX ||
          e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }
      // must be 16-byte key, 1-byte value
      if (e_hdr.key_len == 16 && e_hdr.value_len == 1) {
        unsigned char key_buf[16];
        uint8_t stored_type = 0;

        if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, key_buf,
                                                  &stored_type)) {
          continue;
        }
        if (memcmp(key_buf, series_id, 16) == 0) {
          // found the matching series
          *out_type = (timeseries_field_type_e)stored_type;
          return true; // success
        }
      }
    }
  }

  return false; // not found in any page
}

/**
 * @brief Ensure the given series_id has a matching field_type in metadata,
 *        or create a new entry if it doesn't exist. Returns false on conflict.
 */
bool tsdb_ensure_series_type_in_metadata(timeseries_db_t *db,
                                         const unsigned char series_id[16],
                                         timeseries_field_type_e field_type) {
  if (!db || !series_id) {
    return false;
  }

  // 1) Check if there's already a stored type for this series ID
  timeseries_field_type_e existing_type;
  if (tsdb_lookup_series_type_in_metadata(db, series_id, &existing_type)) {
    // found existing
    if (existing_type != field_type) {
      ESP_LOGE(TAG, "SeriesID conflict: existing type=%d, new=%d",
               (int)existing_type, (int)field_type);
      return false;
    }
    // else it matches => done
    ESP_LOGV(TAG, "SeriesID already has matching type=%d", (int)field_type);
    return true;
  }

  // 2) Not found => create new entry in first meta page
  uint32_t meta_offsets[4];
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, meta_offsets, 4, &meta_count) ||
      meta_count == 0) {
    ESP_LOGE(TAG, "No active metadata page to create new field index");
    return false;
  }

  // pick first meta page
  uint32_t page_offset = meta_offsets[0];
  uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

  timeseries_entity_iterator_t ent_iter;
  if (!timeseries_entity_iterator_init(db, page_offset, page_size, &ent_iter)) {
    return false;
  }
  // find end offset
  uint32_t last_offset = ent_iter.offset;
  timeseries_entry_header_t e_hdr;
  while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
    last_offset = ent_iter.offset;
  }
  uint32_t entry_offset = page_offset + last_offset;

  // build new header
  timeseries_entry_header_t new_hdr;
  memset(&new_hdr, 0, sizeof(new_hdr));
  new_hdr.delete_marker = TIMESERIES_DELETE_MARKER_VALID;
  new_hdr.key_type = TIMESERIES_KEYTYPE_FIELDINDEX;
  new_hdr.key_len = 16;
  new_hdr.value_len = 1;

  esp_err_t err = esp_partition_write(db->partition, entry_offset, &new_hdr,
                                      sizeof(new_hdr));
  if (err != ESP_OK) {
    return false;
  }
  entry_offset += sizeof(new_hdr);

  // write 16-byte series_id
  err = esp_partition_write(db->partition, entry_offset, series_id, 16);
  if (err != ESP_OK) {
    return false;
  }
  entry_offset += 16;

  // write 1 byte type
  uint8_t stored_type = (uint8_t)field_type;
  err = esp_partition_write(db->partition, entry_offset, &stored_type, 1);
  if (err != ESP_OK) {
    return false;
  }

  ESP_LOGV(
      TAG, "Created new fieldindex for series=%.2X%.2X%.2X%.2X..., type=%d",
      series_id[0], series_id[1], series_id[2], series_id[3], (int)field_type);

  return true;
}

bool tsdb_index_tags_for_series(timeseries_db_t *db, uint32_t measurement_id,
                                const char **tag_keys, const char **tag_values,
                                size_t num_tags,
                                const unsigned char series_id[16]) {
  // If you call tsdb_upsert_tagindex_entry() here, it must be declared first!
  if (!db || !series_id) {
    return false;
  }
  if (num_tags == 0) {
    return true;
  }

  // find metadata pages
  uint32_t meta_offsets[4];
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, meta_offsets, 4, &meta_count)) {
    ESP_LOGE(TAG, "No active metadata pages for tag indexing");
    return false;
  }

  // For each tag, build the key and upsert
  for (size_t i = 0; i < num_tags; i++) {
    char key_buf[128];
    int n = snprintf(key_buf, sizeof(key_buf), "%" PRIu32 ":%s:%s",
                     measurement_id, tag_keys[i], tag_values[i]);
    if (n < 0 || (size_t)n >= sizeof(key_buf)) {
      return false;
    }

    if (!tsdb_upsert_tagindex_entry(db, meta_offsets, meta_count, key_buf,
                                    series_id)) {
      ESP_LOGE(TAG, "Failed to upsert tag '%s' => series ID", key_buf);
      return false;
    }
  }

  return true;
}

// -----------------------------------------------------------------------------
// Private (static) Helpers
// -----------------------------------------------------------------------------

static bool tsdb_create_empty_metadata_page(timeseries_db_t *db,
                                            uint32_t page_offset) {
  if (!db) {
    return false;
  }
  ESP_LOGV(TAG, "Creating empty metadata page at offset=0x%08" PRIx32 "...",
           page_offset);

  // Erase 8 KB from page_offset
  esp_err_t err = esp_partition_erase_range(db->partition, page_offset,
                                            TIMESERIES_METADATA_PAGE_SIZE);
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to erase meta page (err=0x%x)", err);
    return false;
  }

  timeseries_page_header_t new_hdr;
  memset(&new_hdr, 0xFF, sizeof(new_hdr));
  new_hdr.magic_number = TIMESERIES_MAGIC_NUM;
  new_hdr.page_type = TIMESERIES_PAGE_TYPE_METADATA;
  new_hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  new_hdr.sequence_num = ++db->sequence_num;
  new_hdr.field_data_level = 0; // not used for metadata
  new_hdr.page_size = TIMESERIES_METADATA_PAGE_SIZE;

  err = esp_partition_write(db->partition, page_offset, &new_hdr,
                            sizeof(new_hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to write new metadata header (err=0x%x)", err);
    return false;
  }

  // Add to page cache
  tsdb_pagecache_add_entry(db, page_offset, &new_hdr);

  ESP_LOGV(TAG,
           "Empty metadata page created at offset=0x%08" PRIx32 " size = %d",
           page_offset, TIMESERIES_METADATA_PAGE_SIZE);
  return true;
}

static bool tsdb_insert_measurement_entry(timeseries_db_t *db,
                                          uint32_t page_offset,
                                          const char *measurement_name,
                                          uint32_t id) {
  if (!db || !measurement_name) {
    return false;
  }

  uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

  // find the end offset of this page
  timeseries_entity_iterator_t ent_iter;
  if (!timeseries_entity_iterator_init(db, page_offset, page_size, &ent_iter)) {
    return false;
  }

  uint32_t last_offset = ent_iter.offset;
  timeseries_entry_header_t e_hdr;
  while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
    last_offset = ent_iter.offset;
  }
  uint32_t entry_offset = page_offset + last_offset;

  // build header
  timeseries_entry_header_t new_hdr;
  memset(&new_hdr, 0, sizeof(new_hdr));
  new_hdr.delete_marker = TIMESERIES_DELETE_MARKER_VALID;
  new_hdr.key_type = TIMESERIES_KEYTYPE_MEASUREMENT;
  new_hdr.key_len = (uint16_t)strlen(measurement_name);
  new_hdr.value_len = sizeof(uint32_t);

  esp_err_t err = esp_partition_write(db->partition, entry_offset, &new_hdr,
                                      sizeof(new_hdr));
  if (err != ESP_OK) {
    return false;
  }

  entry_offset += sizeof(new_hdr);

  // write the key
  err = esp_partition_write(db->partition, entry_offset, measurement_name,
                            new_hdr.key_len);
  if (err != ESP_OK) {
    return false;
  }
  entry_offset += new_hdr.key_len;

  // write the ID
  err = esp_partition_write(db->partition, entry_offset, &id, sizeof(id));
  if (err != ESP_OK) {
    return false;
  }

  ESP_LOGV(TAG,
           "Inserted measurement='%s' ID=%" PRIu32
           " into metadata page @ offset=0x%08" PRIx32,
           measurement_name, id, page_offset);
  return true;
}

/**
 * @brief Finds all offsets in the partition that are active METADATA pages.
 *
 * The new page iterator returns (header, offset, size). We check if
 * page_type==METADATA && state==ACTIVE, then store `offset`.
 */
static bool tsdb_find_metadata_pages(timeseries_db_t *db, uint32_t *out_offsets,
                                     size_t max_pages, size_t *found_count) {
  if (!db || !out_offsets || !found_count) {
    return false;
  }
  *found_count = 0;

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0;
  uint32_t page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM &&
        hdr.page_type == TIMESERIES_PAGE_TYPE_METADATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
      if (*found_count < max_pages) {
        out_offsets[*found_count] = page_offset;
        (*found_count)++;
      } else {
        break;
      }
    }
  }

  return (*found_count > 0);
}

static bool tsdb_soft_delete_entry(timeseries_db_t *db, uint32_t page_offset,
                                   uint32_t current_entry_offset) {
  if (!db) {
    return false;
  }

  uint32_t entry_addr = page_offset + current_entry_offset;

  // Overwrite delete_marker=0x00
  uint8_t marker = TIMESERIES_DELETE_MARKER_DELETED;
  esp_err_t err = esp_partition_write(db->partition, entry_addr, &marker, 1);
  if (err != ESP_OK) {
    return false;
  }

  ESP_LOGD(TAG, "Soft-deleted old tagindex entry at offset=0x%08" PRIx32,
           entry_addr);
  return true;
}

static bool tsdb_create_new_index_entry(timeseries_db_t *db,
                                        uint32_t page_offset,
                                        timeseries_keytype_t key_type,
                                        const char *key_str,
                                        const unsigned char *series_ids,
                                        size_t series_ids_len) {
  if (!db || !key_str || !series_ids) {
    return false;
  }

  // Initialize an entity iterator to find the end of the page
  uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;
  timeseries_entity_iterator_t ent_iter;
  if (!timeseries_entity_iterator_init(db, page_offset, page_size, &ent_iter)) {
    return false;
  }

  // Find last offset in that page
  uint32_t last_offset = ent_iter.offset;
  timeseries_entry_header_t e_hdr;
  while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
    last_offset = ent_iter.offset;
  }
  uint32_t entry_offset = page_offset + last_offset;

  // Build header
  timeseries_entry_header_t new_hdr;
  memset(&new_hdr, 0, sizeof(new_hdr));
  new_hdr.delete_marker = TIMESERIES_DELETE_MARKER_VALID;
  new_hdr.key_type = key_type;
  new_hdr.key_len = (uint16_t)strlen(key_str);
  new_hdr.value_len = (uint16_t)series_ids_len;

  // Write header
  esp_err_t err = esp_partition_write(db->partition, entry_offset, &new_hdr,
                                      sizeof(new_hdr));
  if (err != ESP_OK) {
    return false;
  }
  entry_offset += sizeof(new_hdr);

  // Write the key
  err = esp_partition_write(db->partition, entry_offset, key_str,
                            new_hdr.key_len);
  if (err != ESP_OK) {
    return false;
  }
  entry_offset += new_hdr.key_len;

  // Write the series IDs
  err = esp_partition_write(db->partition, entry_offset, series_ids,
                            series_ids_len);
  if (err != ESP_OK) {
    return false;
  }

  ESP_LOGV(TAG, "Created new index entry (type=%u) for '%s', value_len=%u",
           (unsigned)key_type, key_str, (unsigned)new_hdr.value_len);
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
bool tsdb_index_field_for_series(timeseries_db_t *db, uint32_t measurement_id,
                                 const char *field_name,
                                 const unsigned char series_id[16]) {
  if (!db || !field_name || !series_id) {
    return false;
  }

  // Build the key string "measurementId:fieldName"
  char key_buf[128];
  int n = snprintf(key_buf, sizeof(key_buf), "%" PRIu32 ":%s", measurement_id,
                   field_name);
  if (n < 0 || (size_t)n >= sizeof(key_buf)) {
    ESP_LOGE(TAG, "tsdb_index_field_for_series: key_buf overflow");
    return false;
  }

  // Find all active metadata pages
  uint32_t meta_offsets[4];
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, meta_offsets, 4, &meta_count) ||
      meta_count == 0) {
    ESP_LOGE(TAG, "No active metadata page found for field index");
    return false;
  }

  // Upsert the key => append the seriesId if it doesn't exist.
  return tsdb_upsert_fieldlist_entry(db, meta_offsets, meta_count, key_buf,
                                     series_id);
}

bool tsdb_find_series_ids_for_tag(
    timeseries_db_t *db, uint32_t measurement_id, const char *tag_key,
    const char *tag_value, timeseries_series_id_list_t *out_series_list) {
  if (!db || !tag_key || !tag_value || !out_series_list) {
    return false;
  }

  // 1) Build the key that identifies the tag index entry, e.g.
  // "1234:myTag:someValue"
  char key_buf[128];
  int n = snprintf(key_buf, sizeof(key_buf), "%" PRIu32 ":%s:%s",
                   measurement_id, tag_key, tag_value);
  if (n < 0 || (size_t)n >= sizeof(key_buf)) {
    ESP_LOGE(TAG, "tsdb_find_series_ids_for_tag: key_buf overflow");
    return false;
  }

  // 2) Find active METADATA pages
  uint32_t meta_offsets[4];
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, meta_offsets, 4, &meta_count) ||
      meta_count == 0) {
    // No metadata pages => no result
    ESP_LOGD(TAG, "No active metadata pages for tag '%s'.", key_buf);
    return false;
  }

  // 3) Scan each metadata page for an entry with:
  //    - key_type == TIMESERIES_KEYTYPE_TAGINDEX
  //    - delete_marker == TIMESERIES_DELETE_MARKER_VALID
  //    - matching key == key_buf
  bool found_any = false;

  for (size_t p = 0; p < meta_count; p++) {
    uint32_t page_offset = meta_offsets[p];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE; // e.g. 8KB

    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, page_offset, page_size,
                                         &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      // Check if we have a valid TagIndex entry
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_TAGINDEX ||
          e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }

      // The key_len should match length of our "measurementId:tagKey:tagValue"
      size_t expected_len = strlen(key_buf);
      if (e_hdr.key_len != expected_len) {
        continue;
      }

      // The value_len should be a multiple of 16 (each series ID is 16 bytes)
      if (e_hdr.value_len % 16 != 0) {
        ESP_LOGW(TAG,
                 "Unexpected tagindex value_len=%" PRIu16
                 " (not multiple of 16).",
                 e_hdr.value_len);
        continue;
      }

      // Read the actual key string + the array of series IDs
      char stored_key[128];
      if (expected_len >= sizeof(stored_key)) {
        // Shouldn't happen if we used the same buffer size, but just in case
        ESP_LOGW(TAG, "tagindex key too large to read");
        continue;
      }

      unsigned char *series_ids = malloc(e_hdr.value_len);
      if (!series_ids) {
        ESP_LOGE(TAG, "Out of memory reading tagindex series IDs");
        continue;
      }

      memset(stored_key, 0, sizeof(stored_key));
      if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, stored_key,
                                                series_ids)) {
        ESP_LOGW(TAG, "Failed reading tagindex entry data");
        free(series_ids);
        continue;
      }

      stored_key[expected_len] = '\0';

      // Compare the stored key to our key_buf
      if (strcmp(stored_key, key_buf) == 0) {
        found_any = true;
        size_t num_ids = e_hdr.value_len / 16;

        // Append these series IDs to out_series_list
        for (size_t i = 0; i < num_ids; i++) {
          if (!tsdb_series_id_list_append(out_series_list,
                                          series_ids + (i * 16))) {
            ESP_LOGE(TAG, "Failed appending a series ID (OOM?)");
            free(series_ids);
            return false; // stop on error
          }
        }
      }

      free(series_ids);
    }
  }

  // Return true if we found at least one entry
  // (If you prefer returning 'true' as long as no errors occurred, do so,
  //  but many designs interpret "true" as "found something.")
  return found_any;
}

/**
 * @brief Insert or update an index entry for either TAGINDEX or FIELDLISTINDEX,
 *        storing a list of 16-byte series IDs.
 *
 * This unifies the logic from tsdb_upsert_tagindex_entry and
 * tsdb_upsert_fieldlist_entry.
 */
static bool tsdb_upsert_seriesid_list_entry(
    timeseries_db_t *db, timeseries_keytype_t target_key_type,
    const uint32_t *meta_offsets, size_t meta_count, const char *key_str,
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
    if (!timeseries_entity_iterator_init(
            db, page_offset, TIMESERIES_METADATA_PAGE_SIZE, &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {

      // Skip if not the right key_type or if deleted
      if (e_hdr.key_type != target_key_type ||
          e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
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
        unsigned char *all_series_ids = malloc(e_hdr.value_len);
        if (!all_series_ids) {
          return false;
        }

        if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, stored_key,
                                                  all_series_ids)) {
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

          // If not present, append
          if (!already_present) {
            size_t new_size = (series_count + 1) * 16;
            unsigned char *new_ids = malloc(new_size);
            if (!new_ids) {
              free(all_series_ids);
              return false;
            }
            memcpy(new_ids, all_series_ids, series_count * 16);
            memcpy(new_ids + series_count * 16, series_id, 16);

            // Create new entry with new_size
            if (!tsdb_create_new_index_entry(db, page_offset, target_key_type,
                                             key_str, new_ids, new_size)) {
              free(new_ids);
              free(all_series_ids);
              return false;
            }
            free(new_ids);

            // Soft-delete the old entry
            if (!tsdb_soft_delete_entry(db, page_offset,
                                        ent_iter.current_entry_offset)) {
              ESP_LOGW(TAG, "Failed to soft-delete old entry: '%s'", key_str);
            }
            updated = true;
            ESP_LOGD(TAG, "Appended series_id to %s entry '%s'",
                     (target_key_type == TIMESERIES_KEYTYPE_TAGINDEX)
                         ? "tag"
                         : "fieldlist",
                     key_str);
          } else {
            ESP_LOGD(TAG, "Series ID already present for '%s'; no rewrite",
                     key_str);
          }

          free(all_series_ids);
          break; // done in this page
        } // if matching stored_key

        free(all_series_ids);
      } // if key_len etc
    } // while next entity
  } // for meta_count

  // If we never found an existing entry, create a brand-new one in the first
  // page
  if (!found_existing && meta_count > 0) {
    if (!tsdb_create_new_index_entry(db, meta_offsets[0], target_key_type,
                                     key_str, series_id, 16)) {
      return false;
    }
    ESP_LOGD(TAG,
             "Created new index entry (type=%u) for '%s' w/ single series_id",
             (unsigned)target_key_type, key_str);
  }

  return true;
}

static bool tsdb_upsert_tagindex_entry(timeseries_db_t *db,
                                       const uint32_t *meta_offsets,
                                       size_t meta_count, const char *key_str,
                                       const unsigned char series_id[16]) {
  return tsdb_upsert_seriesid_list_entry(db, TIMESERIES_KEYTYPE_TAGINDEX,
                                         meta_offsets, meta_count, key_str,
                                         series_id);
}

static bool tsdb_upsert_fieldlist_entry(timeseries_db_t *db,
                                        const uint32_t *meta_offsets,
                                        size_t meta_count, const char *key_str,
                                        const unsigned char series_id[16]) {
  return tsdb_upsert_seriesid_list_entry(db, TIMESERIES_KEYTYPE_FIELDLISTINDEX,
                                         meta_offsets, meta_count, key_str,
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
bool tsdb_find_series_ids_for_field(
    timeseries_db_t *db, uint32_t measurement_id, const char *field_name,
    timeseries_series_id_list_t *out_series_list) {
  if (!db || !field_name || !out_series_list) {
    return false;
  }

  // 1) Build the key that identifies the field list entry, e.g.
  // "1234:fieldName"
  char key_buf[128];
  int n = snprintf(key_buf, sizeof(key_buf), "%" PRIu32 ":%s", measurement_id,
                   field_name);
  if (n < 0 || (size_t)n >= sizeof(key_buf)) {
    ESP_LOGE(TAG, "tsdb_find_series_ids_for_field: key_buf overflow");
    return false;
  }

  // 2) Find active METADATA pages
  uint32_t meta_offsets[4];
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, meta_offsets, 4, &meta_count) ||
      meta_count == 0) {
    // No metadata pages => no result
    ESP_LOGD(TAG, "No active metadata pages for field '%s'.", key_buf);
    return false;
  }

  // 3) Scan each metadata page for an entry with:
  //    - key_type == TIMESERIES_KEYTYPE_FIELDLISTINDEX
  //    - delete_marker == TIMESERIES_DELETE_MARKER_VALID
  //    - matching key == key_buf
  bool found_any = false;

  for (size_t p = 0; p < meta_count; p++) {
    uint32_t page_offset = meta_offsets[p];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE; // e.g. 8KB

    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, page_offset, page_size,
                                         &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      // Check if we have a valid FieldList entry
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_FIELDLISTINDEX ||
          e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }

      // The key_len should match length of our "<measurementId>:<fieldName>"
      size_t expected_len = strlen(key_buf);
      if (e_hdr.key_len != expected_len) {
        continue;
      }

      // The value_len should be a multiple of 16 (each series ID is 16 bytes)
      if (e_hdr.value_len % 16 != 0) {
        ESP_LOGW(TAG,
                 "Unexpected fieldlist value_len=%" PRIu16
                 " (not multiple of 16).",
                 e_hdr.value_len);
        continue;
      }

      // Read the actual key string + the array of series IDs
      char stored_key[128];
      if (expected_len >= sizeof(stored_key)) {
        // Shouldn't happen if we used the same buffer size, but just in case
        ESP_LOGW(TAG, "fieldlist key too large to read");
        continue;
      }

      unsigned char *series_ids = malloc(e_hdr.value_len);
      if (!series_ids) {
        ESP_LOGE(TAG, "Out of memory reading fieldlist series IDs");
        continue;
      }

      memset(stored_key, 0, sizeof(stored_key));
      if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, stored_key,
                                                series_ids)) {
        ESP_LOGW(TAG, "Failed reading fieldlist entry data");
        free(series_ids);
        continue;
      }

      stored_key[expected_len] = '\0';

      // Compare the stored key to our key_buf
      if (strcmp(stored_key, key_buf) == 0) {
        found_any = true;
        size_t num_ids = e_hdr.value_len / 16;

        // Append these series IDs to out_series_list
        for (size_t i = 0; i < num_ids; i++) {
          if (!tsdb_series_id_list_append(out_series_list,
                                          series_ids + (i * 16))) {
            ESP_LOGE(TAG, "Failed appending a series ID (OOM?)");
            free(series_ids);
            return false; // stop on error
          }
        }
      }

      free(series_ids);
    }
  }

  // Return true if we found at least one entry
  return found_any;
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
bool tsdb_list_all_measurements(timeseries_db_t *db,
                                timeseries_string_list_t *out_measurements) {
  if (!db || !out_measurements) {
    return false;
  }

  // 1) Find active METADATA pages
  uint32_t meta_offsets[4];
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, meta_offsets, 4, &meta_count) ||
      meta_count == 0) {
    ESP_LOGD(TAG, "No active metadata pages => no measurements");
    return false;
  }

  bool found_any = false;

  // 2) Scan each page for measurement entries
  for (size_t p = 0; p < meta_count; p++) {
    uint32_t page_offset = meta_offsets[p];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, page_offset, page_size,
                                         &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_MEASUREMENT ||
          e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }

      // The measurement name is e_hdr.key_len long
      // We'll read that plus the ID, but we only need the name for listing.
      if (e_hdr.key_len > 0 && e_hdr.key_len < 256) {
        char name_buf[256];
        uint32_t measurement_id;
        memset(name_buf, 0, sizeof(name_buf));

        if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, name_buf,
                                                  &measurement_id)) {
          ESP_LOGW(TAG, "Failed reading measurement entry data.");
          continue;
        }
        // name_buf is now the measurement name
        name_buf[e_hdr.key_len] = '\0';

        // Append unique to the output list
        if (tsdb_string_list_append_unique(out_measurements, name_buf)) {
          found_any = true;
        }
      }
    }
  }

  return found_any;
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
bool tsdb_list_fields_for_measurement(timeseries_db_t *db,
                                      uint32_t measurement_id,
                                      timeseries_string_list_t *out_fields) {
  if (!db || !out_fields) {
    return false;
  }

  // 1) Find active METADATA pages
  uint32_t meta_offsets[4];
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, meta_offsets, 4, &meta_count) ||
      meta_count == 0) {
    ESP_LOGD(TAG, "No active metadata pages => no fields");
    return false;
  }

  bool found_any = false;

  // 2) Scan each metadata page for FIELDLISTINDEX entries
  for (size_t p = 0; p < meta_count; p++) {
    uint32_t page_offset = meta_offsets[p];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, page_offset, page_size,
                                         &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      // We want FIELDLISTINDEX entries
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_FIELDLISTINDEX ||
          e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }

      // The key looks like "<meas_id>:<field_name>"
      // We'll read it + the list of series IDs (which we don't need for this
      // listing).
      if (e_hdr.key_len > 0 && e_hdr.key_len < 128) {
        char key_buf[128];
        memset(key_buf, 0, sizeof(key_buf));

        // The value is a bunch of 16-byte IDs, but we just skip reading them
        // (We can pass a dummy buffer or read them into a throwaway pointer.)
        unsigned char *dummy_series = NULL;
        if (e_hdr.value_len > 0) {
          dummy_series = malloc(e_hdr.value_len);
        }

        if (!dummy_series) {
          // If out of memory, we can either break or continue
          // We'll do a minimal approach
          dummy_series = NULL; // no read
        }

        bool read_ok = timeseries_entity_iterator_read_data(
            &ent_iter, &e_hdr, key_buf, dummy_series ? dummy_series : NULL);

        if (dummy_series) {
          free(dummy_series);
        }
        if (!read_ok) {
          ESP_LOGW(TAG, "Failed reading fieldlist entry data");
          continue;
        }
        key_buf[e_hdr.key_len] = '\0';

        // Now parse out the "meas_id" prefix
        // We'll look for the first ':'
        char *colon_ptr = strchr(key_buf, ':');
        if (!colon_ptr) {
          // Malformed key?
          continue;
        }
        *colon_ptr = '\0';                      // split them
        const char *field_part = colon_ptr + 1; // the portion after ':'

        // Check if the <meas_id> prefix matches our measurement_id
        uint32_t listed_meas_id = (uint32_t)strtoul(key_buf, NULL, 10);
        if (listed_meas_id == measurement_id) {
          // Add field_part to out_fields
          if (tsdb_string_list_append_unique(out_fields, field_part)) {
            found_any = true;
          }
        }
      }
    }
  }

  return found_any;
}

bool tsdb_find_all_series_ids_for_measurement(
    timeseries_db_t *db, uint32_t measurement_id,
    timeseries_series_id_list_t *out_series_list) {
  if (!db || !out_series_list) {
    return false;
  }

  // 1) Find all active METADATA pages
  uint32_t meta_offsets[4];
  size_t meta_count = 0;
  if (!tsdb_find_metadata_pages(db, meta_offsets, 4, &meta_count) ||
      meta_count == 0) {
    ESP_LOGD(TAG, "No active metadata pages => no series to find.");
    return false;
  }

  bool found_any = false;

  // 2) Scan each metadata page for FIELDLISTINDEX entries
  for (size_t p = 0; p < meta_count; p++) {
    uint32_t page_offset = meta_offsets[p];
    uint32_t page_size = TIMESERIES_METADATA_PAGE_SIZE;

    timeseries_entity_iterator_t ent_iter;
    if (!timeseries_entity_iterator_init(db, page_offset, page_size,
                                         &ent_iter)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
      // We only want valid FIELDLISTINDEX entries
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_FIELDLISTINDEX ||
          e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
        continue;
      }

      // The key should look like "1234:someField"
      // Check that it fits in our buffer (just like we do for fields/tags)
      if (e_hdr.key_len == 0 || e_hdr.key_len >= 128) {
        continue; // skip invalid or overly large keys
      }

      // Value must be multiple of 16 (each series ID = 16 bytes)
      if (e_hdr.value_len % 16 != 0) {
        ESP_LOGW(TAG,
                 "Unexpected fieldlist value_len=%" PRIu16
                 " (not multiple of 16).",
                 e_hdr.value_len);
        continue;
      }

      char key_buf[128];
      memset(key_buf, 0, sizeof(key_buf));
      unsigned char *series_ids = malloc(e_hdr.value_len);
      if (!series_ids) {
        ESP_LOGE(TAG, "Out of memory reading fieldlist series IDs");
        continue;
      }

      // Read the key and the array of series IDs
      if (!timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, key_buf,
                                                series_ids)) {
        ESP_LOGW(TAG, "Failed reading fieldlist entry data");
        free(series_ids);
        continue;
      }

      // Null-terminate the key
      key_buf[e_hdr.key_len] = '\0';

      // key_buf has the form "<meas_id>:<field_name>"
      // Let's parse out the meas_id portion before the first colon.
      char *colon_ptr = strchr(key_buf, ':');
      if (!colon_ptr) {
        free(series_ids);
        continue; // malformed key
      }

      *colon_ptr = '\0';                      // split into two strings
      const char *field_part = colon_ptr + 1; // not used for this function

      // Convert the prefix to an integer
      uint32_t listed_meas_id = (uint32_t)strtoul(key_buf, NULL, 10);

      // If the meas_id matches, we want to merge all series IDs
      if (listed_meas_id == measurement_id) {
        found_any = true;
        size_t num_ids = e_hdr.value_len / 16;
        for (size_t i = 0; i < num_ids; i++) {
          unsigned char *sid = series_ids + (i * 16);

          // Optional: check for duplicates so we only add unique
          // series IDs once:
          bool is_duplicate = false;
          for (size_t j = 0; j < out_series_list->count; j++) {
            if (memcmp(out_series_list->ids[j].bytes, sid, 16) == 0) {
              is_duplicate = true;
              break;
            }
          }
          if (!is_duplicate) {
            if (!tsdb_series_id_list_append(out_series_list, sid)) {
              ESP_LOGE(TAG, "Failed appending a series ID (OOM?)");
              free(series_ids);
              return false;
            }
          }
        }
      }

      free(series_ids);
    } // while next entry
  } // for meta_count

  return found_any;
}