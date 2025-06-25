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

#include "timeseries_cache_tagcombo_series.h"
#include "timeseries_data.h"
#include "timeseries_id_list.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_string_list.h"
#include "timeseries_query.h"
#include "timeseries_cache_measurement_id.h"
#include "timeseries_cache_field_names.h"
#include "timeseries_cache_field_series.h"
#include "timeseries_cache_measurement_series.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

static const char* TAG = "TimeseriesMetadata";

#define MEASUREMENT_CACHE_DEFAULT_CAPACITY 32
#define MEASUREMENT_CACHE_MAX_NAME_LEN 63

// -----------------------------------------------------------------------------
// Forward-declared static helpers
// -----------------------------------------------------------------------------

static bool tsdb_create_empty_metadata_page(timeseries_db_t* db, uint32_t page_offset);

static bool tsdb_soft_delete_entry(timeseries_db_t* db, uint32_t page_offset, uint32_t current_entry_offset);

static bool tsdb_create_new_index_entry(timeseries_db_t* db, timeseries_keytype_t key_type, const char* key_str,
                                        const unsigned char* series_ids, size_t series_ids_len);

static bool tsdb_upsert_seriesid_list_entry(timeseries_db_t* db, timeseries_keytype_t target_key_type,
                                            const uint32_t* meta_offsets, size_t meta_count, const char* key_str,
                                            const unsigned char series_id[16]);

static bool tsdb_upsert_tagindex_entry(timeseries_db_t* db, const uint32_t* meta_offsets, size_t meta_count,
                                       const char* key_str, const unsigned char series_id[16]);

static bool tsdb_upsert_fieldlist_entry(timeseries_db_t* db, const uint32_t* meta_offsets, size_t meta_count,
                                        const char* key_str, const unsigned char series_id[16]);

// -----------------------------------------------------------------------------
// Public API Implementations
// -----------------------------------------------------------------------------

static bool tsdb_append_entity_to_metadata(timeseries_db_t* db, uint8_t key_type, const void* key_buf, uint16_t key_len,
                                           const void* val_buf, uint16_t val_len) {
  if (!db || !key_buf) return false;

  const uint32_t entity_size = sizeof(timeseries_entry_header_t) + key_len + val_len;

  /* Two passes max: 0 = scan existing pages, 1 = after creating a new page */
  for (int pass = 0; pass < 2; ++pass) {
    /* ------------------------------------------------ scan pages for space */
    timeseries_metadata_page_iterator_t page_it;
    if (!timeseries_metadata_page_iterator_init(db, &page_it)) return false;

    timeseries_page_header_t hdr;
    uint32_t page_off, page_sz;
    while (timeseries_metadata_page_iterator_next(&page_it, &hdr, &page_off, &page_sz)) {
      /* locate free space at end of page */
      timeseries_entity_iterator_t ent_it;
      if (!timeseries_entity_iterator_init(db, page_off, page_sz, &ent_it)) {
        continue; /* skip broken page           */
      }

      uint32_t free_off = ent_it.offset; /* just after page header     */
      timeseries_entry_header_t eh;
      while (timeseries_entity_iterator_next(&ent_it, &eh)) {
        free_off = ent_it.offset; /* cursor after last entry    */
      }
      timeseries_entity_iterator_deinit(&ent_it);

      uint32_t remaining = page_sz - free_off;
      if (remaining < entity_size) {
        continue; /* not enough room – next pg  */
      }

      /* ----------------------------------- we have room → write the entity */
      uint32_t write_addr = page_off + free_off;

      timeseries_entry_header_t new_hdr = {0};
      new_hdr.delete_marker = TIMESERIES_DELETE_MARKER_VALID;
      new_hdr.key_type = key_type;
      new_hdr.key_len = key_len;
      new_hdr.value_len = val_len;

      esp_err_t err;
      err = esp_partition_write(db->partition, write_addr, &new_hdr, sizeof(new_hdr));
      if (err != ESP_OK) goto write_fail;
      write_addr += sizeof(new_hdr);

      err = esp_partition_write(db->partition, write_addr, key_buf, key_len);
      if (err != ESP_OK) goto write_fail;
      write_addr += key_len;

      if (val_len) {
        err = esp_partition_write(db->partition, write_addr, val_buf, val_len);
        if (err != ESP_OK) goto write_fail;
      }

      return true; /* ✅ success               */

    write_fail:
      ESP_LOGE(TAG, "Partition write failed (err=0x%x)", err);
      return false;
    } /* while pages */

    /* ------------------------------------------------ no space; try to add a page */
    if (pass == 0) {
      ESP_LOGI(TAG, "All metadata pages full – creating a new one…");
      if (!timeseries_metadata_create_page(db)) {
        ESP_LOGE(TAG, "Failed to create a new metadata page");
        return false;
      }
      /* loop again to insert into the freshly created page */
    }
  } /* for pass */

  ESP_LOGE(TAG, "Could not append entity: no space even after new page");
  return false;
}

bool timeseries_metadata_create_page(timeseries_db_t* db) {
  if (!db) {
    return false;
  }

  timeseries_blank_iterator_t blank_iter;
  if (!timeseries_blank_iterator_init(db, &blank_iter, TIMESERIES_METADATA_PAGE_SIZE)) {
    ESP_LOGE(TAG, "Failed to init blank iterator");
    return false;
  }

  uint32_t blank_offset = 0, blank_length = 0;
  bool found_blank = timeseries_blank_iterator_next(&blank_iter, &blank_offset, &blank_length);

  if (!found_blank) {
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

  ESP_LOGV(TAG, "Page cache built => %u total entries", (unsigned)db->page_cache_count);

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

bool tsdb_find_measurement_id(timeseries_db_t* db, const char* measurement_name, uint32_t* out_id) {
  if (!db || !measurement_name || !out_id) {
    return false;
  }

  // Try cache first
  if (tsdb_measurement_cache_lookup(db->meta_cache, measurement_name, out_id)) {
    return true;
  }

  // Cache miss - search on disk
  const size_t wanted_len = strlen(measurement_name);
  bool found = false;
  uint32_t found_id = 0;

  timeseries_metadata_page_iterator_t meta_it;
  if (!timeseries_metadata_page_iterator_init(db, &meta_it)) {
    ESP_LOGW(TAG, "Failed to create metadata-page iterator");
    return false;
  }

  timeseries_page_header_t page_hdr;
  uint32_t page_off, page_sz;
  while (timeseries_metadata_page_iterator_next(&meta_it, &page_hdr, &page_off, &page_sz)) {
    timeseries_entity_iterator_t ent_it;
    if (!timeseries_entity_iterator_init(db, page_off, page_sz, &ent_it)) {
      ESP_LOGW(TAG, "Failed to init entity iterator @0x%08" PRIx32, page_off);
      continue; /* skip this page */
    }

    timeseries_entry_header_t eh;
    while (timeseries_entity_iterator_next(&ent_it, &eh)) {
      if (eh.key_type != TIMESERIES_KEYTYPE_MEASUREMENT || eh.delete_marker != TIMESERIES_DELETE_MARKER_VALID ||
          eh.key_len != wanted_len || eh.key_len >= 64) /* 64-byte key buffer guard */
      {
        continue;
      }

      /* peek the key only */
      char key_buf[64];
      if (!timeseries_entity_iterator_peek_key(&ent_it, &eh, key_buf)) {
        continue;
      }
      key_buf[eh.key_len] = '\0';

      if (strcmp(key_buf, measurement_name) != 0) {
        continue; /* not our measurement */
      }

      /* key matches – read the 32-bit value (measurement ID) */
      uint32_t id_val = 0;
      if (!timeseries_entity_iterator_read_value(&ent_it, &eh, &id_val)) {
        ESP_LOGW(TAG, "Failed reading measurement id value");
        continue;
      }

      found_id = id_val;
      found = true;
      break;
    }

    timeseries_entity_iterator_deinit(&ent_it);
    if (found) break;
  }

  if (found) {
    *out_id = found_id;

    // Cache the result for future lookups
    tsdb_measurement_cache_insert(db->meta_cache, measurement_name, found_id);

    return true;
  }

  return false; /* not found */
}

bool tsdb_create_measurement_id(timeseries_db_t* db, const char* measurement_name, uint32_t* out_id) {
  if (!db || !measurement_name || !out_id) {
    return false;
  }

  /* Allocate the next ID */
  uint32_t new_id = db->next_measurement_id++;
  *out_id = new_id;

  /* Prepare key and value buffers */
  const uint16_t key_len = strlen(measurement_name);
  if (key_len == 0 || key_len >= 128) { /* sanity guard */
    ESP_LOGE(TAG, "Measurement name too long");
    return false;
  }

  /* Use the new helper to append the MEASUREMENT entry */
  bool ok = tsdb_append_entity_to_metadata(db, TIMESERIES_KEYTYPE_MEASUREMENT, measurement_name, key_len, &new_id,
                                           sizeof(new_id));

  if (!ok) {
    ESP_LOGE(TAG, "Failed to insert measurement '%s'", measurement_name);
    return false;
  }

  /* Cache the new measurement */
  tsdb_measurement_cache_insert(db->meta_cache, measurement_name, new_id);

  ESP_LOGV(TAG, "Created measurement '%s' with ID=%" PRIu32, measurement_name, new_id);
  return true;
}

bool tsdb_insert_single_field(timeseries_db_t* db, uint32_t measurement_id, const char* measurement_name,
                              const char* field_name, const timeseries_field_value_t* field_val, const char** tag_keys,
                              const char** tag_values, size_t num_tags, uint64_t timestamp_ms) {
  if (!db || !measurement_name || !field_name || !field_val) {
    return false;
  }

  // 1) Build MD5 input
  char buffer[256];
  memset(buffer, 0, sizeof(buffer));

  size_t offset = 0;
  offset += snprintf(buffer + offset, sizeof(buffer) - offset, "%s", measurement_name);
  for (size_t i = 0; i < num_tags; i++) {
    offset += snprintf(buffer + offset, sizeof(buffer) - offset, ":%s:%s", tag_keys[i], tag_values[i]);
    if (offset >= sizeof(buffer)) {
      ESP_LOGE(TAG, "MD5 input buffer overflow");
      return false;
    }
  }
  offset += snprintf(buffer + offset, sizeof(buffer) - offset, ":%s", field_name);
  if (offset >= sizeof(buffer)) {
    ESP_LOGE(TAG, "MD5 input buffer overflow (field_name too long?)");
    return false;
  }

  ESP_LOGV(TAG, "Series ID calc string: '%s'", buffer);

  // 2) MD5 => series_id
  unsigned char series_id[16];
  mbedtls_md5((const unsigned char*)buffer, strlen(buffer), series_id);

  // 3) Ensure field type in metadata
  if (!tsdb_ensure_series_type_in_metadata(db, series_id, field_val->type)) {
    return false;
  }

  // 4) Index tags
  if (!tsdb_index_tags_for_series(db, measurement_id, tag_keys, tag_values, num_tags, series_id)) {
    return false;
  }

  // 5) Append data to field data page
  if (!tsdb_append_field_data(db, series_id, timestamp_ms, field_val)) {
    return false;
  }

  ESP_LOGV(TAG, "Inserted field='%s' meas_id=%" PRIu32 " ts=%" PRIu64 " (SeriesID=%.2X%.2X%.2X%.2X...)", field_name,
           measurement_id, timestamp_ms, series_id[0], series_id[1], series_id[2], series_id[3]);
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
bool tsdb_lookup_series_type_in_metadata(timeseries_db_t* db, const unsigned char series_id[16],
                                         timeseries_field_type_e* out_type) {
  if (!db || !series_id || !out_type) {
    return false;
  }

  timeseries_metadata_page_iterator_t meta_it;
  if (!timeseries_metadata_page_iterator_init(db, &meta_it)) {
    ESP_LOGE(TAG, "Failed to create metadata-page iterator");
    return false;
  }

  timeseries_page_header_t page_hdr;
  uint32_t page_off, page_sz;
  while (timeseries_metadata_page_iterator_next(&meta_it, &page_hdr, &page_off, &page_sz)) {
    timeseries_entity_iterator_t ent_it;
    if (!timeseries_entity_iterator_init(db, page_off, page_sz, &ent_it)) {
      continue; /* skip page if iterator fails */
    }

    timeseries_entry_header_t eh;
    while (timeseries_entity_iterator_next(&ent_it, &eh)) {
      /* We want FIELDINDEX entries of exactly 16-byte key + 1-byte value */
      if (eh.key_type != TIMESERIES_KEYTYPE_FIELDINDEX || eh.delete_marker != TIMESERIES_DELETE_MARKER_VALID ||
          eh.key_len != 16 || eh.value_len != 1) {
        continue;
      }

      /* Peek the 16-byte key */
      unsigned char key_buf[16];
      if (!timeseries_entity_iterator_peek_key(&ent_it, &eh, key_buf)) {
        continue;
      }

      if (memcmp(key_buf, series_id, 16) != 0) {
        continue; /* not our series            */
      }

      /* Read the single-byte value (field type) */
      uint8_t stored_type = 0;
      if (!timeseries_entity_iterator_read_value(&ent_it, &eh, &stored_type)) {
        ESP_LOGW(TAG, "Failed reading series-type value");
        continue;
      }

      *out_type = (timeseries_field_type_e)stored_type;
      timeseries_entity_iterator_deinit(&ent_it);
      return true; /* success                   */
    }

    timeseries_entity_iterator_deinit(&ent_it);
  }

  return false; /* not found                 */
}

/**
 * @brief Ensure the given series_id has a matching field_type in metadata,
 *        or create a new entry if it doesn't exist. Returns false on conflict.
 */
bool tsdb_ensure_series_type_in_metadata(timeseries_db_t* db, const unsigned char series_id[16],
                                         timeseries_field_type_e field_type) {
  if (!db || !series_id) return false;

  /* 1️⃣  Already present? */
  timeseries_field_type_e existing;
  if (tsdb_lookup_series_type_in_metadata(db, series_id, &existing)) {
    if (existing != field_type) {
      ESP_LOGE(TAG, "Series-ID conflict: stored=%d, requested=%d", (int)existing, (int)field_type);
      return false;
    }
    return true; /* nothing to do */
  }

  /* 2️⃣  Append brand-new FIELDINDEX entry (auto-page select / create) */
  uint8_t type_byte = (uint8_t)field_type;
  if (!tsdb_append_entity_to_metadata(db, TIMESERIES_KEYTYPE_FIELDINDEX, series_id, 16, &type_byte, 1)) {
    ESP_LOGE(TAG, "Failed to create FIELDINDEX for series %.2X%.2X%.2X%.2X…", series_id[0], series_id[1], series_id[2],
             series_id[3]);
    return false;
  }

  ESP_LOGV(TAG, "Created FIELDINDEX for series %.2X%.2X%.2X%.2X… , type=%d", series_id[0], series_id[1], series_id[2],
           series_id[3], (int)field_type);
  return true;
}

bool tsdb_index_tags_for_series(timeseries_db_t* db, uint32_t measurement_id, const char** tag_keys,
                                const char** tag_values, size_t num_tags, const unsigned char series_id[16]) {
  if (!db || !series_id) {
    return false;
  }
  if (num_tags == 0) {
    return true;
  }

  /* ------------------------------------------------------------------ */
  /* 1. Collect *all* active-metadata page offsets                       */
  /* ------------------------------------------------------------------ */
  uint32_t* page_offsets = NULL;
  size_t count = 0;
  size_t cap = 0;

  timeseries_metadata_page_iterator_t meta_it;
  if (!timeseries_metadata_page_iterator_init(db, &meta_it)) {
    ESP_LOGE(TAG, "Failed to create metadata-page iterator");
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t off, sz;
  while (timeseries_metadata_page_iterator_next(&meta_it, &hdr, &off, &sz)) {
    if (count == cap) { /* grow dynamic array           */
      size_t new_cap = (cap == 0) ? 4 : cap * 2;
      uint32_t* nb = realloc(page_offsets, new_cap * sizeof(uint32_t));
      if (!nb) {
        free(page_offsets);
        ESP_LOGE(TAG, "OOM while collecting metadata pages");
        return false;
      }
      page_offsets = nb;
      cap = new_cap;
    }
    page_offsets[count++] = off;
  }

  if (count == 0) {
    ESP_LOGE(TAG, "No active metadata pages for tag indexing");
    free(page_offsets);
    return false;
  }

  /* ------------------------------------------------------------------ */
  /* 2. Upsert one TagIndex entry per tag key/value                      */
  /* ------------------------------------------------------------------ */
  for (size_t i = 0; i < num_tags; ++i) {
    char key_buf[128];
    int n = snprintf(key_buf, sizeof(key_buf), "%" PRIu32 ":%s:%s", measurement_id, tag_keys[i], tag_values[i]);
    if (n < 0 || (size_t)n >= sizeof(key_buf)) {
      ESP_LOGE(TAG, "Tag key buffer overflow for '%s:%s'", tag_keys[i], tag_values[i]);
      free(page_offsets);
      return false;
    }

    if (!tsdb_upsert_tagindex_entry(db, page_offsets, count, key_buf, series_id)) {
      ESP_LOGE(TAG, "Failed to upsert tag '%s'", key_buf);
      free(page_offsets);
      return false;
    }
  }

  free(page_offsets);
  return true;
}

// -----------------------------------------------------------------------------
// Private (static) Helpers
// -----------------------------------------------------------------------------

static bool tsdb_create_empty_metadata_page(timeseries_db_t* db, uint32_t page_offset) {
  if (!db) {
    return false;
  }
  ESP_LOGV(TAG, "Creating empty metadata page at offset=0x%08" PRIx32 "...", page_offset);

  // Erase 8 KB from page_offset
  esp_err_t err = esp_partition_erase_range(db->partition, page_offset, TIMESERIES_METADATA_PAGE_SIZE);
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
  new_hdr.field_data_level = 0;  // not used for metadata
  new_hdr.page_size = TIMESERIES_METADATA_PAGE_SIZE;

  err = esp_partition_write(db->partition, page_offset, &new_hdr, sizeof(new_hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to write new metadata header (err=0x%x)", err);
    return false;
  }

  // Add to page cache
  tsdb_pagecache_add_entry(db, page_offset, &new_hdr);

  ESP_LOGV(TAG, "Empty metadata page created at offset=0x%08" PRIx32 " size = %d", page_offset,
           TIMESERIES_METADATA_PAGE_SIZE);
  return true;
}

static bool tsdb_soft_delete_entry(timeseries_db_t* db, uint32_t page_offset, uint32_t current_entry_offset) {
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

  ESP_LOGD(TAG, "Soft-deleted old tagindex entry at offset=0x%08" PRIx32, entry_addr);
  return true;
}

static bool tsdb_create_new_index_entry(timeseries_db_t* db, timeseries_keytype_t key_type, const char* key_str,
                                        const unsigned char* series_ids, size_t series_ids_len) {
  if (!db || !key_str || !series_ids) return false;

  return tsdb_append_entity_to_metadata(db, (uint8_t)key_type, key_str, (uint16_t)strlen(key_str), series_ids,
                                        (uint16_t)series_ids_len);
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

  /* Build the key "<measurementId>:<fieldName>" */
  char key_buf[128];
  int n = snprintf(key_buf, sizeof(key_buf), "%" PRIu32 ":%s", measurement_id, field_name);
  if (n < 0 || (size_t)n >= sizeof(key_buf)) {
    ESP_LOGE(TAG, "key_buf overflow (field '%s')", field_name);
    return false;
  }

  /* ------------------------------------------------------------ */
  /* Gather *all* active-metadata page offsets                     */
  /* ------------------------------------------------------------ */
  uint32_t* offsets = NULL;
  size_t count = 0;
  size_t cap = 0;

  timeseries_metadata_page_iterator_t meta_it;
  if (!timeseries_metadata_page_iterator_init(db, &meta_it)) {
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t off, size;
  while (timeseries_metadata_page_iterator_next(&meta_it, &hdr, &off, &size)) {
    if (count == cap) { /* grow array */
      size_t new_cap = (cap == 0) ? 4 : cap * 2;
      uint32_t* nb = realloc(offsets, new_cap * sizeof(uint32_t));
      if (!nb) { /* OOM → abort */
        free(offsets);
        return false;
      }
      offsets = nb;
      cap = new_cap;
    }
    offsets[count++] = off;
  }

  if (count == 0) {
    ESP_LOGE(TAG, "No active metadata page found for field index");
    free(offsets);
    return false;
  }

  /* ------------------------------------------------------------ */
  /* Upsert field->seriesId mapping                                */
  /* (tsdb_upsert_fieldlist_entry keeps the old signature)         */
  /* ------------------------------------------------------------ */
  bool ok = tsdb_upsert_fieldlist_entry(db, offsets, count, key_buf, series_id);
  free(offsets);
  return ok;
}

bool tsdb_find_series_ids_for_multiple_tags(timeseries_db_t* db, uint32_t measurement_id, size_t num_tags,
                                            const char** tag_keys, const char** tag_values,
                                            timeseries_series_id_list_t* out_series_list) {
  if (!db || !tag_keys || !tag_values || !out_series_list || num_tags == 0) {
    return false;
  }

  if (db->meta_cache &&
      tsdb_tagcombo_cache_lookup(db->meta_cache, measurement_id, num_tags, tag_keys, tag_values, out_series_list) &&
      out_series_list->count)
    return true; /* cache hit */

  /* Build keys for all tags we're looking for */
  typedef struct {
    char key[128];
    size_t key_len;
    timeseries_series_id_list_t series_ids;
    bool found;
  } tag_search_t;

  tag_search_t* tag_searches = calloc(num_tags, sizeof(tag_search_t));
  if (!tag_searches) {
    return false;
  }

  /* Initialize search structures for each tag */
  for (size_t i = 0; i < num_tags; ++i) {
    int n = snprintf(tag_searches[i].key, sizeof(tag_searches[i].key), "%" PRIu32 ":%s:%s", measurement_id, tag_keys[i],
                     tag_values[i]);
    if (n < 0 || (size_t)n >= sizeof(tag_searches[i].key)) {
      ESP_LOGE(TAG, "key_buf overflow for tag '%s:%s'", tag_keys[i], tag_values[i]);
      goto cleanup;
    }
    tag_searches[i].key_len = strlen(tag_searches[i].key);
    tsdb_series_id_list_init(&tag_searches[i].series_ids);
    tag_searches[i].found = false;
  }

  /* Scratch buffer for value payloads */
  uint8_t* scratch = NULL;
  size_t scratch_cap = 0;

  /* Walk all active metadata pages ONCE */
  timeseries_metadata_page_iterator_t meta_it;
  if (!timeseries_metadata_page_iterator_init(db, &meta_it)) {
    goto cleanup;
  }

  timeseries_page_header_t page_hdr;
  uint32_t page_off, page_sz;
  while (timeseries_metadata_page_iterator_next(&meta_it, &page_hdr, &page_off, &page_sz)) {
    timeseries_entity_iterator_t ent_it;
    if (!timeseries_entity_iterator_init(db, page_off, page_sz, &ent_it)) {
      continue;
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_it, &e_hdr)) {
      /* Filter for TagIndex entries */
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_TAGINDEX || e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID ||
          e_hdr.value_len % 16u) {
        continue;
      }

      /* Check if this entry matches any of our tags */
      int matching_tag = -1;
      for (size_t i = 0; i < num_tags; ++i) {
        if (e_hdr.key_len == tag_searches[i].key_len) {
          matching_tag = i;
          break;
        }
      }

      if (matching_tag == -1) {
        continue; /* Key length doesn't match any tag */
      }

      /* Read key and value */
      char stored_key[128];
      if (e_hdr.key_len >= sizeof(stored_key)) continue;

      if (e_hdr.value_len > scratch_cap) {
        uint8_t* nb = realloc(scratch, e_hdr.value_len);
        if (!nb) {
          free(scratch);
          scratch = NULL;
          timeseries_entity_iterator_deinit(&ent_it);
          goto cleanup;
        }
        scratch = nb;
        scratch_cap = e_hdr.value_len;
      }

      if (!timeseries_entity_iterator_read_data(&ent_it, &e_hdr, stored_key, scratch)) {
        continue;
      }
      stored_key[e_hdr.key_len] = '\0';

      /* Check all tags with matching length */
      for (size_t i = 0; i < num_tags; ++i) {
        if (e_hdr.key_len == tag_searches[i].key_len && strcmp(stored_key, tag_searches[i].key) == 0) {
          /* Found a match! Append series IDs */
          size_t num_ids = e_hdr.value_len / 16u;
          for (size_t j = 0; j < num_ids; ++j) {
            if (!tsdb_series_id_list_append(&tag_searches[i].series_ids, scratch + j * 16u)) {
              ESP_LOGE(TAG, "OOM while appending series-id");
              free(scratch);
              timeseries_entity_iterator_deinit(&ent_it);
              goto cleanup;
            }
          }
          tag_searches[i].found = true;
          break; /* No need to check other tags */
        }
      }
    }
    timeseries_entity_iterator_deinit(&ent_it);
  }

  free(scratch);
  scratch = NULL;

  /* Now perform intersection of all found series IDs */
  bool all_tags_found = true;
  for (size_t i = 0; i < num_tags; ++i) {
    if (!tag_searches[i].found) {
      all_tags_found = false;
      break;
    }
  }

  if (!all_tags_found) {
    /* At least one tag had no matches, so intersection is empty */
    goto cleanup;
  }

  /* Start with the first tag's results */
  tsdb_series_id_list_copy(out_series_list, &tag_searches[0].series_ids);

  /* Intersect with remaining tags */
  for (size_t i = 1; i < num_tags; ++i) {
    intersect_series_id_lists(out_series_list, &tag_searches[i].series_ids);
  }

  tsdb_tagcombo_cache_insert(db->meta_cache, measurement_id, num_tags, tag_keys, tag_values, out_series_list);

cleanup:
  if (tag_searches) {
    for (size_t i = 0; i < num_tags; ++i) {
      tsdb_series_id_list_free(&tag_searches[i].series_ids);
    }
    free(tag_searches);
  }

  return true;
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
        unsigned char* all_series_ids = malloc(e_hdr.value_len);
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

          // If not present, append
          if (!already_present) {
            size_t new_size = (series_count + 1) * 16;
            unsigned char* new_ids = malloc(new_size);
            if (!new_ids) {
              free(all_series_ids);
              timeseries_entity_iterator_deinit(&ent_iter);
              return false;
            }
            memcpy(new_ids, all_series_ids, series_count * 16);
            memcpy(new_ids + series_count * 16, series_id, 16);

            // Create new entry with new_size
            if (!tsdb_create_new_index_entry(db, target_key_type, key_str, new_ids, new_size)) {
              free(new_ids);
              free(all_series_ids);
              timeseries_entity_iterator_deinit(&ent_iter);

              return false;
            }
            free(new_ids);

            // Soft-delete the old entry
            if (!tsdb_soft_delete_entry(db, page_offset, ent_iter.current_entry_offset)) {
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

  // If we never found an existing entry, create a brand-new one in the first
  // page
  if (!found_existing && meta_count > 0) {
    if (!tsdb_create_new_index_entry(db, target_key_type, key_str, series_id, 16)) {
      return false;
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
 * Collect series-id lists for *many* fields in a single metadata pass.
 *
 * @param db               Database handle
 * @param measurement_id   Numeric measurement id
 * @param wanted_fields    List of field names we are interested in
 * @param out_arrays       Parallel array, one element per `wanted_fields`
 *                         PRE-INITIALISED with tsdb_series_id_list_init().
 * @return true            At least one match found for any field
 */
bool tsdb_find_series_ids_for_fields(timeseries_db_t* db, uint32_t measurement_id,
                                     const timeseries_string_list_t* wanted_fields,
                                     timeseries_series_id_list_t* out_arrays) {
  if (!db || !wanted_fields || !out_arrays || wanted_fields->count == 0) return false;

  /* ------------------------------------------------------------------ */
  /* 0)  FAST-PATH – try cache for every field                          */
  /* ------------------------------------------------------------------ */
  bool need_scan = false; /* at least one miss? */
  for (size_t i = 0; i < wanted_fields->count; ++i) {
    if (!tsdb_fieldseries_cache_lookup(db->meta_cache, measurement_id, wanted_fields->items[i], &out_arrays[i]))
      need_scan = true;
  }
  if (!need_scan) { /* all were hits */
    for (size_t i = 0; i < wanted_fields->count; ++i)
      if (out_arrays[i].count) return true;
    return false; /* all empty */
  }

  /* ------------------------------------------------------------------ */
  /* 1)  SLOW-PATH – metadata scan (unchanged core logic)               */
  /* ------------------------------------------------------------------ */
  uint8_t* scratch = NULL;
  size_t scratch_cap = 0;
  bool found_any = false;

  timeseries_metadata_page_iterator_t meta_it;
  if (!timeseries_metadata_page_iterator_init(db, &meta_it)) return false;

  timeseries_page_header_t page_hdr;
  uint32_t page_off, page_sz;
  while (timeseries_metadata_page_iterator_next(&meta_it, &page_hdr, &page_off, &page_sz)) {
    timeseries_entity_iterator_t ent_it;
    if (!timeseries_entity_iterator_init(db, page_off, page_sz, &ent_it)) continue;

    timeseries_entry_header_t eh;
    while (timeseries_entity_iterator_next(&ent_it, &eh)) {
      if (eh.key_type != TIMESERIES_KEYTYPE_FIELDLISTINDEX || eh.delete_marker != TIMESERIES_DELETE_MARKER_VALID ||
          eh.value_len % 16u)
        continue;

      if (eh.key_len >= 128) continue;

      char key_buf[128];
      if (!timeseries_entity_iterator_peek_key(&ent_it, &eh, key_buf)) continue;
      key_buf[eh.key_len] = '\0';

      char* colon = strchr(key_buf, ':');
      if (!colon) continue;
      *colon = '\0';
      if ((uint32_t)strtoul(key_buf, NULL, 10) != measurement_id) continue;
      const char* field_name = colon + 1;

      /* Which wanted field is this? */
      size_t idx = SIZE_MAX;
      for (size_t i = 0; i < wanted_fields->count; ++i)
        if (out_arrays[i].count == 0 && /* skip already from cache */
            strcmp(wanted_fields->items[i], field_name) == 0) {
          idx = i;
          break;
        }
      if (idx == SIZE_MAX) continue;

      /* read value */
      if (eh.value_len > scratch_cap) {
        uint8_t* nb = realloc(scratch, eh.value_len);
        if (!nb) {
          scratch = NULL;
          scratch_cap = 0;
          break;
        }
        scratch = nb;
        scratch_cap = eh.value_len;
      }
      if (!timeseries_entity_iterator_read_value(&ent_it, &eh, scratch)) continue;

      size_t n_ids = eh.value_len / 16u;
      for (size_t k = 0; k < n_ids; ++k) tsdb_series_id_list_append(&out_arrays[idx], scratch + k * 16u);

      found_any = true;
    }
    timeseries_entity_iterator_deinit(&ent_it);
  }
  free(scratch);

  /* ------------------------------------------------------------------ */
  /* 2)  Store freshly gathered lists into cache                        */
  /* ------------------------------------------------------------------ */
  for (size_t i = 0; i < wanted_fields->count; ++i)
    if (out_arrays[i].count) /* skip empty */
      tsdb_fieldseries_cache_insert(db->meta_cache, measurement_id, wanted_fields->items[i], &out_arrays[i]);

  return found_any;
}

bool tsdb_list_all_measurements(timeseries_db_t* db, timeseries_string_list_t* out_measurements) {
  if (!db || !out_measurements) {
    return false;
  }

  /* -------------------------------------------------------------------- */
  /* 1. Iterate all ACTIVE-METADATA pages                                 */
  /* -------------------------------------------------------------------- */
  timeseries_metadata_page_iterator_t meta_it;
  if (!timeseries_metadata_page_iterator_init(db, &meta_it)) {
    ESP_LOGW(TAG, "Failed to create metadata-page iterator");
    return false;
  }

  bool found_any = false;

  timeseries_page_header_t page_hdr;
  uint32_t page_off, page_size;
  while (timeseries_metadata_page_iterator_next(&meta_it, &page_hdr, &page_off, &page_size)) {
    /* ------------------------------------------------------------------ */
    /* 2. Scan individual entities in that page                           */
    /* ------------------------------------------------------------------ */
    timeseries_entity_iterator_t ent_it;
    if (!timeseries_entity_iterator_init(db, page_off, page_size, &ent_it)) {
      continue; /* skip page if iterator fails       */
    }

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_it, &e_hdr)) {
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_MEASUREMENT || e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID ||
          e_hdr.key_len == 0 || e_hdr.key_len >= 256) {
        continue; /* not a valid measurement entry     */
      }

      char name_buf[256] = {0};
      uint32_t measurement_id; /* value part (ignored here)         */

      if (!timeseries_entity_iterator_read_data(&ent_it, &e_hdr, name_buf, &measurement_id)) {
        ESP_LOGW(TAG, "Failed reading measurement entry @0x%08" PRIx32, page_off + ent_it.current_entry_offset);
        continue;
      }
      name_buf[e_hdr.key_len] = '\0';

      if (tsdb_string_list_append_unique(out_measurements, name_buf)) {
        found_any = true;
      }
    }
    timeseries_entity_iterator_deinit(&ent_it);
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
bool tsdb_list_fields_for_measurement(timeseries_db_t* db, uint32_t measurement_id,
                                      timeseries_string_list_t* out_fields) {
  if (!db || !out_fields) return false;

  /* -------------------------------------------------------------- */
  /* 0)  FAST-PATH – try shared cache                               */
  /* -------------------------------------------------------------- */
  if (db->meta_cache) {
    timeseries_string_list_t cached;
    tsdb_string_list_init(&cached);

    if (tsdb_fieldnames_cache_lookup(db->meta_cache, measurement_id, &cached)) /* hit?            */
    {
      /* merge into caller-supplied list (unique) */
      for (size_t i = 0; i < cached.count; ++i) tsdb_string_list_append_unique(out_fields, cached.items[i]);

      bool hit_non_empty = (cached.count > 0);
      tsdb_string_list_free(&cached);
      return hit_non_empty;
    }

    tsdb_string_list_free(&cached); /* miss – free scratch list */
  }

  /* -------------------------------------------------------------- */
  /* 1-N)  SLOW-PATH – original metadata scan                       */
  /* -------------------------------------------------------------- */
  timeseries_metadata_page_iterator_t meta_it;
  if (!timeseries_metadata_page_iterator_init(db, &meta_it)) {
    ESP_LOGW(TAG, "Failed to create metadata-page iterator");
    return false;
  }

  timeseries_string_list_t collected;
  tsdb_string_list_init(&collected);

  timeseries_page_header_t page_hdr;
  uint32_t page_off, page_sz;
  while (timeseries_metadata_page_iterator_next(&meta_it, &page_hdr, &page_off, &page_sz)) {
    timeseries_entity_iterator_t ent_it;
    if (!timeseries_entity_iterator_init(db, page_off, page_sz, &ent_it)) continue; /* skip this page on error */

    timeseries_entry_header_t e_hdr;
    while (timeseries_entity_iterator_next(&ent_it, &e_hdr)) {
      if (e_hdr.key_type != TIMESERIES_KEYTYPE_FIELDLISTINDEX ||
          e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID || e_hdr.key_len == 0 || e_hdr.key_len >= 128)
        continue;

      char key_buf[128];
      if (!timeseries_entity_iterator_peek_key(&ent_it, &e_hdr, key_buf)) continue;
      key_buf[e_hdr.key_len] = '\0';

      char* colon = strchr(key_buf, ':');
      if (!colon) continue; /* malformed */

      *colon = '\0';
      if ((uint32_t)strtoul(key_buf, NULL, 10) != measurement_id) continue; /* other measurement */

      const char* field_name = colon + 1;
      tsdb_string_list_append_unique(&collected, field_name);
      tsdb_string_list_append_unique(out_fields, field_name);
    }
    timeseries_entity_iterator_deinit(&ent_it);
  }

  bool found_any = collected.count > 0;

  /* -------------------------------------------------------------- */
  /* 2)  Store into cache for next call                             */
  /* -------------------------------------------------------------- */
  if (found_any && db->meta_cache) {
    tsdb_fieldnames_cache_insert(db->meta_cache, measurement_id, &collected);
  }

  tsdb_string_list_free(&collected);
  return found_any;
}

bool tsdb_find_all_series_ids_for_measurement(timeseries_db_t* db, uint32_t measurement_id,
                                              timeseries_series_id_list_t* out_series_list) {
  if (!db || !out_series_list) return false;

  /* -------------------------------------------------------------- */
  /* 0)  FAST PATH – check cache                                    */
  /* -------------------------------------------------------------- */
  if (db->meta_cache && tsdb_measser_cache_lookup(db->meta_cache, measurement_id, out_series_list) &&
      out_series_list->count) /* non-empty hit            */
    return true;

  /* -------------------------------------------------------------- */
  /* 1)  SLOW PATH – original metadata scan                         */
  /* -------------------------------------------------------------- */
  uint8_t* scratch = NULL;
  size_t scratch_cap = 0;
  bool found_any = false;

  timeseries_metadata_page_iterator_t meta_it;
  if (!timeseries_metadata_page_iterator_init(db, &meta_it)) return false;

  timeseries_page_header_t page_hdr;
  uint32_t page_off, page_sz;

  while (timeseries_metadata_page_iterator_next(&meta_it, &page_hdr, &page_off, &page_sz)) {
    timeseries_entity_iterator_t ent_it;
    if (!timeseries_entity_iterator_init(db, page_off, page_sz, &ent_it)) continue;

    timeseries_entry_header_t eh;
    while (timeseries_entity_iterator_next(&ent_it, &eh)) {
      if (eh.key_type != TIMESERIES_KEYTYPE_FIELDLISTINDEX || eh.delete_marker != TIMESERIES_DELETE_MARKER_VALID ||
          eh.value_len % 16u || eh.key_len == 0 || eh.key_len >= 128)
        continue;

      char key_buf[128];
      if (!timeseries_entity_iterator_peek_key(&ent_it, &eh, key_buf)) continue;
      key_buf[eh.key_len] = '\0';

      char* colon = strchr(key_buf, ':');
      if (!colon) continue;
      *colon = '\0';
      if ((uint32_t)strtoul(key_buf, NULL, 10) != measurement_id) continue;

      /* ensure scratch buffer */
      if (eh.value_len > scratch_cap) {
        uint8_t* nb = realloc(scratch, eh.value_len);
        if (!nb) {
          scratch = NULL;
          scratch_cap = 0;
          break;
        }
        scratch = nb;
        scratch_cap = eh.value_len;
      }
      if (!timeseries_entity_iterator_read_value(&ent_it, &eh, scratch)) continue;

      size_t n = eh.value_len / 16u;
      for (size_t i = 0; i < n; ++i) {
        if (tsdb_series_id_list_append(out_series_list, scratch + i * 16u))
          found_any = true; /* append() already deduplicates */
      }
    }
    timeseries_entity_iterator_deinit(&ent_it);
  }
  free(scratch);

  /* -------------------------------------------------------------- */
  /* 2)  Store in cache for next query                              */
  /* -------------------------------------------------------------- */
  if (found_any && db->meta_cache) tsdb_measser_cache_insert(db->meta_cache, measurement_id, out_series_list);

  return found_any;
}

/**
 * @brief Determine the next unused measurement-ID.
 *
 * Walks every ACTIVE METADATA page, inspects each MEASUREMENT entry,
 * and returns (max_id + 1).  IDs start at 1, so if no entry exists yet
 * the function returns 1.
 *
 * @param[in]  db       Database context
 * @param[out] out_id   Next available measurement-ID
 * @return true on success, false on iterator/IO error
 */
bool tsdb_get_next_available_measurement_id(timeseries_db_t* db, uint32_t* out_id) {
  if (!db || !out_id) {
    return false;
  }

  uint32_t max_id = 0;

  /* --- iterate every ACTIVE-METADATA page ------------------------- */
  timeseries_metadata_page_iterator_t meta_it;
  if (!timeseries_metadata_page_iterator_init(db, &meta_it)) {
    return false;
  }

  timeseries_page_header_t page_hdr;
  uint32_t page_off, page_sz;

  while (timeseries_metadata_page_iterator_next(&meta_it, &page_hdr, &page_off, &page_sz)) {
    /* --- iterate every entity in the page ----------------------- */
    timeseries_entity_iterator_t ent_it;
    if (!timeseries_entity_iterator_init(db, page_off, page_sz, &ent_it)) {
      continue; /* skip broken page           */
    }

    timeseries_entry_header_t eh;
    while (timeseries_entity_iterator_next(&ent_it, &eh)) {
      if (eh.key_type != TIMESERIES_KEYTYPE_MEASUREMENT || eh.delete_marker != TIMESERIES_DELETE_MARKER_VALID ||
          eh.value_len != sizeof(uint32_t)) {
        continue; /* not a valid measurement    */
      }

      uint32_t id_val = 0;
      if (!timeseries_entity_iterator_read_value(&ent_it, &eh, &id_val)) {
        continue; /* read failed – ignore entry */
      }

      if (id_val > max_id) {
        max_id = id_val;
      }
    }

    timeseries_entity_iterator_deinit(&ent_it);
  }

  *out_id = max_id + 1u; /* 1-based sequence           */
  return true;
}

bool timeseries_metadata_get_tags_for_measurement(timeseries_db_t* db, uint32_t measurement_id, tsdb_tag_pair_t** tags,
                                                  size_t* num_tags) {
  if (!db) {
    return false;
  }
  if (!tags || !num_tags) {
    ESP_LOGE(TAG, "Invalid parameters for getting tags.");
    return false;
  }

  /* reset outputs early so callers never see garbage */
  *tags = NULL;
  *num_tags = 0;

  tsdb_tag_pair_t* pairs = NULL;
  size_t count = 0;
  size_t cap = 0;

  /* helper λ: grow array  */
#define ENSURE_CAPACITY(req)                                          \
  do {                                                                \
    if ((req) > cap) {                                                \
      size_t new_cap = (cap == 0) ? 8 : cap * 2;                      \
      while (new_cap < (req)) new_cap *= 2;                           \
      tsdb_tag_pair_t* nb = realloc(pairs, new_cap * sizeof(*pairs)); \
      if (!nb) {                                                      \
        ESP_LOGE(TAG, "OOM while collecting tags");                   \
        goto oom_cleanup;                                             \
      }                                                               \
      pairs = nb;                                                     \
      cap = new_cap;                                                  \
    }                                                                 \
  } while (0)

  timeseries_metadata_page_iterator_t page_it;
  if (!timeseries_metadata_page_iterator_init(db, &page_it)) {
    ESP_LOGE(TAG, "Failed to create metadata-page iterator");
    return false;
  }

  timeseries_page_header_t page_hdr;
  uint32_t page_off, page_sz;

  while (timeseries_metadata_page_iterator_next(&page_it, &page_hdr, &page_off, &page_sz)) {
    timeseries_entity_iterator_t ent_it;
    if (!timeseries_entity_iterator_init(db, page_off, page_sz, &ent_it)) continue; /* skip damaged page */

    timeseries_entry_header_t eh;
    while (timeseries_entity_iterator_next(&ent_it, &eh)) {
      /* accept only valid TAGINDEX entries ------------------ */
      if (eh.key_type != TIMESERIES_KEYTYPE_TAGINDEX || eh.delete_marker != TIMESERIES_DELETE_MARKER_VALID ||
          eh.key_len == 0 || eh.key_len >= 128)
        continue;

      char key_buf[128];
      if (!timeseries_entity_iterator_peek_key(&ent_it, &eh, key_buf)) continue;
      key_buf[eh.key_len] = '\0';

      /* key format: "<meas_id>:<tag_key>:<tag_val>" ---------- */
      char* p1 = strchr(key_buf, ':'); /* 1st colon */
      if (!p1) continue;
      *p1 = '\0';
      if (strtoul(key_buf, NULL, 10) != measurement_id) continue; /* other meas */

      char* p2 = strchr(++p1, ':'); /* 2nd colon */
      if (!p2) continue;
      *p2 = '\0';

      const char* tag_key = p1;
      const char* tag_val = p2 + 1;

      /* duplicate check (linear scan – datasets are small)   */
      bool already = false;
      for (size_t i = 0; i < count; ++i)
        if (strcmp(pairs[i].key, tag_key) == 0 && strcmp(pairs[i].val, tag_val) == 0) {
          already = true;
          break;
        }
      if (already) continue;

      /* append unique pair ---------------------------------- */
      ENSURE_CAPACITY(count + 1);
      pairs[count].key = strdup(tag_key);
      pairs[count].val = strdup(tag_val);
      if (!pairs[count].key || !pairs[count].val) {
        ESP_LOGE(TAG, "OOM duplicating tag strings");
        goto oom_cleanup;
      }
      ++count;
    }
    timeseries_entity_iterator_deinit(&ent_it);
  }

  /* ------------------------------- success */
  *tags = (count > 0) ? pairs : NULL;
  *num_tags = count;
  return true;

  /* ------------------------------- error / OOM */
oom_cleanup:
  for (size_t i = 0; i < count; ++i) {
    free(pairs[i].key);
    free(pairs[i].val);
  }
  free(pairs);
  /* outputs already set to safe values at function entry */
  return false;

#undef ENSURE_CAPACITY
}