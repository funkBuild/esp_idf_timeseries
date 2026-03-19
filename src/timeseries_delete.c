#include "esp_timer.h"
#include "timeseries_id_list.h"
#include "timeseries_flash_utils.h"
#include "timeseries_internal.h"
#include "timeseries_iterator.h"
#include "timeseries_metadata.h"
#include "timeseries_string_list.h"

#include "esp_log.h"

#include <stddef.h>

static const char* TAG = "TimeseriesDelete";

static bool tsdb_mark_series_deleted_in_page(timeseries_db_t* db, uint32_t page_offset, uint32_t page_size,
                                             timeseries_series_id_list_t* series_to_delete, size_t* deleted_count) {
  timeseries_fielddata_iterator_t fdata_iter;
  if (!timeseries_fielddata_iterator_init(db, page_offset, page_size, &fdata_iter)) {
    ESP_LOGW(TAG, "Failed to init fielddata_iterator on page @0x%08X", (unsigned int)page_offset);
    return false;
  }

  timeseries_field_data_header_t fdh;
  while (timeseries_fielddata_iterator_next(&fdata_iter, &fdh)) {
    // Skip if already flagged as deleted
    if ((fdh.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
      ESP_LOGV(TAG, "Skipping already deleted record @0x%08X",
               (unsigned int)(page_offset + fdata_iter.current_record_offset));
      continue;
    }

    // Check if this series_id is in the delete list
    bool found = false;
    for (size_t i = 0; i < series_to_delete->count; i++) {
      if (memcmp(fdh.series_id, series_to_delete->ids[i].bytes, sizeof(fdh.series_id)) == 0) {
        found = true;
        break;
      }
    }
    if (!found) {
      continue;  // Not in delete list, skip
    }

    // Mark this record as deleted by clearing the deleted flag
    uint8_t new_flags = fdh.flags & ~TSDB_FIELDDATA_FLAG_DELETED;

    // Calculate the absolute offset for the header
    uint32_t header_offset = page_offset + fdata_iter.current_record_offset;

    esp_err_t err = tsdb_flash_write_byte(db->partition,
        header_offset + offsetof(timeseries_field_data_header_t, flags),
        new_flags);
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed to write updated header at offset 0x%08X: %s", (unsigned int)header_offset,
               esp_err_to_name(err));
      // Continue with other deletions despite this error
      continue;
    }

    (*deleted_count)++;
  }

  timeseries_fielddata_iterator_deinit(&fdata_iter);
  return true;
}

static bool tsdb_mark_metadata_deleted_in_page(timeseries_db_t* db, uint32_t page_offset, uint32_t page_size,
                                               timeseries_series_id_list_t* series_to_delete) {
  timeseries_entity_iterator_t ent_iter;
  if (!timeseries_entity_iterator_init(db, page_offset, page_size, &ent_iter)) {
    ESP_LOGW(TAG, "Failed to init entity_iterator on metadata page @0x%08X", (unsigned int)page_offset);
    return false;
  }

  bool success = true;
  timeseries_entry_header_t e_hdr;

  while (timeseries_entity_iterator_next(&ent_iter, &e_hdr)) {
    // Skip already-deleted entries
    if (e_hdr.delete_marker != TIMESERIES_DELETE_MARKER_VALID) {
      continue;
    }

    bool should_delete = false;

    if (e_hdr.key_type == TIMESERIES_KEYTYPE_FIELDINDEX) {
      // FIELDINDEX: key is a 16-byte series_id, value is 1-byte field type
      if (e_hdr.key_len == 16 && e_hdr.value_len == 1) {
        unsigned char key_buf[16];
        uint8_t dummy_val = 0;
        if (timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, key_buf, &dummy_val)) {
          for (size_t i = 0; i < series_to_delete->count; i++) {
            if (memcmp(key_buf, series_to_delete->ids[i].bytes, 16) == 0) {
              should_delete = true;
              break;
            }
          }
        }
      }
    } else if (e_hdr.key_type == TIMESERIES_KEYTYPE_FIELDLISTINDEX ||
               e_hdr.key_type == TIMESERIES_KEYTYPE_TAGINDEX) {
      // FIELDLISTINDEX / TAGINDEX: value is an array of 16-byte series_ids
      if (e_hdr.value_len >= 16 && (e_hdr.value_len % 16 == 0)) {
        unsigned char* series_ids = malloc(e_hdr.value_len);
        if (!series_ids) {
          ESP_LOGE(TAG, "OOM reading metadata entry value");
          success = false;
          break;
        }
        char key_buf[128];
        if (e_hdr.key_len >= sizeof(key_buf)) { free(series_ids); continue; }
        if (timeseries_entity_iterator_read_data(&ent_iter, &e_hdr, key_buf, series_ids)) {
          size_t num_ids = e_hdr.value_len / 16;
          for (size_t s = 0; s < num_ids && !should_delete; s++) {
            for (size_t d = 0; d < series_to_delete->count; d++) {
              if (memcmp(series_ids + s * 16, series_to_delete->ids[d].bytes, 16) == 0) {
                should_delete = true;
                break;
              }
            }
          }
        }
        free(series_ids);
      }
    }

    if (should_delete) {
      // NOR-flash soft-delete: clear the delete_marker byte to 0x00
      uint32_t marker_addr = page_offset + ent_iter.current_entry_offset +
                             offsetof(timeseries_entry_header_t, delete_marker);
      // Use tsdb_flash_write_byte for proper 4-byte-aligned read-modify-write
      // (esp_partition_write requires 4-byte aligned size)
      esp_err_t err = tsdb_flash_write_byte(db->partition, marker_addr,
                                             TIMESERIES_DELETE_MARKER_DELETED);
      if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed to soft-delete metadata entry @0x%08X: %s",
                 (unsigned int)marker_addr, esp_err_to_name(err));
        success = false;
      }
    }
  }

  timeseries_entity_iterator_deinit(&ent_iter);
  return success;
}

bool tsdb_series_delete(timeseries_db_t* db, timeseries_series_id_list_t* series_to_delete) {
  if (!db || !series_to_delete) {
    ESP_LOGE(TAG, "Invalid parameters for series delete");
    return false;
  }

  if (series_to_delete->count == 0) {
    ESP_LOGW(TAG, "No series to delete");
    return true;
  }

  // Initialize the result structure
  int64_t start_time = esp_timer_get_time();
  size_t deleted_count = 0;

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    ESP_LOGE(TAG, "Failed to init page iterator");
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0;
  uint32_t page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset, &page_size)) {
    // Only process FIELD_DATA pages
    if (hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA) {
      tsdb_mark_series_deleted_in_page(db, page_offset, page_size, series_to_delete, &deleted_count);
    } else if (hdr.page_type == TIMESERIES_PAGE_TYPE_METADATA) {
      tsdb_mark_metadata_deleted_in_page(db, page_offset, page_size, series_to_delete);
    }
  }
  timeseries_page_cache_iterator_deinit(&page_iter);

  int64_t end_time = esp_timer_get_time();
  ESP_LOGI(TAG, "Deleted %zu series records in %.3f ms", deleted_count, (end_time - start_time) / 1000.0);

  return true;
}

/**
 * Delete all data for a specific measurement
 *
 * @param db The timeseries database
 * @param measurement_name The name of the measurement to delete
 * @return true on success, false on failure
 */

bool timeseries_delete_by_measurement(timeseries_db_t* db, const char* measurement_name) {
  if (!db || !measurement_name) {
    ESP_LOGE(TAG, "Invalid parameters for delete by measurement");
    return false;
  }

  bool ok = false;
  timeseries_series_id_list_t series_to_delete;
  tsdb_series_id_list_init(&series_to_delete);

  /* -------------------------------------------------------------------- */
  /*  1. Resolve measurement                                              */
  /* -------------------------------------------------------------------- */
  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(db, measurement_name, &measurement_id)) {
    ESP_LOGW(TAG, "Measurement '%s' not found - nothing to delete", measurement_name);
    ok = true; /* nothing to delete is not an error */
    goto cleanup;
  }

  /* -------------------------------------------------------------------- */
  /*  2. Find all series IDs for this measurement                         */
  /* -------------------------------------------------------------------- */
  if (!tsdb_find_all_series_ids_for_measurement(db, measurement_id, &series_to_delete)) {
    ESP_LOGE(TAG, "Failed to find series IDs for measurement '%s'", measurement_name);
    goto cleanup;
  }

  if (series_to_delete.count == 0) {
    ESP_LOGW(TAG, "No series found for measurement '%s' - nothing to delete", measurement_name);
    ok = true;
    goto cleanup;
  }

  ESP_LOGI(TAG, "Found %zu series to delete for measurement '%s'", series_to_delete.count, measurement_name);

  /* -------------------------------------------------------------------- */
  /*  3. TODO: Delete the series data and metadata                        */
  /* -------------------------------------------------------------------- */
  // This is where you would implement the actual deletion logic:
  // - Delete all data points for each series ID
  // - Remove series metadata entries
  // - Remove measurement metadata if no series remain
  // - Update indexes

  ok = tsdb_series_delete(db, &series_to_delete);

  if (!ok) {
    ESP_LOGE(TAG, "Failed to delete series data for measurement '%s'", measurement_name);
    goto cleanup;
  }

  ok = tsdb_remove_measurement_from_metadata(db, measurement_name);
  if (!ok) {
    ESP_LOGE(TAG, "Failed to remove measurement '%s' from metadata", measurement_name);
    goto cleanup;
  }

cleanup:
  tsdb_series_id_list_free(&series_to_delete);

  return ok;
}

/**
 * Delete all data for a specific field within a measurement
 *
 * @param db The timeseries database
 * @param measurement_name The name of the measurement
 * @param field_name The name of the field to delete
 * @return true on success, false on failure
 */
bool timeseries_delete_by_measurement_and_field(timeseries_db_t* db, const char* measurement_name,
                                                const char* field_name) {
  if (!db || !measurement_name || !field_name) {
    ESP_LOGE(TAG, "Invalid parameters for delete by measurement and field");
    return false;
  }

  bool ok = false;
  timeseries_series_id_list_t series_to_delete;
  tsdb_series_id_list_init(&series_to_delete);

  /* 1. Resolve measurement */
  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(db, measurement_name, &measurement_id)) {
    ESP_LOGW(TAG, "Measurement '%s' not found - nothing to delete", measurement_name);
    ok = true;
    goto cleanup;
  }

  /* 2. Find series IDs for this specific field */
  if (!tsdb_find_series_ids_for_field(db, measurement_id, field_name, &series_to_delete)) {
    ESP_LOGW(TAG, "No series found for field '%s' in measurement '%s' - nothing to delete",
             field_name, measurement_name);
    ok = true;
    goto cleanup;
  }

  if (series_to_delete.count == 0) {
    ESP_LOGW(TAG, "No series found for field '%s' in measurement '%s' - nothing to delete",
             field_name, measurement_name);
    ok = true;
    goto cleanup;
  }

  ESP_LOGI(TAG, "Found %zu series to delete for field '%s' in measurement '%s'",
           series_to_delete.count, field_name, measurement_name);

  /* 3. Delete the series data */
  ok = tsdb_series_delete(db, &series_to_delete);
  if (!ok) {
    ESP_LOGE(TAG, "Failed to delete series data for field '%s' in measurement '%s'",
             field_name, measurement_name);
    goto cleanup;
  }

  /* 4. Soft-delete the FIELDLISTINDEX entry from metadata */
  if (!tsdb_soft_delete_fieldlistindex_entry(db, measurement_id, field_name)) {
    ESP_LOGW(TAG, "Failed to remove FIELDLISTINDEX entry for field '%s' in measurement '%s' "
             "(will be rebuilt on demand)", field_name, measurement_name);
  }

cleanup:
  tsdb_series_id_list_free(&series_to_delete);
  return ok;
}