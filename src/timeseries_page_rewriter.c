#include "timeseries_page_rewriter.h"

#include "esp_log.h"
#include "esp_partition.h"
#include "timeseries_iterator.h" // Ensure the blank iterator definitions are available
#include "timeseries_page_cache.h"
#include <stdlib.h>
#include <string.h>

static const char *TAG = "PAGE_REWRITER";

/**
 * Internal helper: Round up x to the next multiple of 4K.
 */
static uint32_t round_up_4k(uint32_t x) {
  const uint32_t align = 4096;
  return ((x + align - 1) / align) * align;
}

/**
 * Internal helper: Write a new page header placeholder at base_offset.
 */
static bool write_initial_page_header(timeseries_page_rewriter_t *rewriter) {
  timeseries_page_header_t hdr;
  memset(&hdr, 0xFF, sizeof(hdr));
  hdr.magic_number = TIMESERIES_MAGIC_NUM;
  hdr.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
  hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  hdr.sequence_num = ++(rewriter->db->sequence_num);
  hdr.field_data_level = rewriter->level;
  // The page_size will be updated upon finalization.
  if (esp_partition_write(rewriter->db->partition, rewriter->base_offset, &hdr,
                          sizeof(hdr)) != ESP_OK) {
    ESP_LOGE(TAG, "Failed writing initial page header at 0x%08X",
             (unsigned int)rewriter->base_offset);
    return false;
  }
  return true;
}

bool timeseries_page_rewriter_start(timeseries_db_t *db, uint8_t level,
                                    uint32_t prev_data_size,
                                    timeseries_page_rewriter_t *rewriter) {
  if (!db || !rewriter) {
    return false;
  }
  memset(rewriter, 0, sizeof(*rewriter));
  rewriter->db = db;
  rewriter->level = level;
  rewriter->finalized = false;

  // Calculate the initial size rounded up to 4K.
  uint32_t initial_size = round_up_4k(prev_data_size);

  // Use the blank iterator to find a blank region of at least initial_size
  // bytes.
  uint32_t region_ofs = 0, region_size = 0;
  timeseries_blank_iterator_t blank_iter;
  timeseries_blank_iterator_init(db, &blank_iter, initial_size);
  if (!timeseries_blank_iterator_next(&blank_iter, &region_ofs, &region_size)) {
    ESP_LOGE(TAG, "Failed to find blank region (size=%u)",
             (unsigned int)initial_size);
    return false;
  }
  // Erase the found region.
  if (esp_partition_erase_range(db->partition, region_ofs, region_size) !=
      ESP_OK) {
    ESP_LOGE(TAG, "Failed to erase region @0x%08X", (unsigned int)region_ofs);
    return false;
  }

  // Set rewriter state.
  rewriter->base_offset = region_ofs;
  rewriter->write_ptr = region_ofs + sizeof(timeseries_page_header_t);
  rewriter->capacity = region_size;

  // Write the placeholder page header.
  if (!write_initial_page_header(rewriter)) {
    return false;
  }

  ESP_LOGI(TAG,
           "Page rewriter started for new page @0x%08X (capacity=%u bytes)",
           (unsigned int)rewriter->base_offset, (unsigned int)region_size);
  return true;
}

bool timeseries_page_rewriter_write_field_data(
    timeseries_page_rewriter_t *rewriter, uint32_t old_page_ofs,
    uint32_t record_offset, const timeseries_field_data_header_t *fd_hdr) {
  if (!rewriter || !fd_hdr || rewriter->finalized) {
    return false;
  }
  // Calculate the total length of the record: header + payload.
  uint32_t record_total_len =
      sizeof(timeseries_field_data_header_t) + fd_hdr->record_length;

  // Allocate temporary buffer to hold the record.
  unsigned char *rec_buf = (unsigned char *)malloc(record_total_len);
  if (!rec_buf) {
    ESP_LOGE(TAG, "OOM copying record from old page @0x%08X",
             (unsigned int)old_page_ofs);
    return false;
  }

  // Calculate the absolute address of the record in flash.
  uint32_t old_record_abs = old_page_ofs + record_offset;

  // Read the record from the old page.
  if (esp_partition_read(rewriter->db->partition, old_record_abs, rec_buf,
                         record_total_len) != ESP_OK) {
    ESP_LOGE(TAG, "Failed reading record from 0x%08X",
             (unsigned int)old_record_abs);
    free(rec_buf);
    return false;
  }

  // Check if there is enough capacity in the new page.
  if ((rewriter->write_ptr + record_total_len) >
      (rewriter->base_offset + rewriter->capacity)) {
    ESP_LOGE(
        TAG,
        "Not enough capacity in new page to write record (needed %u bytes)",
        (unsigned int)record_total_len);
    free(rec_buf);
    return false;
  }

  // Write the record into the new page.
  if (esp_partition_write(rewriter->db->partition, rewriter->write_ptr, rec_buf,
                          record_total_len) != ESP_OK) {
    ESP_LOGE(TAG, "Failed writing record to new page @0x%08X",
             (unsigned int)rewriter->write_ptr);
    free(rec_buf);
    return false;
  }
  rewriter->write_ptr += record_total_len;
  free(rec_buf);
  return true;
}

bool timeseries_page_rewriter_finalize(timeseries_page_rewriter_t *rewriter) {
  if (!rewriter || rewriter->finalized) {
    return false;
  }
  rewriter->finalized = true;

  // Determine the number of bytes used.
  uint32_t used_bytes = rewriter->write_ptr - rewriter->base_offset;
  // Round up to next 4K.
  uint32_t final_size = round_up_4k(used_bytes);

  // Read the existing page header.
  timeseries_page_header_t hdr;
  if (esp_partition_read(rewriter->db->partition, rewriter->base_offset, &hdr,
                         sizeof(hdr)) != ESP_OK) {
    ESP_LOGE(TAG, "Failed reading existing page header at 0x%08X",
             (unsigned int)rewriter->base_offset);
    return false;
  }

  // Update the header with the final page size.
  hdr.page_size = final_size;
  if (esp_partition_write(rewriter->db->partition, rewriter->base_offset, &hdr,
                          sizeof(hdr)) != ESP_OK) {
    ESP_LOGE(TAG, "Failed patching final page header at 0x%08X",
             (unsigned int)rewriter->base_offset);
    return false;
  }

  ESP_LOGI(TAG, "New page finalized at 0x%08X with size %u bytes",
           (unsigned int)rewriter->base_offset, (unsigned int)final_size);
  // Optionally add the new page to the page cache.
  tsdb_pagecache_add_entry(rewriter->db, rewriter->base_offset, &hdr);
  return true;
}

void timeseries_page_rewriter_abort(timeseries_page_rewriter_t *rewriter) {
  if (!rewriter) {
    return;
  }
  ESP_LOGW(TAG, "Aborting page rewriter for page @0x%08X",
           (unsigned int)rewriter->base_offset);
  rewriter->finalized = true;
}
