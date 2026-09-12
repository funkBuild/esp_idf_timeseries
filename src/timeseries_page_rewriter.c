#include "timeseries_page_rewriter.h"
#include "timeseries_revision.h"

#include "esp_log.h"
#include "esp_partition.h"
#include "timeseries_data.h"        // for tsdb_reserve_blank_region
#include "timeseries_flash_utils.h" // for tsdb_round_up_4k, tsdb_safe_rewrite_page_header
#include "timeseries_iterator.h"    // Ensure the blank iterator definitions are available
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "PAGE_REWRITER";

/**
 * Internal helper: Write a new page header placeholder at base_offset.
 */
static bool write_initial_page_header(timeseries_page_rewriter_t *rewriter) {
  timeseries_page_header_t hdr;
  memset(&hdr, 0xFF, sizeof(hdr));
  hdr.magic_number = TIMESERIES_MAGIC_NUM;
  hdr.page_type = TIMESERIES_PAGE_TYPE_FIELD_DATA;
  // The live cache reservation excludes allocators. On flash, FREE keeps a
  // partially copied page invisible to startup until finalize activates it.
  hdr.page_state = TIMESERIES_PAGE_STATE_FREE;
  // Atomic increment to avoid duplicate sequence numbers across concurrent
  // page allocations (the field is _Atomic but ++ is a non-atomic RMW).
  hdr.sequence_num = atomic_fetch_add(&rewriter->db->sequence_num, 1) + 1;
  hdr.field_data_level = rewriter->level;
  hdr.page_size = rewriter->capacity;
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
  if (!rewriter) {
    return false;
  }
  // Preserve batch_snapshot that may have been set before init
  tsdb_page_cache_snapshot_t *saved_batch = rewriter->batch_snapshot;
  memset(rewriter, 0, sizeof(*rewriter));
  rewriter->batch_snapshot = saved_batch;
  rewriter->db = db;
  rewriter->level = level;
  rewriter->finalized = true;
  if (!db || !db->partition || prev_data_size < sizeof(timeseries_page_header_t) ||
      prev_data_size > UINT32_MAX - 4095 || prev_data_size > db->partition->size) return false;

  // Calculate the initial size rounded up to 4K.
  uint32_t initial_size = tsdb_round_up_4k(prev_data_size);

  // Reserve a blank region (mutex-protected, registers placeholder in snapshots)
  uint32_t region_ofs = 0, region_size = 0;
  if (!tsdb_reserve_blank_region(db, initial_size, rewriter->batch_snapshot, &region_ofs, &region_size)) {
    ESP_LOGE(TAG, "Failed to find blank region (size=%u)",
             (unsigned int)initial_size);
    return false;
  }

  // Set rewriter state.
  rewriter->base_offset = region_ofs;
  rewriter->write_ptr = region_ofs + sizeof(timeseries_page_header_t);
  rewriter->capacity = region_size;
  rewriter->reserved = true;
  rewriter->finalized = false;

  // Write the placeholder page header.
  if (!write_initial_page_header(rewriter)) {
    timeseries_page_rewriter_abort(rewriter);
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
  if (!rewriter || rewriter->finalized || rewriter->failed || !rewriter->reserved) return false;
  if (!fd_hdr || !rewriter->db || !rewriter->db->partition) goto fail;
  const esp_partition_t *partition = rewriter->db->partition;
  uint32_t original_length = sizeof(*fd_hdr) + fd_hdr->record_length;
  bool legacy = (fd_hdr->flags & TSDB_FIELDDATA_FLAG_REVISION) != 0;
  if (legacy && fd_hdr->record_length > UINT16_MAX - sizeof(uint64_t)) goto fail;
  uint32_t length = original_length + (legacy ? sizeof(uint64_t) : 0);
  if (old_page_ofs > partition->size || record_offset > UINT32_MAX - old_page_ofs ||
      record_offset > partition->size - old_page_ofs) goto fail;
  uint32_t source = old_page_ofs + record_offset;
  if (original_length > partition->size - source ||
      rewriter->base_offset > partition->size || rewriter->capacity > partition->size - rewriter->base_offset ||
      rewriter->write_ptr < rewriter->base_offset ||
      rewriter->write_ptr - rewriter->base_offset > rewriter->capacity ||
      length > rewriter->capacity - (rewriter->write_ptr - rewriter->base_offset)) goto fail;

  uint64_t revision = 0;
  timeseries_field_data_header_t rewritten = *fd_hdr;
  if (legacy) {
    timeseries_page_header_t header;
    if (sizeof(header) > partition->size - old_page_ofs ||
        esp_partition_read(partition, old_page_ofs, &header, sizeof(header)) != ESP_OK) goto fail;
    revision = tsdb_legacy_revision(header.sequence_num, source + sizeof(*fd_hdr));
    rewritten.flags &= ~TSDB_FIELDDATA_FLAG_REVISION;
    rewritten.flags |= TSDB_FIELDDATA_FLAG_REVISION_VECTOR;
    rewritten.record_length += sizeof(revision);
  }

  uint8_t fallback[512];
  uint8_t *buffer = fallback;
  size_t capacity = sizeof(fallback);
  if (rewriter->copy_buffer && rewriter->copy_buffer_capacity >= sizeof(fallback)) {
    buffer = rewriter->copy_buffer;
    capacity = rewriter->copy_buffer_capacity;
  }
  for (uint32_t copied = 0; copied < length;) {
    size_t bytes = length - copied;
    if (bytes > capacity) bytes = capacity;
    size_t from_source = copied < original_length ? original_length - copied : 0;
    if (from_source > bytes) from_source = bytes;
    if (from_source && esp_partition_read(partition, source + copied, buffer, from_source) != ESP_OK) goto fail;
    if (!copied) {
      if (memcmp(buffer, fd_hdr, sizeof(*fd_hdr))) goto fail;
      if (legacy) memcpy(buffer, &rewritten, sizeof(rewritten));
    }
    if (bytes > from_source) {
      size_t revision_offset = copied > original_length ? copied - original_length : 0;
      memcpy(buffer + from_source, (const uint8_t *)&revision + revision_offset, bytes - from_source);
    }
    if (esp_partition_write(partition, rewriter->write_ptr + copied, buffer, bytes) != ESP_OK) goto fail;
    copied += bytes;
  }
  rewriter->write_ptr += length;
  return true;
fail:
  rewriter->failed = true;
  return false;
}

bool timeseries_page_rewriter_finalize(timeseries_page_rewriter_t *rewriter) {
  if (!rewriter || rewriter->finalized || rewriter->failed || !rewriter->reserved) {
    return false;
  }

  // Retain the reservation. The normal caller has sized it exactly; shrinking
  // an overestimate could require erasing a sector that already contains data.
  uint32_t final_size = rewriter->capacity;

  // Read the existing page header.
  timeseries_page_header_t hdr;
  if (esp_partition_read(rewriter->db->partition, rewriter->base_offset, &hdr,
                         sizeof(hdr)) != ESP_OK) {
    ESP_LOGE(TAG, "Failed reading existing page header at 0x%08X",
             (unsigned int)rewriter->base_offset);
    return false;
  }

  if (hdr.magic_number != TIMESERIES_MAGIC_NUM || hdr.page_state != TIMESERIES_PAGE_STATE_FREE ||
      hdr.page_size != rewriter->capacity) { rewriter->failed = true; return false; }
  // All records are complete. Activation only clears bits, with no erase.
  hdr.page_state = TIMESERIES_PAGE_STATE_ACTIVE;
  // Once activation is attempted, any failure requires abort. In particular,
  // a cache-publication failure must not permit appending to an active page.
  rewriter->failed = true;
  if (esp_partition_write(rewriter->db->partition, rewriter->base_offset, &hdr, sizeof(hdr)) != ESP_OK) {
    rewriter->failed = true;
    return false;
  }

  ESP_LOGI(TAG, "New page finalized at 0x%08X with size %u bytes",
           (unsigned int)rewriter->base_offset, (unsigned int)final_size);
  // Add the new page to the page cache.
  if (rewriter->batch_snapshot) {
    if (!tsdb_pagecache_batch_add(rewriter->batch_snapshot, rewriter->base_offset, &hdr)) {
      ESP_LOGE(TAG, "OOM updating batch cache in rewriter finalize");
      return false;
    }
    tsdb_pagecache_batch_sort(rewriter->batch_snapshot);
  } else {
    if (!tsdb_pagecache_add_entry(rewriter->db, rewriter->base_offset, &hdr)) {
      ESP_LOGE(TAG, "OOM updating page cache in rewriter finalize @0x%08X",
               (unsigned int)rewriter->base_offset);
      return false;
    }
  }
  rewriter->finalized = true;
  rewriter->reserved = false;
  rewriter->failed = false;
  return true;
}

void timeseries_page_rewriter_abort(timeseries_page_rewriter_t *rewriter) {
  if (!rewriter || !rewriter->db || !rewriter->reserved || rewriter->finalized) {
    return;
  }
  ESP_LOGW(TAG, "Aborting page rewriter for page @0x%08X",
           (unsigned int)rewriter->base_offset);
  rewriter->finalized = true;
  rewriter->reserved = false;

  // Retire the reservation, including a page whose activation succeeded but
  // whose cache publication failed.
  // Clearing page_state bits is a valid NOR flash write (only clears bits).
  timeseries_page_header_t hdr;
  if (esp_partition_read(rewriter->db->partition, rewriter->base_offset,
                         &hdr, sizeof(hdr)) == ESP_OK) {
    hdr.page_state = TIMESERIES_PAGE_STATE_OBSOLETE;
    esp_partition_write(rewriter->db->partition, rewriter->base_offset,
                        &hdr, sizeof(hdr));
  }

  // Remove the placeholder from the page cache
  if (rewriter->batch_snapshot) {
    tsdb_pagecache_batch_remove(rewriter->batch_snapshot,
                                rewriter->base_offset);
  }
  // Reservations are registered in live as well as in an optional batch.
  tsdb_pagecache_remove_entry(rewriter->db, rewriter->base_offset);
}
