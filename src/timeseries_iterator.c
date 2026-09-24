// timeseries_iterator.c

#include "timeseries_iterator.h"
#include "esp_err.h"
#include "esp_log.h"
#include "gorilla/gorilla_stream_decoder.h"
#include "timeseries_compression.h"
#include "timeseries_data.h"
#include "timeseries_page_cache_snapshot.h"

#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

static const char* TAG = "TimeseriesIterator";

bool timeseries_page_iterator_init(timeseries_db_t* db, timeseries_page_iterator_t* iter) {
  if (!db || !iter || !db->partition) {
    return false;
  }
  iter->db = db;
  iter->current_offset = 0;  // Start at partition start
  iter->valid = true;
  iter->read_error = false;
  iter->error_offset = 0;
  return true;
}

bool timeseries_page_iterator_next(timeseries_page_iterator_t* iter, timeseries_page_header_t* out_header,
                                   uint32_t* out_offset, uint32_t* out_size) {
  if (!iter || !iter->valid) {
    return false;
  }
  iter->read_error = false;

  while (iter->current_offset < iter->db->partition->size) {
    // Read the full page header in one call
    timeseries_page_header_t hdr;
    esp_err_t err = esp_partition_read(iter->db->partition, iter->current_offset, &hdr, sizeof(hdr));
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading header @0x%08" PRIx32 " (err=0x%x)", iter->current_offset, err);
      // The extent of whatever is here is unknown. Report the sector and step
      // past it, so the caller can resume and still find every later page
      // (ending the scan here hid them, and the allocator reused their space).
      iter->read_error = true;
      iter->error_offset = iter->current_offset & ~0xFFFu;
      iter->current_offset = iter->error_offset + 4096;
      return false;
    }

    // Validate magic, type, etc.
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM) {
      if (hdr.page_type == TIMESERIES_PAGE_TYPE_METADATA || hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA) {
        uint32_t page_size = hdr.page_size;
        // Bound the size by subtraction: current_offset < partition size here,
        // so this cannot wrap. A header torn by power loss (or bit decay) can
        // carry a size near 2^32; the old `offset + size > part_size` form
        // wrapped, accepted it and moved the cursor backwards (lost pages,
        // or an endless scan). The page's real extent is unknown, so skip one
        // sector and keep scanning: the cursor only ever moves forward, and
        // valid pages after the bad header are still found.
        // Every writer (1.0.19 and later) sizes pages in whole 4 KiB sectors;
        // anything else is a torn or decayed header. Accepting it would move
        // the cursor off the sector grid and hide every later page (whose
        // regions the allocator would then erase).
        if (page_size < sizeof(hdr) || (page_size & 0xFFFu) ||
            page_size > iter->db->partition->size - iter->current_offset) {
          ESP_LOGW(TAG, "Invalid page_size 0x%08" PRIx32 " @0x%08" PRIx32 ", skipping sector", page_size,
                   iter->current_offset);
          iter->current_offset = (iter->current_offset & ~0xFFFu) + 4096;
          continue;
        }

        // Now check the page_state
        if (hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
          // => return it as a valid page
          if (out_header) {
            memcpy(out_header, &hdr, sizeof(hdr));
          }
          if (out_offset) {
            *out_offset = iter->current_offset;
          }
          if (out_size) {
            *out_size = page_size;
          }
          iter->current_offset += page_size;
          return true;
        } else {
          // If FREE or OBSOLETE => skip it entirely
          iter->current_offset += page_size;
          continue;
        }
      }
    }

    // If no valid magic or unknown => skip to the next sector boundary
    iter->current_offset = (iter->current_offset & ~0xFFFu) + 4096;
  }

  iter->valid = false;
  return false;
}

// -----------------------------------------------------------------------------
// Metadata (Entity) Iterator
// -----------------------------------------------------------------------------

bool timeseries_entity_iterator_init(timeseries_db_t* db, uint32_t page_offset, uint32_t page_size,
                                     timeseries_entity_iterator_t* ent_iter) {
  if (!db || !ent_iter) {
    return false;
  }
  if (page_offset > db->partition->size || page_size > db->partition->size - page_offset) {
    return false;
  }

  ent_iter->db = db;
  ent_iter->page_offset = page_offset;
  ent_iter->page_size = page_size;

  // The first metadata entry starts after the page header
  ent_iter->offset = sizeof(timeseries_page_header_t);

  // We do NOT set current_entry_offset here; we do it in next()
  ent_iter->current_entry_offset = 0;
  ent_iter->valid = true;
  ent_iter->read_error = false;

  // Use esp_partition_read for all flash access (no mmap).
  // mmap creates a persistent flash cache mapping that can conflict with
  // concurrent flash write operations in QEMU and adds complexity for
  // minimal performance benefit on metadata pages.
  ent_iter->page_ptr = NULL;
  ent_iter->mmap_hdl = 0;
  ent_iter->owns_page_buf = false;
  ent_iter->buf_end = 0;

  return true;
}

// First block of a buffered page; each later block doubles the bytes held.
#define ENTITY_BUF_FIRST_BLOCK 64u

bool timeseries_entity_iterator_buffer(timeseries_entity_iterator_t* ent_iter) {
  if (!ent_iter || !ent_iter->valid) {
    return false;
  }
  if (ent_iter->page_ptr != NULL) {
    return true;
  }
  if (ent_iter->page_size < sizeof(timeseries_page_header_t)) {
    return false;
  }
  uint8_t* buf = malloc(ent_iter->page_size);
  if (!buf) {
    return false;
  }
  ent_iter->page_ptr = buf;
  ent_iter->owns_page_buf = true;
  ent_iter->buf_end = sizeof(timeseries_page_header_t);  // nothing loaded yet
  return true;
}

/* Buffered mode: make page bytes [.., end) available, reading the next block
 * from flash if needed. */
static bool entity_buf_ensure(timeseries_entity_iterator_t* ent_iter, uint32_t end) {
  if (!ent_iter->owns_page_buf || end <= ent_iter->buf_end) {
    return true;
  }
  if (end > ent_iter->page_size) {
    return false;
  }
  uint32_t held = ent_iter->buf_end - (uint32_t)sizeof(timeseries_page_header_t);
  uint32_t grow = held > ENTITY_BUF_FIRST_BLOCK ? held : ENTITY_BUF_FIRST_BLOCK;
  uint32_t new_end = ent_iter->buf_end + grow;
  if (new_end < end) new_end = end;
  if (new_end > ent_iter->page_size) new_end = ent_iter->page_size;
  esp_err_t err = esp_partition_read(ent_iter->db->partition, ent_iter->page_offset + ent_iter->buf_end,
                                     (uint8_t*)ent_iter->page_ptr + ent_iter->buf_end, new_end - ent_iter->buf_end);
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to read metadata page @0x%08" PRIx32 " +0x%" PRIx32 " (err=0x%x)", ent_iter->page_offset,
             ent_iter->buf_end, err);
    return false;
  }
  ent_iter->buf_end = new_end;
  return true;
}

void timeseries_entity_iterator_deinit(timeseries_entity_iterator_t* ent_iter) {
  if (ent_iter && ent_iter->owns_page_buf) {
    free((void*)ent_iter->page_ptr);
    ent_iter->page_ptr = NULL;
    ent_iter->owns_page_buf = false;
    return;
  }
  if (ent_iter && ent_iter->page_ptr != NULL && ent_iter->mmap_hdl != 0) {
    esp_partition_munmap(ent_iter->mmap_hdl);
    ent_iter->page_ptr = NULL;
    ent_iter->mmap_hdl = 0;
  }
}

bool timeseries_entity_iterator_next(timeseries_entity_iterator_t* ent_iter, timeseries_entry_header_t* out_header) {
  if (!ent_iter || !ent_iter->valid) {
    return false;
  }
  uint32_t page_end = ent_iter->page_size;

  // Check space for at least an entry header (relative to page start)
  if (ent_iter->offset + sizeof(timeseries_entry_header_t) > page_end) {
    ent_iter->valid = false;
    return false;
  }

  // Save the entry_offset BEFORE reading
  ent_iter->current_entry_offset = ent_iter->offset;

  // Read the header from the in-RAM page copy if buffered, otherwise from flash
  timeseries_entry_header_t hdr;

  if (ent_iter->page_ptr != NULL) {
    if (!entity_buf_ensure(ent_iter, ent_iter->offset + (uint32_t)sizeof(hdr))) {
      ent_iter->valid = false;
      ent_iter->read_error = true;
      return false;
    }
    memcpy(&hdr, ent_iter->page_ptr + ent_iter->offset, sizeof(hdr));
  } else {
    // Fall back to partition read
    uint32_t curr_offset = ent_iter->page_offset + ent_iter->offset;
    esp_err_t err = esp_partition_read(ent_iter->db->partition, curr_offset, &hdr, sizeof(hdr));
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed to read entity header @0x%08" PRIx32 " (err=0x%x)", curr_offset, err);
      ent_iter->valid = false;
      ent_iter->read_error = true;
      return false;
    }
  }

  // check for blank (all 0xFF)
  const uint8_t* hdr_bytes = (const uint8_t*)&hdr;
  bool blank = true;
  for (size_t i = 0; i < sizeof(hdr); i++) {
    if (hdr_bytes[i] != 0xFF) {
      blank = false;
      break;
    }
  }
  if (blank) {
    // no more data
    ent_iter->valid = false;
    return false;
  }

  uint32_t total_size = sizeof(hdr) + hdr.key_len + hdr.value_len;
  if (ent_iter->offset + total_size > page_end) {
    ESP_LOGW(TAG, "Entry extends beyond boundary @offset=%" PRIu32, ent_iter->offset);
    ent_iter->valid = false;
    return false;
  }

  // Provide the header if requested
  if (out_header) {
    memcpy(out_header, &hdr, sizeof(hdr));
  }

  // Advance ent_iter->offset
  ent_iter->offset += total_size;
  return true;
}

bool timeseries_entity_iterator_read_data(timeseries_entity_iterator_t* ent_iter,
                                          const timeseries_entry_header_t* header, void* key_buf, void* value_buf) {
  if (!ent_iter || !ent_iter->valid || !header) {
    return false;
  }

  // Calculate offsets relative to page start
  uint32_t key_offset_rel = ent_iter->current_entry_offset + sizeof(timeseries_entry_header_t);
  uint32_t value_offset_rel = key_offset_rel + header->key_len;

  if (ent_iter->page_ptr != NULL) {
    if (!entity_buf_ensure(ent_iter, value_offset_rel + header->value_len)) {
      return false;
    }
    if (key_buf && header->key_len > 0) {
      memcpy(key_buf, ent_iter->page_ptr + key_offset_rel, header->key_len);
    }
    if (value_buf && header->value_len > 0) {
      memcpy(value_buf, ent_iter->page_ptr + value_offset_rel, header->value_len);
    }
  } else {
    // Fall back to partition read
    uint32_t key_offset = ent_iter->page_offset + key_offset_rel;
    uint32_t value_offset = ent_iter->page_offset + value_offset_rel;

    // read the key
    if (key_buf && header->key_len > 0) {
      esp_err_t err = esp_partition_read(ent_iter->db->partition, key_offset, key_buf, header->key_len);
      if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed to read entry key (err=0x%x)", err);
        return false;
      }
    }
    // read the value
    if (value_buf && header->value_len > 0) {
      esp_err_t err = esp_partition_read(ent_iter->db->partition, value_offset, value_buf, header->value_len);
      if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed to read entry value (err=0x%x)", err);
        return false;
      }
    }
  }
  return true;
}

// -----------------------------------------------------------------------------
// Field Data Iterator
// -----------------------------------------------------------------------------

bool timeseries_fielddata_iterator_init(timeseries_db_t* db, uint32_t page_offset, uint32_t page_size,
                                        timeseries_fielddata_iterator_t* f_iter) {
  if (!db || !db->partition || !f_iter) {
    return false;
  }
  if (page_size < sizeof(timeseries_page_header_t) ||
      page_offset > db->partition->size || page_size > db->partition->size - page_offset) {
    return false;
  }
  f_iter->window_start = 0;
  f_iter->window_capacity = 0;
  f_iter->read_error = false;
  f_iter->db = db;
  f_iter->page_offset = page_offset;
  f_iter->page_size = page_size;

  // The first record is after the page header
  f_iter->offset = sizeof(timeseries_page_header_t);

  // we do NOT set current_record_offset here; we do it in _next()
  f_iter->current_record_offset = 0;
  f_iter->valid = true;

  // Bulk-read the page data (after header) into RAM for fast iteration
  uint32_t data_size = page_size - sizeof(timeseries_page_header_t);
  f_iter->page_buf = (uint8_t*)malloc(data_size);
  f_iter->page_buf_size = 0;
  f_iter->owns_page_buf = true;
  if (f_iter->page_buf) {
    esp_err_t err = esp_partition_read(db->partition,
                                       page_offset + sizeof(timeseries_page_header_t),
                                       f_iter->page_buf, data_size);
    if (err == ESP_OK) {
      f_iter->page_buf_size = data_size;
    } else {
      free(f_iter->page_buf);
      f_iter->page_buf = NULL;
    }
  }
  return true;
}

bool timeseries_fielddata_iterator_init_with_buffer(timeseries_db_t* db, uint32_t page_offset,
                                                     uint32_t page_size, uint8_t* buf,
                                                     uint32_t buf_capacity,
                                                     timeseries_fielddata_iterator_t* f_iter) {
  if (!db || !db->partition || !f_iter) {
    return false;
  }
  if (page_size < sizeof(timeseries_page_header_t) ||
      page_offset > db->partition->size || page_size > db->partition->size - page_offset) {
    return false;
  }
  f_iter->window_start = 0;
  f_iter->window_capacity = 0;
  f_iter->read_error = false;
  f_iter->db = db;
  f_iter->page_offset = page_offset;
  f_iter->page_size = page_size;
  f_iter->offset = sizeof(timeseries_page_header_t);
  f_iter->current_record_offset = 0;
  f_iter->valid = true;
  f_iter->owns_page_buf = false;

  uint32_t data_size = page_size - sizeof(timeseries_page_header_t);
  if (buf && buf_capacity >= data_size) {
    esp_err_t err = esp_partition_read(db->partition,
                                       page_offset + sizeof(timeseries_page_header_t),
                                       buf, data_size);
    if (err == ESP_OK) {
      f_iter->page_buf = buf;
      f_iter->page_buf_size = data_size;
    } else {
      f_iter->page_buf = NULL;
      f_iter->page_buf_size = 0;
    }
  } else {
    f_iter->page_buf = NULL;
    f_iter->page_buf_size = 0;
  }
  return true;
}

bool timeseries_fielddata_iterator_init_window(timeseries_db_t *db,
    uint32_t page_offset, uint32_t page_size, uint8_t *buf, uint32_t capacity,
    timeseries_fielddata_iterator_t *iter) {
  if (!buf || capacity < sizeof(timeseries_field_data_header_t) ||
      !timeseries_fielddata_iterator_init_with_buffer(db, page_offset, page_size,
                                                       NULL, 0, iter)) return false;
  iter->page_buf = buf;
  iter->window_capacity = capacity;
  return true;
}

void timeseries_fielddata_iterator_deinit(timeseries_fielddata_iterator_t* f_iter) {
  if (f_iter && f_iter->page_buf && f_iter->owns_page_buf) {
    free(f_iter->page_buf);
  }
  if (f_iter) {
    f_iter->page_buf = NULL;
    f_iter->page_buf_size = 0;
  }
}

bool timeseries_fielddata_iterator_read_at(timeseries_fielddata_iterator_t *iter,
                                          uint32_t offset, void *out, size_t length) {
  if (!iter || !iter->valid) return false;
  if (!iter->db || !iter->db->partition || (!out && length) ||
      offset < sizeof(timeseries_page_header_t) || offset > iter->page_size ||
      length > iter->page_size - offset || iter->page_offset > iter->db->partition->size ||
      iter->page_size > iter->db->partition->size - iter->page_offset) goto fail;
  if (!length) return true;
  uint32_t relative = offset - sizeof(timeseries_page_header_t);
  if (iter->page_buf && relative >= iter->window_start &&
      relative - iter->window_start <= iter->page_buf_size &&
      length <= iter->page_buf_size - (relative - iter->window_start)) {
    memcpy(out, iter->page_buf + relative - iter->window_start, length);
    return true;
  }
  if (iter->page_buf && iter->window_capacity && length <= iter->window_capacity) {
    uint32_t bytes = iter->page_size - offset;
    if (bytes > iter->window_capacity) bytes = iter->window_capacity;
    if (esp_partition_read(iter->db->partition, iter->page_offset + offset, iter->page_buf, bytes) != ESP_OK)
      goto fail;
    iter->window_start = relative;
    iter->page_buf_size = bytes;
    memcpy(out, iter->page_buf, length);
    return true;
  }
  if (esp_partition_read(iter->db->partition, iter->page_offset + offset, out, length) == ESP_OK) return true;
fail:
  iter->read_error = true;
  iter->valid = false;
  return false;
}

bool timeseries_fielddata_iterator_next(timeseries_fielddata_iterator_t* f_iter,
                                        timeseries_field_data_header_t* out_hdr) {
  if (!f_iter || !f_iter->valid) {
    return false;
  }

  // offset is relative to page_offset; data starts after page header
  uint32_t hdr_size = sizeof(timeseries_page_header_t);
  uint32_t rel_data_offset = f_iter->offset - hdr_size;  // offset into page_buf

  // Check we can read a field_data_header
  if (f_iter->offset + sizeof(timeseries_field_data_header_t) > f_iter->page_size) {
    f_iter->valid = false;
    return false;
  }

  // The current record is at offset
  f_iter->current_record_offset = f_iter->offset;

  // read the header — from buffer if available, else from flash
  timeseries_field_data_header_t local_hdr;
  if (f_iter->window_capacity &&
      (rel_data_offset < f_iter->window_start ||
       rel_data_offset - f_iter->window_start + sizeof(local_hdr) > f_iter->page_buf_size)) {
    uint32_t remaining = f_iter->page_size - f_iter->offset;
    uint32_t length = remaining < f_iter->window_capacity ? remaining : f_iter->window_capacity;
    if (esp_partition_read(f_iter->db->partition, f_iter->page_offset + f_iter->offset,
                           f_iter->page_buf, length) != ESP_OK) {
      f_iter->read_error = true;
      f_iter->valid = false;
      return false;
    }
    f_iter->window_start = rel_data_offset;
    f_iter->page_buf_size = length;
  }
  uint32_t buffered_offset = rel_data_offset - f_iter->window_start;
  if (f_iter->page_buf && rel_data_offset >= f_iter->window_start &&
      buffered_offset <= f_iter->page_buf_size &&
      sizeof(local_hdr) <= f_iter->page_buf_size - buffered_offset) {
    memcpy(&local_hdr, f_iter->page_buf + buffered_offset, sizeof(local_hdr));
  } else {
    uint32_t curr_offset = f_iter->page_offset + f_iter->offset;
    esp_err_t err = esp_partition_read(f_iter->db->partition, curr_offset, &local_hdr, sizeof(local_hdr));
    if (err != ESP_OK) {
      f_iter->read_error = true;
      f_iter->valid = false;
      return false;
    }
  }

  // check blank => done
  const uint8_t* p = (const uint8_t*)&local_hdr;
  bool blank = true;
  for (size_t i = 0; i < sizeof(local_hdr); i++) {
    if (p[i] != 0xFF) {
      blank = false;
      break;
    }
  }
  if (blank) {
    f_iter->valid = false;
    return false;
  }

  // Debug print the header
  ESP_LOGV(TAG, "FieldData record: series_id=%.2X%.2X%.2X%.2X...", local_hdr.series_id[0], local_hdr.series_id[1],
           local_hdr.series_id[2], local_hdr.series_id[3]);
  ESP_LOGV(TAG, "  flags=0x%02X, record_count=%u, record_length=%u", local_hdr.flags, local_hdr.record_count,
           local_hdr.record_length);

  // Provide the header if requested
  if (out_hdr) {
    memcpy(out_hdr, &local_hdr, sizeof(local_hdr));
  }

  // skip the entire record => header + data
  uint32_t record_size = sizeof(timeseries_field_data_header_t) + local_hdr.record_length;
  if (f_iter->offset + record_size > f_iter->page_size) {
    // A malformed record is an error, not clean EOF. Compaction must retain
    // this source page instead of treating it as empty and retiring it.
    f_iter->read_error = true;
    f_iter->valid = false;
    return false;
  }

  f_iter->offset += record_size;
  return true;
}

bool timeseries_fielddata_iterator_read_data(timeseries_fielddata_iterator_t* f_iter,
                                             const timeseries_field_data_header_t* hdr, void* out_buf, size_t buf_len) {
  if (!f_iter || !f_iter->valid || !hdr || !out_buf) {
    return false;
  }

  // boundary checks
  if (buf_len > hdr->record_length) {
    ESP_LOGW(TAG, "Requested buf_len=%zu but record_length=%u", buf_len, (unsigned int)hdr->record_length);
    return false;
  }

  // Data starts after the header within the current record
  uint32_t data_rel = f_iter->current_record_offset + sizeof(timeseries_field_data_header_t);
  uint32_t page_end_rel = f_iter->page_size;
  if (data_rel + hdr->record_length > page_end_rel) {
    return false;
  }

  // Read from buffer if available, else from flash
  uint32_t buf_offset = data_rel - sizeof(timeseries_page_header_t);
  if (f_iter->page_buf && buf_offset >= f_iter->window_start &&
      buf_offset - f_iter->window_start <= f_iter->page_buf_size &&
      buf_len <= f_iter->page_buf_size - (buf_offset - f_iter->window_start)) {
    memcpy(out_buf, f_iter->page_buf + buf_offset - f_iter->window_start, buf_len);
  } else {
    uint32_t abs_offset = f_iter->page_offset + data_rel;
    esp_err_t err = esp_partition_read(f_iter->db->partition, abs_offset, out_buf, buf_len);
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading field-data entry data (err=0x%x)", err);
      return false;
    }
  }
  return true;
}

// -----------------------------------------------------------------------------
// Blank Iterator
// -----------------------------------------------------------------------------

bool timeseries_blank_iterator_init(timeseries_db_t* db, timeseries_blank_iterator_t* iter, uint32_t min_size) {
  if (!db || !iter || !db->partition) {
    return false;
  }

  iter->db = db;
  iter->partition = db->partition;
  iter->min_size = min_size;
  iter->valid = true;
  iter->current_offset = 0;
  iter->current_index = 0;

  iter->in_blank_run = false;
  iter->run_start = 0;
  iter->run_length = 0;

  // Acquire current snapshot for consistent iteration
  iter->snapshot = tsdb_snapshot_acquire_current(db);
  iter->owns_snapshot = true;

  return true;
}

bool timeseries_blank_iterator_init_with_snapshot(timeseries_db_t* db, timeseries_blank_iterator_t* iter,
                                                   uint32_t min_size, tsdb_page_cache_snapshot_t* snapshot) {
  if (!db || !iter || !db->partition) {
    return false;
  }

  iter->db = db;
  iter->partition = db->partition;
  iter->min_size = min_size;
  iter->valid = true;
  iter->current_offset = 0;
  iter->current_index = 0;

  iter->in_blank_run = false;
  iter->run_start = 0;
  iter->run_length = 0;

  // Use the provided snapshot without acquiring (caller owns it)
  iter->snapshot = snapshot;
  iter->owns_snapshot = false;

  return true;
}

void timeseries_blank_iterator_deinit(timeseries_blank_iterator_t* iter) {
  if (!iter) {
    return;
  }
  if (iter->owns_snapshot && iter->snapshot) {
    tsdb_snapshot_release(iter->snapshot);
    iter->snapshot = NULL;
  }
}

bool timeseries_blank_iterator_next(timeseries_blank_iterator_t* iter, uint32_t* out_offset, uint32_t* out_size) {
  if (!iter || !iter->valid || !iter->snapshot) {
    return false;
  }

  tsdb_page_cache_snapshot_t* snap = iter->snapshot;
  const esp_partition_t* part = iter->partition;
  uint32_t part_size = part->size;

  // Continue until we either find a run >= min_size or exhaust partition
  while (true) {
    // If we've already found a run >= min_size, return it:
    if (iter->in_blank_run && iter->run_length >= iter->min_size) {
      // Return the run
      if (out_offset) {
        *out_offset = iter->run_start;
      }
      if (out_size) {
        *out_size = iter->run_length;
      }
      // Move current_offset to the end of this run
      iter->current_offset = iter->run_start + iter->run_length;

      // Reset for next time
      iter->in_blank_run = false;
      iter->run_start = 0;
      iter->run_length = 0;

      return true;
    }

    // 1) If we've consumed all cached pages, the rest of the partition is free
    if (iter->current_index >= snap->count) {
      // The entire region from current_offset..end is free
      if (iter->current_offset >= part_size) {
        // no more space left
        iter->valid = false;
        return false;
      }

      uint32_t free_start = iter->current_offset;
      uint32_t free_len = part_size - free_start;

      // Merge/extend with any existing run
      if (!iter->in_blank_run) {
        iter->in_blank_run = true;
        iter->run_start = free_start;
        iter->run_length = free_len;
      } else {
        // check for contiguity
        uint32_t expected_next = iter->run_start + iter->run_length;
        if (free_start == expected_next) {
          iter->run_length += free_len;
        } else {
          // finalize old run if >= min_size
          if (iter->run_length >= iter->min_size) {
            if (out_offset) *out_offset = iter->run_start;
            if (out_size) *out_size = iter->run_length;
            iter->current_offset = iter->run_start + iter->run_length;
            // reset
            iter->in_blank_run = false;
            iter->run_start = 0;
            iter->run_length = 0;
            return true;
          }
          // else discard old run, start a new run
          iter->run_start = free_start;
          iter->run_length = free_len;
        }
      }
      // Now check if new run >= min_size
      if (iter->run_length >= iter->min_size) {
        if (out_offset) {
          *out_offset = iter->run_start;
        }
        if (out_size) {
          *out_size = iter->run_length;
        }
        iter->current_offset = iter->run_start + iter->run_length;
        // reset
        iter->in_blank_run = false;
        iter->run_start = 0;
        iter->run_length = 0;
        return true;
      }
      // If still no success, we are done
      iter->valid = false;
      return false;
    }

    // 2) Get the next cached page
    timeseries_cached_page_t* entry = &snap->entries[iter->current_index];
    uint32_t page_offset = entry->offset;
    uint32_t page_size = entry->header.page_size;
    uint8_t page_state = entry->header.page_state;

    // If the page is completely before our current_offset, skip it
    uint32_t page_end = page_offset + page_size;
    if (page_end <= iter->current_offset) {
      // move on to next page
      iter->current_index++;
      continue;
    }

    // if there's a gap from current_offset..page_offset, that's free
    if (page_offset > iter->current_offset) {
      uint32_t gap_start = iter->current_offset;
      uint32_t gap_len = page_offset - gap_start;

      // Merge with existing run if contiguous
      if (iter->in_blank_run) {
        uint32_t expected = iter->run_start + iter->run_length;
        if (gap_start == expected) {
          iter->run_length += gap_len;
        } else {
          // finalize old run if big enough
          if (iter->run_length >= iter->min_size) {
            if (out_offset) *out_offset = iter->run_start;
            if (out_size) *out_size = iter->run_length;
            iter->current_offset = iter->run_start + iter->run_length;
            // reset
            iter->in_blank_run = false;
            iter->run_start = 0;
            iter->run_length = 0;
            return true;
          }
          // else discard old run, start new
          iter->run_start = gap_start;
          iter->run_length = gap_len;
        }
      } else {
        // start new run
        iter->in_blank_run = true;
        iter->run_start = gap_start;
        iter->run_length = gap_len;
      }
      // Now current_offset is at least page_offset
      iter->current_offset = page_offset;
    }

    // Now handle the page itself
    if (page_state == TIMESERIES_PAGE_STATE_OBSOLETE) {
      // This entire page is free => merge with the run if contiguous
      if (!iter->in_blank_run) {
        // start new run
        iter->in_blank_run = true;
        iter->run_start = page_offset;
        iter->run_length = page_size;
      } else {
        // check contiguity
        uint32_t expected = iter->run_start + iter->run_length;
        if (page_offset == expected) {
          iter->run_length += page_size;
        } else {
          // finalize old run if big enough
          if (iter->run_length >= iter->min_size) {
            if (out_offset) *out_offset = iter->run_start;
            if (out_size) *out_size = iter->run_length;
            iter->current_offset = iter->run_start + iter->run_length;
            iter->in_blank_run = false;
            iter->run_start = 0;
            iter->run_length = 0;
            return true;
          }
          // else discard old run, start new
          iter->run_start = page_offset;
          iter->run_length = page_size;
        }
      }
      iter->current_offset = page_offset + page_size;
      iter->current_index++;
    } else {
      // This page is used => finalize any blank run before it
      if (iter->in_blank_run && iter->run_length >= iter->min_size) {
        if (out_offset) *out_offset = iter->run_start;
        if (out_size) *out_size = iter->run_length;
        iter->current_offset = iter->run_start + iter->run_length;
        iter->in_blank_run = false;
        iter->run_start = 0;
        iter->run_length = 0;
        return true;
      }
      // Reset run
      iter->in_blank_run = false;
      iter->run_start = 0;
      iter->run_length = 0;

      // skip the entire used page
      iter->current_offset = page_offset + page_size;
      iter->current_index++;
    }
  }
}

/**
 * @brief Initialize the page cache iterator.
 * Acquires the current snapshot for consistent iteration.
 */
bool timeseries_page_cache_iterator_init(timeseries_db_t* db, timeseries_page_cache_iterator_t* iter) {
  if (!db || !iter) {
    return false;
  }
  iter->snapshot = tsdb_snapshot_acquire_current(db);
  iter->index = 0;
  iter->valid = (iter->snapshot != NULL);
  return iter->valid;
}

/**
 * @brief Return the *next* ACTIVE page from the cache.
 */
bool timeseries_page_cache_iterator_next(timeseries_page_cache_iterator_t* iter, timeseries_page_header_t* out_header,
                                         uint32_t* out_offset, uint32_t* out_size) {
  if (!iter || !iter->valid || !iter->snapshot) {
    return false;
  }

  while (iter->index < iter->snapshot->count) {
    timeseries_cached_page_t* entry = &iter->snapshot->entries[iter->index++];
    if (entry->header.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
      // Return it
      if (out_header) {
        memcpy(out_header, &entry->header, sizeof(*out_header));
      }
      if (out_offset) {
        *out_offset = entry->offset;
      }
      if (out_size) {
        *out_size = entry->header.page_size;
      }
      return true;
    }
  }
  iter->valid = false;
  return false;
}

void timeseries_page_cache_iterator_deinit(timeseries_page_cache_iterator_t* iter) {
  if (!iter) {
    return;
  }
  if (iter->snapshot) {
    tsdb_snapshot_release(iter->snapshot);
    iter->snapshot = NULL;
  }
}
