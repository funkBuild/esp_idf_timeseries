// timeseries_iterator.c

#include "timeseries_iterator.h"
#include "esp_err.h"
#include "esp_log.h"
#include "gorilla/gorilla_stream_decoder.h"
#include "timeseries_compression.h"
#include "timeseries_data.h"

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
  return true;
}

bool timeseries_page_iterator_next(timeseries_page_iterator_t* iter, timeseries_page_header_t* out_header,
                                   uint32_t* out_offset, uint32_t* out_size) {
  if (!iter || !iter->valid) {
    return false;
  }

  while (iter->current_offset < iter->db->partition->size) {
    // Read 4 bytes for magic
    uint32_t possible_magic = 0xFFFFFFFF;
    esp_err_t err =
        esp_partition_read(iter->db->partition, iter->current_offset, &possible_magic, sizeof(possible_magic));
    if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed reading magic @0x%08" PRIx32 " (err=0x%x)", iter->current_offset, err);
      iter->valid = false;
      return false;
    }

    // If it might be a page header
    if (possible_magic == TIMESERIES_MAGIC_NUM) {
      // Read the full page header
      timeseries_page_header_t hdr;
      err = esp_partition_read(iter->db->partition, iter->current_offset, &hdr, sizeof(hdr));
      if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed reading header @0x%08" PRIx32 " (err=0x%x)", iter->current_offset, err);
        iter->valid = false;
        return false;
      }

      // Validate magic, type, etc.
      if (hdr.magic_number == TIMESERIES_MAGIC_NUM &&
          (hdr.page_type == TIMESERIES_PAGE_TYPE_METADATA || hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA)) {
        uint32_t page_size = hdr.page_size;
        if (page_size < sizeof(hdr) || (iter->current_offset + page_size) > iter->db->partition->size) {
          // Invalid size => skip one sector
          iter->current_offset += 4096;
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

    // If no valid magic or unknown => skip one sector (4096)
    iter->current_offset += 4096;
  }

  iter->valid = false;
  return false;
}

bool timeseries_metadata_page_iterator_init(timeseries_db_t* db, timeseries_metadata_page_iterator_t* iter) {
  if (!db || !iter) {
    return false;
  }
  memset(iter, 0, sizeof(*iter));

  if (!timeseries_page_cache_iterator_init(db, &iter->inner)) {
    return false;
  }
  iter->valid = true;
  return true;
}

/* -------------------------------------------------------------------------- */
bool timeseries_metadata_page_iterator_next(timeseries_metadata_page_iterator_t* iter,
                                            timeseries_page_header_t* out_header, uint32_t* out_offset,
                                            uint32_t* out_size) {
  if (!iter || !iter->valid) {
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t off = 0;
  uint32_t size = 0;

  while (timeseries_page_cache_iterator_next(&iter->inner, &hdr, &off, &size)) {
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM && hdr.page_type == TIMESERIES_PAGE_TYPE_METADATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
      /* hand results to caller */
      if (out_header) memcpy(out_header, &hdr, sizeof(hdr));
      if (out_offset) *out_offset = off;
      if (out_size) *out_size = size;
      return true;
    }
    /* else: skip to next page */
  }

  /* exhausted */
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
  if (page_offset + page_size > db->partition->size) {
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
  return true;
}

void timeseries_entity_iterator_deinit(timeseries_entity_iterator_t* ent_iter) {}

bool timeseries_entity_iterator_next(timeseries_entity_iterator_t* ent_iter, timeseries_entry_header_t* out_header) {
  if (!ent_iter || !ent_iter->valid) {
    return false;
  }
  uint32_t page_start = ent_iter->page_offset;
  uint32_t page_end = page_start + ent_iter->page_size;

  // The offset for the *next* entry
  uint32_t curr_offset = page_start + ent_iter->offset;

  // Check space for at least an entry header
  if (curr_offset + sizeof(timeseries_entry_header_t) > page_end) {
    ent_iter->valid = false;
    return false;
  }

  // Save the entry_offset BEFORE reading
  ent_iter->current_entry_offset = ent_iter->offset;

  // read the header
  timeseries_entry_header_t hdr;
  esp_err_t err = esp_partition_read(ent_iter->db->partition, curr_offset, &hdr, sizeof(hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed to read entity header @0x%08" PRIx32 " (err=0x%x)", curr_offset, err);
    ent_iter->valid = false;
    return false;
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
  if (curr_offset + total_size > page_end) {
    ESP_LOGW(TAG, "Entry extends beyond boundary @0x%08" PRIx32, curr_offset);
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

  // The start of this entry is page_offset + current_entry_offset
  uint32_t entry_start = ent_iter->page_offset + ent_iter->current_entry_offset;
  uint32_t key_offset = entry_start + sizeof(timeseries_entry_header_t);
  uint32_t value_offset = key_offset + header->key_len;

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
  return true;
}

bool timeseries_entity_iterator_peek_key(timeseries_entity_iterator_t* it, const timeseries_entry_header_t* hdr,
                                         void* key_buf) {
  return timeseries_entity_iterator_read_data(it, hdr, key_buf, NULL);
}

bool timeseries_entity_iterator_read_value(timeseries_entity_iterator_t* it, const timeseries_entry_header_t* hdr,
                                           void* value_buf) {
  return timeseries_entity_iterator_read_data(it, hdr, NULL, value_buf);
}

// -----------------------------------------------------------------------------
// Field Data Iterator
// -----------------------------------------------------------------------------

bool timeseries_fielddata_iterator_init(timeseries_db_t* db, uint32_t page_offset, uint32_t page_size,
                                        timeseries_fielddata_iterator_t* f_iter) {
  if (!db || !f_iter) {
    return false;
  }
  if (page_offset + page_size > db->partition->size) {
    return false;
  }
  f_iter->db = db;
  f_iter->page_offset = page_offset;
  f_iter->page_size = page_size;

  // The first record is after the page header
  f_iter->offset = sizeof(timeseries_page_header_t);

  // we do NOT set current_record_offset here; we do it in _next()
  f_iter->current_record_offset = 0;
  f_iter->valid = true;
  return true;
}

bool timeseries_fielddata_iterator_next(timeseries_fielddata_iterator_t* f_iter,
                                        timeseries_field_data_header_t* out_hdr) {
  if (!f_iter || !f_iter->valid) {
    return false;
  }
  uint32_t page_end = f_iter->page_offset + f_iter->page_size;
  uint32_t curr_offset = f_iter->page_offset + f_iter->offset;

  // Check we can read a field_data_header
  if (curr_offset + sizeof(timeseries_field_data_header_t) > page_end) {
    f_iter->valid = false;
    return false;
  }

  // The current record is at offset
  f_iter->current_record_offset = f_iter->offset;

  // read the header
  timeseries_field_data_header_t local_hdr;
  esp_err_t err = esp_partition_read(f_iter->db->partition, curr_offset, &local_hdr, sizeof(local_hdr));
  if (err != ESP_OK) {
    f_iter->valid = false;
    return false;
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
  if (curr_offset + record_size > page_end) {
    // out of boundary
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

  // The record header is at (page_offset + current_record_offset)
  uint32_t record_start = f_iter->page_offset + f_iter->current_record_offset;
  uint32_t data_offset = record_start + sizeof(timeseries_field_data_header_t);

  // boundary checks
  if (buf_len > hdr->record_length) {
    ESP_LOGW(TAG, "Requested buf_len=%zu but record_length=%u", buf_len, (unsigned int)hdr->record_length);
    return false;
  }
  uint32_t page_end = f_iter->page_offset + f_iter->page_size;
  if (data_offset + hdr->record_length > page_end) {
    return false;
  }

  esp_err_t err = esp_partition_read(f_iter->db->partition, data_offset, out_buf, buf_len);
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed reading field-data entry data (err=0x%x)", err);
    return false;
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

  return true;
}

bool timeseries_blank_iterator_next(timeseries_blank_iterator_t* iter, uint32_t* out_offset, uint32_t* out_size) {
  if (!iter || !iter->valid) {
    return false;
  }

  timeseries_db_t* db = iter->db;
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
    if (iter->current_index >= db->page_cache_count) {
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
    timeseries_cached_page_t* entry = &db->page_cache[iter->current_index];
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
 */
bool timeseries_page_cache_iterator_init(timeseries_db_t* db, timeseries_page_cache_iterator_t* iter) {
  if (!db || !iter) {
    return false;
  }
  iter->db = db;
  iter->index = 0;
  iter->valid = true;
  return true;
}

/**
 * @brief Return the *next* ACTIVE page from the cache.
 */
bool timeseries_page_cache_iterator_next(timeseries_page_cache_iterator_t* iter, timeseries_page_header_t* out_header,
                                         uint32_t* out_offset, uint32_t* out_size) {
  if (!iter || !iter->valid) {
    return false;
  }
  timeseries_db_t* db = iter->db;

  while (iter->index < db->page_cache_count) {
    timeseries_cached_page_t* entry = &db->page_cache[iter->index++];
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
