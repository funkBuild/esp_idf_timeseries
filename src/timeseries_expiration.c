#include "timeseries_expiration.h"

#include "esp_err.h"
#include "esp_log.h"
#include "esp_partition.h"

#include "timeseries_compaction.h"  // timeseries_compact_page_list() signature
#include "timeseries_data.h"        // TSDB_FIELDDATA_FLAG_DELETED, etc.
#include "timeseries_internal.h"    // timeseries_page_cache_iterator_t
#include "timeseries_iterator.h"    // timeseries_fielddata_iterator_t
#include "timeseries_metadata.h"    // tsdb_lookup_series_type_in_metadata(), etc.
#include "timeseries_page_cache.h"  // tsdb_pagecache_get_total_active_size()

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

static const char* TAG = "TimeseriesExpiration";

/**
 * @brief Descriptor for a single field-data record we might expire.
 *
 * We store enough info to:
 *   - Identify how old it is (`start_time`)
 *   - Find it in flash (`page_offset + record_offset`)
 *   - Know how big it is (`record_length`) so we can count how many bytes
 *     we've freed as we delete old records.
 */
typedef struct {
  uint64_t start_time;
  uint32_t page_offset;    // Base offset of the page in the partition
  uint32_t record_offset;  // Offset from the start of the page to the record
  uint32_t record_length;  // Size of this record, in bytes
} tsdb_oldest_fd_entry_t;

/**
 * @brief Simple list to store unique page offsets that we modified.
 */
typedef struct {
  uint32_t* pages;
  size_t used;
  size_t capacity;
} tsdb_modified_pages_list_t;

// -----------------------------------------------------------------------------
// Forward Declarations
// -----------------------------------------------------------------------------

static bool get_usage_ratio(timeseries_db_t* db, float* out_ratio);
static bool gather_25_oldest_fielddata(timeseries_db_t* db, tsdb_oldest_fd_entry_t* heap_25, size_t* out_count);
static void mark_entries_deleted(timeseries_db_t* db, const tsdb_oldest_fd_entry_t* entries, size_t count,
                                 uint32_t bytes_to_free, tsdb_modified_pages_list_t* modified_out);
static bool mark_record_deleted(timeseries_db_t* db, uint32_t record_hdr_abs);

static void modified_pages_list_init(tsdb_modified_pages_list_t* list);
static void modified_pages_list_add(tsdb_modified_pages_list_t* list, uint32_t page_offset);
static void modified_pages_list_free(tsdb_modified_pages_list_t* list);

// -----------------------------------------------------------------------------
// Static Comparator Function
// -----------------------------------------------------------------------------
// Replaces the C++ lambda used in qsort.
static int compare_fd_entries(const void* a, const void* b) {
  const tsdb_oldest_fd_entry_t* A = (const tsdb_oldest_fd_entry_t*)a;
  const tsdb_oldest_fd_entry_t* B = (const tsdb_oldest_fd_entry_t*)b;
  if (A->start_time < B->start_time) return -1;
  if (A->start_time > B->start_time) return 1;
  return 0;
}

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

/**
 * @brief Expiration routine that will remove old data if the current usage
 *        is above @p usage_threshold, aiming to free a specific fraction
 *        of the total partition size (@p reduction_threshold).
 *
 * @param db                  Database handle
 * @param usage_threshold     If usage is below this fraction, no expiration
 *                            is performed
 * @param reduction_threshold Fraction (0..1) of the partition size we aim
 *                            to free. E.g. 0.05 => 5% of total partition.
 *
 * @return true if the expiration step succeeded or was not needed, false
 * otherwise.
 */
bool timeseries_expiration_run(timeseries_db_t* db, float usage_threshold, float reduction_threshold) {
  // Validate arguments
  if (!db || usage_threshold <= 0.0f || usage_threshold >= 1.0f || reduction_threshold <= 0.0f ||
      reduction_threshold >= 1.0f) {
    ESP_LOGE(TAG, "Invalid arguments to expiration_run");
    return false;
  }

  // 1) Check if usage is above threshold
  float used_ratio;
  if (!get_usage_ratio(db, &used_ratio)) {
    ESP_LOGE(TAG, "Failed to get usage ratio");
    return false;
  }
  ESP_LOGI(TAG, "Current usage=%.2f%%, threshold=%.2f%%", used_ratio * 100.0f, usage_threshold * 100.0f);

  if (used_ratio < usage_threshold) {
    // Nothing to do
    ESP_LOGI(TAG, "Usage < threshold => no expiration needed");
    return true;
  }

  // 2) Calculate how many bytes we want to free in this expiration pass
  //    e.g. if partition is 1 MB and reduction_threshold is 0.05,
  //    we aim to free 0.05 * 1MB = 50 KB.
  uint32_t partition_size = db->partition->size;
  uint32_t bytes_to_free = (uint32_t)((float)partition_size * reduction_threshold);
  if (bytes_to_free == 0) {
    // If rounding caused it to be 0, just exit early or pick a minimum
    bytes_to_free = 1;
  }
  ESP_LOGI(TAG, "Target: freeing at least %u bytes (reduction_threshold=%.2f%%).", (unsigned int)bytes_to_free,
           reduction_threshold * 100.0f);

  // 3) Gather the 25 oldest (non-deleted) field-data entries by `start_time`
  tsdb_oldest_fd_entry_t top25[25];  // static array for max-25
  memset(top25, 0, sizeof(top25));
  size_t found_count = 0;

  if (!gather_25_oldest_fielddata(db, top25, &found_count)) {
    ESP_LOGE(TAG, "Failed scanning DB to find oldest field-data entries.");
    return false;
  }
  if (found_count == 0) {
    ESP_LOGI(TAG, "No active field-data records => nothing to expire.");
    return true;
  }

  ESP_LOGI(TAG, "Found %zu oldest field-data record(s).", found_count);

  // 4) Mark them as deleted, oldest-first, until we've freed enough space
  tsdb_modified_pages_list_t modified;
  modified_pages_list_init(&modified);

  mark_entries_deleted(db, top25, found_count, bytes_to_free, &modified);

  // 5) If any pages got modified, run a page-list compaction
  if (modified.used > 0) {
    ESP_LOGI(TAG, "Compacting %zu modified pages...", modified.used);
    if (!timeseries_compact_page_list(db, modified.pages, modified.used)) {
      ESP_LOGE(TAG, "timeseries_compact_page_list failed");
      modified_pages_list_free(&modified);
      return false;
    }
    ESP_LOGI(TAG, "Compaction of modified pages complete.");
  } else {
    ESP_LOGI(TAG, "No pages were modified => no compaction needed.");
  }

  modified_pages_list_free(&modified);
  return true;
}

// -----------------------------------------------------------------------------
// Internal Helpers
// -----------------------------------------------------------------------------

/**
 * @brief Return usage as a fraction (0.0..1.0).
 */
static bool get_usage_ratio(timeseries_db_t* db, float* out_ratio) {
  if (!db || !out_ratio) {
    return false;
  }
  uint32_t partition_size = db->partition->size;
  uint32_t used_space = tsdb_pagecache_get_total_active_size(db);
  *out_ratio = (float)used_space / (float)partition_size;
  return true;
}

/* --------------------------------------------------------------------------
 * A small Max-Heap (size <= 25)
 *
 * - The "largest" entry is at the top (heap[0]).
 * - We only want to keep the 25 oldest items. "Oldest" means smallest
 *   start_time, so "newer" (larger start_time) is higher priority in
 *   the max-heap.
 *
 * We'll define a few static inline helpers for clarity.
 * -------------------------------------------------------------------------- */
static inline void swap_fd_entries(tsdb_oldest_fd_entry_t* a, tsdb_oldest_fd_entry_t* b) {
  tsdb_oldest_fd_entry_t tmp = *a;
  *a = *b;
  *b = tmp;
}

/**
 * @brief Sift up a newly inserted node in a max-heap
 */
static void heap_sift_up(tsdb_oldest_fd_entry_t* heap, size_t idx) {
  while (idx > 0) {
    size_t parent = (idx - 1) / 2;
    if (heap[parent].start_time >= heap[idx].start_time) {
      // parent is older or same => no swap needed
      break;
    }
    // else swap
    swap_fd_entries(&heap[parent], &heap[idx]);
    idx = parent;
  }
}

/**
 * @brief Sift down the root in a max-heap
 */
static void heap_sift_down(tsdb_oldest_fd_entry_t* heap, size_t count, size_t idx) {
  while (true) {
    size_t left = 2 * idx + 1;
    size_t right = 2 * idx + 2;
    size_t largest = idx;

    if (left < count && heap[left].start_time > heap[largest].start_time) {
      largest = left;
    }
    if (right < count && heap[right].start_time > heap[largest].start_time) {
      largest = right;
    }
    if (largest == idx) {
      // no change
      break;
    }
    swap_fd_entries(&heap[idx], &heap[largest]);
    idx = largest;
  }
}

/**
 * @brief Insert a new entry into the max-heap (size <= 25).
 * @param heap       [in/out] the array
 * @param size       [in/out] current number of elements
 * @param entry      the new entry
 *
 * If size < 25, we just push. If size == 25 and the new entry is older
 * (start_time < largest), we pop the largest and insert the new entry.
 * If the new entry is newer than or equal to the largest, we do nothing.
 */
static void heap_insert_max25(tsdb_oldest_fd_entry_t* heap, size_t* size, const tsdb_oldest_fd_entry_t* entry) {
  // If we have fewer than 25, just push it
  if (*size < 25) {
    heap[*size] = *entry;
    heap_sift_up(heap, *size);
    (*size)++;
    return;
  }

  // If we're at capacity=25, compare to the largest (heap[0]).
  // If the new entry's start_time is >= the largest, do nothing
  // because it is "newer" => not among the "oldest".
  if (entry->start_time >= heap[0].start_time) {
    return;
  }

  // Otherwise, remove the largest (heap[0]), put the new entry at [0], sift
  // down
  heap[0] = *entry;
  heap_sift_down(heap, 25, 0);
}

/**
 * @brief Iterate over all active field-data records and maintain a max-heap
 *        of size up to 25 by their start_time (we only keep the 25 oldest).
 *
 * At the end, the heap contains the 25 smallest start_times, but in "max-heap"
 * form (the root is the largest among them). The count is how many we found
 * (<=25).
 *
 * @param db         [in]  database handle
 * @param heap_25    [out] array of up to 25 entries (as a max-heap)
 * @param out_count  [out] how many entries were actually added
 */
static bool gather_25_oldest_fielddata(timeseries_db_t* db, tsdb_oldest_fd_entry_t* heap_25, size_t* out_count) {
  if (!db || !heap_25 || !out_count) {
    return false;
  }
  *out_count = 0;

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    ESP_LOGE(TAG, "Failed to init page_cache iterator");
    return false;
  }

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  // We'll iterate over every active field-data record, but
  // we only keep track of the 25 oldest in the max-heap.
  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset, &page_size)) {
    // Check for an active field-data page
    if (hdr.magic_number == TIMESERIES_MAGIC_NUM && hdr.page_type == TIMESERIES_PAGE_TYPE_FIELD_DATA &&
        hdr.page_state == TIMESERIES_PAGE_STATE_ACTIVE) {
      timeseries_fielddata_iterator_t f_iter;
      if (!timeseries_fielddata_iterator_init(db, page_offset, page_size, &f_iter)) {
        continue;  // skip
      }

      timeseries_field_data_header_t fd_hdr;
      while (timeseries_fielddata_iterator_next(&f_iter, &fd_hdr)) {
        // If this bit is cleared => record is already "deleted" => skip
        if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
          continue;
        }
        // Otherwise it's active => consider it
        tsdb_oldest_fd_entry_t candidate;
        candidate.start_time = fd_hdr.start_time;
        candidate.page_offset = page_offset;
        candidate.record_offset = f_iter.current_record_offset;
        // Make sure your actual field name matches what's in your header:
        candidate.record_length = fd_hdr.record_length;

        // Insert into the max-heap
        heap_insert_max25(heap_25, out_count, &candidate);
      }
    }
  }

  return true;
}

/**
 * @brief Mark each entry as deleted (in ascending start_time order), stopping
 *        once we've freed at least @p bytes_to_free or run out of entries.
 *
 * Since the array is currently in max-heap form (largest at the root), we
 * must sort it by ascending start_time, then iterate from the truly oldest
 * to the newest in that top-25.
 *
 * @param db              DB handle
 * @param entries         Up to 25 entries (in max-heap order)
 * @param count           Number of entries in `entries`
 * @param bytes_to_free   Target number of bytes to remove from use
 * @param modified_out    Collects the set of pages we changed
 */
static void mark_entries_deleted(timeseries_db_t* db, const tsdb_oldest_fd_entry_t* entries, size_t count,
                                 uint32_t bytes_to_free, tsdb_modified_pages_list_t* modified_out) {
  if (count == 0) {
    return;
  }

  // 1) Copy to a small array we can sort by ascending start_time
  tsdb_oldest_fd_entry_t* sorted = calloc(count, sizeof(*sorted));
  if (!sorted) {
    ESP_LOGE(TAG, "OOM copying top-25 entries");
    return;  // fallback => do nothing
  }
  memcpy(sorted, entries, count * sizeof(*sorted));

  // 2) Sort ascending by start_time
  qsort(sorted, count, sizeof(tsdb_oldest_fd_entry_t), compare_fd_entries);

  // 3) Go oldest to newest, marking each as deleted, until we've freed enough
  uint32_t sum_freed = 0;
  for (size_t i = 0; i < count; i++) {
    if (sum_freed >= bytes_to_free) {
      ESP_LOGI(TAG, "Freed %u bytes, which meets or exceeds the target %u.", (unsigned int)sum_freed,
               (unsigned int)bytes_to_free);
      break;
    }
    uint32_t record_hdr_abs = sorted[i].page_offset + sorted[i].record_offset;
    if (!mark_record_deleted(db, record_hdr_abs)) {
      ESP_LOGW(TAG, "Failed marking record as deleted @0x%08" PRIx32, record_hdr_abs);
      continue;
    }

    // If success, add to sum and track that page in modified_out
    sum_freed += sorted[i].record_length;
    modified_pages_list_add(modified_out, sorted[i].page_offset);
  }

  if (sum_freed < bytes_to_free) {
    ESP_LOGI(TAG,
             "We ran out of candidate records to remove. Freed %u bytes, below "
             "the desired %u bytes.",
             (unsigned int)sum_freed, (unsigned int)bytes_to_free);
  }

  free(sorted);
}

/**
 * @brief Actually clear the TSDB_FIELDDATA_FLAG_DELETED bit (1->0)
 *        in flash for a single record, thus marking it "deleted."
 *
 * @param db             DB handle
 * @param record_hdr_abs Absolute offset to the timeseries_field_data_header_t
 *
 * @return true on success, false on error
 */
static bool mark_record_deleted(timeseries_db_t* db, uint32_t record_hdr_abs) {
  timeseries_field_data_header_t fd_hdr;
  esp_err_t err = esp_partition_read(db->partition, record_hdr_abs, &fd_hdr, sizeof(fd_hdr));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed reading record header @0x%08" PRIx32 " (err=0x%x)", record_hdr_abs, err);
    return false;
  }

  // Check if it's already cleared
  if ((fd_hdr.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
    // Already deleted => no action needed
    return true;
  }

  // Clear the bit
  fd_hdr.flags &= ~(TSDB_FIELDDATA_FLAG_DELETED);

  err = esp_partition_write(db->partition, record_hdr_abs + offsetof(timeseries_field_data_header_t, flags),
                            &fd_hdr.flags, sizeof(fd_hdr.flags));
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "Failed clearing DELETED bit @0x%08" PRIx32 " (err=0x%x)", record_hdr_abs, err);
    return false;
  }

  ESP_LOGW(TAG, "Record @0x%08" PRIx32 " => DELETED bit cleared.", record_hdr_abs);
  return true;
}

// -----------------------------------------------------------------------------
// Modified Pages List
// -----------------------------------------------------------------------------

static void modified_pages_list_init(tsdb_modified_pages_list_t* list) {
  list->pages = NULL;
  list->used = 0;
  list->capacity = 0;
}

static void modified_pages_list_free(tsdb_modified_pages_list_t* list) {
  if (!list) {
    return;
  }
  free(list->pages);
  list->pages = NULL;
  list->used = 0;
  list->capacity = 0;
}

/**
 * @brief Ensure uniqueness of page_offset in the list, then append if not
 *        found.
 */
static void modified_pages_list_add(tsdb_modified_pages_list_t* list, uint32_t page_offset) {
  for (size_t i = 0; i < list->used; i++) {
    if (list->pages[i] == page_offset) {
      return;
    }
  }
  if (list->used == list->capacity) {
    size_t newcap = (list->capacity == 0) ? 8 : list->capacity * 2;
    uint32_t* grow = (uint32_t*)realloc(list->pages, newcap * sizeof(uint32_t));
    if (!grow) {
      ESP_LOGE(TAG, "OOM growing modified_pages_list");
      return;
    }
    list->pages = grow;
    list->capacity = newcap;
  }
  list->pages[list->used++] = page_offset;
}
