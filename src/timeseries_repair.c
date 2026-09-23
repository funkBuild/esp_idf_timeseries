/* Startup repair of field-data records torn by an interrupted write.
 *
 * A record is written with one esp_partition_write, which the flash driver
 * splits into several program operations issued in ascending address order
 * (32-byte chunks from a PSRAM buffer, at most 64 bytes per page program). A
 * reset between operations therefore leaves a prefix of the record programmed
 * and the rest erased; a reset inside one operation leaves that operation's
 * bytes as a bitwise superset of their targets. The format has no CRC, so a
 * torn record is recognised by what it can no longer be: column lengths that
 * do not match the record, or a revision trailer outside the plausible range
 * (see tsdb_revision_plausible).
 *
 * Repair only clears bits, within the existing format, so 1.0.19 reads the
 * result: a torn record is marked deleted (the same bit timeseries_delete
 * clears), and a torn header whose length overflows the page becomes a deleted
 * filler reaching the end of the page. When that length cannot be reached by
 * clearing bits, the page's intact prefix is copied to a new page instead. */
#include "timeseries_repair.h"

#include "esp_log.h"
#include "timeseries_compaction.h"
#include "timeseries_data.h"
#include "timeseries_flash_utils.h"
#include "timeseries_iterator.h"
#include "timeseries_page_cache.h"
#include "timeseries_revision.h"
#include <inttypes.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "TSDB_REPAIR";

typedef enum { RECORD_INTACT, RECORD_TORN, RECORD_READ_FAILED } record_state_t;

static record_state_t classify_record(timeseries_db_t *db, uint32_t data_offset,
                                      const timeseries_field_data_header_t *h) {
  uint32_t length = h->record_length;
  bool compressed = (h->flags & TSDB_FIELDDATA_FLAG_COMPRESSED) == 0;
  bool has_revision = (h->flags & TSDB_FIELDDATA_FLAG_REVISION) == 0;
  bool scalar = (h->flags & TSDB_FIELDDATA_FLAG_REVISION_VECTOR) != 0;
  timeseries_col_data_header_t columns;
  if (length < sizeof(columns)) return RECORD_TORN;
  if (esp_partition_read(db->partition, data_offset, &columns, sizeof(columns)) != ESP_OK) return RECORD_READ_FAILED;
  // Subtraction-only bounds, as in the points iterator: the lengths are raw flash.
  if (columns.ts_len > length - sizeof(columns) || columns.val_len > length - sizeof(columns) - columns.ts_len)
    return RECORD_TORN;
  uint32_t columns_length = sizeof(columns) + columns.ts_len + columns.val_len;
  uint32_t tail = length - columns_length;
  if (!compressed) {
    // Every writer (1.0.19 and later) sizes uncompressed records exactly.
    if (columns.ts_len != (uint32_t)h->record_count * sizeof(uint64_t)) return RECORD_TORN;
    if (!has_revision) return tail == 0 ? RECORD_INTACT : RECORD_TORN;
  } else if (!has_revision) {
    return RECORD_INTACT;  // legacy compressed record: nothing more to check
  }
  if (scalar ? tail != sizeof(uint64_t) : tail <= sizeof(uint64_t)) return RECORD_TORN;
  uint64_t trailer;
  if (esp_partition_read(db->partition, data_offset + length - sizeof(trailer), &trailer, sizeof(trailer)) != ESP_OK)
    return RECORD_READ_FAILED;
  if (tsdb_revision_plausible(db, trailer)) return RECORD_INTACT;
  if (scalar) return RECORD_TORN;
  // An implausible vector maximum is either torn or a complete record that
  // inherited a torn source's revision from an older build. Only the first
  // fails to decode (the stored maximum must equal the decoded one).
  tsdb_revision_t revision;
  bool decoded = tsdb_revision_read(db, data_offset, length, columns_length, h->record_count, h->flags, &revision);
  free(revision.values);
  return decoded ? RECORD_INTACT : RECORD_TORN;
}

static bool mark_deleted(timeseries_db_t *db, uint32_t record_offset, uint8_t flags) {
  return tsdb_flash_write_byte(db->partition, record_offset + offsetof(timeseries_field_data_header_t, flags),
                               flags & ~TSDB_FIELDDATA_FLAG_DELETED) == ESP_OK;
}

/* The iterator stopped at a header whose length overflows the page. Returns
 * true when the page is now well formed (or was retired by the salvage copy). */
static bool repair_overflow(timeseries_db_t *db, uint32_t page_offset, uint32_t page_size, uint32_t record) {
  timeseries_field_data_header_t h;
  if (record > page_size || page_size - record < sizeof(h) ||
      esp_partition_read(db->partition, page_offset + record, &h, sizeof(h)) != ESP_OK)
    return false;
  if (sizeof(h) + (uint32_t)h.record_length <= page_size - record) return false;  // not an overflow: read error
  uint32_t fill = page_size - record - sizeof(h);
  if (fill <= UINT16_MAX && (h.record_length & fill) == fill) {
    // Deleted filler to the end of the page: flags and record_length are
    // adjacent (bytes 0..4), and both only lose bits.
    uint8_t bytes[offsetof(timeseries_field_data_header_t, record_length) + sizeof(uint16_t)];
    memcpy(bytes, &h, sizeof(bytes));
    bytes[0] &= ~TSDB_FIELDDATA_FLAG_DELETED;
    uint16_t length = (uint16_t)fill;
    memcpy(bytes + offsetof(timeseries_field_data_header_t, record_length), &length, sizeof(length));
    if (esp_partition_write(db->partition, page_offset + record, bytes, sizeof(bytes)) == ESP_OK) {
      ESP_LOGW(TAG, "Torn record header @0x%08" PRIx32 " turned into a deleted filler", page_offset + record);
      return true;
    }
    ESP_LOGE(TAG, "Failed writing filler @0x%08" PRIx32, page_offset + record);
  }
  if (timeseries_salvage_page_prefix(db, page_offset)) {
    ESP_LOGW(TAG, "Page @0x%08" PRIx32 " with a torn record header copied without its tail", page_offset);
    return true;
  }
  ESP_LOGE(TAG, "Could not repair torn record header @0x%08" PRIx32 "; page left read-only", page_offset + record);
  return false;
}

bool tsdb_repair_torn_records(timeseries_db_t *db) {
  if (!db || !db->partition) return false;
  // Collect first: salvage changes the page cache.
  size_t count = 0, capacity = 0;
  uint32_t (*pages)[2] = NULL;
  timeseries_page_cache_iterator_t it;
  if (!timeseries_page_cache_iterator_init(db, &it)) return false;
  timeseries_page_header_t page;
  uint32_t offset, size;
  bool ok = true;
  while (timeseries_page_cache_iterator_next(&it, &page, &offset, &size)) {
    if (page.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA || page.page_state != TIMESERIES_PAGE_STATE_ACTIVE)
      continue;
    if (count == capacity) {
      size_t grown = capacity ? capacity * 2 : 16;
      void *p = realloc(pages, grown * sizeof(*pages));
      if (!p) { ok = false; break; }
      pages = p; capacity = grown;
    }
    pages[count][0] = offset;
    pages[count][1] = size;
    count++;
  }
  timeseries_page_cache_iterator_deinit(&it);
  uint8_t *window = ok ? malloc(4096) : NULL;
  if (!window) { free(pages); return false; }

  size_t neutralised = 0;
  for (size_t i = 0; i < count; i++) {
    uint32_t page_offset = pages[i][0], page_size = pages[i][1];
    timeseries_fielddata_iterator_t f;
    if (!timeseries_fielddata_iterator_init_window(db, page_offset, page_size, window, 4096, &f)) continue;
    timeseries_field_data_header_t h;
    while (timeseries_fielddata_iterator_next(&f, &h)) {
      if (!(h.flags & TSDB_FIELDDATA_FLAG_DELETED)) continue;
      uint32_t record = page_offset + f.current_record_offset;
      record_state_t state = classify_record(db, record + sizeof(h), &h);
      if (state != RECORD_TORN) continue;
      if (mark_deleted(db, record, h.flags)) {
        ESP_LOGW(TAG, "Torn record @0x%08" PRIx32 " (len=%u count=%u) marked deleted", record,
                 (unsigned)h.record_length, (unsigned)h.record_count);
        neutralised++;
      } else {
        ESP_LOGE(TAG, "Failed marking torn record @0x%08" PRIx32 " deleted", record);
      }
    }
    bool overflow = f.read_error;
    uint32_t stop = f.current_record_offset;
    timeseries_fielddata_iterator_deinit(&f);
    if (overflow && repair_overflow(db, page_offset, page_size, stop)) neutralised++;
  }
  free(window);
  free(pages);
  if (neutralised) ESP_LOGW(TAG, "Neutralised %u torn field-data record(s)", (unsigned)neutralised);
  return true;
}
