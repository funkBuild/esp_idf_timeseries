#include "timeseries_revision.h"
#include "timeseries_iterator.h"
#include "alp/alp_int_codec.h"
#include <stdlib.h>
#include <string.h>

bool tsdb_revision_next_locked(timeseries_db_t *db, uint64_t *revision) {
  if (!db || !revision || db->write_revision == UINT64_MAX) return false;
  *revision = ++db->write_revision;
  return true;
}

/* Validate before allocating: each packed run is a uint16 count and uint64
 * revision. The final uint64 maximum is shared with the scalar/ALP formats. */
static bool decode_runs(const uint8_t *data, size_t bytes, uint16_t count, tsdb_revision_t *revision) {
  if (!count || !bytes || bytes % 10) return false;
  size_t written = 0;
  uint64_t maximum = 0;
  for (size_t offset = 0; offset < bytes; offset += 10) {
    uint16_t run; uint64_t value;
    memcpy(&run, data + offset, sizeof(run));
    memcpy(&value, data + offset + sizeof(run), sizeof(value));
    if (!run || run > count - written) return false;
    written += run;
    if (value > maximum) maximum = value;
  }
  if (written != count || maximum != revision->value) return false;
  uint64_t *values = malloc((size_t)count * sizeof(*values));
  if (!values) return false;
  written = 0;
  for (size_t offset = 0; offset < bytes; offset += 10) {
    uint16_t run; uint64_t value;
    memcpy(&run, data + offset, sizeof(run));
    memcpy(&value, data + offset + sizeof(run), sizeof(value));
    for (size_t end = written + run; written < end; written++) values[written] = value;
  }
  revision->values = values;
  return true;
}

bool tsdb_revision_decode(uint8_t flags, const uint8_t *data, size_t length,
                           uint16_t count, tsdb_revision_t *revision) {
  if (!revision) return false;
  memset(revision, 0, sizeof(*revision));
  if (flags & TSDB_FIELDDATA_FLAG_REVISION) return true;
  if (!data || length < sizeof(uint64_t)) return false;
  memcpy(&revision->value, data + length - sizeof(uint64_t), sizeof(uint64_t));
  if (flags & TSDB_FIELDDATA_FLAG_REVISION_VECTOR) return length == sizeof(uint64_t);
  if (!(flags & TSDB_FIELDDATA_FLAG_REVISION_RLE))
    return decode_runs(data, length - sizeof(uint64_t), count, revision);
  // The general ALP decoder permits prefix reads. A revision column must
  // describe exactly this record, so also validate its stream count.
  if (!count || length < 20 + sizeof(uint64_t)) return false;
  uint32_t encoded_count;
  memcpy(&encoded_count, data + sizeof(uint32_t), sizeof(encoded_count));
  if (encoded_count != count) return false;
  uint64_t *values = malloc((size_t)count * sizeof(*values));
  if (!values) return false;
  if (!alp_decode_int(data, length - sizeof(uint64_t), (int64_t *)values, count)) {
    free(values); return false;
  }
  uint64_t maximum = 0;
  for (size_t i = 0; i < count; i++) if (values[i] > maximum) maximum = values[i];
  if (maximum != revision->value) { free(values); return false; }
  revision->values = values;
  return true;
}

bool tsdb_revision_read(timeseries_db_t *db, uint32_t offset, uint32_t length,
                         uint32_t columns_length, uint16_t count, uint8_t flags, tsdb_revision_t *revision) {
  if (!revision) return false;
  memset(revision, 0, sizeof(*revision));
  if (flags & TSDB_FIELDDATA_FLAG_REVISION) return true;
  if (!db || !db->partition || offset > db->partition->size || length > db->partition->size - offset ||
      columns_length > length || length - columns_length < sizeof(uint64_t)) return false;
  size_t bytes = length - columns_length;
  uint64_t scalar;
  uint8_t *buffer = bytes == sizeof(scalar) ? (uint8_t *)&scalar : malloc(bytes);
  if (!buffer) return false;
  bool ok = esp_partition_read(db->partition, offset + columns_length, buffer, bytes) == ESP_OK &&
            tsdb_revision_decode(flags, buffer, bytes, count, revision);
  if (buffer != (uint8_t *)&scalar) free(buffer);
  return ok;
}

/* Exact byte count for the existing delta-FFOR stream, without constructing
 * deltas or packing buffers. Needed only when the cheap size bounds overlap. */
static size_t alp_revision_size(const uint64_t *values, size_t count) {
  size_t bytes = 20;
  for (size_t first = 0; first < count; first += ALP_INT_BLOCK_SIZE) {
    size_t n = count - first;
    if (n > ALP_INT_BLOCK_SIZE) n = ALP_INT_BLOCK_SIZE;
    int64_t minimum = INT64_MAX, maximum = INT64_MIN;
    for (size_t i = first; i < first + n; i++) {
      uint64_t bits = i ? values[i] - values[i-1] : 0;
      int64_t delta;
      memcpy(&delta, &bits, sizeof(delta));
      if (delta < minimum) minimum = delta;
      if (delta > maximum) maximum = delta;
    }
    uint64_t span = (uint64_t)maximum - (uint64_t)minimum;
    unsigned width = span ? 64 - __builtin_clzll(span) : 0;
    bytes += 16 + ((n * width + 63) / 64) * sizeof(uint64_t);
  }
  return bytes;
}

static bool encode_runs(const uint64_t *values, size_t count, size_t runs, uint64_t maximum,
                         uint8_t **encoded, size_t *length, uint8_t *flags) {
  size_t bytes = runs * 10 + sizeof(maximum);
  uint8_t *buffer = malloc(bytes);
  if (!buffer) return false;
  size_t offset = 0;
  for (size_t first = 0; first < count;) {
    size_t end = first + 1;
    while (end < count && values[end] == values[first]) end++;
    uint16_t run = (uint16_t)(end - first); // count is already bounded by UINT16_MAX
    memcpy(buffer + offset, &run, sizeof(run));
    memcpy(buffer + offset + sizeof(run), &values[first], sizeof(uint64_t));
    offset += 10;
    first = end;
  }
  memcpy(buffer + offset, &maximum, sizeof(maximum));
  *encoded = buffer; *length = bytes;
  *flags &= ~(TSDB_FIELDDATA_FLAG_REVISION_VECTOR | TSDB_FIELDDATA_FLAG_REVISION_RLE);
  return true;
}

bool tsdb_revision_encode(const uint64_t *values, size_t count, uint64_t scalar,
                           uint8_t **encoded, size_t *length, uint8_t *flags) {
  if (!encoded || !length || !flags) return false;
  *encoded = NULL; *length = 0;
  if (count > UINT16_MAX) return false;
  uint64_t maximum = values && count ? values[0] : scalar;
  size_t runs = 1;
  for (size_t i = 1; values && i < count; i++) {
    if (values[i] != values[i-1]) runs++;
    if (values[i] > maximum) maximum = values[i];
  }
  *flags &= ~TSDB_FIELDDATA_FLAG_REVISION;
  *flags |= TSDB_FIELDDATA_FLAG_REVISION_VECTOR | TSDB_FIELDDATA_FLAG_REVISION_RLE;
  if (runs == 1) {
    uint8_t *buffer = malloc(sizeof(maximum));
    if (!buffer) return false;
    memcpy(buffer, &maximum, sizeof(maximum));
    *encoded = buffer; *length = sizeof(maximum);
    return true;
  }
  size_t blocks = (count + ALP_INT_BLOCK_SIZE - 1) / ALP_INT_BLOCK_SIZE;
  size_t alp_minimum = 20 + blocks * 16;
  size_t run_bytes = runs * 10;
  bool use_runs = run_bytes <= alp_minimum;
  if (!use_runs && run_bytes <= alp_minimum + count * sizeof(uint64_t))
    use_runs = run_bytes <= alp_revision_size(values, count);
  if (use_runs) return encode_runs(values, count, runs, maximum, encoded, length, flags);

  size_t bytes = alp_encode_int((const int64_t *)values, count, encoded);
  if (!bytes) return false;
  if (bytes > SIZE_MAX - sizeof(maximum)) { free(*encoded); *encoded = NULL; return false; }
  uint8_t *buffer = realloc(*encoded, bytes + sizeof(maximum));
  if (!buffer) { free(*encoded); *encoded = NULL; return false; }
  memcpy(buffer + bytes, &maximum, sizeof(maximum));
  *encoded = buffer; *length = bytes + sizeof(maximum);
  *flags &= ~TSDB_FIELDDATA_FLAG_REVISION_VECTOR;
  return true;
}

bool tsdb_revision_recover(timeseries_db_t *db) {
  if (!db || !db->partition) return false;
  timeseries_page_cache_iterator_t pages;
  if (!timeseries_page_cache_iterator_init(db, &pages)) return false;
  uint8_t *buffer = NULL;
  uint64_t maximum = 0;
  timeseries_page_header_t page;
  uint32_t offset, size;
  bool ok = true;
  while (ok && timeseries_page_cache_iterator_next(&pages, &page, &offset, &size)) {
    if (page.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA || page.page_state != TIMESERIES_PAGE_STATE_ACTIVE) continue;
    if (!buffer) {
      buffer = malloc(4096);
      if (!buffer) { ok = false; break; }
    }
    timeseries_fielddata_iterator_t fields;
    if (!timeseries_fielddata_iterator_init_window(db, offset, size, buffer, 4096, &fields)) { ok = false; break; }
    timeseries_field_data_header_t header;
    while (timeseries_fielddata_iterator_next(&fields, &header)) {
      if (!(header.flags & TSDB_FIELDDATA_FLAG_DELETED)) continue;
      uint32_t data_offset = offset + fields.current_record_offset + sizeof(header);
      uint64_t revision = tsdb_legacy_revision(page.sequence_num, data_offset);
      if (!(header.flags & TSDB_FIELDDATA_FLAG_REVISION)) {
        if (header.record_length < sizeof(uint64_t) ||
            !timeseries_fielddata_iterator_read_at(&fields,
                data_offset - offset + header.record_length - sizeof(uint64_t), &revision, sizeof(revision))) { ok = false; break; }
      }
      if (revision > maximum) maximum = revision;
    }
    ok = ok && !fields.read_error;
    timeseries_fielddata_iterator_deinit(&fields);
  }
  free(buffer);
  timeseries_page_cache_iterator_deinit(&pages);
  if (ok) db->write_revision = maximum;
  return ok;
}
