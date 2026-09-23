#include "timeseries_compaction_merge.h"
#include "timeseries_data.h"
#include "timeseries_iterator.h"
#include "timeseries_metadata.h"
#include "timeseries_page_stream_writer.h"
#include "timeseries_points_iterator.h"
#include "alp/alp_encoder.h"
#include "alp/alp_int_codec.h"
#include "esp_log.h"
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

static const char* TAG = "TimeseriesCompactionMerge";

/* Descriptors scale with records; decoded storage is needed only for records
 * whose timestamp ranges overlap the current merge position. Raw records use
 * fixed-size read windows instead of copying their complete columns. */
#define RAW_WINDOW_POINTS 64
#define ISOLATED_WINDOW_POINTS 512
#define OUTPUT_CHUNK_POINTS 1024
#define STRING_CHUNK_BYTES 8192

typedef struct {
  uint8_t series[16];
  uint64_t first, last;
  uint32_t offset, sequence;
  uint16_t count, length;
  uint8_t flags;
  timeseries_field_type_e type;
} merge_record_t;

typedef struct {
  const merge_record_t* record;
  timeseries_points_iterator_t* decoded;
  uint8_t* raw;
  uint32_t ts_offset, val_offset, val_end;
  size_t read, cached, index, width, window_points;
  uint64_t timestamp;
  tsdb_revision_t revisions;
  uint64_t revision;
  timeseries_field_value_t value;
} merge_cursor_t;

static void value_free(timeseries_field_value_t* v) {
  if (v->type == TIMESERIES_FIELD_TYPE_STRING) free(v->data.string_val.str);
  memset(v, 0, sizeof(*v));
}

static void cursor_free(merge_cursor_t* c) {
  if (!c) return;
  value_free(&c->value);
  if (c->decoded) {
    timeseries_points_iterator_deinit(c->decoded);
    free(c->decoded);
  }
  free(c->revisions.values);
  free(c->raw);
  free(c);
}

/* Called exactly record_count times. A decode failure is an error, never EOF. */
static bool cursor_read(timeseries_db_t* db, merge_cursor_t* c) {
  uint64_t ts;
  timeseries_field_value_t v = {.type = c->record->type};
  if (c->decoded) {
    if (!timeseries_points_iterator_next_timestamp(c->decoded, &ts) ||
        !timeseries_points_iterator_next_value(c->decoded, &v))
      return false;
  } else {
    if (c->index == c->cached) {
      c->cached = c->record->count - c->read;
      if (c->cached > c->window_points) c->cached = c->window_points;
      c->index = 0;
      if (esp_partition_read(db->partition, c->ts_offset + c->read * 8, c->raw, c->cached * 8) != ESP_OK) return false;
      if (c->width && esp_partition_read(db->partition, c->val_offset + c->read * c->width,
                                         c->raw + c->window_points * 8, c->cached * c->width) != ESP_OK)
        return false;
    }
    memcpy(&ts, c->raw + c->index * 8, 8);
    const uint8_t* p = c->raw + c->window_points * 8 + c->index * c->width;
    switch (v.type) {
      case TIMESERIES_FIELD_TYPE_INT:
        memcpy(&v.data.int_val, p, 8);
        break;
      case TIMESERIES_FIELD_TYPE_FLOAT:
        memcpy(&v.data.float_val, p, 8);
        break;
      case TIMESERIES_FIELD_TYPE_BOOL:
        v.data.bool_val = *p != 0;
        break;
      case TIMESERIES_FIELD_TYPE_STRING: {
        uint32_t len;
        if (c->val_offset > c->val_end || c->val_end - c->val_offset < 4 ||
            esp_partition_read(db->partition, c->val_offset, &len, 4) != ESP_OK)
          return false;
        c->val_offset += 4;
        if (len > c->val_end - c->val_offset) return false;
        char* str = malloc((size_t)len + 1);
        if (!str) return false;
        if (len && esp_partition_read(db->partition, c->val_offset, str, len) != ESP_OK) {
          free(str);
          return false;
        }
        str[len] = 0;
        v.data.string_val.str = str;
        v.data.string_val.length = len;
        c->val_offset += len;
        break;
      }
      default:
        return false;
    }
    c->index++;
  }
  if (ts < c->record->first || ts > c->record->last || (c->read && ts < c->timestamp)) {
    value_free(&v);
    return false;
  }
  const tsdb_revision_t *revision = c->decoded ? &c->decoded->revisions : &c->revisions;
  bool has_revision = c->decoded ? c->decoded->has_revision : !(c->record->flags & TSDB_FIELDDATA_FLAG_REVISION);
  c->revision = has_revision ? tsdb_revision_at(revision, c->read)
      : tsdb_legacy_revision(c->record->sequence, c->record->offset);
  c->timestamp = ts;
  c->value = v;
  c->read++;
  return true;
}

static merge_cursor_t* cursor_open(timeseries_db_t* db, const merge_record_t* r, bool isolated) {
  merge_cursor_t* c = calloc(1, sizeof(*c));
  if (!c) return NULL;
  c->record = r;
  c->window_points = isolated ? ISOLATED_WINDOW_POINTS : RAW_WINDOW_POINTS;
  if (c->window_points > r->count) c->window_points = r->count;
  if ((r->flags & TSDB_FIELDDATA_FLAG_COMPRESSED) == 0) {
    c->decoded = calloc(1, sizeof(*c->decoded));
    if (!c->decoded ||
        !timeseries_points_iterator_init(db, r->offset, r->length, r->count, r->type, r->flags, c->decoded))
      goto fail;
  } else {
    timeseries_col_data_header_t h;
    if (r->length < sizeof(h) || r->offset > db->partition->size || r->length > db->partition->size - r->offset ||
        esp_partition_read(db->partition, r->offset, &h, sizeof(h)) != ESP_OK)
      goto fail;
    if (h.ts_len != (size_t)r->count * 8 || h.ts_len > r->length - sizeof(h) ||
        h.val_len > r->length - sizeof(h) - h.ts_len)
      goto fail;
    if (!tsdb_revision_read(db, r->offset, r->length, sizeof(h) + h.ts_len + h.val_len,
                             r->count, r->flags, &c->revisions)) goto fail;
    c->width = r->type == TIMESERIES_FIELD_TYPE_STRING ? 0 : r->type == TIMESERIES_FIELD_TYPE_BOOL ? 1 : 8;
    if (c->width && h.val_len < c->width * r->count) goto fail;
    c->ts_offset = r->offset + sizeof(h);
    c->val_offset = c->ts_offset + h.ts_len;
    c->val_end = c->val_offset + h.val_len;
    c->raw = malloc(c->window_points * (8 + c->width));
    if (!c->raw) goto fail;
  }
  if (!cursor_read(db, c)) goto fail;
  return c;
fail:
  cursor_free(c);
  return NULL;
}

static bool record_less(const merge_record_t* a, const merge_record_t* b) {
  int series = memcmp(a->series, b->series, 16);
  if (series) return series < 0;
  if (a->first != b->first) return a->first < b->first;
  if (a->sequence != b->sequence) return a->sequence < b->sequence;
  return a->offset < b->offset;
}

static void records_sift(merge_record_t* a, size_t root, size_t n) {
  merge_record_t item = a[root];
  while (root < n / 2) {
    size_t child = root * 2 + 1;
    if (child + 1 < n && record_less(&a[child], &a[child + 1])) child++;
    if (!record_less(&item, &a[child])) break;
    a[root] = a[child];
    root = child;
  }
  a[root] = item;
}
static void records_sort(merge_record_t* a, size_t n) {
  for (size_t i = n / 2; i; i--) records_sift(a, i - 1, n);
  for (size_t end = n; end > 1;) {
    --end;
    merge_record_t tmp = a[0];
    a[0] = a[end];
    a[end] = tmp;
    records_sift(a, 0, end);
  }
}

static void heap_push(merge_cursor_t** heap, size_t* n, merge_cursor_t* c) {
  size_t i = (*n)++;
  while (i && heap[(i - 1) / 2]->timestamp > c->timestamp) {
    heap[i] = heap[(i - 1) / 2];
    i = (i - 1) / 2;
  }
  heap[i] = c;
}
static void heap_fix(merge_cursor_t** heap, size_t n) {
  if (!n) return;
  merge_cursor_t* c = heap[0];
  size_t i = 0;
  while (i < n / 2) {
    size_t child = i * 2 + 1;
    if (child + 1 < n && heap[child + 1]->timestamp < heap[child]->timestamp) child++;
    if (c->timestamp <= heap[child]->timestamp) break;
    heap[i] = heap[child];
    i = child;
  }
  heap[i] = c;
}

typedef struct {
  uint64_t* timestamps;
  uint64_t* revisions;
  uint64_t scalar_revision;
  void* values;
  size_t count, capacity, string_bytes;
  timeseries_field_type_e type;
} output_chunk_t;

static void chunk_clear(output_chunk_t* out) {
  if (out->type == TIMESERIES_FIELD_TYPE_STRING) {
    string_array_t* strings = out->values;
    for (size_t i = 0; i < out->count; i++) free(strings[i].str);
  }
  out->count = out->string_bytes = 0;
}

static bool chunk_flush(timeseries_page_stream_writer_t* writer, const uint8_t* series, output_chunk_t* out) {
  if (!out->count) return true;
  bool ok = false;
  writer->point_revisions = out->revisions ? out->revisions : &out->scalar_revision;
  writer->point_revision_count = out->revisions ? out->count : 1;
  if (out->type == TIMESERIES_FIELD_TYPE_INT || out->type == TIMESERIES_FIELD_TYPE_FLOAT) {
    uint8_t *ts_encoded = NULL, *val_encoded = NULL;
    size_t ts_len = alp_encode_int((int64_t*)out->timestamps, out->count, &ts_encoded);
    size_t val_len = 0;
    if (ts_len)
      val_len = out->type == TIMESERIES_FIELD_TYPE_INT ? alp_encode_int(out->values, out->count, &val_encoded)
                                                       : alp_encode(out->values, out->count, &val_encoded);
    if (ts_len && val_len)
      ok = timeseries_page_stream_writer_write_alp_series(writer, series, ts_encoded, ts_len, val_encoded, val_len,
                                                          (uint16_t)out->count, out->timestamps[0],
                                                          out->timestamps[out->count - 1]);
    free(ts_encoded);
    free(val_encoded);
  } else {
    if (!timeseries_page_stream_writer_begin_series(writer, series, out->type)) goto done;
    for (size_t i = 0; i < out->count; i++)
      if (!timeseries_page_stream_writer_write_timestamp(writer, out->timestamps[i])) goto done;
    if (!timeseries_page_stream_writer_finalize_timestamp(writer)) goto done;
    for (size_t i = 0; i < out->count; i++) {
      timeseries_field_value_t v = {.type = out->type};
      if (out->type == TIMESERIES_FIELD_TYPE_BOOL)
        v.data.bool_val = ((bool*)out->values)[i];
      else {
        string_array_t* str = &((string_array_t*)out->values)[i];
        v.data.string_val.str = str->str;
        v.data.string_val.length = str->length;
      }
      if (!timeseries_page_stream_writer_write_value(writer, &v)) goto done;
    }
    ok = timeseries_page_stream_writer_end_series(writer);
  }
done:
  writer->point_revisions = NULL;
  writer->point_revision_count = 0;
  chunk_clear(out);
  return ok;
}

/* Transfers ownership, including when a flush fails. */
static bool chunk_put(timeseries_page_stream_writer_t* writer, const uint8_t* series, output_chunk_t* out, uint64_t ts,
                      timeseries_field_value_t* v, uint64_t revision) {
  if (out->count == out->capacity || (out->type == TIMESERIES_FIELD_TYPE_STRING && out->count &&
                                      v->data.string_val.length > STRING_CHUNK_BYTES - out->string_bytes)) {
    if (!chunk_flush(writer, series, out)) {
      value_free(v);
      return false;
    }
  }
  if (!out->count) out->scalar_revision = revision;
  if (!out->revisions && revision != out->scalar_revision) {
    out->revisions = malloc(out->capacity * sizeof(*out->revisions));
    if (!out->revisions) { value_free(v); return false; }
    for (size_t i = 0; i < out->count; i++) out->revisions[i] = out->scalar_revision;
  }
  out->timestamps[out->count] = ts;
  if (out->revisions) out->revisions[out->count] = revision;
  switch (out->type) {
    case TIMESERIES_FIELD_TYPE_INT:
      ((int64_t*)out->values)[out->count] = v->data.int_val;
      break;
    case TIMESERIES_FIELD_TYPE_FLOAT:
      ((double*)out->values)[out->count] = v->data.float_val;
      break;
    case TIMESERIES_FIELD_TYPE_BOOL:
      ((bool*)out->values)[out->count] = v->data.bool_val;
      break;
    case TIMESERIES_FIELD_TYPE_STRING: {
      string_array_t* str = &((string_array_t*)out->values)[out->count];
      str->str = v->data.string_val.str;
      str->length = v->data.string_val.length;
      out->string_bytes += str->length;
      v->data.string_val.str = NULL;
      break;
    }
    default:
      return false;
  }
  out->count++;
  if (out->string_bytes >= STRING_CHUNK_BYTES) return chunk_flush(writer, series, out);
  return true;
}

// Points before min_ts are left out of the output (expiration trims records
// that straddle its horizon); compaction passes 0.
static bool merge_series(timeseries_db_t* db, timeseries_page_stream_writer_t* writer, merge_record_t* records,
                         size_t n, uint64_t min_ts) {
  output_chunk_t out = {.capacity = TSDB_ALP_CHUNK_MAX_POINTS, .type = records[0].type};
  if (out.capacity > OUTPUT_CHUNK_POINTS) out.capacity = OUTPUT_CHUNK_POINTS;
  if (out.type == TIMESERIES_FIELD_TYPE_STRING && out.capacity > 128) out.capacity = 128;
  size_t width = out.type == TIMESERIES_FIELD_TYPE_STRING ? sizeof(string_array_t)
                 : out.type == TIMESERIES_FIELD_TYPE_BOOL ? sizeof(bool)
                                                          : 8;
  out.timestamps = malloc(out.capacity * 8);
  out.values = malloc(out.capacity * width);
  merge_cursor_t** heap = calloc(n, sizeof(*heap));
  bool ok = false, pending = false;
  size_t heap_count = 0, next = 0;
  uint64_t timestamp = 0;
  uint64_t revision = 0;
  timeseries_field_value_t value = {0};
  if (!out.timestamps || !out.values || !heap) goto done;
  while (next < n || heap_count) {
    // Equal starts may be consumed immediately: the pending output point
    // resolves ties, avoiding one live cursor per one-point duplicate record.
    while (next < n && (!heap_count || records[next].first < heap[0]->timestamp)) {
      if (records[next].type != out.type) goto done;
      // A disjoint record is the only live cursor. Use larger reads there
      // without multiplying that buffer by the overlapping-record count.
      bool isolated = heap_count == 0 && (next + 1 == n || records[next + 1].first > records[next].last);
      merge_cursor_t* c = cursor_open(db, &records[next++], isolated);
      if (!c) goto done;
      heap_push(heap, &heap_count, c);
    }
    merge_cursor_t* c = heap[0];
    uint64_t ts = c->timestamp;
    timeseries_field_value_t v = c->value;
    memset(&c->value, 0, sizeof(c->value));
    const merge_record_t* r = c->record;
    if (ts < min_ts) {
      value_free(&v);  // before the expiration horizon: not kept
    } else {
      if (pending && ts != timestamp) {
        if (!chunk_put(writer, records[0].series, &out, timestamp, &value, revision)) {
          value_free(&v);
          goto done;
        }
        pending = false;
      }
      if (!pending || c->revision >= revision) {
        value_free(&value);
        value = v;
        timestamp = ts;
        revision = c->revision;
        pending = true;
      } else {
        value_free(&v);
      }
    }
    if (c->read == r->count) {
      cursor_free(c);
      heap[0] = heap[--heap_count];
    } else if (!cursor_read(db, c))
      goto done;
    heap_fix(heap, heap_count);
  }
  if (pending && !chunk_put(writer, records[0].series, &out, timestamp, &value, revision)) goto done;
  ok = chunk_flush(writer, records[0].series, &out);
done:
  value_free(&value);
  for (size_t i = 0; i < heap_count; i++) cursor_free(heap[i]);
  chunk_clear(&out);
  free(out.timestamps);
  free(out.revisions);
  free(out.values);
  free(heap);
  return ok;
}

bool tsdb_compact_stream_records(timeseries_db_t* db, const tsdb_level_page_t* pages, size_t page_count,
                                 uint8_t to_level, tsdb_page_cache_snapshot_t* batch) {
  merge_record_t* records = NULL;
  uint8_t* scan_buffer = malloc(4096);
  if (!scan_buffer) return false;
  size_t used = 0, capacity = 0;
  uint32_t source_bytes = 0;
  bool ok = false;
  for (size_t p = 0; p < page_count; p++) {
    timeseries_page_header_t header;
    if (esp_partition_read(db->partition, pages[p].offset, &header, sizeof(header)) != ESP_OK) goto done;
    timeseries_fielddata_iterator_t it;
    if (!timeseries_fielddata_iterator_init_window(db, pages[p].offset, pages[p].size, scan_buffer, 4096, &it))
      goto done;
    timeseries_field_data_header_t h;
    bool read_ok = true;
    while (timeseries_fielddata_iterator_next(&it, &h)) {
      if (!(h.flags & TSDB_FIELDDATA_FLAG_DELETED)) continue;
      if (!h.record_count || h.start_time > h.end_time) {
        read_ok = false;
        break;
      }
      if (used == capacity) {
        size_t cap = capacity ? capacity * 2 : 16;
        if (cap < capacity || cap > SIZE_MAX / sizeof(*records)) {
          read_ok = false;
          break;
        }
        merge_record_t* grow = realloc(records, cap * sizeof(*records));
        if (!grow) {
          read_ok = false;
          break;
        }
        records = grow;
        capacity = cap;
      }
      merge_record_t* r = &records[used];
      memcpy(r->series, h.series_id, 16);
      tsdb_series_type_status_t type_status = tsdb_lookup_series_type_status(db, r->series, &r->type);
      if (type_status == TSDB_SERIES_TYPE_ABSENT) {
        // No metadata describes this series any more (it was deleted while a
        // record of it was in flight, before deletes were ordered against
        // inserts and compaction). No query can reach it and it cannot be
        // decoded; leave it out of the output instead of failing the level.
        ESP_LOGW(TAG, "Dropping a record of a series with no metadata from page @0x%08" PRIx32, pages[p].offset);
        continue;
      }
      if (type_status != TSDB_SERIES_TYPE_FOUND) {
        read_ok = false;
        break;
      }
      r->first = h.start_time;
      r->last = h.end_time;
      r->offset = pages[p].offset + it.current_record_offset + sizeof(h);
      r->sequence = header.sequence_num;
      r->count = h.record_count;
      r->length = h.record_length;
      r->flags = h.flags;
      if (h.record_length > UINT32_MAX - source_bytes) {
        read_ok = false;
        break;
      }
      source_bytes += h.record_length;
      used++;
    }
    read_ok = read_ok && !it.read_error;
    timeseries_fielddata_iterator_deinit(&it);
    if (!read_ok) goto done;
  }
  free(scan_buffer);
  scan_buffer = NULL;
  if (!used) {
    ok = true;
    goto done;
  }
  records_sort(records, used);
  timeseries_page_stream_writer_t writer = {.batch_snapshot = batch};
  if (!timeseries_page_stream_writer_init(db, &writer, to_level, source_bytes)) goto done;
  ok = true;
  for (size_t first = 0; first < used;) {
    size_t end = first + 1;
    while (end < used && memcmp(records[first].series, records[end].series, 16) == 0) end++;
    if (!merge_series(db, &writer, records + first, end - first, 0)) {
      ok = false;
      break;
    }
    first = end;
  }
  if (ok) ok = timeseries_page_stream_writer_finalize(&writer);
  if (!ok) timeseries_page_stream_writer_abort(&writer);
done:
  free(scan_buffer);
  free(records);
  return ok;
}

bool tsdb_trim_stream_records(timeseries_db_t* db, const tsdb_record_ref_t* refs, size_t n, uint64_t horizon,
                              uint8_t to_level, tsdb_page_cache_snapshot_t* batch) {
  if (!db || (!refs && n) || !batch) return false;
  if (!n) return true;
  merge_record_t* records = calloc(n, sizeof(*records));
  if (!records) return false;
  size_t used = 0;
  uint32_t source_bytes = 0;
  bool ok = false;
  for (size_t i = 0; i < n; i++) {
    const timeseries_field_data_header_t* h = &refs[i].header;
    merge_record_t* r = &records[used];
    memcpy(r->series, h->series_id, 16);
    tsdb_series_type_status_t type_status = tsdb_lookup_series_type_status(db, r->series, &r->type);
    // A series with no metadata cannot be decoded or queried; its records are
    // dropped whole by the caller.
    if (type_status == TSDB_SERIES_TYPE_ABSENT) continue;
    if (type_status != TSDB_SERIES_TYPE_FOUND || !h->record_count || h->start_time > h->end_time) goto done;
    r->first = h->start_time;
    r->last = h->end_time;
    r->offset = refs[i].page_offset + refs[i].record_offset + sizeof(*h);
    r->sequence = refs[i].page_sequence;
    r->count = h->record_count;
    r->length = h->record_length;
    r->flags = h->flags;
    if (h->record_length > UINT32_MAX - source_bytes) goto done;
    source_bytes += h->record_length;
    used++;
  }
  if (!used) {
    ok = true;
    goto done;
  }
  records_sort(records, used);
  timeseries_page_stream_writer_t writer = {.batch_snapshot = batch};
  if (!timeseries_page_stream_writer_init(db, &writer, to_level, source_bytes)) goto done;
  ok = true;
  for (size_t first = 0; first < used && ok;) {
    size_t end = first + 1;
    while (end < used && memcmp(records[first].series, records[end].series, 16) == 0) end++;
    ok = merge_series(db, &writer, records + first, end - first, horizon);
    first = end;
  }
  if (ok) ok = timeseries_page_stream_writer_finalize(&writer);
  if (!ok) timeseries_page_stream_writer_abort(&writer);
done:
  free(records);
  return ok;
}
