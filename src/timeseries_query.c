#include "timeseries_epoch.h"
#include "timeseries_query.h"
#include "timeseries_data.h"
#include "timeseries_id_list.h"
#include "timeseries_iterator.h"
#include "timeseries_metadata.h"
#include "timeseries_multi_series_iterator.h"
#include "timeseries_page_cache_snapshot.h"
#include "timeseries_points_iterator.h"
#include "timeseries_string_list.h"

#include "esp_log.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "timeseries_query";

// Snapshots own headers, not flash bytes. Hold flash_write_mutex only while
// this query can still read flash (L0 pages are appended in place); iterator
// next/deinit operate on owned RAM. The pages this query's snapshot lists
// cannot be erased or reused while it runs (timeseries_epoch.h), so it does
// not need to hold off region allocation.
typedef struct {
  timeseries_db_t *db;
  bool held;
} query_read_guard_t;

static void query_read_unlock(query_read_guard_t *guard) {
  if (!guard->held) return;
  guard->held = false;
  if (guard->db->flash_write_mutex) xSemaphoreGive(guard->db->flash_write_mutex);
}

/* Very small 32-bit FNV-1a hash for 16-byte IDs – good enough
   for < 2 k entries and has no multiplier overflow on 32-bit MCUs. */
static inline uint32_t hash_series_id16(const uint8_t id[16]) {
  uint32_t h = 2166136261u;
  for (int i = 0; i < 16; ++i) {
    h ^= id[i];
    h *= 16777619u;
  }
  return h;
}

static size_t next_pow2(size_t n) {
  if (n < 2)
    return 2;
  --n;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  return n + 1;
}

/* --------------------------------------------------------------------- */
/* Build the table from every (field × series) once, before page scan.   */
/* Returns true on success; false = OOM.                                 */
static bool series_lookup_build(field_info_t *fields, size_t num_fields,
                                series_lookup_t *tbl_out) {
  /* 1. Count series */
  size_t total = 0;
  for (size_t f = 0; f < num_fields; ++f)
    total += fields[f].num_series;

  /* 2. Allocate a table at ≤50 % load factor, power-of-two size   */
  size_t cap = next_pow2(total * 2);
  series_lookup_entry_t *ent = calloc(cap, sizeof(*ent));
  if (!ent)
    return false;

  tbl_out->entries = ent;
  tbl_out->capacity = cap;

  /* 3. Insert */
  for (size_t f = 0; f < num_fields; ++f)
    for (size_t s = 0; s < fields[f].num_series; ++s) {
      series_record_list_t *srl = &fields[f].series_lists[s];
      const uint8_t *id = srl->series_id.bytes;
      uint32_t h = hash_series_id16(id);
      size_t idx = h & (cap - 1);

      while (ent[idx].used) /* linear probe */
        idx = (idx + 1) & (cap - 1);

      memcpy(ent[idx].id.bytes, id, 16);
      ent[idx].srl = srl;
      ent[idx].used = true;
    }
  return true;
}

static inline series_record_list_t *
series_lookup_find(const series_lookup_t *tbl, const uint8_t id[16]) {
  size_t cap = tbl->capacity;
  size_t idx = hash_series_id16(id) & (cap - 1);

  while (tbl->entries[idx].used) {
    if (memcmp(tbl->entries[idx].id.bytes, id, 16) == 0)
      return tbl->entries[idx].srl; /* hit */
    idx = (idx + 1) & (cap - 1);
  }
  return NULL; /* miss */
}

static void series_lookup_free(series_lookup_t *tbl) {
  free(tbl->entries);
  tbl->entries = NULL;
  tbl->capacity = 0;
}

/**
 * @brief Our single function that will read data for multiple fields (and
 *        their series lists) in one pass.
 */
static size_t fetch_series_data(timeseries_db_t *db, field_info_t *fields_array,
                                size_t num_fields,
                                const timeseries_query_t *query,
                                timeseries_query_result_t *result, bool *out_ok,
                                query_read_guard_t *guard);

/**
 * @brief Combine two series ID lists by intersecting them, storing the result
 *        in \p out_list.
 *
 * @param[in,out] out_list  The resulting intersection is stored here.
 * @param[in]     new_list  The list to intersect with \p out_list.
 */
static void
intersect_series_id_lists(timeseries_series_id_list_t *out_list,
                          const timeseries_series_id_list_t *new_list);

static void free_record_array(series_record_list_t *srl) {
  if (srl && srl->records) {
    free(srl->records);
    srl->records = NULL;
    srl->count = 0;
    srl->capacity = 0;
  }
}

bool timeseries_query_execute(timeseries_db_t *db,
                              const timeseries_query_t *query,
                              timeseries_query_result_t *result) {
  if (!db || !query || !result || !query->measurement_name) {
    return false;
  }
  memset(result, 0, sizeof(*result));
  if ((query->num_tags && (!query->tag_keys || !query->tag_values)) ||
      (query->num_fields && !query->field_names)) return false;
  for (size_t t = 0; t < query->num_tags; t++) {
    if (!query->tag_keys[t] || !query->tag_values[t]) return false;
  }
  for (size_t f = 0; f < query->num_fields; f++) {
    if (!query->field_names[f]) return false;
  }

  query_read_guard_t guard = {.db = db, .held = true};
  if (db->flash_write_mutex) tsdb_write_lock(db);

  /* -------------------------------------------------------------------- */
  /*  House-keeping vars                                                  */
  /* -------------------------------------------------------------------- */
  bool ok = false; /* final return value       */
  size_t actual_fields_count = 0;
  field_info_t *fields_array = NULL;
  timeseries_series_id_list_t *tag_series = NULL;   /* one per tag filter */
  timeseries_series_id_list_t *field_series = NULL; /* parallel to fields_to_query */
  size_t field_series_count = 0;

  timeseries_series_id_list_t matched_series;
  tsdb_series_id_list_init(&matched_series);

  timeseries_string_list_t fields_to_query;
  tsdb_string_list_init(&fields_to_query);

  /* -------------------------------------------------------------------- */
  /*  Result initialisation                                               */
  /* -------------------------------------------------------------------- */
  memset(result, 0, sizeof(*result));

  /* -------------------------------------------------------------------- */
  /*  1.  Resolve measurement                                             */
  /* -------------------------------------------------------------------- */
  /* The measurement-ID cache is guarded by flash_write_mutex, held above. */
  uint32_t measurement_id = 0;
  if (!tsdb_meas_cache_lookup(db, query->measurement_name, &measurement_id)) {
    if (!tsdb_find_measurement_id(db, query->measurement_name, &measurement_id)) {
      ESP_LOGW(TAG, "Measurement '%s' not found.", query->measurement_name);
      ok = true; /* empty result   */
      goto cleanup;
    }
    tsdb_meas_cache_store(db, query->measurement_name, measurement_id);
  }

  /* -------------------------------------------------------------------- */
  /*  2.  One metadata scan: series per tag filter and per field          */
  /* -------------------------------------------------------------------- */
  if (query->num_fields > 0) {
    for (size_t i = 0; i < query->num_fields; ++i) {
      tsdb_string_list_append_unique(&fields_to_query, query->field_names[i]);
    }
  }
  if (query->num_tags > 0) {
    tag_series = calloc(query->num_tags, sizeof(*tag_series));
    if (!tag_series) {
      ESP_LOGE(TAG, "Out of memory for tag series lists");
      goto cleanup;
    }
    for (size_t i = 0; i < query->num_tags; ++i) {
      tsdb_series_id_list_init(&tag_series[i]);
    }
  }
  if (!tsdb_collect_query_series(db, measurement_id, query->tag_keys,
                                 query->tag_values, query->num_tags,
                                 tag_series, &fields_to_query,
                                 query->num_fields == 0, &field_series)) {
    ESP_LOGE(TAG, "Failed to collect series for measurement '%s'",
             query->measurement_name);
    goto cleanup;
  }
  field_series_count = fields_to_query.count;

  /* Tag filters intersect in query order. Without tag filters every field
   * list is used as is; a measurement with no series ends with no fields. */
  if (query->num_tags > 0) {
    for (size_t i = 0; i < query->num_tags; ++i) {
      if (i == 0) {
        tsdb_series_id_list_copy(&matched_series, &tag_series[i]);
      } else {
        intersect_series_id_lists(&matched_series, &tag_series[i]);
      }
    }
    if (matched_series.count == 0) {
      ESP_LOGD(TAG, "No series matched the tag filters (or none exist).");
      ok = true; /* empty result   */
      goto cleanup;
    }
  }

  if (fields_to_query.count == 0) {
    ESP_LOGD(TAG, "No fields found for measurement '%s'.",
             query->measurement_name);
    ok = true; /* empty result   */
    goto cleanup;
  }

  /* -------------------------------------------------------------------- */
  /*  3.  Allocate & populate `fields_array`                              */
  /* -------------------------------------------------------------------- */

  fields_array = calloc(fields_to_query.count, sizeof(field_info_t));
  if (!fields_array) {
    ESP_LOGE(TAG, "Out of memory for fields_array");
    goto cleanup;
  }

  for (size_t f = 0; f < fields_to_query.count; ++f) {
    const char *fname = fields_to_query.items[f];

    /* Take ownership of this field's list. */
    timeseries_series_id_list_t field_series_f = field_series[f];
    tsdb_series_id_list_init(&field_series[f]);
    ESP_LOGD(TAG, "Field '%s' (meas_id=%" PRIu32 "): found %zu series_ids",
             fname, measurement_id, field_series_f.count);
    for (size_t si = 0; si < field_series_f.count && si < 3; si++) {
      ESP_LOGD(TAG, "  series_id[%zu]: %02X%02X%02X%02X%02X%02X%02X%02X...",
               si,
               field_series_f.ids[si].bytes[0], field_series_f.ids[si].bytes[1],
               field_series_f.ids[si].bytes[2], field_series_f.ids[si].bytes[3],
               field_series_f.ids[si].bytes[4], field_series_f.ids[si].bytes[5],
               field_series_f.ids[si].bytes[6], field_series_f.ids[si].bytes[7]);
    }
    if (field_series_f.count == 0) {
      tsdb_series_id_list_free(&field_series_f);
      continue;
    }

    if (query->num_tags > 0) {
      intersect_series_id_lists(&field_series_f, &matched_series);
    }
    if (field_series_f.count == 0) {
      tsdb_series_id_list_free(&field_series_f);
      continue;
    }

    field_info_t *fi = &fields_array[actual_fields_count];

    fi->field_name = fname;
    fi->series_ids = field_series_f;
    fi->num_series = field_series_f.count;
    fi->series_lists = calloc(field_series_f.count, sizeof(series_record_list_t));

    if (!fi->series_lists) {
      ESP_LOGE(TAG, "Out of memory for series_lists");
      tsdb_series_id_list_free(&field_series_f);
      goto cleanup; /* early error    */
    }

    for (size_t s = 0; s < field_series_f.count; ++s) {
      fi->series_lists[s].series_id = field_series_f.ids[s];
      fi->series_lists[s].records = NULL;
      fi->series_lists[s].count = 0;
      fi->series_lists[s].capacity = 0;
    }
    ++actual_fields_count;
  }

  if (actual_fields_count == 0) {
    ok = true; /* nothing to do  */
    goto cleanup;
  }

  // Log which series_ids we're looking for
  for (size_t f = 0; f < actual_fields_count; ++f) {
    ESP_LOGD(TAG, "Looking for field '%s' with %zu series:", fields_array[f].field_name, fields_array[f].num_series);
    for (size_t s = 0; s < fields_array[f].num_series; ++s) {
      ESP_LOGD(TAG, "  Series %zu: %02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X",
               s,
               fields_array[f].series_lists[s].series_id.bytes[0], fields_array[f].series_lists[s].series_id.bytes[1],
               fields_array[f].series_lists[s].series_id.bytes[2], fields_array[f].series_lists[s].series_id.bytes[3],
               fields_array[f].series_lists[s].series_id.bytes[4], fields_array[f].series_lists[s].series_id.bytes[5],
               fields_array[f].series_lists[s].series_id.bytes[6], fields_array[f].series_lists[s].series_id.bytes[7],
               fields_array[f].series_lists[s].series_id.bytes[8], fields_array[f].series_lists[s].series_id.bytes[9],
               fields_array[f].series_lists[s].series_id.bytes[10], fields_array[f].series_lists[s].series_id.bytes[11],
               fields_array[f].series_lists[s].series_id.bytes[12], fields_array[f].series_lists[s].series_id.bytes[13],
               fields_array[f].series_lists[s].series_id.bytes[14], fields_array[f].series_lists[s].series_id.bytes[15]);
    }
  }

  /* -------------------------------------------------------------------- */
  /*  5.  Fetch & aggregate                                               */
  /* -------------------------------------------------------------------- */

  size_t points_fetched = fetch_series_data(db, fields_array, actual_fields_count, query, result, &ok, &guard);
  if (points_fetched == 0 && result->num_points == 0) {
    ESP_LOGD(TAG, "fetch_series_data returned no points");
  }

cleanup:
  query_read_unlock(&guard);
  /* -------------------------------------------------------------------- */
  /*  6.  Release every allocation made above                             */
  /* -------------------------------------------------------------------- */

  if (fields_array) {
    for (size_t i = 0; i < actual_fields_count; ++i) {
      /* 6a. free record arrays generated in fetch_series_data */
      for (size_t s = 0; s < fields_array[i].num_series; ++s) {
        free_record_array(&fields_array[i].series_lists[s]);
      }

      /* 6b. free per-field arrays & lists */
      free(fields_array[i].series_lists);
      tsdb_series_id_list_free(&fields_array[i].series_ids);
    }
    free(fields_array);
  }

  if (tag_series) {
    for (size_t i = 0; i < query->num_tags; ++i) {
      tsdb_series_id_list_free(&tag_series[i]);
    }
    free(tag_series);
  }
  if (field_series) {
    for (size_t i = 0; i < field_series_count; ++i) {
      tsdb_series_id_list_free(&field_series[i]);
    }
    free(field_series);
  }
  tsdb_string_list_free(&fields_to_query);
  tsdb_series_id_list_free(&matched_series);
  if (!ok) timeseries_query_free_result(result);

  return ok;
}

// -----------------------------------------------------------------------------
// Private (static) Helpers
// -----------------------------------------------------------------------------

/**
 * @brief Find a column by name; if none, create a new one with the given type.
 *
 * @param[in,out] result      The query result
 * @param[in]     field_name  The column name
 * @param[in]     field_type  The timeseries_field_type_e from metadata
 * @return The column index
 */
static size_t find_or_create_column(timeseries_query_result_t *result,
                                    const char *field_name,
                                    timeseries_field_type_e field_type,
                                    size_t result_capacity) {
  // A) Search for an existing column with the same name
  for (size_t c = 0; c < result->num_columns; c++) {
    if (strcmp(result->columns[c].name, field_name) == 0) {
      // If there's a type mismatch, decide how to handle it
      if (result->columns[c].type != field_type) {
        ESP_LOGW(TAG, "Column '%s' has conflicting types (existing=%d, new=%d)",
                 field_name, (int)result->columns[c].type, (int)field_type);
      }
      return c;
    }
  }

  // B) Not found => create a new column
  size_t new_col_index = result->num_columns;
  size_t new_num_cols = new_col_index + 1;

  timeseries_query_result_column_t *new_cols =
      realloc(result->columns, new_num_cols * sizeof(*new_cols));
  if (!new_cols) {
    ESP_LOGE(TAG, "Out of memory creating new column '%s'", field_name);
    return SIZE_MAX; /* signal fatal OOM */
  }
  result->columns = new_cols;

  // Initialize the new column before incrementing num_columns
  memset(&result->columns[new_col_index], 0,
         sizeof(result->columns[new_col_index]));
  result->columns[new_col_index].name = strdup(field_name);
  if (!result->columns[new_col_index].name) {
    ESP_LOGE(TAG, "OOM allocating column name '%s'", field_name);
    return SIZE_MAX;
  }
  result->columns[new_col_index].type = field_type;

  // Allocate values array at the current result capacity (which may have been
  // pre-allocated during estimation). This avoids the need for NULL checks on
  // every ensure_result_capacity call. When alloc_size is 0 (empty result set),
  // skip allocation — values stays NULL and ensure_result_capacity will allocate
  // on demand if rows are added later.
  size_t alloc_size = result_capacity > result->num_points
                          ? result_capacity : result->num_points;
  if (alloc_size > 0) {
    result->columns[new_col_index].values =
        calloc(alloc_size, sizeof(timeseries_field_value_t));
    if (!result->columns[new_col_index].values) {
      free((void *)result->columns[new_col_index].name);
      result->columns[new_col_index].name = NULL;
      ESP_LOGE(TAG, "OOM allocating %" PRIu32 " cells (%" PRIu32 " bytes) for column '%s'",
               (uint32_t)alloc_size, (uint32_t)(alloc_size * sizeof(timeseries_field_value_t)),
               field_name);
      return SIZE_MAX;
    }
  }

  // Only increment num_columns after the column is fully initialized
  result->num_columns = new_num_cols;

  return new_col_index;
}

/**
 * @brief Ensure the result has capacity for the requested number of rows.
 *        Grows by doubling to amortize allocation cost.
 */
static bool ensure_result_capacity(timeseries_query_result_t *result,
                                   size_t *capacity, size_t required) {
  if (required <= *capacity) {
    return true;
  }
  const size_t max_capacity = SIZE_MAX / sizeof(timeseries_field_value_t);
  if (required > max_capacity) return false;
  size_t new_cap = *capacity ? *capacity : 64;
  while (new_cap < required) {
    if (new_cap > max_capacity / 2) {
      new_cap = required;
      break;
    }
    new_cap *= 2;
  }
  uint64_t *new_ts = realloc(result->timestamps, new_cap * sizeof(uint64_t));
  if (!new_ts) {
    ESP_LOGE(TAG, "OOM expanding timestamps capacity to %zu", new_cap);
    return false;
  }
  result->timestamps = new_ts;

  // Note: if a column realloc below fails, timestamps (and any earlier
  // columns) will already be at new_cap while *capacity stays at the old
  // value.  This is intentional — the oversized arrays waste a little memory
  // but remain valid, and *capacity is only updated after ALL reallocs
  // succeed so the caller never indexes beyond the smallest allocation.
  for (size_t c = 0; c < result->num_columns; c++) {
    timeseries_field_value_t *new_vals =
        realloc(result->columns[c].values,
                new_cap * sizeof(timeseries_field_value_t));
    if (!new_vals) {
      ESP_LOGE(TAG, "OOM expanding column %zu capacity to %zu", c, new_cap);
      return false;
    }
    result->columns[c].values = new_vals;
  }
  *capacity = new_cap;
  return true;
}

/**
 * @brief Find a row index with the given timestamp; if none, add a new row.
 *
 * Uses a forward cursor hint for O(1) amortized lookup when timestamps
 * arrive in sorted order (the common case for aggregation rollup).
 *
 * @param[in,out] result   The query result
 * @param[in]     ts       The timestamp to match
 * @param[in,out] capacity Current capacity of result arrays
 * @param[in,out] hint     Forward cursor — start search here, updated on return
 * @return The row index corresponding to this timestamp
 */
static size_t find_or_create_row(timeseries_query_result_t *result,
                                 uint64_t ts, size_t *capacity,
                                 size_t *hint) {
  // Forward scan from hint (both input and existing data are sorted)
  size_t h = *hint;
  if (h > result->num_points) h = 0;

  while (h < result->num_points && result->timestamps[h] < ts) {
    h++;
  }
  if (h < result->num_points && result->timestamps[h] == ts) {
    *hint = h + 1;
    return h;
  }

  // Not found — insert at position h
  size_t insert_pos = h;
  if (!ensure_result_capacity(result, capacity, result->num_points + 1)) {
    return SIZE_MAX;
  }

  if (insert_pos < result->num_points) {
    size_t shift = result->num_points - insert_pos;
    memmove(&result->timestamps[insert_pos + 1],
            &result->timestamps[insert_pos],
            shift * sizeof(uint64_t));
    for (size_t c = 0; c < result->num_columns; c++) {
      memmove(&result->columns[c].values[insert_pos + 1],
              &result->columns[c].values[insert_pos],
              shift * sizeof(timeseries_field_value_t));
    }
  }

  result->timestamps[insert_pos] = ts;
  for (size_t c = 0; c < result->num_columns; c++) {
    memset(&result->columns[c].values[insert_pos], 0,
           sizeof(timeseries_field_value_t));
  }

  result->num_points++;
  *hint = insert_pos + 1;
  return insert_pos;
}

/**
 * @brief Merge sorted field data into the result in O(n + m) time.
 *
 * Both the existing result timestamps and the new timestamps must be sorted.
 * Instead of per-point memmove (O(n²)), this counts new insertions, expands
 * the arrays once, and fills from right-to-left in a single pass.
 */
static bool merge_sorted_field_data(timeseries_query_result_t *result,
                                    size_t col_index,
                                    const uint64_t *new_ts,
                                    const timeseries_field_value_t *new_vals,
                                    size_t new_count,
                                    size_t *capacity) {
  if (new_count == 0) return true;

  size_t old_count = result->num_points;

  // Fast path: no existing data — bulk copy
  if (old_count == 0) {
    if (!ensure_result_capacity(result, capacity, new_count)) return false;
    memcpy(result->timestamps, new_ts, new_count * sizeof(uint64_t));
    memcpy(result->columns[col_index].values, new_vals,
           new_count * sizeof(timeseries_field_value_t));
    for (size_t c = 0; c < result->num_columns; c++) {
      if (c != col_index) {
        memset(result->columns[c].values, 0,
               new_count * sizeof(timeseries_field_value_t));
      }
    }
    result->num_points = new_count;
    return true;
  }

  // Count insertions before transferring any string ownership. The reverse
  // merge below must be the only pass that replaces/frees existing values.
  size_t insert_count = 0;
  size_t old_pos = 0;
  for (size_t ni = 0; ni < new_count; ni++) {
    while (old_pos < old_count && result->timestamps[old_pos] < new_ts[ni])
      old_pos++;
    if (old_pos == old_count || result->timestamps[old_pos] != new_ts[ni])
      insert_count++;
    else
      old_pos++;
  }
  if (insert_count > SIZE_MAX - old_count) return false;
  size_t merged_count = old_count + insert_count;
  if (!ensure_result_capacity(result, capacity, merged_count)) return false;

  // Reverse merge: fill from right to left so we never overwrite unread data
  ssize_t oi = (ssize_t)old_count - 1;
  ssize_t ni = (ssize_t)new_count - 1;
  ssize_t ki = (ssize_t)merged_count - 1;

  while (ki >= 0) {
    bool take_old;
    if (oi < 0) {
      take_old = false;
    } else if (ni < 0) {
      take_old = true;
    } else if (result->timestamps[oi] > new_ts[ni]) {
      take_old = true;
    } else if (result->timestamps[oi] < new_ts[ni]) {
      take_old = false;
    } else {
      // Equal timestamps — keep old row, set value for current column.
      if ((size_t)ki != (size_t)oi) {
        result->timestamps[ki] = result->timestamps[oi];
        for (size_t c = 0; c < result->num_columns; c++) {
          if (c != col_index) {
            result->columns[c].values[ki] = result->columns[c].values[oi];
          }
        }
      }
      // The old value for this column at row `oi` is being superseded by the
      // newer series' value (col_index is intentionally not copied above). Free
      // it if it owns a string so we don't leak it. This slot is never read
      // again (only overwritten as ki descends), so NULLing is just insurance.
      {
        timeseries_field_value_t *old_cv = &result->columns[col_index].values[oi];
        if (old_cv->type == TIMESERIES_FIELD_TYPE_STRING && old_cv->data.string_val.str) {
          free(old_cv->data.string_val.str);
          old_cv->data.string_val.str = NULL;
        }
      }
      result->columns[col_index].values[ki] = new_vals[ni];
      oi--; ni--; ki--;
      continue;
    }

    if (take_old) {
      if ((size_t)ki != (size_t)oi) {
        result->timestamps[ki] = result->timestamps[oi];
        for (size_t c = 0; c < result->num_columns; c++) {
          result->columns[c].values[ki] = result->columns[c].values[oi];
        }
      }
      oi--;
    } else {
      // New timestamp — insert with zero-initialized values for other columns
      result->timestamps[ki] = new_ts[ni];
      for (size_t c = 0; c < result->num_columns; c++) {
        if (c == col_index) {
          result->columns[c].values[ki] = new_vals[ni];
        } else {
          memset(&result->columns[c].values[ki], 0,
                 sizeof(timeseries_field_value_t));
        }
      }
      ni--;
    }
    ki--;
  }

  result->num_points = merged_count;
  return true;
}

static int cmp_series_id_asc(const void *a, const void *b) {
  return memcmp(((const timeseries_series_id_t *)a)->bytes,
                ((const timeseries_series_id_t *)b)->bytes, 16);
}

static void
intersect_series_id_lists(timeseries_series_id_list_t *out,
                          const timeseries_series_id_list_t *other) {
  if (!out || !other)
    return;

  /* Trivial early exit */
  if (out->count == 0 || other->count == 0) {
    tsdb_series_id_list_clear(out);
    return;
  }

  /* Sort both lists in-place (16 B keys; qsort is fine for ≤1k) */
  qsort(out->ids, out->count, sizeof(timeseries_series_id_t),
        cmp_series_id_asc);

  /* We must not modify 'other'; copy to sort. Use stack for small counts. */
  timeseries_series_id_t stack_tmp[16]; /* 256 bytes on stack */
  timeseries_series_id_t *tmp;
  bool tmp_on_heap = false;
  if (other->count <= 16) {
    tmp = stack_tmp;
  } else {
    tmp = malloc(other->count * sizeof(timeseries_series_id_t));
    if (!tmp) {
      ESP_LOGE(TAG, "OOM in intersect_series_id_lists (%zu entries)", other->count);
      tsdb_series_id_list_clear(out);
      return;
    }
    tmp_on_heap = true;
  }
  memcpy(tmp, other->ids, other->count * sizeof(timeseries_series_id_t));
  qsort(tmp, other->count, sizeof(timeseries_series_id_t), cmp_series_id_asc);

  /* Linear sweep */
  timeseries_series_id_list_t result;
  tsdb_series_id_list_init(&result);

  size_t i = 0, j = 0;
  while (i < out->count && j < other->count) {
    int cmp = memcmp(out->ids[i].bytes, tmp[j].bytes, 16);
    if (cmp == 0) { /* match */
      tsdb_series_id_list_append(&result, out->ids[i].bytes);
      ++i;
      ++j;
    } else if (cmp < 0) {
      ++i;
    } else {
      ++j;
    }
  }

  if (tmp_on_heap) free(tmp);

  /* Replace 'out' with the intersection */
  tsdb_series_id_list_clear(out);
  tsdb_series_id_list_copy(out, &result);
  tsdb_series_id_list_free(&result);
}

/**
 * @brief Append a record to the growable array (unsorted).
 *        Persisted revisions resolve duplicates without sorting this array.
 */
static bool field_record_array_append(series_record_list_t *srl,
                                      const timeseries_field_data_header_t *fdh,
                                      uint32_t absolute_offset,
                                      uint32_t page_sequence, const timeseries_fielddata_iterator_t *iterator) {
  if (srl->count >= srl->capacity) {
    size_t new_cap = srl->capacity ? srl->capacity * 2 : 8;
    field_record_info_t *new_arr =
        realloc(srl->records, new_cap * sizeof(field_record_info_t));
    if (!new_arr) {
      return false;
    }
    srl->records = new_arr;
    srl->capacity = new_cap;
  }

  field_record_info_t *rec = &srl->records[srl->count++];
  rec->record_offset = absolute_offset;
  rec->revision = tsdb_legacy_revision(page_sequence, absolute_offset);
  rec->cached_revision = false;
  // Reuse bytes already in the header-scan window. A boundary-crossing or
  // larger record falls back to the iterator's checked trailer read.
  if ((fdh->flags & TSDB_FIELDDATA_FLAG_COMPRESSED) &&
      !(fdh->flags & TSDB_FIELDDATA_FLAG_REVISION) &&
      (fdh->flags & TSDB_FIELDDATA_FLAG_REVISION_VECTOR) && fdh->record_length >= sizeof(uint64_t)) {
    uint32_t relative = iterator->current_record_offset + sizeof(*fdh) + fdh->record_length -
                        sizeof(uint64_t) - sizeof(timeseries_page_header_t);
    if (iterator->page_buf && relative >= iterator->window_start &&
        relative - iterator->window_start <= iterator->page_buf_size &&
        sizeof(uint64_t) <= iterator->page_buf_size - (relative - iterator->window_start)) {
      memcpy(&rec->revision, iterator->page_buf + relative - iterator->window_start, sizeof(uint64_t));
      rec->cached_revision = true;
    }
  }
  rec->record_count = fdh->record_count;
  rec->record_length = fdh->record_length;
  rec->data_flags = fdh->flags;
  return true;
}

/* Frees a string value the result did not take ownership of. */
static void query_value_release(timeseries_field_value_t *v) {
  if (v->type == TIMESERIES_FIELD_TYPE_STRING && v->data.string_val.str)
    free(v->data.string_val.str);
}

/* Opens one points iterator per record of a series, in record order. Records
 * that fail to open mark the query incomplete and are left out. Returns the
 * number of iterators placed in sub_iters. */
static size_t query_open_record_iters(timeseries_db_t *db,
                                      series_record_list_t *srl,
                                      timeseries_field_type_e field_type,
                                      const timeseries_query_t *query,
                                      timeseries_points_iterator_t *pit_batch,
                                      timeseries_points_iterator_t **sub_iters,
                                      bool *complete) {
  size_t idx = 0;
  for (size_t ri = 0; ri < srl->count; ri++) {
    field_record_info_t *rec = &srl->records[ri];
    timeseries_points_iterator_t *pit = &pit_batch[ri];
    bool ok = timeseries_points_iterator_init_range_with_revision(
        db, rec->record_offset, rec->record_length,
        rec->record_count, field_type, rec->data_flags, pit, query->start_ms, query->end_ms,
        rec->cached_revision ? &rec->revision : NULL);
    if (!ok) { *complete = false; continue; }
    if (!pit->has_revision) {
      pit->has_revision = true;
      pit->revisions.value = rec->revision;
    }
    sub_iters[idx] = pit;
    idx++;
  }
  return idx;
}

static void query_close_record_iters(timeseries_points_iterator_t **sub_iters,
                                     size_t count) {
  for (size_t i = 0; i < count; i++) {
    if (sub_iters[i]) timeseries_points_iterator_deinit(sub_iters[i]);
  }
}

/* Deinits a multi-points iterator together with the record iterators it merges. */
static void query_close_multi_points(timeseries_multi_points_iterator_t *mpit) {
  for (size_t i = 0; i < mpit->sub_count; i++) {
    if (mpit->subs[i].sub_iter)
      timeseries_points_iterator_deinit(mpit->subs[i].sub_iter);
  }
  timeseries_multi_points_iterator_deinit(mpit);
}

static size_t fetch_series_data(timeseries_db_t *db, field_info_t *fields_array,
                                size_t num_fields,
                                const timeseries_query_t *query,
                                timeseries_query_result_t *result, bool *out_ok,
                                query_read_guard_t *guard) {
  *out_ok = false;
  bool complete = true;
  if (!db || !fields_array || num_fields == 0 || !query || !result) {
    ESP_LOGW(TAG, "Invalid input to fetch_series_data");
    return 0;
  }

  size_t result_capacity = 0; /* local capacity tracker (thread-safe) */

  // --------------------------------------------------------------------------
  // PART A: Single pass to gather the record_info linked-lists for each series
  // --------------------------------------------------------------------------
  series_lookup_t srl_tbl = {0};
  if (!series_lookup_build(fields_array, num_fields, &srl_tbl)) {
    ESP_LOGE(TAG, "OOM building series-id lookup table");
    return 0;
  }

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    ESP_LOGE(TAG, "Failed to init page iterator");
    series_lookup_free(&srl_tbl);
    return 0;
  }

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0;
  uint32_t page_size = 0;
  int pages_processed = 0;
  int field_data_pages = 0;
  int records_found = 0;

  // Bounded header window: large record payloads are read only if selected.
  uint8_t *shared_page_buf = NULL;
  uint32_t shared_page_buf_cap = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset,
                                             &page_size)) {
    pages_processed++;
    // Only process FIELD_DATA pages
    if (hdr.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA) {
      ESP_LOGV(TAG, "Skipping non-field-data page @0x%08X type=%u state=%u",
               (unsigned int)page_offset, hdr.page_type, hdr.page_state);
      continue;
    }
    field_data_pages++;
    ESP_LOGD(TAG, "Scanning FIELD_DATA page @0x%08X size=%u level=%u state=%u",
             (unsigned int)page_offset, (unsigned int)page_size, hdr.field_data_level, hdr.page_state);

    // A cached L0 used length also avoids reading the erased tail. The same
    // sequence check as the insert path rejects occupancy from recycled pages.
    size_t usage_slot = (page_offset / TIMESERIES_FIELD_DATA_PAGE_SIZE) % TSDB_L0_USAGE_CACHE_SIZE;
    if (db->l0_usage_cache && hdr.field_data_level == 0 && hdr.sequence_num != 0 &&
        db->l0_usage_cache[usage_slot].offset == page_offset &&
        db->l0_usage_cache[usage_slot].sequence == hdr.sequence_num &&
        db->l0_usage_cache[usage_slot].used <= page_size) {
      if ((query->start_ms > 0 && db->l0_usage_cache[usage_slot].max_time < query->start_ms) ||
          (query->end_ms > 0 && db->l0_usage_cache[usage_slot].min_time > query->end_ms))
        continue;
      page_size = db->l0_usage_cache[usage_slot].used;
    }

    // Grow shared buffer if this page is larger
    if (page_size <= sizeof(timeseries_page_header_t)) {
      ESP_LOGW(TAG, "Corrupt page @0x%08X: page_size=%u too small, skipping",
               (unsigned int)page_offset, (unsigned int)page_size);
      continue;
    }
    uint32_t data_size = page_size - sizeof(timeseries_page_header_t);
    if (data_size > 4096) data_size = 4096;
    if (data_size > shared_page_buf_cap) {
      uint8_t *new_buf = realloc(shared_page_buf, data_size);
      if (!new_buf) {
        complete = false;
        ESP_LOGW(TAG, "realloc(%u) failed for page @0x%08X, skipping",
                 (unsigned int)data_size, (unsigned int)page_offset);
        continue;
      }
      shared_page_buf = new_buf;
      shared_page_buf_cap = data_size;
    }

    timeseries_fielddata_iterator_t fdata_iter;
    if (!timeseries_fielddata_iterator_init_window(
            db, page_offset, page_size, shared_page_buf,
            shared_page_buf_cap, &fdata_iter)) {
      ESP_LOGW(TAG, "Failed to init fielddata_iterator on page @0x%08X",
               (unsigned int)page_offset);
      complete = false;
      continue;
    }

    timeseries_field_data_header_t fdh;
    uint32_t used = sizeof(timeseries_page_header_t);
    uint64_t min_time = UINT64_MAX, max_time = 0;
    int page_records = 0;
    int page_records_matched = 0;
    while (timeseries_fielddata_iterator_next(&fdata_iter, &fdh)) {
      used = fdata_iter.offset;
      if (fdh.start_time < min_time) min_time = fdh.start_time;
      if (fdh.end_time > max_time) max_time = fdh.end_time;
      page_records++;
      // Skip if flagged as deleted
      if ((fdh.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
        ESP_LOGD(TAG, "Skipping deleted record @0x%08X flags=0x%02X",
                 (unsigned int)fdata_iter.current_record_offset, fdh.flags);
        continue;
      }

      // Calculate the absolute offset for the raw data portion
      uint32_t absolute_offset = page_offset +
                                 fdata_iter.current_record_offset +
                                 sizeof(timeseries_field_data_header_t);

      /* Fast path: single probe instead of nested loops */
      series_record_list_t *srl = series_lookup_find(&srl_tbl, fdh.series_id);
      if (srl) {
        // Skip records entirely outside the query time range
        if (query->end_ms > 0 && fdh.start_time > (uint64_t)query->end_ms) continue;
        if (query->start_ms > 0 && fdh.end_time < (uint64_t)query->start_ms) continue;
        records_found++;
        page_records_matched++;
        bool is_compressed = (fdh.flags & TSDB_FIELDDATA_FLAG_COMPRESSED) == 0;
        ESP_LOGD(TAG, "MATCH: series %02X%02X%02X%02X... @0x%08X count=%u len=%u compressed=%d",
                 fdh.series_id[0], fdh.series_id[1], fdh.series_id[2], fdh.series_id[3],
                 (unsigned int)absolute_offset, fdh.record_count, fdh.record_length, is_compressed);
        if (!field_record_array_append(srl, &fdh, absolute_offset,
                                       hdr.sequence_num, &fdata_iter)) complete = false;
      } else {
        ESP_LOGD(TAG, "No match for series %02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X (flags=0x%02X, records=%u)",
                 fdh.series_id[0], fdh.series_id[1], fdh.series_id[2], fdh.series_id[3],
                 fdh.series_id[4], fdh.series_id[5], fdh.series_id[6], fdh.series_id[7],
                 fdh.series_id[8], fdh.series_id[9], fdh.series_id[10], fdh.series_id[11],
                 fdh.series_id[12], fdh.series_id[13], fdh.series_id[14], fdh.series_id[15],
                 fdh.flags, fdh.record_count);
      }
    } // end while fielddata_iterator
    if (fdata_iter.read_error) complete = false;
    else if (db->l0_usage_cache && hdr.field_data_level == 0 && hdr.sequence_num != 0) {
      // Warm occupancy and time bounds even when the first access after boot
      // is a query. All record headers, including other series, were scanned.
      db->l0_usage_cache[usage_slot].offset = page_offset;
      db->l0_usage_cache[usage_slot].sequence = hdr.sequence_num;
      db->l0_usage_cache[usage_slot].used = used;
      db->l0_usage_cache[usage_slot].min_time = min_time;
      db->l0_usage_cache[usage_slot].max_time = max_time;
    }
    timeseries_fielddata_iterator_deinit(&fdata_iter);
    ESP_LOGD(TAG, "Page @0x%08X: %d records scanned, %d matched",
             (unsigned int)page_offset, page_records, page_records_matched);
  } // end while page_iterator
  timeseries_page_cache_iterator_deinit(&page_iter);
  free(shared_page_buf);

  ESP_LOGD(TAG, "Page scan: %d pages, %d field_data pages, %d records",
           pages_processed, field_data_pages, records_found);

  // Pre-estimate result capacity from collected record counts.
  // Take the max across fields (not the sum), because each field stores the
  // same timestamps independently — summing would over-count by num_fields×.
  {
    size_t estimated_points = 0;
    for (size_t f = 0; f < num_fields; f++) {
      size_t field_points = 0;
      for (size_t s = 0; s < fields_array[f].num_series; s++) {
        for (size_t i = 0; i < fields_array[f].series_lists[s].count; i++) {
          field_points += fields_array[f].series_lists[s].records[i].record_count;
        }
      }
      if (field_points > estimated_points)
        estimated_points = field_points;
    }
    // Bound by rollup interval: aggregation reduces point count
    if (query->rollup_interval > 0 && query->start_ms > 0 && query->end_ms > query->start_ms) {
      size_t rollup_estimate = (size_t)((query->end_ms - query->start_ms) / query->rollup_interval) + 1;
      if (rollup_estimate < estimated_points) {
        estimated_points = rollup_estimate;
      }
    }
    if (query->limit > 0 && estimated_points > (size_t)query->limit) {
      estimated_points = (size_t)query->limit;
    }
    ESP_LOGD(TAG, "Estimated %" PRIu32 " points from record headers", (uint32_t)estimated_points);
    if (estimated_points > 0) {
      result_capacity = estimated_points;
      result->timestamps = malloc(result_capacity * sizeof(uint64_t));
      if (!result->timestamps) {
        ESP_LOGE(TAG, "OOM pre-allocating %" PRIu32 " timestamps", (uint32_t)result_capacity);
        result_capacity = 0;
      }
    }
  }

  // Early exit: no records found — return empty result without creating columns
  if (result_capacity == 0 && result->num_points == 0) {
    bool any_records = false;
    for (size_t f = 0; f < num_fields && !any_records; f++) {
      for (size_t s = 0; s < fields_array[f].num_series && !any_records; s++) {
        if (fields_array[f].series_lists[s].count > 0) any_records = true;
      }
    }
    if (!any_records) {
      *out_ok = complete;
      series_lookup_free(&srl_tbl);
      return 0;
    }
  }

  // --------------------------------------------------------------------------
  // PART B: Verify that all series in each field share the same type
  // --------------------------------------------------------------------------
  size_t total_points_aggregated = 0;

  timeseries_aggregation_method_e agg_method = query->aggregate_method;

  for (size_t f = 0; f < num_fields; f++) {
    field_info_t *fld = &fields_array[f];
    if (fld->num_series == 0) {
      continue;
    }

    // Determine a consistent field_type for all series in this field
    timeseries_field_type_e field_type = TIMESERIES_FIELD_TYPE_FLOAT;
    bool field_type_determined = false;
    bool skip_this_field = false;

    for (size_t s = 0; s < fld->num_series; s++) {
      timeseries_field_type_e series_type;
      // Use cached lookup (Phase 2 optimization)
      bool found_type = tsdb_lookup_series_type_cached(
          db, fld->series_lists[s].series_id.bytes, &series_type);
      if (!found_type) {
        ESP_LOGW(TAG,
                 "No type in metadata for series=%.2X%.2X%.2X%.2X..., "
                 "field='%s'. Skipping.",
                 fld->series_lists[s].series_id.bytes[0],
                 fld->series_lists[s].series_id.bytes[1],
                 fld->series_lists[s].series_id.bytes[2],
                 fld->series_lists[s].series_id.bytes[3], fld->field_name);
        skip_this_field = true;
        break;
      }

      if (!field_type_determined) {
        field_type = series_type;
        field_type_determined = true;
      } else if (series_type != field_type) {
        ESP_LOGE(
            TAG,
            "Field '%s' has conflicting series types (%d != %d). Skipping.",
            fld->field_name, (int)series_type, (int)field_type);
        skip_this_field = true;
        break;
      }
    }

    if (!field_type_determined || skip_this_field) {
      // No valid type or conflict => skip aggregator for this field
      continue;
    }

    // ----------------------------------------------------------------------
    // PART C: Build multi_points_iterator per series, unify via
    // multi_series_iterator
    //         and store the aggregator results in the `result` struct.
    // ----------------------------------------------------------------------
    // We'll create a column in `result` for this field (if not already
    // present). This is the column we'll store all aggregated points into.
    size_t col_index =
        find_or_create_column(result, fld->field_name, field_type,
                              result_capacity);

    if (col_index == SIZE_MAX) {
      complete = false;
      /* Fatal for this field – skip it but keep processing others */
      continue;
    }

    // Keep track of how many data points we've inserted for this field
    // so we can respect the `query->limit` per-field (as requested).
    size_t inserted_for_this_field = 0;
    bool field_limit_reached = false;

    if (query->rollup_interval == 0) {
      // ====================================================================
      // PASS-THROUGH MODE: process each series independently.
      // Only one series' iterators are alive at a time, reducing peak memory
      // from O(num_series × data_per_series) to O(data_per_series).
      // find_or_create_row maintains sorted timestamp order via insertion.
      // ====================================================================
      for (size_t s = 0; s < fld->num_series && !field_limit_reached; s++) {
        series_record_list_t *srl = &fld->series_lists[s];
        if (srl->count == 0) continue;

        // Allocate iterators for this series only
        timeseries_points_iterator_t *pit_batch =
            calloc(srl->count, sizeof(timeseries_points_iterator_t));
        timeseries_points_iterator_t **sub_iters =
            calloc(srl->count, sizeof(*sub_iters));
        if (!pit_batch || !sub_iters) {
          free(pit_batch); free(sub_iters);
          complete = false;
          ESP_LOGE(TAG, "OOM: series iter alloc for field='%s'", fld->field_name);
          continue;
        }

        size_t idx = query_open_record_iters(db, srl, field_type, query,
                                             pit_batch, sub_iters, &complete);

        if (f + 1 == num_fields && s + 1 == fld->num_series)
          query_read_unlock(guard);

        if (idx > 0) {
          timeseries_multi_points_iterator_t mpit;
          if (timeseries_multi_points_iterator_init(sub_iters, NULL, idx, &mpit)) {
            // Pre-compute total record count to avoid repeated buffer doubling,
            // which causes OOM due to peak memory (old + new buffer during realloc).
            size_t total_records = 0;
            for (size_t ri = 0; ri < srl->count; ri++) {
              total_records += srl->records[ri].record_count;
            }
            // Apply query limit if set
            if (query->limit != 0 && total_records > query->limit) {
              total_records = query->limit;
            }
            // Use at least 64 to avoid zero-size alloc, cap at total_records
            size_t buf_cap = (total_records > 64) ? total_records : 64;

            // Buffer points from iterator, then merge into result in O(n+m)
            size_t buf_count = 0;
            uint64_t *buf_ts = malloc(buf_cap * sizeof(uint64_t));
            timeseries_field_value_t *buf_vals = malloc(buf_cap * sizeof(timeseries_field_value_t));
            bool buf_ok = (buf_ts && buf_vals);

            if (buf_ok) {
              uint64_t ts;
              timeseries_field_value_t val;
              while (timeseries_multi_points_iterator_next(&mpit, &ts, &val)) {
                if (query->start_ms != 0 && ts < query->start_ms) {
                  query_value_release(&val);
                  continue;
                }
                if (query->end_ms != 0 && ts > query->end_ms) {
                  query_value_release(&val);
                  break;
                }
                if (query->limit != 0 && inserted_for_this_field >= query->limit) {
                  query_value_release(&val);
                  field_limit_reached = true;
                  break;
                }

                // Grow buffer if needed
                if (buf_count == buf_cap) {
                  size_t new_cap = buf_cap * 2;
                  uint64_t *nt = realloc(buf_ts, new_cap * sizeof(uint64_t));
                  timeseries_field_value_t *nv = realloc(buf_vals, new_cap * sizeof(timeseries_field_value_t));
                  if (!nt || !nv) {
                    if (nt) buf_ts = nt;
                    if (nv) buf_vals = nv;
                    complete = false;
                    ESP_LOGE(TAG, "OOM growing point buffer for field '%s'", fld->field_name);
                    query_value_release(&val);
                    field_limit_reached = true;
                    break;
                  }
                  buf_ts = nt;
                  buf_vals = nv;
                  buf_cap = new_cap;
                }

                buf_ts[buf_count] = ts;
                buf_vals[buf_count] = val;
                buf_count++;
                inserted_for_this_field++;
                total_points_aggregated++;
              }

              // Merge buffered points into result — O(n + m) instead of O(n²)
              if (buf_count > 0) {
                if (!merge_sorted_field_data(result, col_index, buf_ts, buf_vals,
                                             buf_count, &result_capacity)) {
                  complete = false;
                  ESP_LOGE(TAG, "OOM merging result for field '%s'", fld->field_name);
                  // A failed merge has not transferred ownership to the result.
                  for (size_t bi = 0; bi < buf_count; bi++) {
                    query_value_release(&buf_vals[bi]);
                  }
                }
              }
            } else {
              complete = false;
              ESP_LOGE(TAG, "OOM allocating point buffer for field '%s'", fld->field_name);
            }

            free(buf_ts);
            free(buf_vals);

            // Cleanup immediately: deinit sub-iterators through the multi_points_iterator
            query_close_multi_points(&mpit);
          } else {
            complete = false;
            query_close_record_iters(sub_iters, idx);
          }
        }

        free(sub_iters);
        free(pit_batch);
      } // end sequential series processing
    } else {
      // ====================================================================
      // AGGREGATION MODE: all iterators alive, use multi_series_iterator
      // ====================================================================

      // Build a multi_points_iterator for each series.
      timeseries_multi_points_iterator_t **series_multi_iters =
          calloc(fld->num_series, sizeof(*series_multi_iters));
      timeseries_multi_points_iterator_t *mpit_batch =
          calloc(fld->num_series, sizeof(timeseries_multi_points_iterator_t));
      timeseries_points_iterator_t **pit_batches =
          calloc(fld->num_series, sizeof(timeseries_points_iterator_t *));
      if (!series_multi_iters || !mpit_batch || !pit_batches) {
        free(series_multi_iters);
        free(mpit_batch);
        free(pit_batches);
        complete = false;
        ESP_LOGE(TAG, "OOM: cannot allocate iterator arrays for field='%s'",
                 fld->field_name);
        continue;
      }

      for (size_t s = 0; s < fld->num_series; s++) {
        series_record_list_t *srl = &fld->series_lists[s];
        if (srl->count == 0) continue;

        timeseries_points_iterator_t *pit_batch =
            calloc(srl->count, sizeof(timeseries_points_iterator_t));
        timeseries_points_iterator_t **sub_iters =
            calloc(srl->count, sizeof(*sub_iters));
        if (!pit_batch || !sub_iters) {
          free(pit_batch); free(sub_iters);
          complete = false;
          ESP_LOGE(TAG, "OOM building sub-iter structures for field='%s'",
                   fld->field_name);
          continue;
        }
        pit_batches[s] = pit_batch;

        size_t idx = query_open_record_iters(db, srl, field_type, query,
                                             pit_batch, sub_iters, &complete);

        if (f + 1 == num_fields && s + 1 == fld->num_series)
          query_read_unlock(guard);

        if (idx > 0) {
          timeseries_multi_points_iterator_t *mpit = &mpit_batch[s];
          if (timeseries_multi_points_iterator_init(sub_iters, NULL, idx, mpit)) {
            series_multi_iters[s] = mpit;
          } else {
            complete = false;
            query_close_record_iters(sub_iters, idx);
          }
        }

        free(sub_iters);
      } // end for each series in field

      timeseries_multi_series_iterator_t ms_iter;
      if (!timeseries_multi_series_iterator_init(
              series_multi_iters, fld->num_series, query->rollup_interval,
              agg_method, &ms_iter)) {
        complete = false;
        ESP_LOGW(TAG, "Failed multi-series aggregator for '%s'", fld->field_name);
      } else {
        /* One window grid for every field (the query start, else the epoch):
         * windows starting at each field's own first sample never lined up,
         * so fields sampled a few ms apart landed in separate rows. */
        timeseries_multi_series_iterator_align(&ms_iter, query->start_ms > 0 ? (uint64_t)query->start_ms : 0);
        size_t agg_hint = 0;
        while (true) {
          uint64_t rollup_ts = 0;
          timeseries_field_value_t val_agg;
          if (!timeseries_multi_series_iterator_next(&ms_iter, &rollup_ts, &val_agg))
            break;

          if (query->start_ms != 0 && rollup_ts < query->start_ms) {
            query_value_release(&val_agg);
            continue;
          }
          if (query->end_ms != 0 && rollup_ts > query->end_ms) {
            query_value_release(&val_agg);
            break;
          }
          if (query->limit != 0 && inserted_for_this_field >= query->limit) {
            query_value_release(&val_agg);
            break;
          }

          size_t row_index = find_or_create_row(result, rollup_ts, &result_capacity, &agg_hint);
          if (row_index == SIZE_MAX) {
            complete = false;
            ESP_LOGE(TAG, "OOM inserting new result row – aborting field '%s'", fld->field_name);
            query_value_release(&val_agg);
            break;
          }

          if (result->columns[col_index].values[row_index].type == TIMESERIES_FIELD_TYPE_STRING &&
              result->columns[col_index].values[row_index].data.string_val.str) {
            free(result->columns[col_index].values[row_index].data.string_val.str);
          }
          result->columns[col_index].values[row_index].type = val_agg.type;
          result->columns[col_index].values[row_index].data = val_agg.data;

          inserted_for_this_field++;
          total_points_aggregated++;
        } // end while aggregator
        timeseries_multi_series_iterator_deinit(&ms_iter);
      }

      // Cleanup: deinit iterators, then free batch allocations
      for (size_t s = 0; s < fld->num_series; s++) {
        if (series_multi_iters[s]) {
          query_close_multi_points(series_multi_iters[s]);
        }
        free(pit_batches[s]);
      }
      free(series_multi_iters);
      free(mpit_batch);
      free(pit_batches);
    } // end aggregation mode
  } // end for each field

  series_lookup_free(&srl_tbl);

  *out_ok = complete;
  return total_points_aggregated;
}

// =============================================================================
// Timestamp Range (header-only scan, no decompression)
// =============================================================================

bool timeseries_query_get_timestamp_range(timeseries_db_t *db,
                                          const char *measurement_name,
                                          uint64_t *out_min_ms,
                                          uint64_t *out_max_ms,
                                          uint32_t *out_point_count) {
  if (!db || !measurement_name || !out_min_ms || !out_max_ms) {
    return false;
  }

  *out_min_ms = 0;
  *out_max_ms = 0;
  if (out_point_count) *out_point_count = 0;

  // 1. Resolve measurement
  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(db, measurement_name, &measurement_id)) {
    return false;
  }

  // 2. Get all series IDs for this measurement
  timeseries_series_id_list_t all_series;
  tsdb_series_id_list_init(&all_series);
  if (!tsdb_find_all_series_ids_for_measurement(db, measurement_id, &all_series) ||
      all_series.count == 0) {
    tsdb_series_id_list_free(&all_series);
    return false;
  }

  // 3. Build a hash set of series IDs for fast lookup
  size_t cap = 2;
  while (cap < all_series.count * 2) cap *= 2;

  typedef struct { unsigned char id[16]; bool used; } id_entry_t;
  id_entry_t *set = calloc(cap, sizeof(id_entry_t));
  if (!set) {
    tsdb_series_id_list_free(&all_series);
    return false;
  }

  for (size_t i = 0; i < all_series.count; i++) {
    uint32_t h = hash_series_id16(all_series.ids[i].bytes);
    size_t idx = h & (cap - 1);
    while (set[idx].used) idx = (idx + 1) & (cap - 1);
    memcpy(set[idx].id, all_series.ids[i].bytes, 16);
    set[idx].used = true;
  }
  tsdb_series_id_list_free(&all_series);

  // 4. Scan all field data pages, reading only headers
  uint64_t global_min = UINT64_MAX;
  uint64_t global_max = 0;
  uint32_t total_points = 0;
  bool found_any = false;

  timeseries_page_cache_iterator_t page_iter;
  if (!timeseries_page_cache_iterator_init(db, &page_iter)) {
    free(set);
    return false;
  }

  uint8_t *shared_buf = NULL;
  uint32_t shared_buf_cap = 0;
  timeseries_page_header_t hdr;
  uint32_t page_offset = 0, page_size = 0;

  while (timeseries_page_cache_iterator_next(&page_iter, &hdr, &page_offset, &page_size)) {
    if (hdr.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA) continue;

    if (page_size <= sizeof(timeseries_page_header_t)) {
      ESP_LOGW(TAG, "Corrupt page @0x%08X: page_size=%u too small, skipping",
               (unsigned int)page_offset, (unsigned int)page_size);
      continue;
    }
    uint32_t data_size = page_size - sizeof(timeseries_page_header_t);
    if (data_size > shared_buf_cap) {
      uint8_t *new_buf = realloc(shared_buf, data_size);
      if (!new_buf) {
        ESP_LOGW(TAG, "realloc(%u) failed for page @0x%08X, skipping",
                 (unsigned int)data_size, (unsigned int)page_offset);
        continue;
      }
      shared_buf = new_buf;
      shared_buf_cap = data_size;
    }

    timeseries_fielddata_iterator_t fdata_iter;
    if (!timeseries_fielddata_iterator_init_with_buffer(
            db, page_offset, page_size, shared_buf, shared_buf_cap, &fdata_iter)) {
      continue;
    }

    timeseries_field_data_header_t fdh;
    while (timeseries_fielddata_iterator_next(&fdata_iter, &fdh)) {
      if ((fdh.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) continue;

      // Check if this series belongs to our measurement
      uint32_t h = hash_series_id16(fdh.series_id);
      size_t idx = h & (cap - 1);
      bool match = false;
      while (set[idx].used) {
        if (memcmp(set[idx].id, fdh.series_id, 16) == 0) { match = true; break; }
        idx = (idx + 1) & (cap - 1);
      }
      if (!match) continue;

      found_any = true;
      if (fdh.start_time < global_min) global_min = fdh.start_time;
      if (fdh.end_time > global_max) global_max = fdh.end_time;
      total_points += fdh.record_count;
    }
    timeseries_fielddata_iterator_deinit(&fdata_iter);
  }
  timeseries_page_cache_iterator_deinit(&page_iter);
  free(shared_buf);
  free(set);

  if (!found_any) {
    return false;  // no data found
  }

  *out_min_ms = global_min;
  *out_max_ms = global_max;
  if (out_point_count) *out_point_count = total_points;
  return true;
}
