#include "timeseries_query.h"
#include "esp_timer.h"
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
                                timeseries_query_result_t *result);

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

  uint64_t start_time, end_time;

  /* -------------------------------------------------------------------- */
  /*  House-keeping vars                                                  */
  /* -------------------------------------------------------------------- */
  bool ok = false; /* final return value       */
  size_t actual_fields_count = 0;
  field_info_t *fields_array = NULL;

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
  start_time = esp_timer_get_time();

  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(db, query->measurement_name, &measurement_id)) {
    ESP_LOGW(TAG, "Measurement '%s' not found.", query->measurement_name);
    ok = true; /* empty result   */
    goto cleanup;
  }

  end_time = esp_timer_get_time();
  ESP_LOGD(TAG, "Resolved measurement in %.3f ms",
           (end_time - start_time) / 1000.0);

  /* -------------------------------------------------------------------- */
  /*  2.  Build `matched_series`                                          */
  /* -------------------------------------------------------------------- */
  start_time = esp_timer_get_time();

  if (query->num_tags > 0) {
    timeseries_series_id_list_t temp_list;
    tsdb_series_id_list_init(&temp_list);

    for (size_t i = 0; i < query->num_tags; ++i) {
      tsdb_series_id_list_clear(&temp_list);
      tsdb_find_series_ids_for_tag(db, measurement_id, query->tag_keys[i],
                                   query->tag_values[i], &temp_list);

      if (i == 0) {
        tsdb_series_id_list_copy(&matched_series, &temp_list);
      } else {
        intersect_series_id_lists(&matched_series, &temp_list);
      }
    }
    tsdb_series_id_list_free(&temp_list);
  } else {
    if (!tsdb_find_all_series_ids_for_measurement(db, measurement_id,
                                                  &matched_series) ||
        matched_series.count == 0) {
      ESP_LOGD(TAG, "No series found for measurement '%s'.",
               query->measurement_name);
      ok = true; /* empty result   */
      goto cleanup;
    }
  }

  if (matched_series.count == 0) {
    ESP_LOGD(TAG, "No series matched the tag filters (or none exist).");
    ok = true; /* empty result   */
    goto cleanup;
  }

  end_time = esp_timer_get_time();
  ESP_LOGD(TAG, "Found %zu series for measurement '%s' in %.3f ms",
           matched_series.count, query->measurement_name,
           (end_time - start_time) / 1000.0);

  /* -------------------------------------------------------------------- */
  /*  3.  Build list of field names                                       */
  /* -------------------------------------------------------------------- */
  start_time = esp_timer_get_time();

  if (query->num_fields == 0) {
    if (!tsdb_list_fields_for_measurement(db, measurement_id,
                                          &fields_to_query) ||
        fields_to_query.count == 0) {
      ESP_LOGD(TAG, "No fields found for measurement '%s'.",
               query->measurement_name);
      ok = true; /* empty result   */
      goto cleanup;
    }
  } else {
    for (size_t i = 0; i < query->num_fields; ++i) {
      tsdb_string_list_append_unique(&fields_to_query, query->field_names[i]);
    }
  }

  end_time = esp_timer_get_time();
  ESP_LOGD(TAG, "Found fields for measurement in %.3f ms",
           (end_time - start_time) / 1000.0);

  /* -------------------------------------------------------------------- */
  /*  4.  Allocate & populate `fields_array`                              */
  /* -------------------------------------------------------------------- */

  start_time = esp_timer_get_time();

  fields_array = calloc(fields_to_query.count, sizeof(field_info_t));
  if (!fields_array) {
    ESP_LOGE(TAG, "Out of memory for fields_array");
    goto cleanup;
  }

  for (size_t f = 0; f < fields_to_query.count; ++f) {
    const char *fname = fields_to_query.items[f];

    timeseries_series_id_list_t field_series;
    tsdb_series_id_list_init(&field_series);

    bool got = tsdb_find_series_ids_for_field(db, measurement_id, fname,
                                              &field_series);
    ESP_LOGD(TAG, "Field '%s' (meas_id=%" PRIu32 "): found %zu series_ids (got=%d)",
             fname, measurement_id, field_series.count, (int)got);
    for (size_t si = 0; si < field_series.count && si < 3; si++) {
      ESP_LOGD(TAG, "  series_id[%zu]: %02X%02X%02X%02X%02X%02X%02X%02X...",
               si,
               field_series.ids[si].bytes[0], field_series.ids[si].bytes[1],
               field_series.ids[si].bytes[2], field_series.ids[si].bytes[3],
               field_series.ids[si].bytes[4], field_series.ids[si].bytes[5],
               field_series.ids[si].bytes[6], field_series.ids[si].bytes[7]);
    }
    if (!got || field_series.count == 0) {
      tsdb_series_id_list_free(&field_series);
      continue;
    }

    if (query->num_tags > 0) {
      intersect_series_id_lists(&field_series, &matched_series);
    }
    if (field_series.count == 0) {
      tsdb_series_id_list_free(&field_series);
      continue;
    }

    field_info_t *fi = &fields_array[actual_fields_count];

    fi->field_name = fname;
    fi->series_ids = field_series;
    fi->num_series = field_series.count;
    fi->series_lists = calloc(field_series.count, sizeof(series_record_list_t));

    if (!fi->series_lists) {
      ESP_LOGE(TAG, "Out of memory for series_lists");
      tsdb_series_id_list_free(&field_series);
      goto cleanup; /* early error    */
    }

    for (size_t s = 0; s < field_series.count; ++s) {
      fi->series_lists[s].series_id = field_series.ids[s];
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

  end_time = esp_timer_get_time();
  ESP_LOGD(TAG, "Prepared fields for measurement in %.3f ms",
           (end_time - start_time) / 1000.0);

  /* -------------------------------------------------------------------- */
  /*  5.  Fetch & aggregate                                               */
  /* -------------------------------------------------------------------- */

  start_time = esp_timer_get_time();

  size_t points_fetched = fetch_series_data(db, fields_array, actual_fields_count, query, result);
  if (points_fetched == 0 && result->num_points == 0) {
    ESP_LOGD(TAG, "fetch_series_data returned no points");
  }

  ok = true; /* At this point the function succeeded – result is valid */

  end_time = esp_timer_get_time();
  ESP_LOGD(TAG, "Fetched data in %.3f ms", (end_time - start_time) / 1000.0);

cleanup:
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

  tsdb_string_list_free(&fields_to_query);
  tsdb_series_id_list_free(&matched_series);

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
 * @brief Ensure the result has capacity for at least one more row.
 *        Grows by doubling to amortize allocation cost.
 */
static bool ensure_result_capacity(timeseries_query_result_t *result,
                                   size_t *capacity) {
  if (result->num_points < *capacity) {
    return true;
  }
  size_t new_cap = *capacity ? *capacity * 2 : 64;
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
  if (!ensure_result_capacity(result, capacity)) {
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
    while (*capacity < new_count) {
      if (!ensure_result_capacity(result, capacity)) return false;
    }
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

  // Single-pass: try placing values assuming all timestamps exist.
  // If we encounter a missing timestamp, count remaining inserts and fall through to merge.
  size_t insert_count = 0;
  {
    size_t oi = 0;
    size_t ni = 0;
    for (; ni < new_count; ni++) {
      while (oi < old_count && result->timestamps[oi] < new_ts[ni]) {
        oi++;
      }
      if (oi >= old_count || result->timestamps[oi] != new_ts[ni]) {
        // Found a new timestamp — count it and finish counting the rest
        insert_count = 1;
        oi = oi; // keep oi position for continued counting
        for (size_t ni2 = ni + 1; ni2 < new_count; ni2++) {
          while (oi < old_count && result->timestamps[oi] < new_ts[ni2]) {
            oi++;
          }
          if (oi >= old_count || result->timestamps[oi] != new_ts[ni2]) {
            insert_count++;
          } else {
            oi++;
          }
        }
        break;
      }
      // Timestamp exists — place value immediately (combines count + placement)
      result->columns[col_index].values[oi] = new_vals[ni];
      oi++;
    }
    if (insert_count == 0) {
      return true;  // All timestamps existed, values already placed
    }
  }

  // Ensure capacity for merged result
  size_t merged_count = old_count + insert_count;
  while (*capacity < merged_count) {
    if (!ensure_result_capacity(result, capacity)) return false;
  }

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
      // Equal timestamps — keep old row, set value for current column
      if ((size_t)ki != (size_t)oi) {
        result->timestamps[ki] = result->timestamps[oi];
        for (size_t c = 0; c < result->num_columns; c++) {
          if (c != col_index) {
            result->columns[c].values[ki] = result->columns[c].values[oi];
          }
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

static int cmp_record_by_start_time(const void *a, const void *b) {
  const field_record_info_t *ra = (const field_record_info_t *)a;
  const field_record_info_t *rb = (const field_record_info_t *)b;
  if (ra->start_time < rb->start_time) return -1;
  if (ra->start_time > rb->start_time) return 1;
  return 0;
}

/**
 * @brief Append a record to the growable array (unsorted).
 *        Sorting is deferred to a single qsort after all records are collected.
 */
static bool field_record_array_append(series_record_list_t *srl,
                                      const timeseries_field_data_header_t *fdh,
                                      uint32_t absolute_offset,
                                      unsigned int limit) {
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
  rec->start_time = fdh->start_time;
  rec->end_time = fdh->end_time;
  rec->record_count = fdh->record_count;
  rec->record_length = fdh->record_length;
  rec->data_flags = fdh->flags;
  return true;
}

/**
 * @brief Sort the record array by start_time, then prune to keep only
 *        enough records to cover `limit` data points.
 */
static void field_record_array_sort_and_prune(series_record_list_t *srl,
                                              unsigned int limit) {
  if (!srl || srl->count == 0) return;

  qsort(srl->records, srl->count, sizeof(field_record_info_t),
        cmp_record_by_start_time);

  if (limit == 0) return; /* no limit => keep everything */

  uint64_t coverage = 0;
  size_t keep = srl->count;
  for (size_t i = 0; i < srl->count; i++) {
    coverage += srl->records[i].record_count;
    if (coverage >= limit) {
      keep = i + 1;
      break;
    }
  }
  srl->count = keep;
}

static size_t fetch_series_data(timeseries_db_t *db, field_info_t *fields_array,
                                size_t num_fields,
                                const timeseries_query_t *query,
                                timeseries_query_result_t *result) {
  if (!db || !fields_array || num_fields == 0 || !query || !result) {
    ESP_LOGW(TAG, "Invalid input to fetch_series_data");
    return 0;
  }

  size_t result_capacity = 0; /* local capacity tracker (thread-safe) */

  // Profiling timers for Phase 3 optimization
  uint64_t prof_start, prof_end;

  // --------------------------------------------------------------------------
  // PART A: Single pass to gather the record_info linked-lists for each series
  // --------------------------------------------------------------------------
  prof_start = esp_timer_get_time();

  series_lookup_t srl_tbl = {0};
  if (!series_lookup_build(fields_array, num_fields, &srl_tbl)) {
    ESP_LOGE(TAG, "OOM building series-id lookup table");
    return 0;
  }

  prof_end = esp_timer_get_time();
  ESP_LOGD(TAG, "[PROFILE] Lookup table build: %.3f ms", (prof_end - prof_start) / 1000.0);

  prof_start = esp_timer_get_time();

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

  // Shared page buffer — avoids malloc/free per field-data page
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

    // Grow shared buffer if this page is larger
    if (page_size <= sizeof(timeseries_page_header_t)) {
      ESP_LOGW(TAG, "Corrupt page @0x%08X: page_size=%u too small, skipping",
               (unsigned int)page_offset, (unsigned int)page_size);
      continue;
    }
    uint32_t data_size = page_size - sizeof(timeseries_page_header_t);
    if (data_size > shared_page_buf_cap) {
      uint8_t *new_buf = realloc(shared_page_buf, data_size);
      if (!new_buf) {
        ESP_LOGW(TAG, "realloc(%u) failed for page @0x%08X, skipping",
                 (unsigned int)data_size, (unsigned int)page_offset);
        continue;
      }
      shared_page_buf = new_buf;
      shared_page_buf_cap = data_size;
    }

    timeseries_fielddata_iterator_t fdata_iter;
    if (!timeseries_fielddata_iterator_init_with_buffer(
            db, page_offset, page_size, shared_page_buf,
            shared_page_buf_cap, &fdata_iter)) {
      ESP_LOGW(TAG, "Failed to init fielddata_iterator on page @0x%08X",
               (unsigned int)page_offset);
      continue;
    }

    timeseries_field_data_header_t fdh;
    int page_records = 0;
    int page_records_matched = 0;
    while (timeseries_fielddata_iterator_next(&fdata_iter, &fdh)) {
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
        field_record_array_append(srl, &fdh, absolute_offset,
                                  query->limit);
      } else {
        ESP_LOGD(TAG, "No match for series %02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X (flags=0x%02X, records=%u)",
                 fdh.series_id[0], fdh.series_id[1], fdh.series_id[2], fdh.series_id[3],
                 fdh.series_id[4], fdh.series_id[5], fdh.series_id[6], fdh.series_id[7],
                 fdh.series_id[8], fdh.series_id[9], fdh.series_id[10], fdh.series_id[11],
                 fdh.series_id[12], fdh.series_id[13], fdh.series_id[14], fdh.series_id[15],
                 fdh.flags, fdh.record_count);
      }
    } // end while fielddata_iterator
    timeseries_fielddata_iterator_deinit(&fdata_iter);
    ESP_LOGD(TAG, "Page @0x%08X: %d records scanned, %d matched",
             (unsigned int)page_offset, page_records, page_records_matched);
  } // end while page_iterator
  timeseries_page_cache_iterator_deinit(&page_iter);
  free(shared_page_buf);

  prof_end = esp_timer_get_time();
  ESP_LOGD(TAG, "[PROFILE] Page scanning: %.3f ms (%d pages, %d field_data pages, %d records)",
           (prof_end - prof_start) / 1000.0, pages_processed, field_data_pages, records_found);

  // Optimization #1: Prune once per series after all records collected (instead of after each append)
  prof_start = esp_timer_get_time();
  for (size_t f = 0; f < num_fields; f++) {
    for (size_t s = 0; s < fields_array[f].num_series; s++) {
      field_record_array_sort_and_prune(&fields_array[f].series_lists[s], query->limit);
    }
  }
  prof_end = esp_timer_get_time();
  ESP_LOGD(TAG, "[PROFILE] Prune lists: %.3f ms", (prof_end - prof_start) / 1000.0);

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
      series_lookup_free(&srl_tbl);
      return 0;
    }
  }

  // --------------------------------------------------------------------------
  // PART B: Verify that all series in each field share the same type
  // --------------------------------------------------------------------------
  prof_start = esp_timer_get_time();

  // Fine-grained profiling for Phase 3
  uint64_t prof_type_verify = 0;
  uint64_t prof_iter_create = 0;
  uint64_t prof_decompress = 0;
  uint64_t prof_result_build = 0;
  uint64_t prof_temp;

  size_t total_points_aggregated = 0;

  timeseries_aggregation_method_e agg_method = query->aggregate_method;

  for (size_t f = 0; f < num_fields; f++) {
    prof_temp = esp_timer_get_time();
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

    prof_type_verify += (esp_timer_get_time() - prof_temp);

    // ----------------------------------------------------------------------
    // PART C: Build multi_points_iterator per series, unify via
    // multi_series_iterator
    //         and store the aggregator results in the `result` struct.
    // ----------------------------------------------------------------------
    prof_temp = esp_timer_get_time();

    // We'll create a column in `result` for this field (if not already
    // present). This is the column we'll store all aggregated points into.
    size_t col_index =
        find_or_create_column(result, fld->field_name, field_type,
                              result_capacity);

    if (col_index == SIZE_MAX) {
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
      prof_iter_create += (esp_timer_get_time() - prof_temp);
      prof_temp = esp_timer_get_time();

      for (size_t s = 0; s < fld->num_series && !field_limit_reached; s++) {
        series_record_list_t *srl = &fld->series_lists[s];
        if (srl->count == 0) continue;

        // Allocate iterators for this series only
        timeseries_points_iterator_t *pit_batch =
            calloc(srl->count, sizeof(timeseries_points_iterator_t));
        timeseries_points_iterator_t **sub_iters =
            calloc(srl->count, sizeof(*sub_iters));
        uint32_t *page_seqs = calloc(srl->count, sizeof(uint32_t));
        if (!pit_batch || !sub_iters || !page_seqs) {
          free(pit_batch); free(sub_iters); free(page_seqs);
          ESP_LOGE(TAG, "OOM: series iter alloc for field='%s'", fld->field_name);
          continue;
        }

        size_t idx = 0;
        for (size_t ri = 0; ri < srl->count; ri++) {
          field_record_info_t *rec = &srl->records[ri];
          timeseries_points_iterator_t *pit = &pit_batch[ri];
          bool ok = timeseries_points_iterator_init(
              db, rec->record_offset, rec->record_length,
              rec->record_count, field_type, rec->data_flags, pit);
          if (!ok) continue;
          timeseries_points_iterator_set_time_range(pit, query->start_ms, query->end_ms);
          sub_iters[idx] = pit;
          page_seqs[idx] = 0;
          idx++;
        }

        if (idx > 0) {
          timeseries_multi_points_iterator_t mpit;
          if (timeseries_multi_points_iterator_init(sub_iters, page_seqs, idx, &mpit)) {
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
                  if (val.type == TIMESERIES_FIELD_TYPE_STRING && val.data.string_val.str)
                    free(val.data.string_val.str);
                  continue;
                }
                if (query->end_ms != 0 && ts > query->end_ms) {
                  if (val.type == TIMESERIES_FIELD_TYPE_STRING && val.data.string_val.str)
                    free(val.data.string_val.str);
                  break;
                }
                if (query->limit != 0 && inserted_for_this_field >= query->limit) {
                  if (val.type == TIMESERIES_FIELD_TYPE_STRING && val.data.string_val.str)
                    free(val.data.string_val.str);
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
                    ESP_LOGE(TAG, "OOM growing point buffer for field '%s'", fld->field_name);
                    if (val.type == TIMESERIES_FIELD_TYPE_STRING && val.data.string_val.str)
                      free(val.data.string_val.str);
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
                  ESP_LOGE(TAG, "OOM merging result for field '%s'", fld->field_name);
                  // The forward pass in merge_sorted_field_data may have shallow-
                  // copied some buf_vals entries into the result column.  NULL those
                  // out so timeseries_query_free_result won't double-free them.
                  for (size_t ri = 0; ri < result->num_points; ri++) {
                    timeseries_field_value_t *rv =
                        &result->columns[col_index].values[ri];
                    if (rv->type == TIMESERIES_FIELD_TYPE_STRING &&
                        rv->data.string_val.str) {
                      for (size_t bi = 0; bi < buf_count; bi++) {
                        if (buf_vals[bi].data.string_val.str ==
                            rv->data.string_val.str) {
                          rv->data.string_val.str = NULL;
                          break;
                        }
                      }
                    }
                  }
                  // Now safe to free all string values from the buffer
                  for (size_t bi = 0; bi < buf_count; bi++) {
                    if (buf_vals[bi].type == TIMESERIES_FIELD_TYPE_STRING &&
                        buf_vals[bi].data.string_val.str)
                      free(buf_vals[bi].data.string_val.str);
                  }
                }
              }
            } else {
              ESP_LOGE(TAG, "OOM allocating point buffer for field '%s'", fld->field_name);
            }

            free(buf_ts);
            free(buf_vals);

            // Cleanup immediately: deinit sub-iterators through the multi_points_iterator
            for (size_t i_sub = 0; i_sub < mpit.sub_count; i_sub++) {
              if (mpit.subs[i_sub].sub_iter)
                timeseries_points_iterator_deinit(mpit.subs[i_sub].sub_iter);
            }
            timeseries_multi_points_iterator_deinit(&mpit);
          } else {
            for (size_t i2 = 0; i2 < idx; i2++) {
              if (sub_iters[i2])
                timeseries_points_iterator_deinit(sub_iters[i2]);
            }
          }
        }

        free(sub_iters);
        free(page_seqs);
        free(pit_batch);
      } // end sequential series processing

      prof_decompress += (esp_timer_get_time() - prof_temp);
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
        uint32_t *page_seqs = calloc(srl->count, sizeof(uint32_t));
        if (!pit_batch || !sub_iters || !page_seqs) {
          free(pit_batch); free(sub_iters); free(page_seqs);
          ESP_LOGE(TAG, "OOM building sub-iter structures for field='%s'",
                   fld->field_name);
          continue;
        }
        pit_batches[s] = pit_batch;

        size_t idx = 0;
        for (size_t ri = 0; ri < srl->count; ri++) {
          field_record_info_t *rec = &srl->records[ri];
          timeseries_points_iterator_t *pit = &pit_batch[ri];
          bool ok = timeseries_points_iterator_init(
              db, rec->record_offset, rec->record_length,
              rec->record_count, field_type, rec->data_flags, pit);
          if (!ok) continue;
          timeseries_points_iterator_set_time_range(pit, query->start_ms, query->end_ms);
          sub_iters[idx] = pit;
          page_seqs[idx] = 0;
          idx++;
        }

        if (idx > 0) {
          timeseries_multi_points_iterator_t *mpit = &mpit_batch[s];
          if (timeseries_multi_points_iterator_init(sub_iters, page_seqs, idx, mpit)) {
            series_multi_iters[s] = mpit;
          } else {
            for (size_t i2 = 0; i2 < idx; i2++) {
              if (sub_iters[i2])
                timeseries_points_iterator_deinit(sub_iters[i2]);
            }
          }
        }

        free(sub_iters);
        free(page_seqs);
      } // end for each series in field

      prof_iter_create += (esp_timer_get_time() - prof_temp);

      timeseries_multi_series_iterator_t ms_iter;
      if (!timeseries_multi_series_iterator_init(
              series_multi_iters, fld->num_series, query->rollup_interval,
              agg_method, &ms_iter)) {
        ESP_LOGW(TAG, "Failed multi-series aggregator for '%s'", fld->field_name);
      } else {
        prof_temp = esp_timer_get_time();
        size_t agg_hint = 0;
        while (true) {
          uint64_t rollup_ts = 0;
          timeseries_field_value_t val_agg;
          if (!timeseries_multi_series_iterator_next(&ms_iter, &rollup_ts, &val_agg))
            break;

          if (query->start_ms != 0 && rollup_ts < query->start_ms) {
            if (val_agg.type == TIMESERIES_FIELD_TYPE_STRING && val_agg.data.string_val.str)
              free(val_agg.data.string_val.str);
            continue;
          }
          if (query->end_ms != 0 && rollup_ts > query->end_ms) {
            if (val_agg.type == TIMESERIES_FIELD_TYPE_STRING && val_agg.data.string_val.str)
              free(val_agg.data.string_val.str);
            break;
          }
          if (query->limit != 0 && inserted_for_this_field >= query->limit) {
            if (val_agg.type == TIMESERIES_FIELD_TYPE_STRING && val_agg.data.string_val.str)
              free(val_agg.data.string_val.str);
            break;
          }

          size_t row_index = find_or_create_row(result, rollup_ts, &result_capacity, &agg_hint);
          if (row_index == SIZE_MAX) {
            ESP_LOGE(TAG, "OOM inserting new result row – aborting field '%s'", fld->field_name);
            if (val_agg.type == TIMESERIES_FIELD_TYPE_STRING && val_agg.data.string_val.str)
              free(val_agg.data.string_val.str);
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
        prof_decompress += (esp_timer_get_time() - prof_temp);
        timeseries_multi_series_iterator_deinit(&ms_iter);
      }

      // Cleanup: deinit iterators, then free batch allocations
      for (size_t s = 0; s < fld->num_series; s++) {
        if (series_multi_iters[s]) {
          for (size_t i_sub = 0; i_sub < series_multi_iters[s]->sub_count; ++i_sub) {
            timeseries_points_iterator_t *pit = series_multi_iters[s]->subs[i_sub].sub_iter;
            if (pit) timeseries_points_iterator_deinit(pit);
          }
          timeseries_multi_points_iterator_deinit(series_multi_iters[s]);
        }
        free(pit_batches[s]);
      }
      free(series_multi_iters);
      free(mpit_batch);
      free(pit_batches);
    } // end aggregation mode
  } // end for each field

  prof_end = esp_timer_get_time();
  ESP_LOGD(TAG, "[PROFILE] Type verification + iteration + aggregation: %.3f ms",
           (prof_end - prof_start) / 1000.0);
  ESP_LOGD(TAG, "[PROFILE DETAIL] Type verify: %.3f ms, Iter create: %.3f ms, Decompress: %.3f ms, Result build: %.3f ms",
           prof_type_verify / 1000.0, prof_iter_create / 1000.0,
           prof_decompress / 1000.0, prof_result_build / 1000.0);

  series_lookup_free(&srl_tbl);

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
