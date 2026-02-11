#include "timeseries_query.h"
#include "esp_timer.h"
#include "timeseries_data.h"    // hypothetical data-fetch stubs
#include "timeseries_id_list.h" // for timeseries_series_id_list_t
#include "timeseries_iterator.h"
#include "timeseries_metadata.h" // for tsdb_find_measurement_id, etc.
#include "timeseries_multi_series_iterator.h"
#include "timeseries_page_cache_snapshot.h"
#include "timeseries_points_iterator.h"

#include "esp_log.h"

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

static void free_record_list(field_record_info_t *head) {
  while (head) {
    field_record_info_t *next = head->next;
    free(head);
    head = next;
  }
}

bool timeseries_query_execute(timeseries_db_t *db,
                              const timeseries_query_t *query,
                              timeseries_query_result_t *result) {
  if (!db || !query || !result) {
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
  ESP_LOGI(TAG, "Resolved measurement in %.3f ms",
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
      ESP_LOGI(TAG, "No series found for measurement '%s'.",
               query->measurement_name);
      ok = true; /* empty result   */
      goto cleanup;
    }
  }

  if (matched_series.count == 0) {
    ESP_LOGI(TAG, "No series matched the tag filters (or none exist).");
    ok = true; /* empty result   */
    goto cleanup;
  }

  end_time = esp_timer_get_time();
  ESP_LOGI(TAG, "Found %zu series for measurement '%s' in %.3f ms",
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
      ESP_LOGI(TAG, "No fields found for measurement '%s'.",
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
  ESP_LOGI(TAG, "Found fields for measurement in %.3f ms",
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
    ESP_LOGI(TAG, "Field '%s' (meas_id=%" PRIu32 "): found %zu series_ids (got=%d)",
             fname, measurement_id, field_series.count, (int)got);
    for (size_t si = 0; si < field_series.count && si < 3; si++) {
      ESP_LOGI(TAG, "  series_id[%zu]: %02X%02X%02X%02X%02X%02X%02X%02X...",
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
      fi->series_lists[s].records_head = NULL;
    }
    ++actual_fields_count;
  }

  if (actual_fields_count == 0) {
    ok = true; /* nothing to do  */
    goto cleanup;
  }

  // Log which series_ids we're looking for
  for (size_t f = 0; f < actual_fields_count; ++f) {
    ESP_LOGI(TAG, "Looking for field '%s' with %zu series:", fields_array[f].field_name, fields_array[f].num_series);
    for (size_t s = 0; s < fields_array[f].num_series; ++s) {
      ESP_LOGI(TAG, "  Series %zu: %02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X",
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
  ESP_LOGI(TAG, "Prepared fields for measurement in %.3f ms",
           (end_time - start_time) / 1000.0);

  /* -------------------------------------------------------------------- */
  /*  5.  Fetch & aggregate                                               */
  /* -------------------------------------------------------------------- */

  start_time = esp_timer_get_time();

  size_t points_fetched = fetch_series_data(db, fields_array, actual_fields_count, query,
                         result);
  // Note: 0 points is a valid result, not an error

  ok = true; /* At this point the function succeeded – result is valid */

  end_time = esp_timer_get_time();
  ESP_LOGI(TAG, "Fetched data in %.3f ms", (end_time - start_time) / 1000.0);

cleanup:
  /* -------------------------------------------------------------------- */
  /*  6.  Release every allocation made above                             */
  /* -------------------------------------------------------------------- */

  if (fields_array) {
    for (size_t i = 0; i < actual_fields_count; ++i) {
      /* 6a. free record chains generated in fetch_series_data */
      for (size_t s = 0; s < fields_array[i].num_series; ++s) {
        free_record_list(fields_array[i].series_lists[s].records_head);
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
                                    timeseries_field_type_e field_type) {
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
  result->num_columns = new_num_cols;

  // Initialize the new column
  memset(&result->columns[new_col_index], 0,
         sizeof(result->columns[new_col_index]));
  result->columns[new_col_index].name = strdup(field_name);
  result->columns[new_col_index].type = field_type;

  // Because we already have 'result->num_points' rows, we must allocate
  // enough space for that many values in this new column.
  /* Allocate value storage for existing rows. Free 'name' if that fails to
     avoid a leak, then propagate OOM via SIZE_MAX. */
  if (result->num_points) {
    result->columns[new_col_index].values =
        calloc(result->num_points, sizeof(timeseries_field_value_t));
    if (!result->columns[new_col_index].values) {
      free((void *)result->columns[new_col_index].name);
      ESP_LOGE(TAG, "OOM allocating initial cells for column '%s'", field_name);
      return SIZE_MAX;
    }
  }

  return new_col_index;
}

/**
 * @brief Find a row index with the given timestamp; if none, add a new row.
 *        Ensures all columns' .values arrays are expanded by 1 if a new row is
 * added.
 *
 * @param[in,out] result   The query result
 * @param[in]     ts       The timestamp to match
 * @return The row index corresponding to this timestamp
 */
static size_t find_or_create_row(timeseries_query_result_t *result,
                                 uint64_t ts) {
  // A) Check if it already exists
  for (size_t r = 0; r < result->num_points; r++) {
    if (result->timestamps[r] == ts) {
      return r; // Found existing row
    }
  }

  // B) Not found => create a new row at index = result->num_points
  size_t new_row_index = result->num_points;

  // Expand the timestamps array by 1
  size_t new_count = result->num_points + 1;
  uint64_t *new_ts_array =
      realloc(result->timestamps, new_count * sizeof(uint64_t));
  if (!new_ts_array) {
    // In a real system, handle OOM robustly
    ESP_LOGE(TAG, "Out of memory expanding timestamps");
    return SIZE_MAX; // fallback, or handle error
  }
  result->timestamps = new_ts_array;
  result->timestamps[new_row_index] = ts;
  result->num_points = new_count;

  // Expand each column's values array by 1
  for (size_t c = 0; c < result->num_columns; c++) {
    timeseries_field_value_t *new_values =
        realloc(result->columns[c].values,
                new_count * sizeof(timeseries_field_value_t));
    if (!new_values) {
      ESP_LOGE(TAG, "Out of memory expanding column=%zu", c);
      return SIZE_MAX;
    }
    result->columns[c].values = new_values;

    // Initialize the new cell to type=0 or your own sentinel
    memset(&result->columns[c].values[new_row_index], 0,
           sizeof(timeseries_field_value_t));
  }

  return new_row_index;
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

  /* We must not modify 'other'; make a heap copy to sort */
  timeseries_series_id_t *tmp =
      malloc(other->count * sizeof(timeseries_series_id_t));
  if (!tmp) {
    ESP_LOGE(TAG, "OOM in intersect_series_id_lists (%zu entries)", other->count);
    tsdb_series_id_list_clear(out);
    return;
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

  free(tmp);

  /* Replace 'out' with the intersection */
  tsdb_series_id_list_clear(out);
  tsdb_series_id_list_copy(out, &result);
  tsdb_series_id_list_free(&result);
}

/**
 * @brief Create a new node from \p fdh and insert it into the linked list
 *        in ascending order by start_time.
 *
 * @param[in,out] list    Pointer to the HEAD pointer of the linked list.
 *                        If *list == NULL, we'll become the first node.
 * @param[in] fdh         The field data header to create a new node from.
 * @param[in] limit       The maximum # of data points, used if you want to skip
 *                        insertion if limit is already met. (In this example,
 *                        we always insert, and rely on prune() to remove.)
 * @return true if appended, false if skipped
 */
static bool field_record_list_append(field_record_info_t **list,
                                     const timeseries_field_data_header_t *fdh,
                                     uint32_t absolute_offset,
                                     unsigned int limit) {
  // 1) Check if we already have coverage >= limit => skip?
  //    Alternatively, we can always insert and let prune() handle it.
  //    We'll show "always insert" to let the pruning step do the removal:
  //    If you'd prefer to skip immediately if coverage >= limit, add coverage
  //    logic here.

  // 2) Allocate a new node
  field_record_info_t *new_node =
      (field_record_info_t *)malloc(sizeof(field_record_info_t));
  if (!new_node) {
    return false; // out of memory
  }
  memset(new_node, 0, sizeof(*new_node));

  new_node->page_offset = 0;  // Not used anymore
  new_node->record_offset = absolute_offset;
  new_node->start_time = fdh->start_time;
  new_node->end_time = fdh->end_time;
  new_node->record_count = fdh->record_count;
  new_node->record_length = fdh->record_length;
  new_node->compressed = (fdh->flags & TSDB_FIELDDATA_FLAG_COMPRESSED) == 0;
  new_node->next = NULL;

  // 4) Insert into singly linked list in ascending order by start_time
  if (*list == NULL) {
    // Empty list => we're the head
    *list = new_node;
    return true;
  }

  // If the new record’s start_time is earlier than the head => prepend
  if (new_node->start_time < (*list)->start_time) {
    new_node->next = *list;
    *list = new_node;
    return true;
  }

  // Otherwise, find the insertion spot
  field_record_info_t *prev = *list;
  field_record_info_t *curr = prev->next;
  while (curr) {
    if (new_node->start_time < curr->start_time) {
      // insert here
      break;
    }
    prev = curr;
    curr = curr->next;
  }
  // Insert after 'prev'
  new_node->next = curr;
  prev->next = new_node;

  return true;
}

/**
 * @brief Traverse the list in ascending order, summing record_count
 *        until we reach 'limit'. Then we keep any record that
 *        might overlap older data (start_time <= last_included_start),
 *        and drop the rest.
 *
 * @param[in,out] list   The HEAD pointer to the linked list
 * @param[in]     limit  The max # of data points we want
 */
static void field_record_list_prune(field_record_info_t **list,
                                    unsigned int limit) {
  if (!list || !(*list) || limit == 0) {
    return; // no limit => keep everything
  }

  // 1) Summation pass: find how many points from the earliest nodes
  //    we need to get coverage >= limit
  uint64_t coverage = 0;
  uint64_t last_included_start = 0;

  field_record_info_t *cur = *list;
  while (cur) {
    coverage += cur->record_count;
    last_included_start = cur->start_time;
    if (coverage >= limit) {
      // We have enough coverage => note the start_time
      // of the last included record
      break;
    }
    cur = cur->next;
  }

  if (coverage < limit) {
    // coverage never reached limit => keep all
    return;
  }
  // else we only keep records that start_time <= last_included_start

  // 2) Build the new list by skipping records that
  //    have start_time > last_included_start
  field_record_info_t dummy_head; // dummy node
  dummy_head.next = *list;

  field_record_info_t *prev = &dummy_head;
  cur = dummy_head.next;

  while (cur) {
    if (cur->start_time > last_included_start) {
      // remove
      field_record_info_t *to_remove = cur;
      prev->next = cur->next;
      cur = cur->next;
      free(to_remove);
    } else {
      // keep
      prev = cur;
      cur = cur->next;
    }
  }

  // done => new HEAD
  *list = dummy_head.next;
}

static size_t fetch_series_data(timeseries_db_t *db, field_info_t *fields_array,
                                size_t num_fields,
                                const timeseries_query_t *query,
                                timeseries_query_result_t *result) {
  if (!db || !fields_array || num_fields == 0 || !query || !result) {
    ESP_LOGW(TAG, "Invalid input to fetch_series_data");
    return 0;
  }

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
  ESP_LOGI(TAG, "[PROFILE] Lookup table build: %.3f ms", (prof_end - prof_start) / 1000.0);

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
    ESP_LOGI(TAG, "Scanning FIELD_DATA page @0x%08X size=%u level=%u state=%u",
             (unsigned int)page_offset, (unsigned int)page_size, hdr.field_data_level, hdr.page_state);

    timeseries_fielddata_iterator_t fdata_iter;
    if (!timeseries_fielddata_iterator_init(db, page_offset, page_size,
                                            &fdata_iter)) {
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
        records_found++;
        page_records_matched++;
        bool is_compressed = (fdh.flags & TSDB_FIELDDATA_FLAG_COMPRESSED) == 0;
        ESP_LOGI(TAG, "MATCH: series %02X%02X%02X%02X... @0x%08X count=%u len=%u compressed=%d",
                 fdh.series_id[0], fdh.series_id[1], fdh.series_id[2], fdh.series_id[3],
                 (unsigned int)absolute_offset, fdh.record_count, fdh.record_length, is_compressed);
        field_record_list_append(&srl->records_head, &fdh, absolute_offset,
                                 query->limit);
      } else {
        ESP_LOGI(TAG, "No match for series %02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X%02X (flags=0x%02X, records=%u)",
                 fdh.series_id[0], fdh.series_id[1], fdh.series_id[2], fdh.series_id[3],
                 fdh.series_id[4], fdh.series_id[5], fdh.series_id[6], fdh.series_id[7],
                 fdh.series_id[8], fdh.series_id[9], fdh.series_id[10], fdh.series_id[11],
                 fdh.series_id[12], fdh.series_id[13], fdh.series_id[14], fdh.series_id[15],
                 fdh.flags, fdh.record_count);
      }
    } // end while fielddata_iterator
    ESP_LOGI(TAG, "Page @0x%08X: %d records scanned, %d matched",
             (unsigned int)page_offset, page_records, page_records_matched);
  } // end while page_iterator
  timeseries_page_cache_iterator_deinit(&page_iter);

  prof_end = esp_timer_get_time();
  ESP_LOGI(TAG, "[PROFILE] Page scanning: %.3f ms (%d pages, %d field_data pages, %d records)",
           (prof_end - prof_start) / 1000.0, pages_processed, field_data_pages, records_found);

  // Optimization #1: Prune once per series after all records collected (instead of after each append)
  prof_start = esp_timer_get_time();
  for (size_t f = 0; f < num_fields; f++) {
    for (size_t s = 0; s < fields_array[f].num_series; s++) {
      field_record_list_prune(&fields_array[f].series_lists[s].records_head, query->limit);
    }
  }
  prof_end = esp_timer_get_time();
  ESP_LOGI(TAG, "[PROFILE] Prune lists: %.3f ms", (prof_end - prof_start) / 1000.0);

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

  // Example aggregator settings. In real usage, these might come from the
  // query:
  timeseries_aggregation_method_e agg_method = TSDB_AGGREGATION_AVG;

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
        find_or_create_column(result, fld->field_name, field_type);

    if (col_index == SIZE_MAX) {
      /* Fatal for this field – skip it but keep processing others */
      continue;
    }

    // Keep track of how many data points we've inserted for this field
    // so we can respect the `query->limit` per-field (as requested).
    size_t inserted_for_this_field = 0;

    // Build a multi_points_iterator for each series
    timeseries_multi_points_iterator_t **series_multi_iters =
        calloc(fld->num_series, sizeof(*series_multi_iters));
    if (!series_multi_iters) {
      ESP_LOGE(TAG, "OOM: cannot allocate multi-iter array for field='%s'",
               fld->field_name);
      continue;
    }

    for (size_t s = 0; s < fld->num_series; s++) {
      series_record_list_t *srl = &fld->series_lists[s];

      // Count how many record-info nodes we have
      size_t record_count = 0;
      field_record_info_t *rec = srl->records_head;
      while (rec) {
        record_count++;
        rec = rec->next;
      }
      if (record_count == 0) {
        continue;
      }

      // Prepare arrays for sub-iterators
      timeseries_points_iterator_t **sub_iters =
          calloc(record_count, sizeof(*sub_iters));
      uint32_t *page_seqs = calloc(record_count, sizeof(uint32_t));
      if (!sub_iters || !page_seqs) {
        free(sub_iters);
        free(page_seqs);
        ESP_LOGE(TAG, "OOM building sub-iter structures for field='%s'",
                 fld->field_name);
        continue;
      }

      // Initialize each record's points_iterator
      rec = srl->records_head;
      size_t idx = 0;
      while (rec) {
        timeseries_points_iterator_t *pit = calloc(1, sizeof(timeseries_points_iterator_t));
        if (!pit) {
          ESP_LOGW(TAG, "OOM creating sub-iterator for '%s'", fld->field_name);
          rec = rec->next;
          continue;
        }
        // Debug: log the actual offset being used
        ESP_LOGV(TAG, "Initializing points_iter: absolute_offset=0x%08X, length=%u, count=%u",
                 rec->record_offset, rec->record_length, rec->record_count);
        bool ok = timeseries_points_iterator_init(
            db, rec->record_offset, rec->record_length,
            rec->record_count, field_type, rec->compressed, pit);
        if (!ok) {
          ESP_LOGW(TAG, "Failed init of points_iter offset=0x%08X, length=%u, count=%u, type=%d",
                   (unsigned)(rec->record_offset),
                   rec->record_length, rec->record_count, field_type);
          free(pit);
          rec = rec->next;
          continue;
        }
        sub_iters[idx] = pit;
        page_seqs[idx] = 0; // or rec->page_seq if you track that
        idx++;
        rec = rec->next;
      }

      // Merge them into one multi_points_iterator
      if (idx > 0) {
        timeseries_multi_points_iterator_t *mpit =
            calloc(1, sizeof(timeseries_multi_points_iterator_t));
        if (mpit && timeseries_multi_points_iterator_init(sub_iters, page_seqs,
                                                          idx, mpit)) {
          series_multi_iters[s] = mpit;
        } else {
          // Cleanup partial
          if (mpit) {
            free(mpit);
          }
          for (size_t i2 = 0; i2 < idx; i2++) {
            if (sub_iters[i2]) {
              timeseries_points_iterator_deinit(sub_iters[i2]);
              free(sub_iters[i2]);
            }
          }
        }
      }

      free(sub_iters);
      free(page_seqs);
    } // end for each series in field

    // Use multi_series_iterator to aggregate across all series in the field
    prof_iter_create += (esp_timer_get_time() - prof_temp);

    timeseries_multi_series_iterator_t ms_iter;
    if (!timeseries_multi_series_iterator_init(
            series_multi_iters, fld->num_series, query->rollup_interval,
            agg_method, &ms_iter)) {
      ESP_LOGW(TAG, "Failed multi-series aggregator for '%s'", fld->field_name);
    } else {
      // We'll pull aggregated points until exhausted, or until limit/time-range
      // reached
      while (true) {
        prof_temp = esp_timer_get_time();
        uint64_t rollup_ts = 0;
        timeseries_field_value_t val_agg;
        bool got_data = timeseries_multi_series_iterator_next(
            &ms_iter, &rollup_ts, &val_agg);
        prof_decompress += (esp_timer_get_time() - prof_temp);

        if (!got_data) {
          break; // no more data
        }

        prof_temp = esp_timer_get_time();

        // Optional time-range filtering (if requested)
        if (query->start_ms != 0 && rollup_ts < query->start_ms) {
          continue;
        }
        if (query->end_ms != 0 && rollup_ts > query->end_ms) {
          break; // aggregator times are in ascending order, so we can break
        }

        // Check limit for this field
        if (query->limit != 0 && inserted_for_this_field >= query->limit) {
          // We've reached the maximum number of points we allow for this field
          break;
        }

        // Actually store this aggregator result into the result struct
        size_t row_index = find_or_create_row(result, rollup_ts);

        if (row_index == SIZE_MAX) {
          ESP_LOGE(TAG, "OOM inserting new result row – aborting field '%s'",
                   fld->field_name);
          break;
        }

        // Use the type from the aggregated value as it may be different from
        // the original field type
        result->columns[col_index].values[row_index].type = val_agg.type;

        // Copy the aggregated value into the result
        result->columns[col_index].values[row_index].data = val_agg.data;

        inserted_for_this_field++;
        total_points_aggregated++;

        prof_result_build += (esp_timer_get_time() - prof_temp);

      } // end while aggregator
      timeseries_multi_series_iterator_deinit(&ms_iter);
    }

    // Cleanup each multi_points_iterator
    for (size_t s = 0; s < fld->num_series; s++) {
      if (series_multi_iters[s]) {
        /* First clean up every sub-iterator we created */
        for (size_t i_sub = 0; i_sub < series_multi_iters[s]->sub_count;
             ++i_sub) {
          timeseries_points_iterator_t *pit =
              series_multi_iters[s]->subs[i_sub].sub_iter;
          if (pit) {
            timeseries_points_iterator_deinit(pit);
            free(pit);
          }
        }
        /* Then free the multi-iterator itself */
        timeseries_multi_points_iterator_deinit(series_multi_iters[s]);
        free(series_multi_iters[s]);
      }
    }
    free(series_multi_iters);
  } // end for each field

  prof_end = esp_timer_get_time();
  ESP_LOGI(TAG, "[PROFILE] Type verification + iteration + aggregation: %.3f ms",
           (prof_end - prof_start) / 1000.0);
  ESP_LOGI(TAG, "[PROFILE DETAIL] Type verify: %.3f ms, Iter create: %.3f ms, Decompress: %.3f ms, Result build: %.3f ms",
           prof_type_verify / 1000.0, prof_iter_create / 1000.0,
           prof_decompress / 1000.0, prof_result_build / 1000.0);

  series_lookup_free(&srl_tbl);

  return total_points_aggregated;
}
