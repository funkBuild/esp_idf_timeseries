#include "timeseries_query.h"
#include "timeseries_data.h"    // hypothetical data-fetch stubs
#include "timeseries_id_list.h" // for timeseries_series_id_list_t
#include "timeseries_iterator.h"
#include "timeseries_metadata.h" // for tsdb_find_measurement_id, etc.
#include "timeseries_multi_series_iterator.h"
#include "timeseries_points_iterator.h"

#include "esp_log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "timeseries_query";

typedef struct field_record_info_t {
  uint32_t page_offset;   // start of the page (if needed)
  uint32_t record_offset; // offset (within the page) for this record
  uint64_t start_time;
  uint64_t end_time;
  uint16_t record_count;
  uint16_t record_length;
  bool compressed;
  struct field_record_info_t *next;
} field_record_info_t;

// New struct to associate a series_id with its linked-list head
typedef struct series_record_list_t {
  timeseries_series_id_t series_id;
  field_record_info_t *records_head;
} series_record_list_t;

/**
 * @brief Each field_info_t now includes a field_record_list_t
 *        to store discovered records relevant to that field.
 */
typedef struct {
  const char *field_name;
  timeseries_series_id_list_t series_ids;

  // New fields to store an actual record-list pointer for each series:
  series_record_list_t *series_lists; // parallel to series_ids
  size_t num_series;                  // same as series_ids.count
} field_info_t;

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

bool timeseries_query_execute(timeseries_db_t *db,
                              const timeseries_query_t *query,
                              timeseries_query_result_t *result) {
  if (!db || !query || !result) {
    return false;
  }

  // Initialize output result
  memset(result, 0, sizeof(*result));

  // 1) Find measurement ID
  uint32_t measurement_id = 0;
  if (!tsdb_find_measurement_id(db, query->measurement_name, &measurement_id)) {
    ESP_LOGW(TAG, "Measurement '%s' not found.", query->measurement_name);
    return true; // empty result
  }

  // 2) Collect all series IDs either by intersecting tags or by grabbing
  //    all series for the measurement if no tags were specified.
  timeseries_series_id_list_t matched_series;
  tsdb_series_id_list_init(&matched_series);

  if (query->num_tags > 0) {
    // For the first tag, gather matched series, then intersect with subsequent
    // tags.
    timeseries_series_id_list_t temp_list;
    tsdb_series_id_list_init(&temp_list);

    for (size_t i = 0; i < query->num_tags; i++) {
      tsdb_series_id_list_clear(&temp_list);
      tsdb_series_id_list_init(&temp_list);

      bool found =
          tsdb_find_series_ids_for_tag(db, measurement_id, query->tag_keys[i],
                                       query->tag_values[i], &temp_list);

      if (i == 0) {
        // first tag => copy into matched_series
        tsdb_series_id_list_copy(&matched_series, &temp_list);
      } else {
        // intersect
        intersect_series_id_lists(&matched_series, &temp_list);
      }
    }
  } else {
    // If no tags => gather all series for this measurement (skip tag filtering)
    if (!tsdb_find_all_series_ids_for_measurement(db, measurement_id,
                                                  &matched_series) ||
        matched_series.count == 0) {
      ESP_LOGI(TAG, "No series found for measurement '%s'.",
               query->measurement_name);
      tsdb_series_id_list_free(&matched_series);
      return true;
    }
  }

  // If, after tag intersection (or “all series” lookup), we have no series:
  if (matched_series.count == 0) {
    ESP_LOGI(TAG, "No series matched the tag filters (or none exist).");
    tsdb_series_id_list_free(&matched_series);
    return true;
  }

  // 3) Gather the field names
  timeseries_string_list_t fields_to_query;
  tsdb_string_list_init(&fields_to_query);

  if (query->num_fields == 0) {
    // => get all fields from metadata
    if (!tsdb_list_fields_for_measurement(db, measurement_id,
                                          &fields_to_query) ||
        fields_to_query.count == 0) {
      ESP_LOGI(TAG, "No fields found for measurement '%s'.",
               query->measurement_name);
      tsdb_string_list_free(&fields_to_query);
      tsdb_series_id_list_free(&matched_series);
      return true;
    }
  } else {
    // => user-specified fields
    for (size_t i = 0; i < query->num_fields; i++) {
      tsdb_string_list_append_unique(&fields_to_query, query->field_names[i]);
    }
  }

  // 4) For each field, find the relevant series.
  //    - We first find all series for that field
  //    - then, if tags were used, we intersect with matched_series
  field_info_t *fields_array =
      calloc(fields_to_query.count, sizeof(field_info_t));
  if (!fields_array) {
    ESP_LOGE(TAG, "Out of memory for fields_array");
    tsdb_string_list_free(&fields_to_query);
    tsdb_series_id_list_free(&matched_series);
    return false;
  }

  size_t actual_fields_count = 0;

  for (size_t f = 0; f < fields_to_query.count; f++) {
    const char *fname = fields_to_query.items[f];

    // 1) find all series that have this field
    timeseries_series_id_list_t field_series;
    tsdb_series_id_list_init(&field_series);

    bool got = tsdb_find_series_ids_for_field(db, measurement_id, fname,
                                              &field_series);
    if (!got || field_series.count == 0) {
      ESP_LOGI(TAG, "No series found for field '%s'", fname);
      tsdb_series_id_list_free(&field_series);
      continue;
    }

    // 2) If tags were used, intersect with matched_series (otherwise skip)
    if (query->num_tags > 0) {
      intersect_series_id_lists(&field_series, &matched_series);
    }
    if (field_series.count == 0) {
      // no relevant series => skip
      tsdb_series_id_list_free(&field_series);
      continue;
    }

    // 3) store in fields_array
    fields_array[actual_fields_count].field_name =
        fname; // pointer from fields_to_query
    fields_array[actual_fields_count].series_ids = field_series;

    fields_array[actual_fields_count].num_series = field_series.count;
    fields_array[actual_fields_count].series_lists =
        calloc(field_series.count, sizeof(series_record_list_t));
    if (!fields_array[actual_fields_count].series_lists) {
      ESP_LOGE(TAG, "Out of memory for series_lists");
      tsdb_series_id_list_free(&field_series);
      return false;
    }

    for (size_t sid = 0; sid < field_series.count; sid++) {
      fields_array[actual_fields_count].series_lists[sid].series_id =
          field_series.ids[sid]; // copy the timeseries_series_id_t
      fields_array[actual_fields_count].series_lists[sid].records_head = NULL;
    }

    actual_fields_count++;
  }

  // 5) If no fields in the end => return
  if (actual_fields_count == 0) {
    free(fields_array);
    tsdb_string_list_free(&fields_to_query);
    tsdb_series_id_list_free(&matched_series);
    return true;
  }

  // 6) single pass => fetch data for all fields at once
  if (!fetch_series_data(db, fields_array, actual_fields_count, query,
                         result)) {
    ESP_LOGE(TAG, "fetch_series_data(...) returned error");
  }

  // Cleanup
  for (size_t i = 0; i < actual_fields_count; i++) {
    tsdb_series_id_list_free(&fields_array[i].series_ids);
  }
  free(fields_array);
  tsdb_string_list_free(&fields_to_query);
  tsdb_series_id_list_free(&matched_series);

  return true;
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
    return 0; // fallback
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
  if (result->num_points > 0) {
    result->columns[new_col_index].values =
        calloc(result->num_points, sizeof(timeseries_field_value_t));
    if (!result->columns[new_col_index].values) {
      ESP_LOGE(TAG, "Out of memory allocating initial values for column '%s'",
               field_name);
      // We'll continue, but the column is half-initialized
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
    return 0; // fallback, or handle error
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
      // OOM fallback
      ESP_LOGE(TAG, "Out of memory expanding column=%zu", c);
      // We won't back out changes for brevity; a real system might roll back
      continue;
    }
    result->columns[c].values = new_values;

    // Initialize the new cell to type=0 or your own sentinel
    memset(&result->columns[c].values[new_row_index], 0,
           sizeof(timeseries_field_value_t));
  }

  return new_row_index;
}

static void
intersect_series_id_lists(timeseries_series_id_list_t *out_list,
                          const timeseries_series_id_list_t *new_list) {
  if (!out_list || !new_list)
    return;
  if (out_list->count == 0 || new_list->count == 0) {
    tsdb_series_id_list_clear(out_list);
    return;
  }

  timeseries_series_id_list_t temp;
  tsdb_series_id_list_init(&temp);

  // check membership
  for (size_t i = 0; i < out_list->count; i++) {
    for (size_t j = 0; j < new_list->count; j++) {
      if (memcmp(out_list->ids[i].bytes, new_list->ids[j].bytes, 16) == 0) {
        tsdb_series_id_list_append(&temp, out_list->ids[i].bytes);
        break;
      }
    }
  }

  tsdb_series_id_list_clear(out_list);
  tsdb_series_id_list_copy(out_list, &temp);
  tsdb_series_id_list_free(&temp);
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

  new_node->page_offset = 0;
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

  // --------------------------------------------------------------------------
  // PART A: Single pass to gather the record_info linked-lists for each series
  // --------------------------------------------------------------------------

  timeseries_page_iterator_t page_iter;
  if (!timeseries_page_iterator_init(db, &page_iter)) {
    ESP_LOGE(TAG, "Failed to init page iterator");
    return 0;
  }

  timeseries_page_header_t hdr;
  uint32_t page_offset = 0;
  uint32_t page_size = 0;

  while (timeseries_page_iterator_next(&page_iter, &hdr, &page_offset,
                                       &page_size)) {
    // Only process FIELD_DATA pages
    if (hdr.page_type != TIMESERIES_PAGE_TYPE_FIELD_DATA) {
      continue;
    }

    timeseries_fielddata_iterator_t fdata_iter;
    if (!timeseries_fielddata_iterator_init(db, page_offset, page_size,
                                            &fdata_iter)) {
      ESP_LOGW(TAG, "Failed to init fielddata_iterator on page @0x%08X",
               (unsigned int)page_offset);
      continue;
    }

    timeseries_field_data_header_t fdh;
    while (timeseries_fielddata_iterator_next(&fdata_iter, &fdh)) {
      // Skip if flagged as deleted
      if ((fdh.flags & TSDB_FIELDDATA_FLAG_DELETED) == 0) {
        ESP_LOGW(TAG, "Skipping deleted record @0x%08X",
                 (unsigned int)fdata_iter.current_record_offset);
        continue;
      }

      // Calculate the offset for the raw data portion
      uint32_t absolute_offset = page_offset +
                                 fdata_iter.current_record_offset +
                                 sizeof(timeseries_field_data_header_t);

      // Distribute to the appropriate field/series's linked list
      for (size_t f = 0; f < num_fields; f++) {
        field_info_t *fld = &fields_array[f];

        for (size_t s = 0; s < fld->num_series; s++) {
          series_record_list_t *srl = &fld->series_lists[s];

          // Compare the record's series_id to our series list
          if (memcmp(srl->series_id.bytes, fdh.series_id, 16) == 0) {
            // If it belongs, append to the linked list
            if (field_record_list_append(&srl->records_head, &fdh,
                                         absolute_offset, query->limit)) {
              // Optionally prune if we exceed limit
              field_record_list_prune(&srl->records_head, query->limit);
            }
            break; // Found a match => no need to check more
          }
        }
      }
    } // end while fielddata_iterator
  } // end while page_iterator

  // --------------------------------------------------------------------------
  // PART B: Verify that all series in each field share the same type
  // --------------------------------------------------------------------------
  size_t total_points_aggregated = 0;

  // Example aggregator settings. In real usage, these might come from the
  // query:
  timeseries_aggregation_method_e agg_method = TSDB_AGGREGATION_AVG;

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
      bool found_type = tsdb_lookup_series_type_in_metadata(
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
        find_or_create_column(result, fld->field_name, field_type);

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
        timeseries_points_iterator_t *pit =
            calloc(1, sizeof(timeseries_points_iterator_t));
        if (!pit) {
          ESP_LOGW(TAG, "OOM creating sub-iterator for '%s'", fld->field_name);
          rec = rec->next;
          continue;
        }
        bool ok = timeseries_points_iterator_init(
            db, rec->page_offset + rec->record_offset, rec->record_length,
            rec->record_count, field_type, rec->compressed, pit);
        if (!ok) {
          ESP_LOGW(TAG, "Failed init of points_iter offset=0x%08X",
                   (unsigned)(rec->page_offset + rec->record_offset));
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
    timeseries_multi_series_iterator_t ms_iter;
    if (!timeseries_multi_series_iterator_init(
            series_multi_iters, fld->num_series, query->rollup_interval,
            agg_method, &ms_iter)) {
      ESP_LOGW(TAG, "Failed multi-series aggregator for '%s'", fld->field_name);
    } else {
      // We'll pull aggregated points until exhausted, or until limit/time-range
      // reached
      while (true) {
        uint64_t rollup_ts = 0;
        timeseries_field_value_t val_agg;
        bool got_data = timeseries_multi_series_iterator_next(
            &ms_iter, &rollup_ts, &val_agg);
        if (!got_data) {
          break; // no more data
        }

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

        // Use the type from the aggregated value as it may be different from
        // the original field type
        result->columns[col_index].values[row_index].type = val_agg.type;

        // Copy the aggregated value into the result
        result->columns[col_index].values[row_index].data = val_agg.data;

        inserted_for_this_field++;
        total_points_aggregated++;

      } // end while aggregator
      timeseries_multi_series_iterator_deinit(&ms_iter);
    }

    // Cleanup each multi_points_iterator
    for (size_t s = 0; s < fld->num_series; s++) {
      if (series_multi_iters[s]) {
        timeseries_multi_points_iterator_deinit(series_multi_iters[s]);
        free(series_multi_iters[s]);
      }
    }
    free(series_multi_iters);
  } // end for each field

  return total_points_aggregated;
}
