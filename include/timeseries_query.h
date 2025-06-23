#ifndef TIMESERIES_QUERY_H
#define TIMESERIES_QUERY_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "timeseries_data.h"
#include "timeseries_id_list.h"
#include "timeseries_metadata.h"

typedef struct field_record_info_t {
  uint32_t page_offset;    // start of the page (if needed)
  uint32_t record_offset;  // offset (within the page) for this record
  uint64_t start_time;
  uint64_t end_time;
  uint16_t record_count;
  uint16_t record_length;
  bool compressed;
  struct field_record_info_t* next;
} field_record_info_t;

typedef struct series_record_list_t {
  timeseries_series_id_t series_id;
  field_record_info_t* records_head;
} series_record_list_t;

typedef struct {
  timeseries_series_id_t id; /* key (16 B)                     */
  series_record_list_t* srl; /* value                          */
  bool used;
} series_lookup_entry_t;

typedef struct {
  series_lookup_entry_t* entries;
  size_t capacity; /* always a power-of-two       */
} series_lookup_t;

/**
 * @brief Each field_info_t now includes a field_record_list_t
 *        to store discovered records relevant to that field.
 */
typedef struct {
  const char* field_name;
  timeseries_series_id_list_t series_ids;

  // New fields to store an actual record-list pointer for each series:
  series_record_list_t* series_lists;  // parallel to series_ids
  size_t num_series;                   // same as series_ids.count
} field_info_t;

typedef struct {
  const char* field_name;
  timeseries_series_id_list_t series_ids;
} bulk_field_entry_t;

/**
 * @brief Execute the query using the given database context.
 *
 * @param db      Pointer to the time-series DB context
 * @param query   Pointer to the query descriptor
 * @param result  Output structure to be filled with query results
 * @return true on success (including possibly empty results), false on error
 */
bool timeseries_query_execute(timeseries_db_t* db, const timeseries_query_t* query, timeseries_query_result_t* result);

/**
 * @brief Frees the memory allocated in a timeseries_query_result_t by
 *        timeseries_query_execute.
 *
 * @param result  The result object to free
 */
void timeseries_query_free_result(timeseries_query_result_t* result);

void intersect_series_id_lists(timeseries_series_id_list_t* out, const timeseries_series_id_list_t* other);

#ifdef __cplusplus
}
#endif

#endif /* TIMESERIES_QUERY_H */
