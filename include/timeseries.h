#ifndef TIMESERIES_H
#define TIMESERIES_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  TIMESERIES_FIELD_TYPE_FLOAT,
  TIMESERIES_FIELD_TYPE_INT,
  TIMESERIES_FIELD_TYPE_BOOL,
  TIMESERIES_FIELD_TYPE_STRING,
} timeseries_field_type_e;

typedef struct {
  timeseries_field_type_e type;
  union {
    double float_val;
    int64_t int_val;
    bool bool_val;
    struct {
      char* str;
      size_t length;
    } string_val;
  } data;
} timeseries_field_value_t;

/**
 * @brief Insert descriptor that supports many timestamps + data points.
 *
 * For each field, the data is stored in row-major form:
 *   field_values[i * num_points + p]
 * Timestamps are timestamps_ms[p].
 */
typedef struct {
  const char* measurement_name;

  // Tags
  const char** tag_keys;
  const char** tag_values;
  size_t num_tags;

  // Fields
  const char** field_names;
  timeseries_field_value_t* field_values;  // length = num_fields * num_points
  size_t num_fields;

  // Timestamps
  uint64_t* timestamps_ms;  // length = num_points
  size_t num_points;
} timeseries_insert_data_t;

/**
 * @brief Query descriptor for selecting data from the TSDB.
 *
 * Fill out the fields below to specify:
 *   - Which measurement to query
 *   - Which tag filters (key-value) to match
 *   - Which fields to return (or pass num_fields=0 for "all fields")
 *   - Optional time range (start_ms, end_ms) if your query logic supports it
 *   - A limit on the total number of data points to return
 */
typedef struct timeseries_query_t {
  /**
   * The measurement name to query, e.g. "weather".
   */
  const char* measurement_name;

  /**
   * Arrays of tag keys and values. If num_tags=2, then tag_keys[0] matches
   * tag_values[0], etc. If num_tags=0, no tag-based filtering is applied.
   */
  const char** tag_keys;
  const char** tag_values;
  size_t num_tags;

  /**
   * Array of field names to select. If num_fields=0, treat it as "all fields".
   */
  const char** field_names;
  size_t num_fields;

  /**
   * Optional time-range filtering (if supported in your query logic).
   * If start_ms or end_ms is 0, it might signify "no bound" on that side.
   */
  int64_t start_ms;
  int64_t end_ms;

  /**
   * A maximum total number of data points to return across all columns.
   * If limit=0, treat as "no limit".
   */
  size_t limit;
  uint32_t rollup_interval;

} timeseries_query_t;

/**
 * @brief A single result column in a query, holding:
 *  - a column name (e.g., the field name)
 *  - the data type
 *  - an array of values (one per row/point).
 */
typedef struct timeseries_query_result_column_t {
  /**
   * The name of this column, typically the field name in the measurement.
   * Dynamically allocated if needed (strdup, etc.).
   */
  char* name;

  /**
   * The field type (e.g., float, int, bool, string).
   */
  timeseries_field_type_e type;

  /**
   * An array of timeseries_field_value_t, one for each data point.
   * The length of this array matches timeseries_query_result_t.num_points.
   */
  timeseries_field_value_t* values;
} timeseries_query_result_column_t;

/**
 * @brief The overall result of a timeseries query, including:
 *  - An array of timestamps (one per row/point).
 *  - One or more columns (each column is a field).
 *  - The number of rows (num_points) and columns (num_columns).
 */
typedef struct timeseries_query_result_t {
  /**
   * Array of timestamps (in milliseconds).
   * Length = num_points.
   */
  uint64_t* timestamps;

  /**
   * The number of data points (rows).
   */
  size_t num_points;

  /**
   * An array of columns, each representing a field.
   * Length = num_columns.
   */
  timeseries_query_result_column_t* columns;

  /**
   * The number of columns (distinct fields) in the result.
   */
  size_t num_columns;
} timeseries_query_result_t;

typedef struct {
  char* key;
  char* val;
} tsdb_tag_pair_t;

bool timeseries_init(void);

/**
 * @brief Insert multiple data points for multiple fields in one call.
 *        Each field is stored in a single entry (multi (ts,value) array).
 */
bool timeseries_insert(const timeseries_insert_data_t* data);

bool timeseries_compact(void);

bool timeseries_expire(void);

bool timeseries_query(const timeseries_query_t* query, timeseries_query_result_t* result);

void timeseries_query_free_result(timeseries_query_result_t* result);

bool timeseries_clear_all();

bool timeseries_get_measurements(char*** measurements, size_t* num_measurements);

bool timeseries_get_fields_for_measurement(const char* measurement_name, char*** fields, size_t* num_fields);

bool timeseries_get_tags_for_measurement(const char* measurement_name, tsdb_tag_pair_t** tags, size_t* num_tags);

#ifdef __cplusplus
}
#endif

#endif  // TIMESERIES_H
