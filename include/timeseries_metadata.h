// timeseries_metadata.h

#ifndef TIMESERIES_METADATA_H
#define TIMESERIES_METADATA_H

#include "timeseries.h"
#include "timeseries_id_list.h" // Needed for timeseries_series_id_list_t
#include "timeseries_internal.h"
#include "timeseries_string_list.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Load existing pages from flash into the in-memory page cache
 *        and ensure at least one active metadata page exists.
 *
 * @param db    Pointer to the timeseries database context
 * @return true if successful, false otherwise
 */
bool tsdb_load_pages_into_memory(timeseries_db_t *db);

/**
 * @brief Look for an existing measurement ID for the given measurement name.
 *
 * @param db                Pointer to the database context
 * @param measurement_name  Null-terminated measurement name string
 * @param out_id            Pointer to store the found measurement ID
 * @return true if found, false otherwise
 */
bool tsdb_find_measurement_id(timeseries_db_t *db, const char *measurement_name,
                              uint32_t *out_id);

/**
 * @brief Create a new measurement ID for the given measurement name and
 *        store an entry in the metadata.
 *
 * @param db                Pointer to the database context
 * @param measurement_name  Null-terminated measurement name string
 * @param out_id            Pointer to store the newly created measurement ID
 * @return true if creation succeeded, false otherwise
 */
bool tsdb_create_measurement_id(timeseries_db_t *db,
                                const char *measurement_name, uint32_t *out_id);

/**
 * @brief Insert a single field (with tags) into the time-series database.
 *        This will:
 *          1) Calculate the series ID (MD5 over measurement, tags, field).
 *          2) Ensure that the field type is tracked in metadata.
 *          3) Index the tags for this series in metadata.
 *          4) Append the actual data point to a field data page.
 *
 * @param db                Pointer to the database context
 * @param measurement_id    Numeric measurement ID
 * @param measurement_name  Name of the measurement
 * @param field_name        The field key name
 * @param field_val         Pointer to the field value (numeric/string)
 * @param tag_keys          Array of tag keys
 * @param tag_values        Array of corresponding tag values
 * @param num_tags          Number of tags
 * @param timestamp_ms      The timestamp in milliseconds
 * @return true if successfully inserted, false otherwise
 */
bool tsdb_insert_single_field(timeseries_db_t *db, uint32_t measurement_id,
                              const char *measurement_name,
                              const char *field_name,
                              const timeseries_field_value_t *field_val,
                              const char **tag_keys, const char **tag_values,
                              size_t num_tags, uint64_t timestamp_ms);

/**
 * @brief Look up the existing field type for a given 16-byte series ID.
 *
 * @param db        Pointer to the database context
 * @param series_id 16-byte series identifier
 * @param out_type  Pointer to store the field type
 * @return true if found, false otherwise
 */
bool tsdb_lookup_series_type_in_metadata(timeseries_db_t *db,
                                         const unsigned char series_id[16],
                                         timeseries_field_type_e *out_type);

/**
 * @brief Ensure the metadata has an entry for this series_id and field type.
 *        If it does not exist, creates it. If it exists with a conflicting
 *        type, returns false.
 *
 * @param db        Pointer to the database context
 * @param series_id 16-byte series identifier
 * @param field_type Type of the field (e.g., float, int)
 * @return true on success or if it already matches, false on error/conflict
 */
bool tsdb_ensure_series_type_in_metadata(timeseries_db_t *db,
                                         const unsigned char series_id[16],
                                         timeseries_field_type_e field_type);

/**
 * @brief Insert or update tag indexing for a given series ID. Each tag
 * key-value pair is associated with the measurement ID so we can query later.
 *
 * @param db             Pointer to the database context
 * @param measurement_id Numeric measurement ID
 * @param tag_keys       Array of tag keys
 * @param tag_values     Array of corresponding tag values
 * @param num_tags       Number of tags
 * @param series_id      16-byte series identifier
 * @return true if successfully indexed, false otherwise
 */
bool tsdb_index_tags_for_series(timeseries_db_t *db, uint32_t measurement_id,
                                const char **tag_keys, const char **tag_values,
                                size_t num_tags,
                                const unsigned char series_id[16]);

/**
 * @brief Record that a given series ID belongs to (measurement_id, field_name).
 *        Internally, this upserts an index entry so that future queries can
 *        find all series for a measurement/field pair.
 *
 * @param db             Pointer to the database context
 * @param measurement_id Numeric measurement ID
 * @param field_name     The field key name
 * @param series_id      16-byte series identifier
 * @return true if successfully updated, false otherwise
 */
bool tsdb_index_field_for_series(timeseries_db_t *db, uint32_t measurement_id,
                                 const char *field_name,
                                 const unsigned char series_id[16]);

/**
 * @brief Look up all series IDs matching a given (measurement_id, tag_key,
 *        tag_value).
 *
 * @param db                Pointer to the database context
 * @param measurement_id    Numeric measurement ID
 * @param tag_key           The tag key to search for
 * @param tag_value         The tag value to match
 * @param out_series_list   A timeseries_series_id_list_t object to which
 *                          matching series IDs will be appended
 * @return true if one or more series IDs were found, false otherwise
 */
bool tsdb_find_series_ids_for_tag(timeseries_db_t *db, uint32_t measurement_id,
                                  const char *tag_key, const char *tag_value,
                                  timeseries_series_id_list_t *out_series_list);

bool tsdb_find_series_ids_for_field(
    timeseries_db_t *db, uint32_t measurement_id, const char *field_name,
    timeseries_series_id_list_t *out_series_list);

/**
 * @brief Retrieve all known measurement names (no duplicates).
 *
 * @param db               Pointer to the database context
 * @param out_measurements A string list to which measurement names are appended
 * @return true if at least one measurement was found, false otherwise
 */
bool tsdb_list_all_measurements(timeseries_db_t *db,
                                timeseries_string_list_t *out_measurements);

/**
 * @brief Retrieve all distinct field names for a given measurement ID.
 *
 * @param db              Pointer to the database context
 * @param measurement_id  Numeric measurement ID
 * @param out_fields      A string list to which field names are appended
 * @return true if at least one field name was found, false otherwise
 */
bool tsdb_list_fields_for_measurement(timeseries_db_t *db,
                                      uint32_t measurement_id,
                                      timeseries_string_list_t *out_fields);

bool timeseries_metadata_create_page(timeseries_db_t *db);

bool tsdb_find_all_series_ids_for_measurement(
    timeseries_db_t *db, uint32_t measurement_id,
    timeseries_series_id_list_t *out_series_list);

#ifdef __cplusplus
}
#endif

#endif // TIMESERIES_METADATA_H
