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

/**
 * @brief Execute the query using the given database context.
 *
 * @param db      Pointer to the time-series DB context
 * @param query   Pointer to the query descriptor
 * @param result  Output structure to be filled with query results
 * @return true on success (including possibly empty results), false on error
 */
bool timeseries_query_execute(timeseries_db_t *db,
                              const timeseries_query_t *query,
                              timeseries_query_result_t *result);

/**
 * @brief Frees the memory allocated in a timeseries_query_result_t by
 *        timeseries_query_execute.
 *
 * @param result  The result object to free
 */
void timeseries_query_free_result(timeseries_query_result_t *result);

#ifdef __cplusplus
}
#endif

#endif /* TIMESERIES_QUERY_H */
