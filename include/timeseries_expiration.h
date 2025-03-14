#ifndef TIMESERIES_EXPIRATION_H
#define TIMESERIES_EXPIRATION_H

#include "timeseries_internal.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Checks database usage and if above threshold, expires (deletes)
 *        oldest data and triggers compaction.
 *
 * @param db                Pointer to your timeseries database handle.
 * @param usage_threshold   Fraction of total partition size at which to
 *                          trigger expiration (e.g. 0.90 = 90%).
 * @return true on success, false if any error occurred.
 */
bool timeseries_expiration_run(timeseries_db_t *db, float usage_threshold,
                               float reduction_threshold);

#ifdef __cplusplus
}
#endif

#endif /* TIMESERIES_EXPIRATION_H */
