#ifndef TIMESERIES_MULTI_SERIES_ITERATOR_H
#define TIMESERIES_MULTI_SERIES_ITERATOR_H

#include "timeseries_multi_iterator.h"
#include "timeseries_points_iterator.h"
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Supported aggregation methods for rollups
 */
typedef enum {
  TSDB_AGGREGATION_NONE = 0,
  TSDB_AGGREGATION_MIN,
  TSDB_AGGREGATION_MAX,
  TSDB_AGGREGATION_AVG,
  TSDB_AGGREGATION_LATEST,
  // etc.
} timeseries_aggregation_method_e;

/**
 * @brief A high-level iterator that merges multiple multi_points_iterators
 *        and returns aggregated points at regular rollup intervals.
 */
typedef struct {
  timeseries_multi_points_iterator_t **sub_iters;
  size_t sub_count;

  timeseries_aggregation_method_e aggregator;
  uint64_t rollup_interval; ///< e.g. in microseconds, or ms, or however you
                            ///< store times

  bool valid;

  // Bookkeeping for the current rollup window:
  uint64_t current_window_start;
  uint64_t current_window_end;

  // For each sub-iterator, we keep the "next" point we haven't consumed yet
  // But in practice, we may simply rely on sub_iters themselves.

  // If you want to produce one aggregated value per sub-iterator,
  // you can do that. If you prefer to produce a single aggregate
  // across *all* sub-iterators, you'll adjust accordingly.

} timeseries_multi_series_iterator_t;

/**
 * @brief Initialize a multi-series iterator that merges data from
 *        \p sub_iters into rollup windows and aggregates with \p method.
 *
 * @param sub_iters       Array of multi_points_iterator pointers
 * @param sub_count       How many entries in \p sub_iters
 * @param rollup_interval The size of each time window
 * @param method          The aggregation method (min, max, avg, last, etc.)
 * @param[out] out_iter   The resulting multi-series iterator
 * @return true if OK
 */
bool timeseries_multi_series_iterator_init(
    timeseries_multi_points_iterator_t **sub_iters, size_t sub_count,
    uint64_t rollup_interval, timeseries_aggregation_method_e method,
    timeseries_multi_series_iterator_t *out_iter);

/**
 * @brief Advance to the next rollup window.
 *        Accumulate all points from *all sub-iterators* whose timestamps
 *        lie in [current_window_start, current_window_end).
 *        Returns the aggregated timestamp (which may be the window_start
 *        or the average of all timestamps, depending on your needs).
 *        \p out_vals can store one aggregated value per sub-iterator (field).
 *
 * @param iter    The multi-series rollup iterator
 * @param out_ts  The timestamp for this rollup window
 * @param out_vals An array of values, one for each sub-iterator
 * @return true if a new aggregated window is produced, false if done
 */
bool timeseries_multi_series_iterator_next(
    timeseries_multi_series_iterator_t *iter, uint64_t *out_ts,
    timeseries_field_value_t *out_vals);

/**
 * @brief Free resources. (Does not necessarily deinit the sub_iters themselves,
 *        depending on your ownership model.)
 */
void timeseries_multi_series_iterator_deinit(
    timeseries_multi_series_iterator_t *iter);

#ifdef __cplusplus
}
#endif

#endif // TIMESERIES_MULTI_SERIES_ITERATOR_H
