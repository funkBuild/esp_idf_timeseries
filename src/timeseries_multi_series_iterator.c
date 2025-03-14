#include "timeseries_multi_series_iterator.h"

#include <inttypes.h> // for PRIu64 etc.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Helper function to update aggregator
static void update_aggregator(timeseries_aggregation_method_e method,
                              const timeseries_field_value_t *val,
                              double *accumulator, double *count, double *min_v,
                              double *max_v,
                              timeseries_field_value_t *last_val) {
  if (!val)
    return;

  // Interpret the union as a double. (Adjust if your field type is int, bool,
  // etc.)
  double numeric = val->data.float_val;

  // Accumulate
  *accumulator += numeric;
  (*count)++;

  if (numeric < *min_v)
    *min_v = numeric;
  if (numeric > *max_v)
    *max_v = numeric;

  // "last" just overwrites
  *last_val = *val;
}

static timeseries_field_value_t
finalize_aggregator(timeseries_aggregation_method_e method, double accumulator,
                    double count, double min_v, double max_v,
                    const timeseries_field_value_t *last_val) {
  timeseries_field_value_t result;
  memset(&result, 0, sizeof(result));

  switch (method) {
  case TSDB_AGGREGATION_MIN:
    if (count > 0) {
      result.data.float_val = min_v;
    }
    break;
  case TSDB_AGGREGATION_MAX:
    if (count > 0) {
      result.data.float_val = max_v;
    }
    break;
  case TSDB_AGGREGATION_AVG:
    if (count > 0) {
      result.data.float_val = accumulator / count;
    }
    break;
  case TSDB_AGGREGATION_LAST:
    if (count > 0) {
      // just take the last overwritten value
      result = *last_val;
    }
    break;
  default:
    // TSDB_AGGREGATION_NONE or unknown => no transform
    if (count > 0) {
      // Store the total as an example
      result.data.float_val = accumulator;
    }
    break;
  }

  return result;
}

bool timeseries_multi_series_iterator_init(
    timeseries_multi_points_iterator_t **sub_iters, size_t sub_count,
    uint64_t rollup_interval, timeseries_aggregation_method_e method,
    timeseries_multi_series_iterator_t *out_iter) {
  if (!out_iter || !sub_iters || sub_count == 0 || rollup_interval == 0) {
    return false;
  }

  memset(out_iter, 0, sizeof(*out_iter));
  out_iter->sub_iters = sub_iters;
  out_iter->sub_count = sub_count;
  out_iter->rollup_interval = rollup_interval;
  out_iter->aggregator = method;
  out_iter->valid = true;

  // Find earliest start time among all sub_iters
  uint64_t earliest_ts = UINT64_MAX;
  for (size_t i = 0; i < sub_count; i++) {
    uint64_t ts;
    timeseries_field_value_t dummy;
    // *** Use the proper "peek" from multi_iterator now: ***
    if (timeseries_multi_points_iterator_peek(sub_iters[i], &ts, &dummy)) {
      if (ts < earliest_ts) {
        earliest_ts = ts;
      }
    }
  }
  if (earliest_ts == UINT64_MAX) {
    // means all iterators are empty
    out_iter->valid = false;
    return true; // no data
  }

  out_iter->current_window_start = earliest_ts;
  out_iter->current_window_end = earliest_ts + rollup_interval;

  return true;
}

/**
 * @brief Gather all points from [current_window_start, current_window_end)
 * from each sub-iterator, aggregate them, and advance to the next window.
 *
 * @param iter      The multi-series rollup iterator
 * @param out_ts    The timestamp for the rollup bucket (often window_start)
 * @param out_vals  A single aggregated value across *all sub-iterators
 * combined* OR you can store one aggregated value *per sub-iterator* if you
 * prefer. Adjust logic as needed.
 */
bool timeseries_multi_series_iterator_next(
    timeseries_multi_series_iterator_t *iter, uint64_t *out_ts,
    timeseries_field_value_t *out_vals) {
  if (!iter || !iter->valid) {
    return false;
  }

  uint64_t window_start = iter->current_window_start;
  uint64_t window_end = iter->current_window_end;

  double accumulator = 0.0;
  double sample_count = 0.0;
  double min_v = +1.0e38;
  double max_v = -1.0e38;
  timeseries_field_value_t last_val;
  memset(&last_val, 0, sizeof(last_val));

  // Gather points from each sub-iterator in [window_start, window_end)
  for (size_t s = 0; s < iter->sub_count; s++) {
    while (true) {
      uint64_t ts_cur;
      timeseries_field_value_t val_cur;
      // 'Peek' the top
      if (!timeseries_multi_points_iterator_peek(iter->sub_iters[s], &ts_cur,
                                                 &val_cur)) {
        // no more data
        break;
      }
      if (ts_cur < window_start) {
        // data is behind our window => consume & skip
        (void)timeseries_multi_points_iterator_next(iter->sub_iters[s], &ts_cur,
                                                    &val_cur);
        continue;
      }
      if (ts_cur >= window_end) {
        // belongs to a future window
        break;
      }

      // If we reach here, ts_cur is in [window_start, window_end).
      update_aggregator(iter->aggregator, &val_cur, &accumulator, &sample_count,
                        &min_v, &max_v, &last_val);

      // consume it
      (void)timeseries_multi_points_iterator_next(iter->sub_iters[s], &ts_cur,
                                                  &val_cur);
    }
  }

  // Finalize aggregator
  timeseries_field_value_t aggregated = finalize_aggregator(
      iter->aggregator, accumulator, sample_count, min_v, max_v, &last_val);

  if (out_ts) {
    *out_ts = window_start;
  }
  if (out_vals) {
    // This example: only storing one aggregated value in out_vals[0].
    // If you want one per sub-iterator or field, expand logic.
    out_vals[0] = aggregated;
  }

  // Advance the window
  iter->current_window_start += iter->rollup_interval;
  iter->current_window_end += iter->rollup_interval;

  // Check if we have future data
  bool has_more_data = false;
  for (size_t s = 0; s < iter->sub_count; s++) {
    uint64_t ts_cur;
    timeseries_field_value_t dummy;
    if (timeseries_multi_points_iterator_peek(iter->sub_iters[s], &ts_cur,
                                              &dummy)) {
      if (ts_cur >= iter->current_window_start) {
        has_more_data = true;
        break;
      }
    }
  }
  if (!has_more_data) {
    iter->valid = false;
  }

  return true;
}

void timeseries_multi_series_iterator_deinit(
    timeseries_multi_series_iterator_t *iter) {
  if (!iter) {
    return;
  }
  // If you own sub_iters, deinit or free them here. Otherwise, skip.
  memset(iter, 0, sizeof(*iter));
}
