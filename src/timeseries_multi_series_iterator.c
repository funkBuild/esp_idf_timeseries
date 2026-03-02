#include "timeseries_multi_series_iterator.h"

#include <inttypes.h> // for PRIu64, etc.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Existing field/value types:
 *
 * typedef enum {
 *   TIMESERIES_FIELD_TYPE_FLOAT,
 *   TIMESERIES_FIELD_TYPE_INT,
 *   TIMESERIES_FIELD_TYPE_BOOL,
 *   TIMESERIES_FIELD_TYPE_STRING,
 * } timeseries_field_type_e;
 *
 * typedef struct {
 *   timeseries_field_type_e type;
 *   union {
 *     double float_val;
 *     int64_t int_val;
 *     bool bool_val;
 *     struct {
 *       const char *str;
 *       size_t length;
 *     } string_val;
 *   } data;
 * } timeseries_field_value_t;
 *
 * Aggregation methods (example):
 *
 * typedef enum {
 *   TSDB_AGGREGATION_NONE,
 *   TSDB_AGGREGATION_MIN,
 *   TSDB_AGGREGATION_MAX,
 *   TSDB_AGGREGATION_AVG,
 *   TSDB_AGGREGATION_LAST,
 * } timeseries_aggregation_method_e;
 */

/*-------------------------------------------------------------------------------------
 *  Aggregator state — holds both int64 and double accumulators.
 *  The int64 path avoids costly double conversions on ESP32-S3 (no hw double FPU).
 *-------------------------------------------------------------------------------------*/
typedef struct {
  int64_t  i_sum;
  int64_t  i_min;
  int64_t  i_max;
  double   f_sum;
  double   f_min;
  double   f_max;
  uint32_t count;
  bool     is_int;       // true if all samples so far are INT
  timeseries_field_value_t last_val;
} aggregator_state_t;

static inline void aggregator_init(aggregator_state_t *agg) {
  agg->i_sum = 0;
  agg->i_min = INT64_MAX;
  agg->i_max = INT64_MIN;
  agg->f_sum = 0.0;
  agg->f_min = +1.0e38;
  agg->f_max = -1.0e38;
  agg->count = 0;
  agg->is_int = true;    // assume int until we see a non-int value
  memset(&agg->last_val, 0, sizeof(agg->last_val));
}

/*-------------------------------------------------------------------------------------
 *  HELPER: Update aggregator with one sample
 *-------------------------------------------------------------------------------------*/
static void update_aggregator(const timeseries_field_value_t *val,
                              aggregator_state_t *agg) {
  if (!val) {
    return;
  }

  // Free previous last_val string if needed
  if (agg->last_val.type == TIMESERIES_FIELD_TYPE_STRING &&
      agg->last_val.data.string_val.str != NULL) {
    if (val->type != TIMESERIES_FIELD_TYPE_STRING ||
        agg->last_val.data.string_val.str != val->data.string_val.str) {
      free(agg->last_val.data.string_val.str);
    }
  }
  agg->last_val = *val;

  if (val->type == TIMESERIES_FIELD_TYPE_STRING) {
    return;
  }

  if (val->type == TIMESERIES_FIELD_TYPE_INT) {
    int64_t v = val->data.int_val;
    agg->i_sum += v;
    if (v < agg->i_min) agg->i_min = v;
    if (v > agg->i_max) agg->i_max = v;
    // Also maintain float accumulators in case we see mixed types later
    if (!agg->is_int) {
      agg->f_sum += (double)v;
      if ((double)v < agg->f_min) agg->f_min = (double)v;
      if ((double)v > agg->f_max) agg->f_max = (double)v;
    }
  } else {
    // Float or bool — switch to double path
    double numeric;
    if (val->type == TIMESERIES_FIELD_TYPE_FLOAT) {
      numeric = val->data.float_val;
    } else { // BOOL
      numeric = val->data.bool_val ? 1.0 : 0.0;
    }

    if (agg->is_int && agg->count > 0) {
      // Switching from int to float: backfill float accumulators
      agg->f_sum = (double)agg->i_sum;
      agg->f_min = (double)agg->i_min;
      agg->f_max = (double)agg->i_max;
    }
    agg->is_int = false;

    agg->f_sum += numeric;
    if (numeric < agg->f_min) agg->f_min = numeric;
    if (numeric > agg->f_max) agg->f_max = numeric;
  }

  agg->count++;
}

/*-------------------------------------------------------------------------------------
 *  HELPER: Finalize aggregator into one result
 *-------------------------------------------------------------------------------------*/
static timeseries_field_value_t
finalize_aggregator(timeseries_aggregation_method_e method,
                    const aggregator_state_t *agg) {
  timeseries_field_value_t result;
  memset(&result, 0, sizeof(result));

  result.type = TIMESERIES_FIELD_TYPE_FLOAT;
  result.data.float_val = 0.0;

  if (method == TSDB_AGGREGATION_LAST) {
    return agg->last_val;
  }

  if (agg->count == 0) {
    return result;
  }

  // Integer fast path: avoid all double conversions
  if (agg->is_int && agg->last_val.type == TIMESERIES_FIELD_TYPE_INT) {
    result.type = TIMESERIES_FIELD_TYPE_INT;
    switch (method) {
    case TSDB_AGGREGATION_MIN:
      result.data.int_val = agg->i_min;
      return result;
    case TSDB_AGGREGATION_MAX:
      result.data.int_val = agg->i_max;
      return result;
    case TSDB_AGGREGATION_AVG:
      // Match double-path rounding: (int64_t)(avg + 0.5), i.e. truncation toward zero
      result.data.int_val = (2 * agg->i_sum + (int64_t)agg->count) / (2 * (int64_t)agg->count);
      return result;
    case TSDB_AGGREGATION_COUNT:
      result.data.int_val = (int64_t)agg->count;
      return result;
    case TSDB_AGGREGATION_SUM:
    case TSDB_AGGREGATION_NONE:
      result.data.int_val = agg->i_sum;
      return result;
    default:
      result.data.int_val = agg->i_sum;
      return result;
    }
  }

  // Float / mixed / bool path
  switch (method) {
  case TSDB_AGGREGATION_MIN:
    if (agg->last_val.type == TIMESERIES_FIELD_TYPE_BOOL) {
      result.type = TIMESERIES_FIELD_TYPE_BOOL;
      result.data.bool_val = (agg->f_min > 0.5);
    } else {
      result.type = TIMESERIES_FIELD_TYPE_FLOAT;
      result.data.float_val = agg->f_min;
    }
    break;

  case TSDB_AGGREGATION_MAX:
    if (agg->last_val.type == TIMESERIES_FIELD_TYPE_BOOL) {
      result.type = TIMESERIES_FIELD_TYPE_BOOL;
      result.data.bool_val = (agg->f_max > 0.5);
    } else {
      result.type = TIMESERIES_FIELD_TYPE_FLOAT;
      result.data.float_val = agg->f_max;
    }
    break;

  case TSDB_AGGREGATION_AVG: {
    double avg = agg->f_sum / (double)agg->count;
    if (agg->last_val.type == TIMESERIES_FIELD_TYPE_BOOL) {
      result.type = TIMESERIES_FIELD_TYPE_BOOL;
      result.data.bool_val = (avg > 0.5);
    } else {
      result.type = TIMESERIES_FIELD_TYPE_FLOAT;
      result.data.float_val = avg;
    }
    break;
  }

  case TSDB_AGGREGATION_COUNT:
    result.type = TIMESERIES_FIELD_TYPE_INT;
    result.data.int_val = (int64_t)agg->count;
    break;

  case TSDB_AGGREGATION_SUM:
  case TSDB_AGGREGATION_NONE:
    if (agg->last_val.type == TIMESERIES_FIELD_TYPE_BOOL) {
      result.type = TIMESERIES_FIELD_TYPE_INT;
      result.data.int_val = (int64_t)agg->f_sum;
    } else {
      result.type = TIMESERIES_FIELD_TYPE_FLOAT;
      result.data.float_val = agg->f_sum;
    }
    break;

  default:
    result.type = TIMESERIES_FIELD_TYPE_FLOAT;
    result.data.float_val = agg->f_sum;
    break;
  }

  return result;
}

/*-------------------------------------------------------------------------------------
 *  INIT: We allow rollup_interval == 0 => pass-through mode
 *-------------------------------------------------------------------------------------*/
bool timeseries_multi_series_iterator_init(
    timeseries_multi_points_iterator_t **sub_iters, size_t sub_count,
    uint64_t rollup_interval, timeseries_aggregation_method_e method,
    timeseries_multi_series_iterator_t *out_iter) {
  if (!out_iter || !sub_iters || sub_count == 0) {
    return false;
  }

  memset(out_iter, 0, sizeof(*out_iter));
  out_iter->sub_iters = sub_iters;
  out_iter->sub_count = sub_count;
  out_iter->rollup_interval = rollup_interval;
  out_iter->aggregator = method;
  out_iter->valid = true;

  // If rollup_interval == 0 => pass-through mode
  if (rollup_interval == 0) {
    return true;
  }

  // Otherwise, find earliest start time
  uint64_t earliest_ts = UINT64_MAX;
  for (size_t i = 0; i < sub_count; i++) {
    uint64_t ts;
    timeseries_field_value_t dummy;
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

/*-------------------------------------------------------------------------------------
 *  NEXT
 *-------------------------------------------------------------------------------------*/
bool timeseries_multi_series_iterator_next(
    timeseries_multi_series_iterator_t *iter, uint64_t *out_ts,
    timeseries_field_value_t *out_val) {
  if (!iter || !iter->valid) {
    return false;
  }

  /*-----------------------------------------------------------------------------
   *  DISABLED ROLLUP => pass-through: return the single earliest data point
   *unmodified.
   *-----------------------------------------------------------------------------*/
  if (iter->rollup_interval == 0) {
    // Single-series fast path: skip scan loops entirely
    if (iter->sub_count == 1) {
      uint64_t ts;
      timeseries_field_value_t val;
      if (!timeseries_multi_points_iterator_next(iter->sub_iters[0], &ts, &val)) {
        iter->valid = false;
        return false;
      }
      if (out_ts) *out_ts = ts;
      if (out_val) {
        *out_val = val;
      } else if (val.type == TIMESERIES_FIELD_TYPE_STRING && val.data.string_val.str) {
        free(val.data.string_val.str);
      }
      // Check if more data remains
      uint64_t peek_ts;
      timeseries_field_value_t peek_val;
      if (!timeseries_multi_points_iterator_peek(iter->sub_iters[0], &peek_ts, &peek_val)) {
        iter->valid = false;
      }
      return true;
    }

    // Multi-series pass-through: find earliest across all sub-iterators
    uint64_t earliest_ts = UINT64_MAX;
    int earliest_idx = -1;

    for (size_t s = 0; s < iter->sub_count; s++) {
      uint64_t ts_cur;
      timeseries_field_value_t dummy;
      if (timeseries_multi_points_iterator_peek(iter->sub_iters[s], &ts_cur,
                                                &dummy)) {
        if (ts_cur < earliest_ts) {
          earliest_ts = ts_cur;
          earliest_idx = (int)s;
        }
      }
    }

    // If none has data => done
    if (earliest_idx < 0 || earliest_ts == UINT64_MAX) {
      iter->valid = false;
      return false;
    }

    // Pop that earliest data point
    uint64_t actual_ts;
    timeseries_field_value_t actual_val;
    if (!timeseries_multi_points_iterator_next(iter->sub_iters[earliest_idx],
                                               &actual_ts, &actual_val)) {
      iter->valid = false;
      return false;
    }

    if (out_ts) {
      *out_ts = actual_ts;
    }
    if (out_val) {
      *out_val = actual_val;
    } else {
      if (actual_val.type == TIMESERIES_FIELD_TYPE_STRING &&
          actual_val.data.string_val.str) {
        free(actual_val.data.string_val.str);
      }
    }

    // has_more check is deferred: next call's "find earliest" scan will
    // set iter->valid = false when no sub-iterators have data.
    return true;
  }

  /*-----------------------------------------------------------------------------
   *  ENABLED ROLLUP => bucket-based aggregation
   *  Skip empty windows — advance until we find a window with data or exhaust
   *  all sub-iterators.
   *-----------------------------------------------------------------------------*/
  while (true) {
    uint64_t window_start = iter->current_window_start;
    uint64_t window_end = iter->current_window_end;

    aggregator_state_t agg;
    aggregator_init(&agg);

    // Gather points from each sub-iterator in [window_start, window_end)
    for (size_t s = 0; s < iter->sub_count; s++) {
      while (true) {
        uint64_t ts_cur;
        timeseries_field_value_t val_cur;
        // Peek to check timestamp without consuming
        if (!timeseries_multi_points_iterator_peek(iter->sub_iters[s], &ts_cur,
                                                   &val_cur)) {
          break;
        }
        if (ts_cur < window_start) {
          // Skip old data -- consume the point and free any owned string
          (void)timeseries_multi_points_iterator_next(iter->sub_iters[s],
                                                      &ts_cur, &val_cur);
          if (val_cur.type == TIMESERIES_FIELD_TYPE_STRING) {
            free(val_cur.data.string_val.str);
            val_cur.data.string_val.str = NULL;
          }
          continue;
        }
        if (ts_cur >= window_end) {
          break;
        }

        // Consume first to get properly deduplicated value with ownership,
        // then feed to the aggregator.
        (void)timeseries_multi_points_iterator_next(iter->sub_iters[s], &ts_cur,
                                                    &val_cur);

        update_aggregator(&val_cur, &agg);
      }
    }

    // Advance the window for the next call
    iter->current_window_start += iter->rollup_interval;
    iter->current_window_end += iter->rollup_interval;

    // Check if there's more data beyond this window
    bool has_more_data = false;
    for (size_t s = 0; s < iter->sub_count; s++) {
      uint64_t ts_cur;
      timeseries_field_value_t dummy;
      if (timeseries_multi_points_iterator_peek(iter->sub_iters[s], &ts_cur,
                                                &dummy)) {
        has_more_data = true;
        break;
      }
    }

    // If this window had no samples, skip it
    if (agg.count == 0 &&
        iter->aggregator != TSDB_AGGREGATION_LAST) {
      if (!has_more_data) {
        iter->valid = false;
        return false;
      }
      // Try the next window
      continue;
    }

    // Finalize aggregator
    timeseries_field_value_t aggregated =
        finalize_aggregator(iter->aggregator, &agg);

    // If the aggregator did not transfer last_val's string into the result
    // (i.e. non-LAST aggregation on string data), free it to avoid a leak.
    if (agg.last_val.type == TIMESERIES_FIELD_TYPE_STRING &&
        agg.last_val.data.string_val.str != NULL) {
      if (aggregated.type != TIMESERIES_FIELD_TYPE_STRING ||
          aggregated.data.string_val.str != agg.last_val.data.string_val.str) {
        free(agg.last_val.data.string_val.str);
      }
    }

    // Output
    if (out_ts) {
      *out_ts = window_start;
    }
    if (out_val) {
      *out_val = aggregated;
    }

    if (!has_more_data) {
      iter->valid = false;
    }

    return true;
  }
}

/*-------------------------------------------------------------------------------------
 *  DEINIT
 *-------------------------------------------------------------------------------------*/
void timeseries_multi_series_iterator_deinit(
    timeseries_multi_series_iterator_t *iter) {
  if (!iter) {
    return;
  }
  // If you own sub_iters, deinit/free them here. Otherwise, skip.
  memset(iter, 0, sizeof(*iter));
}
