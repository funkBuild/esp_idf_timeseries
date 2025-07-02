#include "timeseries_multi_series_iterator.h"

#include <inttypes.h>  // for PRIu64, etc.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
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
 *   TSDB_AGGREGATION_LATEST,
 * } timeseries_aggregation_method_e;
 */

/*-------------------------------------------------------------------------------------
 *  HELPER: Convert an incoming val => double for aggregator (only if numeric).
 *          Returns (0.0) if it's a string or unknown type.
 *-------------------------------------------------------------------------------------*/
static double to_double_for_agg(const timeseries_field_value_t* val) {
  if (!val) {
    return 0.0;
  }

  switch (val->type) {
    case TIMESERIES_FIELD_TYPE_BOOL:
      return (val->data.bool_val) ? 1.0 : 0.0;

    case TIMESERIES_FIELD_TYPE_INT:
      return val->data.int_val;

    case TIMESERIES_FIELD_TYPE_FLOAT:
      return val->data.float_val;

    case TIMESERIES_FIELD_TYPE_STRING:
      // For numeric aggregations, we skip strings by treating them as 0.0
      // (or you could choose to skip them entirely in update_aggregator).
      return 0.0;

    default:
      return 0.0;
  }
}

/*-------------------------------------------------------------------------------------
 *  HELPER: Update aggregator with one sample
 *          - We only aggregate numeric types (bool, int, float).
 *          - If it's a string, we skip it for min/max/sum/avg; but we
 *            still set "last_val" if aggregator = LAST.
 *-------------------------------------------------------------------------------------*/
static void update_aggregator(timeseries_aggregation_method_e method, const timeseries_field_value_t* val,
                              double* accumulator, double* count, double* min_v, double* max_v,
                              timeseries_field_value_t* last_val) {
  if (!val) {
    return;
  }

  // Always update "last_val" if aggregator is LAST (or if you'd like to keep
  // the last for any aggregator?). For simplicity, we always store it here:
  *last_val = *val;

  // If it's not numeric, skip it unless aggregator = LAST. But "last_val" is
  // already captured above.
  if (val->type == TIMESERIES_FIELD_TYPE_STRING) {
    // For numeric aggregator methods (min, max, sum, avg), ignore string.
    // So do *not* update accumulator/min/max. Return now.
    return;
  }

  // Convert numeric to double:
  double numeric = to_double_for_agg(val);

  // Accumulate for numeric aggregations:
  *accumulator += numeric;
  (*count)++;

  if (numeric < *min_v) {
    *min_v = numeric;
  }
  if (numeric > *max_v) {
    *max_v = numeric;
  }
}

/*-------------------------------------------------------------------------------------
 *  HELPER: Finalize aggregator into one result
 *-------------------------------------------------------------------------------------*/
static timeseries_field_value_t finalize_aggregator(timeseries_aggregation_method_e method, double accumulator,
                                                    double count, double min_v, double max_v,
                                                    const timeseries_field_value_t* last_val) {
  timeseries_field_value_t result;
  memset(&result, 0, sizeof(result));

  // If we had no numeric samples in the window/bucket, count == 0.
  // aggregator => defaults to float=0.0 unless aggregator = LAST & last_val is
  // something. We'll handle aggregator = LAST below.
  result.type = TIMESERIES_FIELD_TYPE_FLOAT;
  result.data.float_val = 0.0;

  // If aggregator = LAST, we always return the last_val as-is, even if
  // it's a string or a bool, int, etc.
  if (method == TSDB_AGGREGATION_LATEST) {
    // If we never saw any point at all, last_val might be empty.
    // But typically, you only call finalize if something was processed.
    return *last_val;
  }

  // If aggregator is MIN, MAX, AVG, or NONE/SUM, but no numeric samples =>
  // just return 0.0 float. (Or you can leave as an "invalid" marker.)
  if (count <= 0) {
    return result;
  }

  switch (method) {
    case TSDB_AGGREGATION_MIN:
      // If last_val was originally int or bool, produce an int/bool min
      if (last_val->type == TIMESERIES_FIELD_TYPE_INT) {
        result.type = TIMESERIES_FIELD_TYPE_INT;
        result.data.int_val = (int64_t)min_v;
      } else if (last_val->type == TIMESERIES_FIELD_TYPE_BOOL) {
        result.type = TIMESERIES_FIELD_TYPE_BOOL;
        result.data.bool_val = (min_v > 0.5);
      } else {
        // float
        result.type = TIMESERIES_FIELD_TYPE_FLOAT;
        result.data.float_val = min_v;
      }
      break;

    case TSDB_AGGREGATION_MAX:
      if (last_val->type == TIMESERIES_FIELD_TYPE_INT) {
        result.type = TIMESERIES_FIELD_TYPE_INT;
        result.data.int_val = (int64_t)max_v;
      } else if (last_val->type == TIMESERIES_FIELD_TYPE_BOOL) {
        result.type = TIMESERIES_FIELD_TYPE_BOOL;
        result.data.bool_val = (max_v > 0.5);
      } else {
        result.type = TIMESERIES_FIELD_TYPE_FLOAT;
        result.data.float_val = max_v;
      }
      break;

    case TSDB_AGGREGATION_AVG:
      // always produce float
      result.type = TIMESERIES_FIELD_TYPE_FLOAT;

      if (count <= 0) {
        // No samples => return 0.0
        result.data.float_val = 0.0;
      } else {
        // Otherwise, float average
        result.data.float_val = accumulator / count;
      }

      break;

    case TSDB_AGGREGATION_NONE:
      // By convention, treat as SUM
      // (you could do something else for "none")
      /* fallthrough */
    default:
      // Summation
      // If last_val was int or bool, produce an int sum (0 or 1 for bool).
      // Otherwise float sum.
      if (last_val->type == TIMESERIES_FIELD_TYPE_INT || last_val->type == TIMESERIES_FIELD_TYPE_BOOL) {
        int64_t sum_as_int = (int64_t)accumulator;  // watch for overflow
        result.type = TIMESERIES_FIELD_TYPE_INT;
        result.data.int_val = sum_as_int;
      } else {
        result.type = TIMESERIES_FIELD_TYPE_FLOAT;
        result.data.float_val = accumulator;
      }
      break;
  }

  return result;
}

/*-------------------------------------------------------------------------------------
 *  INIT: We allow rollup_interval == 0 => pass-through mode
 *-------------------------------------------------------------------------------------*/
bool timeseries_multi_series_iterator_init(timeseries_multi_points_iterator_t** sub_iters, size_t sub_count,
                                           uint64_t rollup_interval, timeseries_aggregation_method_e method,
                                           timeseries_multi_series_iterator_t* out_iter) {
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
    return true;  // no data
  }

  out_iter->current_window_start = earliest_ts;
  out_iter->current_window_end = earliest_ts + rollup_interval;
  return true;
}

/*-------------------------------------------------------------------------------------
 *  NEXT
 *-------------------------------------------------------------------------------------*/
bool timeseries_multi_series_iterator_next(timeseries_multi_series_iterator_t* iter, uint64_t* out_ts,
                                           timeseries_field_value_t* out_val) {
  if (!iter || !iter->valid) {
    return false;
  }

  /*-----------------------------------------------------------------------------
   *  DISABLED ROLLUP => pass-through: return the single earliest data point
   *unmodified.
   *-----------------------------------------------------------------------------*/
  if (iter->rollup_interval == 0) {
    // 1. Find the sub-iterator whose next point is earliest
    uint64_t earliest_ts = UINT64_MAX;
    int earliest_idx = -1;

    for (size_t s = 0; s < iter->sub_count; s++) {
      uint64_t ts_cur;
      timeseries_field_value_t dummy;
      if (timeseries_multi_points_iterator_peek(iter->sub_iters[s], &ts_cur, &dummy)) {
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

    // 2. Pop that earliest data point
    uint64_t actual_ts;
    timeseries_field_value_t actual_val;
    if (!timeseries_multi_points_iterator_next(iter->sub_iters[earliest_idx], &actual_ts, &actual_val)) {
      // Should not fail if peek said it was valid
      iter->valid = false;
      return false;
    }

    // 3. Return as-is
    if (out_ts) {
      *out_ts = actual_ts;
    }
    if (out_val) {
      *out_val = actual_val;
    }

    // 4. Check if we have more data left
    bool has_more_data = false;
    for (size_t s = 0; s < iter->sub_count; s++) {
      uint64_t check_ts;
      timeseries_field_value_t dummy;
      if (timeseries_multi_points_iterator_peek(iter->sub_iters[s], &check_ts, &dummy)) {
        has_more_data = true;
        break;
      }
    }
    if (!has_more_data) {
      iter->valid = false;
    }

    return true;
  }

  /*-----------------------------------------------------------------------------
   *  ENABLED ROLLUP => bucket-based aggregation
   *-----------------------------------------------------------------------------*/
  while (true) { /* ← new outer loop */
    uint64_t window_start = iter->current_window_start;
    uint64_t window_end = iter->current_window_end;

    /* --- per-bucket state ---------------------------------------------------- */
    bool window_has_values = false;
    double accumulator = 0.0;
    double sample_count = 0.0;
    double min_v = DBL_MAX, max_v = -DBL_MAX;
    timeseries_field_value_t last_val = {0};

    /* --- gather points that fall into [window_start, window_end) ------------ */
    for (size_t s = 0; s < iter->sub_count; ++s) {
      while (true) {
        uint64_t ts_cur;
        timeseries_field_value_t v_cur;
        if (!timeseries_multi_points_iterator_peek(iter->sub_iters[s], &ts_cur, &v_cur))
          break; /* no more from this series */

        if (ts_cur < window_start) { /* discard stale sample */
          (void)timeseries_multi_points_iterator_next(iter->sub_iters[s], &ts_cur, &v_cur);
          continue;
        }
        if (ts_cur >= window_end) /* future bucket -> stop scanning this series */
          break;

        /* sample belongs to the current bucket */
        window_has_values = true;
        update_aggregator(iter->aggregator, &v_cur, &accumulator, &sample_count, &min_v, &max_v, &last_val);

        (void)timeseries_multi_points_iterator_next(iter->sub_iters[s], &ts_cur, &v_cur);
      }
    }

    /* --- empty bucket? ------------------------------------------------------- */
    if (!window_has_values) {
      /* advance the window and keep looking until we hit data or exhaust input */
      iter->current_window_start += iter->rollup_interval;
      iter->current_window_end += iter->rollup_interval;

      /* still any data ≥ new window? */
      bool still_has_data = false;
      for (size_t s = 0; s < iter->sub_count; ++s) {
        uint64_t ts_next;
        timeseries_field_value_t dummy;
        if (timeseries_multi_points_iterator_peek(iter->sub_iters[s], &ts_next, &dummy) &&
            ts_next >= iter->current_window_start) {
          still_has_data = true;
          break;
        }
      }
      if (!still_has_data) { /* nothing left anywhere */
        iter->valid = false;
        return false;
      }
      continue; /* loop again – do NOT emit */
    }

    /* --- non-empty bucket: finalise & emit ----------------------------------- */
    timeseries_field_value_t agg =
        finalize_aggregator(iter->aggregator, accumulator, sample_count, min_v, max_v, &last_val);

    if (out_ts) *out_ts = window_start;
    if (out_val) *out_val = agg;

    /* advance window for next call */
    iter->current_window_start += iter->rollup_interval;
    iter->current_window_end += iter->rollup_interval;

    return true; /* exactly one value emitted */
  }
}

/*-------------------------------------------------------------------------------------
 *  DEINIT
 *-------------------------------------------------------------------------------------*/
void timeseries_multi_series_iterator_deinit(timeseries_multi_series_iterator_t* iter) {
  if (!iter) {
    return;
  }
  // If you own sub_iters, deinit/free them here. Otherwise, skip.
  memset(iter, 0, sizeof(*iter));
}
