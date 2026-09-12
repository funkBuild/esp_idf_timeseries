#include "timeseries_multi_series_iterator.h"

#include <inttypes.h> // for PRIu64, etc.
#include <math.h>
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
  int64_t  i_sum_carry; // Exact total = i_sum + carry * 2^64.
  int64_t  i_min;
  int64_t  i_max;
  double   f_sum;
  double   f_min;
  double   f_max;
  uint64_t count;
  uint64_t last_timestamp;
  bool     is_int;       // true if all samples so far are INT
  timeseries_field_value_t last_val;
} aggregator_state_t;

static inline void aggregator_init(aggregator_state_t *agg) {
  agg->i_sum = 0;
  agg->i_sum_carry = 0;
  agg->i_min = INT64_MAX;
  agg->i_max = INT64_MIN;
  agg->f_sum = 0.0;
  agg->f_min = INFINITY;
  agg->f_max = -INFINITY;
  agg->count = 0;
  agg->last_timestamp = 0;
  agg->is_int = true;    // assume int until we see a non-int value
  memset(&agg->last_val, 0, sizeof(agg->last_val));
}

/* Keep the common case in int64_t, extending only on signed overflow. GCC's
 * overflow builtin defines the wrapped low word without undefined arithmetic.
 * Two words hold the exact sum of up to UINT64_MAX int64_t samples. */
static inline void integer_sum_add(aggregator_state_t *agg, int64_t value) {
  if (__builtin_add_overflow(agg->i_sum, value, &agg->i_sum)) {
    agg->i_sum_carry += value < 0 ? -1 : 1;
  }
}

static bool integer_sum_magnitude(const aggregator_state_t *agg,
                                  uint64_t *high, uint64_t *low) {
  bool negative = agg->i_sum_carry < 0 ||
                  (agg->i_sum_carry == 0 && agg->i_sum < 0);
  *low = (uint64_t)agg->i_sum;
  *high = (uint64_t)agg->i_sum_carry - (agg->i_sum < 0);
  if (negative) {
    *high = ~*high + (*low == 0);
    *low = 0 - *low;
  }
  return negative;
}

static double integer_sum_as_double(const aggregator_state_t *agg) {
  if (agg->i_sum_carry == 0) return (double)agg->i_sum;
  uint64_t high, low;
  bool negative = integer_sum_magnitude(agg, &high, &low);
  double magnitude = (double)high * 0x1p64 + (double)low;
  return negative ? -magnitude : magnitude;
}

static int64_t integer_average(const aggregator_state_t *agg) {
  if (agg->i_min == agg->i_max) return agg->i_min;
  uint64_t high, low;
  bool negative = integer_sum_magnitude(agg, &high, &low);
  uint64_t quotient, remainder;
  if ((agg->count & (agg->count - 1)) == 0) {
    // Count == 1 was handled by the identical-values fast path above.
    unsigned shift = (unsigned)__builtin_ctzll(agg->count);
    quotient = (low >> shift) | (high << (64 - shift));
    remainder = low & (agg->count - 1);
  } else if (high == 0) {
    quotient = low / agg->count;
    remainder = low - quotient * agg->count;
  } else {
    // The quotient is at most 2^63 (an average stays inside the input range),
    // so high < count. Long division is needed only for large accumulated sums.
    quotient = 0;
    remainder = high;
    for (int bit = 63; bit >= 0; bit--) {
      bool carry = (remainder >> 63) != 0;
      remainder = (remainder << 1) | ((low >> bit) & 1);
      if (carry || remainder >= agg->count) {
        remainder -= agg->count;
        quotient |= UINT64_C(1) << bit;
      }
    }
  }
  // Round nearest, ties away from zero, without doubling the sum/remainder.
  if (remainder >= agg->count / 2 + agg->count % 2) quotient++;
  if (!negative) return (int64_t)quotient;
  // This form also represents INT64_MIN without negating it or casting 2^63.
  return quotient == 0 ? 0 : -(int64_t)(quotient - 1) - 1;
}

/*-------------------------------------------------------------------------------------
 *  HELPER: Update aggregator with one sample
 *-------------------------------------------------------------------------------------*/
static void update_aggregator(const timeseries_field_value_t *val,
                              uint64_t timestamp,
                              timeseries_aggregation_method_e method,
                              aggregator_state_t *agg) {
  if (!val) {
    return;
  }

  if (method == TSDB_AGGREGATION_COUNT) {
    agg->count++;
    if (val->type == TIMESERIES_FIELD_TYPE_STRING) {
      free(val->data.string_val.str);
    }
    return;
  }
  // LAST follows timestamp order across series, with later series winning ties.
  if (method == TSDB_AGGREGATION_LAST && agg->count != 0 &&
      timestamp < agg->last_timestamp) {
    if (val->type == TIMESERIES_FIELD_TYPE_STRING) {
      free(val->data.string_val.str);
    }
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
  agg->count++;
  agg->last_timestamp = timestamp;
  if (method == TSDB_AGGREGATION_LAST) {
    agg->count = 1; // Only presence matters for LAST.
    return;
  }

  const bool needs_sum = method != TSDB_AGGREGATION_MIN &&
                         method != TSDB_AGGREGATION_MAX;
  if (val->type == TIMESERIES_FIELD_TYPE_STRING) {
    return;
  }

  if (val->type == TIMESERIES_FIELD_TYPE_INT) {
    int64_t v = val->data.int_val;
    if (needs_sum && agg->is_int) integer_sum_add(agg, v);
    if (v < agg->i_min) agg->i_min = v;
    if (v > agg->i_max) agg->i_max = v;
    // Also maintain float accumulators in case we see mixed types later
    if (!agg->is_int) {
      if (needs_sum) agg->f_sum += (double)v;
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

    if (agg->is_int && agg->count > 1) {
      // Switching from int to float: backfill float accumulators
      if (needs_sum) agg->f_sum = integer_sum_as_double(agg);
      agg->f_min = (double)agg->i_min;
      agg->f_max = (double)agg->i_max;
    }
    agg->is_int = false;

    if (needs_sum) agg->f_sum += numeric;
    if (numeric < agg->f_min) agg->f_min = numeric;
    if (numeric > agg->f_max) agg->f_max = numeric;
  }
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

  if (method == TSDB_AGGREGATION_COUNT) {
    if (agg->count <= INT64_MAX) {
      result.type = TIMESERIES_FIELD_TYPE_INT;
      result.data.int_val = (int64_t)agg->count;
    } else {
      result.data.float_val = (double)agg->count;
    }
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
      result.data.int_val = integer_average(agg);
      return result;
    case TSDB_AGGREGATION_COUNT:
      result.data.int_val = (int64_t)agg->count;
      return result;
    case TSDB_AGGREGATION_SUM:
    case TSDB_AGGREGATION_NONE:
    default:
      if (agg->i_sum_carry == 0) {
        result.data.int_val = agg->i_sum;
      } else {
        result.type = TIMESERIES_FIELD_TYPE_FLOAT;
        result.data.float_val = integer_sum_as_double(agg);
      }
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
  bool found = false;
  for (size_t i = 0; i < sub_count; i++) {
    uint64_t ts;
    timeseries_field_value_t dummy;
    if (timeseries_multi_points_iterator_peek(sub_iters[i], &ts, &dummy)) {
      if (!found || ts < earliest_ts) earliest_ts = ts;
      found = true;
    }
  }
  if (!found) {
    // means all iterators are empty
    out_iter->valid = false;
    return true; // no data
  }

  out_iter->current_window_start = earliest_ts;
  out_iter->current_window_end = rollup_interval > UINT64_MAX - earliest_ts
      ? UINT64_MAX : earliest_ts + rollup_interval;
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
    size_t earliest_idx = SIZE_MAX;

    for (size_t s = 0; s < iter->sub_count; s++) {
      uint64_t ts_cur;
      timeseries_field_value_t dummy;
      if (timeseries_multi_points_iterator_peek(iter->sub_iters[s], &ts_cur,
                                                &dummy)) {
        if (earliest_idx == SIZE_MAX || ts_cur < earliest_ts) {
          earliest_ts = ts_cur;
          earliest_idx = s;
        }
      }
    }

    // If none has data => done
    if (earliest_idx == SIZE_MAX) {
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

    aggregator_state_t agg;
    aggregator_init(&agg);
    bool has_more_data = false;
    uint64_t next_ts = UINT64_MAX;

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
        // Subtraction also handles the final window extending beyond UINT64_MAX.
        if (ts_cur - window_start >= iter->rollup_interval) {
          if (!has_more_data || ts_cur < next_ts) next_ts = ts_cur;
          has_more_data = true;
          break;
        }

        // Consume first to get properly deduplicated value with ownership,
        // then feed to the aggregator.
        (void)timeseries_multi_points_iterator_next(iter->sub_iters[s], &ts_cur,
                                                    &val_cur);

        update_aggregator(&val_cur, ts_cur, iter->aggregator, &agg);
      }
    }

    // Find the next occupied window directly, preserving the original grid.
    // Work depends on series/point counts, not the number of empty intervals.
    if (has_more_data) {
      uint64_t delta = next_ts - window_start;
      // Adjacent occupied windows are common; avoid a 64-bit division there.
      uint64_t advance = delta - iter->rollup_interval < iter->rollup_interval
          ? iter->rollup_interval
          : (delta / iter->rollup_interval) * iter->rollup_interval;
      uint64_t next_start = window_start + advance;
      iter->current_window_start = next_start;
      iter->current_window_end = iter->rollup_interval > UINT64_MAX - next_start
          ? UINT64_MAX : next_start + iter->rollup_interval;
    }

    // If this window had no samples, skip it
    if (agg.count == 0) {
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
    } else if (aggregated.type == TIMESERIES_FIELD_TYPE_STRING &&
               aggregated.data.string_val.str) {
      free(aggregated.data.string_val.str);
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
