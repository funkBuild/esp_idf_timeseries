#ifndef TIMESERIES_MULTI_ITERATOR_H
#define TIMESERIES_MULTI_ITERATOR_H

#include "timeseries_points_iterator.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct multi_points_sub_iter_t {
  timeseries_points_iterator_t *sub_iter;
  uint64_t current_ts;
  timeseries_field_value_t current_val;
  uint32_t page_seq;
  bool valid;
} multi_points_sub_iter_t;

typedef struct timeseries_multi_points_iterator_t {
  multi_points_sub_iter_t *subs;
  size_t *heap;     ///< min-heap of sub-indexes
  size_t heap_size; ///< how many valid sub-iterators remain
  size_t sub_count;

  bool valid;
} timeseries_multi_points_iterator_t;

/**
 * @brief Initialize a multi-points iterator that merges multiple
 *        timeseries_points_iterator_t sub-iterators.
 */
bool timeseries_multi_points_iterator_init(
    timeseries_points_iterator_t **sub_iters, const uint32_t *page_seqs,
    size_t sub_count, timeseries_multi_points_iterator_t *out_iter);

/**
 * @brief Return the next (timestamp,value) from this multi-iterator in
 *        ascending timestamp order. If multiple sub-iterators share the
 *        same timestamp, a tie-break is used (e.g. page_seq).
 */
bool timeseries_multi_points_iterator_next(
    timeseries_multi_points_iterator_t *multi_iter, uint64_t *out_ts,
    timeseries_field_value_t *out_val);

/**
 * @brief A proper "peek" function that returns the next (timestamp,value)
 *        that would be returned by `..._next()` but does *not* consume it.
 *        If no data remain, returns false.
 */
bool timeseries_multi_points_iterator_peek(
    timeseries_multi_points_iterator_t *multi_iter, uint64_t *out_ts,
    timeseries_field_value_t *out_val);

/**
 * @brief Deinit the multi-points iterator (frees internal arrays, but does
 *        not necessarily free or deinit the underlying sub-iterators).
 */
void timeseries_multi_points_iterator_deinit(
    timeseries_multi_points_iterator_t *multi_iter);

#ifdef __cplusplus
}
#endif

#endif // TIMESERIES_MULTI_ITERATOR_H
