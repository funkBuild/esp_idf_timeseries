// timeseries_id_list.h

#ifndef TIMESERIES_SERIES_ID_LIST_H
#define TIMESERIES_SERIES_ID_LIST_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/**
 * @brief A 16-byte series ID container.
 */
typedef struct {
  unsigned char bytes[16];
} timeseries_series_id_t;

/**
 * @brief A dynamic array of 16-byte series IDs.
 *
 * Each element is a `timeseries_series_id_t` with `bytes[16]`.
 */
typedef struct {
  timeseries_series_id_t *ids; // pointer to an array of `capacity` elements
  size_t count;                // number of valid IDs in the array
  size_t capacity;             // how many elements allocated
} timeseries_series_id_list_t;

/**
 * @brief Initialize the list (zero it out).
 */
static inline void tsdb_series_id_list_init(timeseries_series_id_list_t *list) {
  if (!list) {
    return;
  }
  list->ids = NULL;
  list->count = 0;
  list->capacity = 0;
}

/**
 * @brief Ensure there's space to append one more 16-byte series ID.
 *
 * @return true on success, false if out of memory
 */
static inline bool
tsdb_series_id_list_reserve_one(timeseries_series_id_list_t *list) {
  if (!list) {
    return false;
  }
  // If we have no capacity or it's full, grow the buffer
  if (list->count >= list->capacity) {
    size_t new_capacity = (list->capacity == 0) ? 8 : (list->capacity * 2);
    timeseries_series_id_t *new_ptr = (timeseries_series_id_t *)realloc(
        list->ids, new_capacity * sizeof(*new_ptr));
    if (!new_ptr) {
      return false; // allocation failure
    }
    list->ids = new_ptr;
    list->capacity = new_capacity;
  }
  return true;
}

/**
 * @brief Append a single 16-byte series ID to the list.
 * @return true on success, false on error (allocation fail, etc.)
 */
static inline bool
tsdb_series_id_list_append(timeseries_series_id_list_t *list,
                           const unsigned char series_id[16]) {
  if (!tsdb_series_id_list_reserve_one(list)) {
    return false;
  }
  // Copy 16 bytes into the newly available slot
  memcpy(list->ids[list->count].bytes, series_id, 16);
  list->count++;
  return true;
}

/**
 * @brief Set the list count to zero (does NOT free memory).
 *
 * The data is logically discarded, but capacity remains for reuse.
 */
static inline void
tsdb_series_id_list_clear(timeseries_series_id_list_t *list) {
  if (!list) {
    return;
  }
  list->count = 0;
}

/**
 * @brief Free the list memory entirely.
 */
static inline void tsdb_series_id_list_free(timeseries_series_id_list_t *list) {
  if (!list) {
    return;
  }
  if (list->ids) {
    free(list->ids);
    list->ids = NULL;
  }
  list->count = 0;
  list->capacity = 0;
}

/**
 * @brief Copy all IDs from `src` into `dst`, replacing `dst` contents.
 *
 * After this call:
 *   - dst->count = src->count
 *   - All series IDs from src are duplicated in dst
 *   - dst->capacity >= src->count
 *
 * @return true on success, false on allocation error
 */
static inline bool
tsdb_series_id_list_copy(timeseries_series_id_list_t *dst,
                         const timeseries_series_id_list_t *src) {
  if (!dst || !src) {
    return false;
  }
  // If src is empty, just clear dst
  if (src->count == 0) {
    tsdb_series_id_list_clear(dst);
    return true;
  }
  // Ensure capacity
  if (dst->capacity < src->count) {
    timeseries_series_id_t *new_ptr = (timeseries_series_id_t *)realloc(
        dst->ids, src->count * sizeof(*new_ptr));
    if (!new_ptr) {
      return false;
    }
    dst->ids = new_ptr;
    dst->capacity = src->count;
  }
  // Copy data
  memcpy(dst->ids, src->ids, src->count * sizeof(timeseries_series_id_t));
  dst->count = src->count;
  return true;
}

#endif // TIMESERIES_SERIES_ID_LIST_H
