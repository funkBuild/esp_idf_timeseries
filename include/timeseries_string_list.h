// timeseries_string_list.h

#ifndef TIMESERIES_STRING_LIST_H
#define TIMESERIES_STRING_LIST_H

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

/**
 * @brief A dynamic list of dynamically allocated strings.
 */
typedef struct {
  char **items;
  size_t count;
  size_t capacity;
} timeseries_string_list_t;

/**
 * @brief Initialize the string list (empty).
 */
static inline void tsdb_string_list_init(timeseries_string_list_t *list) {
  if (!list)
    return;
  list->items = NULL;
  list->count = 0;
  list->capacity = 0;
}

/**
 * @brief Reserve space for at least one more string.
 */
static inline bool
tsdb_string_list_reserve_one(timeseries_string_list_t *list) {
  if (!list)
    return false;
  if (list->count >= list->capacity) {
    size_t new_capacity = (list->capacity == 0 ? 8 : list->capacity * 2);
    char **new_ptr =
        (char **)realloc(list->items, new_capacity * sizeof(char *));
    if (!new_ptr) {
      return false;
    }
    list->items = new_ptr;
    list->capacity = new_capacity;
  }
  return true;
}

/**
 * @brief Append a copy of `str` to the list, skipping if it already exists.
 *        For uniqueness, we do a simple linear search. Adjust as needed for
 * performance.
 */
static inline bool
tsdb_string_list_append_unique(timeseries_string_list_t *list,
                               const char *str) {
  if (!list || !str)
    return false;
  // Check duplicates
  for (size_t i = 0; i < list->count; i++) {
    if (strcmp(list->items[i], str) == 0) {
      // Already in the list
      return true; // Not an error, just skip
    }
  }
  // Reserve space
  if (!tsdb_string_list_reserve_one(list)) {
    return false;
  }
  // Duplicate the string
  char *copy = strdup(str);
  if (!copy) {
    return false;
  }
  list->items[list->count++] = copy;
  return true;
}

/**
 * @brief Free all strings and the array itself.
 */
static inline void tsdb_string_list_free(timeseries_string_list_t *list) {
  if (!list)
    return;
  if (list->items) {
    for (size_t i = 0; i < list->count; i++) {
      if (list->items[i]) {
        free(list->items[i]);
      }
    }
    free(list->items);
  }
  list->items = NULL;
  list->count = 0;
  list->capacity = 0;
}

#endif // TIMESERIES_STRING_LIST_H
