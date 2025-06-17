#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "gorilla/bit_reader.h"

/**
 * @brief Callback signature for filling the internal buffer with compressed
 * data.
 *
 * When the internal buffer is exhausted or more data is required, this callback
 * is invoked to retrieve additional compressed data. The callback should copy
 * up to `max_len` bytes into `buffer` and store the number of bytes actually
 * copied in `*filled`.
 *
 * @param context  Opaque pointer provided by the user.
 * @param buffer   Buffer to be filled with new data.
 * @param max_len  Maximum number of bytes to copy into the buffer.
 * @param filled   Pointer to a variable where the number of bytes copied will
 * be stored.
 *
 * @return true on success, false on failure or if no more data is available.
 */
typedef bool (*gorilla_decoder_fill_cb)(void *context, uint8_t *buffer,
                                        size_t max_len, size_t *filled);

/**
 * @brief Gorilla stream type (e.g. timestamps vs. float or int).
 *        Expand as your compression logic requires.
 */
typedef enum {
  GORILLA_STREAM_TIMESTAMP,
  GORILLA_STREAM_FLOAT,
  GORILLA_STREAM_INT,
  GORILLA_STREAM_TYPE_SIMPLE8B,
  GORILLA_STREAM_BOOL,
  GORILLA_STREAM_STRING,
} gorilla_stream_type_t;
