#ifndef GORILLA_STREAM_H
#define GORILLA_STREAM_H

#include "gorilla/gorilla_stream_types.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h> // For memset

/**
 * @brief Callback signature for flushing compressed data.
 *
 * When the internal buffer is ready to be flushed (full or finalize),
 * this callback is invoked. Return true on success, false otherwise.
 */
typedef bool (*gorilla_stream_flush_cb)(void *context, const uint8_t *data,
                                        size_t len);

/**
 * @brief Gorilla stream structure.
 *
 * This structure now contains the fields that were previously hidden behind a
 * C++-only implementation. In this pure C version, the fields are exposed (or
 * you could opt to hide them by making this an incomplete type and moving the
 * definition to the .c file).
 */
typedef struct {
  gorilla_stream_type_t stream_type; /**< Type of data in the stream */
  gorilla_stream_flush_cb flush_cb;  /**< Callback to flush the buffer */
  void *flush_ctx;                   /**< Context for the flush callback */
  size_t bit_index;                  /**< Current write index in bit_buffer */
  void *encoder_impl; /**< Pointer to integer encoder instance (used if
                         stream_type==GORILLA_STREAM_INT) */
} gorilla_stream_t;

/**
 * @brief Initialize a Gorilla streaming encoder.
 *
 * @param[out] stream          The gorilla_stream_t to initialize.
 * @param[in]  stream_type     The type (timestamps, floats, ints, etc.) of the
 * stream.
 * @param[in]  bit_buffer      Caller-allocated buffer used for bit-packing.
 * @param[in]  bit_buffer_size Size of bit_buffer in bytes.
 * @param[in]  flush_cb        Callback invoked to flush compressed data.
 * @param[in]  flush_ctx       Opaque pointer passed to flush_cb.
 *
 * @return true on success, false on failure.
 */
bool gorilla_stream_init(gorilla_stream_t *stream,
                         gorilla_stream_type_t stream_type, uint8_t *bit_buffer,
                         size_t bit_buffer_size,
                         gorilla_stream_flush_cb flush_cb, void *flush_ctx);

/**
 * @brief Add a timestamp (uint64_t) to the Gorilla stream.
 *
 * @param[in] stream    The gorilla_stream_t.
 * @param[in] timestamp A 64-bit timestamp.
 *
 * @return true on success, false on failure.
 */
bool gorilla_stream_add_timestamp(gorilla_stream_t *stream, uint64_t timestamp);

bool gorilla_stream_add_float(gorilla_stream_t *stream, double value);

bool gorilla_stream_add_boolean(gorilla_stream_t *stream, bool value);

bool gorilla_stream_add_string(gorilla_stream_t *stream, const char *data,
                               size_t length);

/**
 * @brief Finish the Gorilla stream, flushing remaining bits and writing any
 * footer.
 *
 * @param[in] stream  The gorilla_stream_t.
 *
 * @return true on success, false on failure.
 */
bool gorilla_stream_finish(gorilla_stream_t *stream);

/**
 * @brief Deinitialize the Gorilla stream, freeing any internal resources.
 *
 * @param[in] stream  The gorilla_stream_t.
 */
void gorilla_stream_deinit(gorilla_stream_t *stream);

#endif // GORILLA_STREAM_H
