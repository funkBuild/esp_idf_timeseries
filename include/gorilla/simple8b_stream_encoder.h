#ifndef SIMPLE8B_STREAM_ENCODER_H
#define SIMPLE8B_STREAM_ENCODER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/**
 * @brief Flush callback type.
 * The callback should write out `len` bytes from `data` using the user‐supplied
 * context.
 */
typedef bool (*FlushCallback)(const uint8_t *data, size_t len, void *context);

/**
 * @brief Streaming Simple8b encoder.
 *
 * This encoder accumulates 64‑bit unsigned integer values into an internal
 * dynamic array. When the flush (or finish) function is called, the encoder
 * runs the Simple8b packing algorithm on the accumulated values. The algorithm
 * tries the selectors in a fixed order (see implementation) and calls the flush
 * callback with each 64‐bit packed word.
 */
typedef struct {
  FlushCallback flush_cb;
  void *flush_ctx;
  uint64_t *values; // dynamically allocated array of input values
  size_t count;     // number of values stored
  size_t capacity;  // current capacity of values array
} Simple8bStreamEncoder;

/**
 * @brief Create a new Simple8bStreamEncoder instance.
 *
 * @param flush_cb  The flush callback.
 * @param flush_ctx User‐supplied context for the flush callback.
 * @return Pointer to a new instance, or NULL on allocation failure.
 */
Simple8bStreamEncoder *simple8b_stream_encoder_create(FlushCallback flush_cb,
                                                      void *flush_ctx);

/**
 * @brief Destroy a Simple8bStreamEncoder instance.
 *
 * @param encoder Pointer to the encoder instance.
 */
void simple8b_stream_encoder_destroy(Simple8bStreamEncoder *encoder);

/**
 * @brief Add a 64‐bit value to the encoder.
 *
 * @param encoder Pointer to the encoder.
 * @param value   The value to add.
 * @return true on success, false on failure.
 */
bool simple8b_stream_encoder_add(Simple8bStreamEncoder *encoder,
                                 uint64_t value);

/**
 * @brief Flush the accumulated values by encoding them.
 *
 * This function runs the Simple8b encoding algorithm on the internally stored
 * values and calls the flush callback on each packed 64‐bit word. On success
 * the internal value buffer is cleared.
 *
 * @param encoder Pointer to the encoder.
 * @return true on success, false on failure.
 */
bool simple8b_stream_encoder_flush(Simple8bStreamEncoder *encoder);

/**
 * @brief Finish encoding by flushing any remaining values.
 *
 * @param encoder Pointer to the encoder.
 * @return true on success, false on failure.
 */
bool simple8b_stream_encoder_finish(Simple8bStreamEncoder *encoder);

#endif // SIMPLE8B_STREAM_ENCODER_H
