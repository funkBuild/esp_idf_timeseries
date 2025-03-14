#ifndef INTEGER_STREAM_ENCODER_H
#define INTEGER_STREAM_ENCODER_H

#include "gorilla/simple8b_stream_encoder.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/**
 * @brief Flush callback type.
 * The callback writes out `len` bytes from `data` using the user-supplied
 * context.
 */
typedef bool (*FlushCallback)(const uint8_t *data, size_t len, void *context);

/**
 * @brief IntegerStreamEncoder implements a streaming encoder for 64-bit
 * integers. It uses a Simple8bStreamEncoder to accumulate encoded values,
 * flushing every 64 bytes. The first value is stored as-is, the second as a
 * delta from the first, and subsequent values are encoded as second-order
 * differences (all using ZigZag encoding).
 */
typedef struct {
  FlushCallback flush_cb;
  void *flush_ctx;

  // State for delta encoding
  bool has_first;
  bool has_second;
  uint64_t first_value;
  uint64_t last_value;
  int64_t last_delta;

  // Underlying streaming Simple8b encoder.
  Simple8bStreamEncoder *simple8b_encoder;
} IntegerStreamEncoder;

/**
 * @brief Create a new IntegerStreamEncoder instance.
 *
 * @param flush_cb  The flush callback to write out encoded blocks.
 * @param flush_ctx User-supplied context for the flush callback.
 * @return Pointer to a new encoder instance, or NULL on failure.
 */
IntegerStreamEncoder *integer_stream_encoder_create(FlushCallback flush_cb,
                                                    void *flush_ctx);

/**
 * @brief Destroy an IntegerStreamEncoder instance.
 */
void integer_stream_encoder_destroy(IntegerStreamEncoder *encoder);

/**
 * @brief Add a new 64-bit integer value to the stream.
 *
 * @param encoder Pointer to the encoder.
 * @param value   The value to add.
 * @return true on success, false on failure.
 */
bool integer_stream_encoder_add_value(IntegerStreamEncoder *encoder,
                                      uint64_t value);

/**
 * @brief Flush any pending data from the encoder.
 *
 * @param encoder Pointer to the encoder.
 * @return true on success, false on failure.
 */
bool integer_stream_encoder_flush(IntegerStreamEncoder *encoder);

/**
 * @brief Finish encoding and flush any remaining data.
 *
 * @param encoder Pointer to the encoder.
 * @return true on success, false on failure.
 */
bool integer_stream_encoder_finish(IntegerStreamEncoder *encoder);

#endif // INTEGER_STREAM_ENCODER_H
