#ifndef INTEGER_STREAM_DECODER_H
#define INTEGER_STREAM_DECODER_H

#include "gorilla/gorilla_stream_types.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Opaque type for the integer stream decoder */
typedef struct IntegerStreamDecoder IntegerStreamDecoder;

/**
 * @brief Create a new IntegerStreamDecoder.
 *
 * @param fill_cb The callback function to fill the decoder.
 * @param fill_ctx The context pointer for the fill callback.
 *
 * @return Pointer to a new IntegerStreamDecoder on success, or NULL on error.
 */
IntegerStreamDecoder *
integer_stream_decoder_create(gorilla_decoder_fill_cb fill_cb, void *fill_ctx);

/**
 * @brief Destroy the IntegerStreamDecoder and free resources.
 *
 * @param decoder The decoder instance to destroy.
 */
void integer_stream_decoder_destroy(IntegerStreamDecoder *decoder);

/**
 * @brief Decode and return the next integer value from the stream.
 *
 * @param decoder The IntegerStreamDecoder instance.
 * @param value Out parameter to store the decoded integer.
 *
 * @return true if a value was successfully decoded, false otherwise.
 */
bool integer_stream_decoder_get_value(IntegerStreamDecoder *decoder,
                                      uint64_t *value);

/**
 * @brief Finalize the decoding process.
 *
 * @param decoder The IntegerStreamDecoder instance.
 *
 * @return true if finalization succeeded, false otherwise.
 */
bool integer_stream_decoder_finish(IntegerStreamDecoder *decoder);

#endif /* INTEGER_STREAM_DECODER_H */
