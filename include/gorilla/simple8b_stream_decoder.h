#ifndef SIMPLE8B_STREAM_DECODER_H
#define SIMPLE8B_STREAM_DECODER_H

#include "gorilla/gorilla_stream_decoder.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Opaque type for the simple8b stream decoder */
typedef struct Simple8bStreamDecoder Simple8bStreamDecoder;

/**
 * @brief Create a new Simple8bStreamDecoder.
 *
 * @param fill_cb  Callback function to fill the decoder with data.
 * @param fill_ctx User context for the fill callback.
 *
 * @return Pointer to a new Simple8bStreamDecoder on success, or NULL on error.
 */
Simple8bStreamDecoder *
simple8b_stream_decoder_create(gorilla_decoder_fill_cb fill_cb, void *fill_ctx);

/**
 * @brief Destroy the Simple8bStreamDecoder and free all associated resources.
 *
 * @param decoder The decoder instance to destroy.
 */
void simple8b_stream_decoder_destroy(Simple8bStreamDecoder *decoder);

/**
 * @brief Retrieve the next decoded value from the stream.
 *
 * @param decoder The Simple8bStreamDecoder instance.
 * @param value   Out parameter to store the next decoded value.
 *
 * @return true if a value was successfully decoded; false if no further data
 *         is available or an error occurred.
 */
bool simple8b_stream_decoder_get(Simple8bStreamDecoder *decoder,
                                 uint64_t *value);

/**
 * @brief Finalize the decoding process.
 *
 * This function may be used to verify that the stream was decoded completely.
 *
 * @param decoder The Simple8bStreamDecoder instance.
 *
 * @return true if finalization succeeded; false otherwise.
 */
bool simple8b_stream_decoder_finish(Simple8bStreamDecoder *decoder);

#endif /* SIMPLE8B_STREAM_DECODER_H */
