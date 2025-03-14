#ifndef FLOAT_STREAM_DECODER_H
#define FLOAT_STREAM_DECODER_H

#include "gorilla/bit_reader.h"
#include "gorilla/float_stream_common.h" // Defines FillCallback

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque type for the floating-point stream decoder */
typedef struct FloatStreamDecoder FloatStreamDecoder;

/**
 * @brief Create a new floating-point stream decoder.
 *
 * @param fill_cb   Callback to fill data for decoding.
 * @param fill_ctx  User-supplied context for the fill callback.
 *
 * @return Pointer to a new FloatStreamDecoder on success, or NULL on error.
 */
FloatStreamDecoder *float_stream_decoder_create(FillCallback fill_cb,
                                                void *fill_ctx);

/**
 * @brief Decode the next double value from the stream.
 *
 * @param dec    Pointer to the FloatStreamDecoder.
 * @param value  Out parameter to store the decoded double.
 *
 * @return true if a value was successfully decoded; false on error or
 * end-of-data.
 */
bool float_stream_decoder_get_value(FloatStreamDecoder *dec, double *value);

/**
 * @brief Finalize the decoding process.
 *
 * @param dec Pointer to the FloatStreamDecoder.
 *
 * @return true on success, false on error.
 */
bool float_stream_decoder_finish(FloatStreamDecoder *dec);

/**
 * @brief Destroy the floating-point stream decoder.
 *
 * Frees all associated resources.
 *
 * @param dec Pointer to the FloatStreamDecoder to destroy.
 */
void float_stream_decoder_destroy(FloatStreamDecoder *dec);

#ifdef __cplusplus
}
#endif

#endif /* FLOAT_STREAM_DECODER_H */
