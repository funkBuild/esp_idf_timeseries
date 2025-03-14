#ifndef FLOAT_STREAM_ENCODER_H
#define FLOAT_STREAM_ENCODER_H

#include "gorilla/bit_writer.h"
#include "gorilla/float_stream_common.h" // Defines FlushCallback

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque type for the floating-point stream encoder */
typedef struct FloatStreamEncoder FloatStreamEncoder;

/**
 * @brief Create a new floating-point stream encoder.
 *
 * @param flush_cb   Callback to flush complete bytes of encoded data.
 * @param flush_ctx  User-supplied context for the flush callback.
 *
 * @return Pointer to a new FloatStreamEncoder on success, or NULL on error.
 */
FloatStreamEncoder *float_stream_encoder_create(FlushCallback flush_cb,
                                                void *flush_ctx);

/**
 * @brief Add a double value to the stream for compression.
 *
 * @param enc    Pointer to the FloatStreamEncoder.
 * @param value  The double value to encode.
 *
 * @return true on success, false on error.
 */
bool float_stream_encoder_add_value(FloatStreamEncoder *enc, double value);

/**
 * @brief Finalize the encoding process.
 *
 * Flushes any remaining bits.
 *
 * @param enc Pointer to the FloatStreamEncoder.
 *
 * @return true on success, false on error.
 */
bool float_stream_encoder_finish(FloatStreamEncoder *enc);

/**
 * @brief Destroy the floating-point stream encoder.
 *
 * Frees all associated resources.
 *
 * @param enc Pointer to the FloatStreamEncoder to destroy.
 */
void float_stream_encoder_destroy(FloatStreamEncoder *enc);

#ifdef __cplusplus
}
#endif

#endif /* FLOAT_STREAM_ENCODER_H */
