#ifndef CHIMP128_FLOAT_ENCODER_H
#define CHIMP128_FLOAT_ENCODER_H

#include "gorilla/bit_writer.h"
#include "gorilla/float_stream_common.h"
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque type for the CHIMP128 float encoder.
typedef struct Chimp128FloatEncoder Chimp128FloatEncoder;

/**
 * Creates a new CHIMP128 float encoder.
 *
 * @param flush_cb The flush callback function used by the BitWriter.
 * @param flush_ctx The context for the flush callback.
 * @param previous_values Number of previous values to use in compression (e.g.
 * 128).
 * @return A pointer to a new Chimp128FloatEncoder, or NULL on failure.
 */
Chimp128FloatEncoder *chimp128_float_encoder_create(FlushCallback flush_cb,
                                                    void *flush_ctx,
                                                    int previous_values);

/**
 * Adds a new double value to the CHIMP128 encoder.
 *
 * The double is converted to its 64-bit representation and compressed.
 *
 * @param enc Pointer to the encoder.
 * @param value The double value to add.
 * @return true on success, false on failure.
 */
bool chimp128_float_encoder_add_value(Chimp128FloatEncoder *enc, double value);

/**
 * Finalizes the encoding process.
 *
 * This function writes a sentinel NaN value, an extra 0 bit, and then flushes
 * the BitWriter.
 *
 * @param enc Pointer to the encoder.
 * @return true on success, false on failure.
 */
bool chimp128_float_encoder_finish(Chimp128FloatEncoder *enc);

/**
 * Destroys the CHIMP128 encoder and frees any allocated memory.
 *
 * @param enc Pointer to the encoder to destroy.
 */
void chimp128_float_encoder_destroy(Chimp128FloatEncoder *enc);

#ifdef __cplusplus
}
#endif

#endif // CHIMP128_FLOAT_ENCODER_H
