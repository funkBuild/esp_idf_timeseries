#ifndef CHIMP_FLOAT_ENCODER_H
#define CHIMP_FLOAT_ENCODER_H

#include "gorilla/bit_writer.h"
#include "gorilla/float_stream_common.h"
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque type for the CHIMP float encoder.
typedef struct ChimpFloatEncoder ChimpFloatEncoder;

/**
 * Creates a new CHIMP float encoder.
 *
 * @param flush_cb The flush callback function used by the BitWriter.
 * @param flush_ctx The context for the flush callback.
 * @return A pointer to a new ChimpFloatEncoder, or NULL on failure.
 */
ChimpFloatEncoder *chimp_float_encoder_create(FlushCallback flush_cb,
                                              void *flush_ctx);

/**
 * Adds a new double value to the CHIMP float encoder.
 *
 * The value is compressed using the CHIMP algorithm and written to the
 * underlying BitWriter.
 *
 * @param enc Pointer to the encoder.
 * @param value The double value to add.
 * @return true on success, false on failure.
 */
bool chimp_float_encoder_add_value(ChimpFloatEncoder *enc, double value);

/**
 * Finalizes the encoding process.
 *
 * According to the CHIMP algorithm, this function writes a sentinel NaN value,
 * an extra bit, and then flushes the BitWriter.
 *
 * @param enc Pointer to the encoder.
 * @return true on success, false on failure.
 */
bool chimp_float_encoder_finish(ChimpFloatEncoder *enc);

/**
 * Destroys the CHIMP float encoder and frees any allocated memory.
 *
 * @param enc Pointer to the encoder to destroy.
 */
void chimp_float_encoder_destroy(ChimpFloatEncoder *enc);

#ifdef __cplusplus
}
#endif

#endif // CHIMP_FLOAT_ENCODER_H
