#ifndef CHIMP128_FLOAT_DECODER_H
#define CHIMP128_FLOAT_DECODER_H

#include "gorilla/bit_reader.h"
#include "gorilla/float_stream_common.h"
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque type for the CHIMP128 float decoder.
typedef struct Chimp128FloatDecoder Chimp128FloatDecoder;

/**
 * Creates a new CHIMP128 float decoder.
 *
 * @param fill_cb The fill callback function used by the BitReader.
 * @param fill_ctx The context for the fill callback.
 * @param previous_values The number of previous values to use (e.g. 128).
 * @return A pointer to a new Chimp128FloatDecoder, or NULL on failure.
 */
Chimp128FloatDecoder *chimp128_float_decoder_create(FillCallback fill_cb,
                                                    void *fill_ctx,
                                                    int previous_values);

/**
 * Decodes the next double value from the CHIMP128 compressed stream.
 *
 * For the first value, a full 64-bit value is read. For subsequent values,
 * the next value is decoded according to the CHIMP128 algorithm.
 *
 * @param dec Pointer to the decoder.
 * @param value Pointer where the decoded double will be stored.
 * @return true if a value was successfully decoded, false on error or if
 * end-of-stream is reached.
 */
bool chimp128_float_decoder_get_value(Chimp128FloatDecoder *dec, double *value);

/**
 * Finalizes the decoding process.
 *
 * This function does not require any special finalization steps.
 *
 * @param dec Pointer to the decoder.
 * @return true on success, false otherwise.
 */
bool chimp128_float_decoder_finish(Chimp128FloatDecoder *dec);

/**
 * Destroys the CHIMP128 float decoder and frees any allocated memory.
 *
 * @param dec Pointer to the decoder to destroy.
 */
void chimp128_float_decoder_destroy(Chimp128FloatDecoder *dec);

#ifdef __cplusplus
}
#endif

#endif // CHIMP128_FLOAT_DECODER_H
