#ifndef chimp_decoder_H
#define chimp_decoder_H

#include "gorilla/bit_reader.h"
#include "gorilla/float_stream_common.h"
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque type for the CHIMP float decoder.
typedef struct ChimpFloatDecoder ChimpFloatDecoder;

/**
 * Creates a new CHIMP float decoder.
 *
 * @param fill_cb The fill callback function used by the BitReader.
 * @param fill_ctx The context for the fill callback.
 * @return A pointer to a new ChimpFloatDecoder, or NULL on failure.
 */
ChimpFloatDecoder *chimp_decoder_create(FillCallback fill_cb, void *fill_ctx);

/**
 * Decodes the next double value from the compressed stream.
 *
 * This function reads from the underlying BitReader using the CHIMP algorithm.
 * For the first value, it reads a full 64-bit value; for subsequent values, it
 * reads a 2-bit control code followed by additional bits as required.
 *
 * @param dec Pointer to the decoder.
 * @param value Pointer where the decoded double will be stored.
 * @return true if a value was successfully decoded, false on error or
 * end-of-stream.
 */
bool chimp_decoder_get_value(ChimpFloatDecoder *dec, double *value);

/**
 * Finalizes the decoding process.
 *
 * @param dec Pointer to the decoder.
 * @return true on success.
 */
bool chimp_decoder_finish(ChimpFloatDecoder *dec);

/**
 * Destroys the CHIMP float decoder and frees any allocated memory.
 *
 * @param dec Pointer to the decoder to destroy.
 */
void chimp_decoder_destroy(ChimpFloatDecoder *dec);

#ifdef __cplusplus
}
#endif

#endif // chimp_decoder_H
