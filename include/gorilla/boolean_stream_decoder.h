#ifndef BOOLEAN_STREAM_DECODER_H
#define BOOLEAN_STREAM_DECODER_H

#include "gorilla/bit_reader.h"
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// BooleanStreamDecoder: encapsulates a BitReader for boolean decoding.
typedef struct {
  BitReader br;
} BooleanStreamDecoder;

// Creates a BooleanStreamDecoder using the provided fill callback.
BooleanStreamDecoder *boolean_stream_decoder_create(FillCallback fill_cb,
                                                    void *fill_ctx);

// Reads a single boolean value (1 bit) into *value.
// Returns true on success, false on failure.
bool boolean_stream_decoder_get_value(BooleanStreamDecoder *dec, bool *value);

// Finalizes the decoder (no extra cleanup is needed).
bool boolean_stream_decoder_finish(BooleanStreamDecoder *dec);

// Destroys the BooleanStreamDecoder and frees its memory.
void boolean_stream_decoder_destroy(BooleanStreamDecoder *dec);

#ifdef __cplusplus
}
#endif

#endif // BOOLEAN_STREAM_DECODER_H
