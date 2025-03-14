#ifndef BOOLEAN_STREAM_ENCODER_H
#define BOOLEAN_STREAM_ENCODER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// FlushCallback: Function pointer for flushing buffered data.
typedef bool (*FlushCallback)(const uint8_t *data, size_t len, void *ctx);

// Opaque BooleanStreamEncoder type.
typedef struct BooleanStreamEncoder BooleanStreamEncoder;

// Create a BooleanStreamEncoder.
BooleanStreamEncoder *boolean_stream_encoder_create(FlushCallback flush_cb,
                                                    void *flush_ctx);

// Add a boolean value to the encoder.
bool boolean_stream_encoder_add_value(BooleanStreamEncoder *enc, bool value);

// Flush any remaining buffered data.
bool boolean_stream_encoder_finish(BooleanStreamEncoder *enc);

// Free the encoder.
void boolean_stream_encoder_destroy(BooleanStreamEncoder *enc);

#ifdef __cplusplus
}
#endif

#endif // BOOLEAN_STREAM_ENCODER_H
