#include "gorilla/boolean_stream_encoder.h"
#include "gorilla/bit_writer.h"
#include <stdlib.h>

struct BooleanStreamEncoder {
  BitWriter bw; // BitWriter handles bit-level buffering and flushing.
};

BooleanStreamEncoder *boolean_stream_encoder_create(FlushCallback flush_cb,
                                                    void *flush_ctx) {
  if (!flush_cb)
    return NULL;
  BooleanStreamEncoder *enc =
      (BooleanStreamEncoder *)malloc(sizeof(BooleanStreamEncoder));
  if (!enc)
    return NULL;
  bitwriter_init(&enc->bw, flush_cb, flush_ctx);
  return enc;
}

bool boolean_stream_encoder_add_value(BooleanStreamEncoder *enc, bool value) {
  if (!enc)
    return false;
  // Write a single bit (1 if true, 0 if false) using the BitWriter.
  return bitwriter_write(&enc->bw, value ? 1ULL : 0ULL, 1);
}

bool boolean_stream_encoder_finish(BooleanStreamEncoder *enc) {
  if (!enc)
    return false;
  // Flush any remaining bits (if the last byte is not fully filled, it is
  // padded with zeros).
  return bitwriter_flush(&enc->bw);
}

void boolean_stream_encoder_destroy(BooleanStreamEncoder *enc) {
  if (enc)
    free(enc);
}
