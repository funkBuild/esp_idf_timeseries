#include "gorilla/bit_writer.h"
#include "gorilla/float_stream_common.h"
#include <stdio.h>
#include <stdlib.h>

// The streaming encoder state.
typedef struct {
  FlushCallback flush_cb;
  void *flush_ctx;
  BitWriter bw; // Writer for output bits.

  // Encoding state.
  bool has_first;
  uint64_t last_value;
  int prev_lzb;  // Previous leading zero bits (-1 initially).
  int prev_tzb;  // Previous trailing zero bits (-1 initially).
  int data_bits; // Number of bits for the XOR difference.
} FloatStreamEncoder;

FloatStreamEncoder *float_stream_encoder_create(FlushCallback flush_cb,
                                                void *flush_ctx) {
  if (!flush_cb)
    return NULL;
  FloatStreamEncoder *enc = malloc(sizeof(FloatStreamEncoder));
  if (!enc)
    return NULL;
  // Initialize flush callback and BitWriter.
  enc->flush_cb = flush_cb;
  enc->flush_ctx = flush_ctx;

  bitwriter_init(&enc->bw, flush_cb, flush_ctx);

  // Initialize encoder state.
  enc->has_first = false;
  enc->prev_lzb = -1;
  enc->prev_tzb = -1;
  enc->data_bits = 0;
  return enc;
}

bool float_stream_encoder_add_value(FloatStreamEncoder *enc, double value) {
  if (!enc)
    return false;

  uint64_t current_value = getUint64Representation(value);

  if (!enc->has_first) {
    // Write the first value in full (64 bits).
    if (!bitwriter_write(&enc->bw, current_value, 64))
      return false;
    enc->last_value = current_value;
    enc->has_first = true;
    return true;
  }

  uint64_t xor_value = current_value ^ enc->last_value;

  if (xor_value == 0) {
    // Write a 0 bit.
    if (!bitwriter_write(&enc->bw, 0, 1))
      return false;

  } else {
    int lzb = getLeadingZeroBits(xor_value);
    int tzb = getTrailingZeroBits(xor_value);

    // Use previous header if possible.
    if (enc->data_bits != 0 && enc->prev_lzb <= lzb && enc->prev_tzb <= tzb) {

      if (!bitwriter_write(&enc->bw, 0b10, 2))
        return false;

    } else {
      if (lzb > 31)
        lzb = 31;

      enc->data_bits = 64 - lzb - tzb;

      if (!bitwriter_write(&enc->bw, 0b11, 2)) {
        return false;
      }

      uint64_t header = (((uint64_t)lzb) << 6) |
                        (((enc->data_bits != 64) ? enc->data_bits : 0) & 0x3F);

      if (!bitwriter_write(&enc->bw, header, 11))
        return false;

      enc->prev_lzb = lzb;
      enc->prev_tzb = tzb;
    }

    // Write the significant bits: (xor_value >> prev_tzb) using data_bits.
    uint64_t significant_bits = xor_value >> enc->prev_tzb;
    if (!bitwriter_write(&enc->bw, significant_bits, enc->data_bits))
      return false;
  }

  enc->last_value = current_value;

  return true;
}

bool float_stream_encoder_finish(FloatStreamEncoder *enc) {
  if (!enc)
    return false;
  return bitwriter_flush(&enc->bw);
}

void float_stream_encoder_destroy(FloatStreamEncoder *enc) {
  if (!enc)
    return;
  free(enc);
}
