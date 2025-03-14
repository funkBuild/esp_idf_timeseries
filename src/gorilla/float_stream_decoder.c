#include "gorilla/bit_reader.h"
#include "gorilla/float_stream_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// The streaming decoder state.
typedef struct {
  FillCallback fill_cb;
  void *fill_ctx;
  BitReader br; // Reader for input bits.

  // Decoding state.
  bool has_first;
  uint64_t last_value;
  int data_bits; // Number of bits for the XOR difference.
  int prev_lzb;  // Stored header: leading zeros.
  int prev_tzb;  // Stored header: trailing zeros (computed as 64 - prev_lzb -
                 // data_bits).
} FloatStreamDecoder;

FloatStreamDecoder *float_stream_decoder_create(FillCallback fill_cb,
                                                void *fill_ctx) {
  if (!fill_cb)
    return NULL;
  FloatStreamDecoder *dec = malloc(sizeof(FloatStreamDecoder));
  if (!dec)
    return NULL;
  memset(dec, 0, sizeof(FloatStreamDecoder));
  dec->fill_cb = fill_cb;
  dec->fill_ctx = fill_ctx;
  bitreader_init(&dec->br, fill_cb, fill_ctx);

  dec->has_first = false;
  dec->prev_lzb = -1;
  dec->prev_tzb = -1;
  dec->data_bits = 0;
  return dec;
}

bool float_stream_decoder_get_value(FloatStreamDecoder *dec, double *value) {
  if (!dec || !value)
    return false;

  // For the first value, read the full 64 bits.
  if (!dec->has_first) {
    if (!bitreader_read(&dec->br, 64, &dec->last_value))
      return false;
    *value = getDoubleRepresentation(dec->last_value);
    dec->has_first = true;
    return true;
  }

  // Read the first control bit.
  uint64_t flag;
  if (!bitreader_read(&dec->br, 1, &flag))
    return false;

  if (flag == 0) {
    // Control bit 0: XOR==0, no change.
    // Nothing further to read.
  } else {
    // Control bit 1: a second control bit follows.
    uint64_t second_bit;
    if (!bitreader_read(&dec->br, 1, &second_bit))
      return false;

    if (second_bit == 0) {
      // Control code "10": reuse previous header.
      // No new header is read; stored header remains.
    } else if (second_bit == 1) {
      // Control code "11": new header follows.
      uint64_t header;
      if (!bitreader_read(&dec->br, 11, &header))
        return false;
      // Header format: top 5 bits = new lzb, bottom 6 bits = new data_bits (0
      // means 64)
      dec->prev_lzb = (int)(header >> 6);
      uint64_t db = header & 0x3F;
      dec->data_bits = (db == 0) ? 64 : (int)db;
      dec->prev_tzb = 64 - dec->prev_lzb - dec->data_bits;
    } else {
      return false;
    }

    // Read the significant bits (using the stored data_bits).
    uint64_t xor_bits = 0;
    if (dec->data_bits > 0) {
      if (!bitreader_read(&dec->br, dec->data_bits, &xor_bits))
        return false;
    }
    // Reconstruct the new value: shift the read bits left by the stored
    // trailing zero count.
    dec->last_value ^= (xor_bits << dec->prev_tzb);
  }

  *value = getDoubleRepresentation(dec->last_value);

  return true;
}

bool float_stream_decoder_finish(FloatStreamDecoder *dec) { return true; }

void float_stream_decoder_destroy(FloatStreamDecoder *dec) {
  if (!dec)
    return;
  free(dec);
}
