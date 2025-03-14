// chimp_float_decoder.c

#include "gorilla/bit_reader.h"
#include "gorilla/float_stream_common.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Sentinel value (NaN) used by CHIMP.
#define NAN_LONG 0x7ff8000000000000ULL

// Lookup table for converting a 3-bit index to a rounded leading-zero count.
static const unsigned short leadingRepresentation[8] = {0,  8,  12, 16,
                                                        18, 20, 22, 24};

// CHIMP float decoder state.
typedef struct {
  FillCallback fill_cb;
  void *fill_ctx;
  BitReader br; // Reader for input bits.

  // Decoding state.
  bool first;
  bool end_of_stream;
  uint64_t stored_value;
  int stored_leading_zeros;
  int stored_trailing_zeros;
} ChimpFloatDecoder;

/**
 * Creates a new CHIMP float decoder.
 *
 * @param fill_cb The fill callback function used by the BitReader.
 * @param fill_ctx The context for the fill callback.
 * @return A pointer to a new ChimpFloatDecoder, or NULL on failure.
 */
ChimpFloatDecoder *chimp_decoder_create(FillCallback fill_cb, void *fill_ctx) {
  if (!fill_cb)
    return NULL;
  ChimpFloatDecoder *dec = malloc(sizeof(ChimpFloatDecoder));
  if (!dec)
    return NULL;
  memset(dec, 0, sizeof(ChimpFloatDecoder));
  dec->fill_cb = fill_cb;
  dec->fill_ctx = fill_ctx;
  bitreader_init(&dec->br, fill_cb, fill_ctx);
  dec->first = true;
  dec->end_of_stream = false;
  dec->stored_value = 0;
  dec->stored_leading_zeros = 0;
  dec->stored_trailing_zeros = 0;
  return dec;
}

/**
 * Decodes the next double value from the compressed stream.
 *
 * For the first value the full 64 bits are read. For subsequent values a 2-bit
 * control code is read to determine the decompression strategy:
 *
 *  - 00: Value unchanged.
 *  - 01: New header is read—3 bits for a lookup of the rounded leading zeros,
 *         6 bits for the number of significant bits (0 means 64), then the next
 *         bits are read, shifted by the computed trailing zero count, and XORed
 *         with the stored value.
 *  - 10: Reuse the stored header and read (64 - stored_leading_zeros) bits
 * which are XORed with the stored value.
 *  - 11: New header is read—3 bits for a lookup of the rounded leading zeros
 * and then (64 - stored_leading_zeros) bits are read and XORed with the stored
 *         value.
 *
 * If the decoded value equals the sentinel (NaN), end-of-stream is reached.
 *
 * @param dec Pointer to the decoder.
 * @param value Pointer where the decoded double will be stored.
 * @return true on successfully decoding a value, false on error or
 * end-of-stream.
 */
bool chimp_decoder_get_value(ChimpFloatDecoder *dec, double *value) {
  if (!dec || !value)
    return false;

  // If the stream has ended, signal no more values.
  if (dec->end_of_stream)
    return false;

  // For the first value, read full 64 bits.
  if (dec->first) {
    if (!bitreader_read(&dec->br, 64, &dec->stored_value))
      return false;
    dec->first = false;
    if (dec->stored_value == NAN_LONG) {
      dec->end_of_stream = true;
      return false;
    }
    *value = getDoubleRepresentation(dec->stored_value);
    return true;
  }

  uint64_t flag;
  if (!bitreader_read(&dec->br, 2, &flag))
    return false;

  switch (flag) {
  case 0:
    // Control code "00": No change. stored_value remains unchanged.
    break;
  case 1: {
    // Control code "01": New header with explicit significant bits.
    uint64_t tmp;
    // Read 3 bits to update stored_leading_zeros from the lookup table.
    if (!bitreader_read(&dec->br, 3, &tmp))
      return false;
    if (tmp > 7)
      return false; // Invalid header.
    dec->stored_leading_zeros = leadingRepresentation[tmp];
    uint64_t bits;
    // Read 6 bits for the significant bits count.
    if (!bitreader_read(&dec->br, 6, &bits))
      return false;
    int significant_bits = (int)bits;
    if (significant_bits == 0)
      significant_bits = 64;
    dec->stored_trailing_zeros =
        64 - significant_bits - dec->stored_leading_zeros;
    int to_read = 64 - dec->stored_leading_zeros - dec->stored_trailing_zeros;
    uint64_t value_bits = 0;
    if (to_read > 0) {
      if (!bitreader_read(&dec->br, to_read, &value_bits))
        return false;
    }
    // Shift the bits left by the stored trailing zero count and XOR.
    value_bits <<= dec->stored_trailing_zeros;
    dec->stored_value ^= value_bits;
    break;
  }
  case 2: {
    // Control code "10": Reuse stored_leading_zeros.
    int significant_bits = 64 - dec->stored_leading_zeros;
    if (significant_bits == 0)
      significant_bits = 64;
    uint64_t value_bits = 0;
    if (significant_bits > 0) {
      if (!bitreader_read(&dec->br, significant_bits, &value_bits))
        return false;
    }
    dec->stored_value ^= value_bits;
    break;
  }
  case 3: {
    // Control code "11": New header where significant bits span (64 -
    // stored_leading_zeros).
    uint64_t tmp;
    if (!bitreader_read(&dec->br, 3, &tmp))
      return false;
    if (tmp > 7)
      return false;
    dec->stored_leading_zeros = leadingRepresentation[tmp];
    int significant_bits = 64 - dec->stored_leading_zeros;
    if (significant_bits == 0)
      significant_bits = 64;
    uint64_t value_bits = 0;
    if (significant_bits > 0) {
      if (!bitreader_read(&dec->br, significant_bits, &value_bits))
        return false;
    }
    dec->stored_value ^= value_bits;
    break;
  }
  default:
    return false;
  }

  // If the decoded value equals the sentinel, mark end-of-stream.
  if (dec->stored_value == NAN_LONG) {
    dec->end_of_stream = true;
    return false;
  }

  *value = getDoubleRepresentation(dec->stored_value);
  return true;
}

/**
 * Finalizes the decoding process.
 *
 * This implementation does not require special actions on finish.
 *
 * @param dec Pointer to the decoder.
 * @return true on success.
 */
bool chimp_decoder_finish(ChimpFloatDecoder *dec) { return true; }

/**
 * Destroys the CHIMP float decoder and frees any allocated memory.
 *
 * @param dec Pointer to the decoder to destroy.
 */
void chimp_decoder_destroy(ChimpFloatDecoder *dec) {
  if (!dec)
    return;
  free(dec);
}
