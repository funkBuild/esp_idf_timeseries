// chimp128_float_decoder.c

#include "gorilla/bit_reader.h"
#include "gorilla/float_stream_common.h"
#include <math.h>
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

// CHIMP128 Float Decoder state.
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

  // CHIMP128 specific state.
  uint64_t *stored_values;  // Array of previous values.
  int previous_values;      // Number of previous values maintained.
  int previous_values_log2; // Computed as log2(previous_values).
  int initial_fill;         // Computed as previous_values_log2 + 9.
  int current; // Current index in the circular stored_values array.
} Chimp128FloatDecoder;

/**
 * Creates a new CHIMP128 float decoder.
 *
 * @param fill_cb The fill callback function used by the BitReader.
 * @param fill_ctx The context for the fill callback.
 * @param previous_values Number of previous values to use (e.g. 128).
 * @return A pointer to a new Chimp128FloatDecoder, or NULL on failure.
 */
Chimp128FloatDecoder *chimp128_float_decoder_create(FillCallback fill_cb,
                                                    void *fill_ctx,
                                                    int previous_values) {
  if (!fill_cb || previous_values <= 0)
    return NULL;
  Chimp128FloatDecoder *dec = malloc(sizeof(Chimp128FloatDecoder));
  if (!dec)
    return NULL;
  memset(dec, 0, sizeof(Chimp128FloatDecoder));
  dec->fill_cb = fill_cb;
  dec->fill_ctx = fill_ctx;
  bitreader_init(&dec->br, fill_cb, fill_ctx);

  dec->first = true;
  dec->end_of_stream = false;
  dec->stored_value = 0;
  dec->stored_leading_zeros = 0;
  dec->stored_trailing_zeros = 0;

  dec->previous_values = previous_values;
  dec->stored_values = malloc(previous_values * sizeof(uint64_t));
  if (!dec->stored_values) {
    free(dec);
    return NULL;
  }
  // Compute floor(log2(previous_values)).
  dec->previous_values_log2 = (int)(log(previous_values) / log(2));
  dec->initial_fill = dec->previous_values_log2 + 9;
  dec->current = 0;
  return dec;
}

/**
 * Internal helper that reads the next value from the stream
 * using the CHIMP128 algorithm.
 *
 * The function reads a 2-bit flag and then follows one of the four branches:
 *
 *   - Flag == 3 (binary 11): A new header is read (3 bits for leading zeros),
 *     then (64 - stored_leading_zeros) bits are read. The new value is obtained
 * by XORing the read bits with the previously stored value.
 *
 *   - Flag == 2 (binary 10): The stored header is reused; (64 -
 * stored_leading_zeros) bits are read and XORed with the stored value.
 *
 *   - Flag == 1 (binary 01): A composite header is read from an initial number
 * of bits. From this header an index (into the stored values array), a new
 * leading-zero count, and the number of significant bits are extracted. Then
 * the remaining bits are read, shifted by the computed trailing zeros, and
 * XORed with the value stored at the given index.
 *
 *   - Default (flag == 0, binary 00): An index is read (using
 * previous_values_log2 bits) and the stored value from that index is used as
 * the new value.
 *
 * If the decoded value equals NAN_LONG, the end-of-stream is flagged.
 */
static bool chimp128_next_value(Chimp128FloatDecoder *dec) {
  uint64_t flag;
  if (!bitreader_read(&dec->br, 2, &flag))
    return false;

  uint64_t value = 0;
  switch (flag) {
  case 3: {
    // New header: read 3 bits to determine stored_leading_zeros.
    uint64_t tmp;
    if (!bitreader_read(&dec->br, 3, &tmp))
      return false;
    if (tmp > 7)
      return false; // Should never happen with 3 bits.
    dec->stored_leading_zeros = leadingRepresentation[tmp];
    int bits_to_read = 64 - dec->stored_leading_zeros;
    if (!bitreader_read(&dec->br, bits_to_read, &value))
      return false;
    value = dec->stored_value ^ value;
    if (value == NAN_LONG) {
      dec->end_of_stream = true;
    } else {
      dec->stored_value = value;
      dec->current = (dec->current + 1) % dec->previous_values;
      dec->stored_values[dec->current] = dec->stored_value;
    }
    break;
  }
  case 2: {
    // Reuse stored header: read (64 - stored_leading_zeros) bits.
    int bits_to_read = 64 - dec->stored_leading_zeros;
    if (!bitreader_read(&dec->br, bits_to_read, &value))
      return false;
    value = dec->stored_value ^ value;
    if (value == NAN_LONG) {
      dec->end_of_stream = true;
    } else {
      dec->stored_value = value;
      dec->current = (dec->current + 1) % dec->previous_values;
      dec->stored_values[dec->current] = dec->stored_value;
    }
    break;
  }
  case 1: {
    // Composite header case.
    int fill = dec->initial_fill;
    uint64_t temp;
    if (!bitreader_read(&dec->br, fill, &temp))
      return false;
    fill -= dec->previous_values_log2;
    int index_val =
        (int)((temp >> fill) & ((1 << dec->previous_values_log2) - 1));
    fill -= 3;
    int lz_index = (int)((temp >> fill) & ((1 << 3) - 1));
    dec->stored_leading_zeros = leadingRepresentation[lz_index];
    fill -= 6;
    int significant_bits = (int)((temp >> fill) & ((1 << 6) - 1));
    dec->stored_value = dec->stored_values[index_val];
    if (significant_bits == 0)
      significant_bits = 64;
    dec->stored_trailing_zeros =
        64 - significant_bits - dec->stored_leading_zeros;
    int bits_to_read =
        64 - dec->stored_leading_zeros - dec->stored_trailing_zeros;
    uint64_t value_bits = 0;
    if (!bitreader_read(&dec->br, bits_to_read, &value_bits))
      return false;
    value_bits <<= dec->stored_trailing_zeros;
    value = dec->stored_value ^ value_bits;
    if (value == NAN_LONG) {
      dec->end_of_stream = true;
    } else {
      dec->stored_value = value;
      dec->current = (dec->current + 1) % dec->previous_values;
      dec->stored_values[dec->current] = dec->stored_value;
    }
    break;
  }
  default: {
    // Default case (flag == 0): read an index (previous_values_log2 bits)
    // and use the stored value from that index.
    uint64_t idx;
    if (!bitreader_read(&dec->br, dec->previous_values_log2, &idx))
      return false;
    int index_val = (int)idx;
    if (index_val < 0 || index_val >= dec->previous_values)
      return false;
    dec->stored_value = dec->stored_values[index_val];
    dec->current = (dec->current + 1) % dec->previous_values;
    dec->stored_values[dec->current] = dec->stored_value;
    break;
  }
  }
  return true;
}

/**
 * Decodes the next double value from the CHIMP128 compressed stream.
 *
 * For the first value, a full 64-bit value is read. For subsequent values, the
 * next value is decoded via the chimp128_next_value helper.
 *
 * @param dec Pointer to the decoder.
 * @param value Pointer where the decoded double will be stored.
 * @return true if a value was successfully decoded, false on error or
 * end-of-stream.
 */
bool chimp128_float_decoder_get_value(Chimp128FloatDecoder *dec,
                                      double *value) {
  if (!dec || !value)
    return false;
  if (dec->end_of_stream)
    return false;

  if (dec->first) {
    if (!bitreader_read(&dec->br, 64, &dec->stored_value))
      return false;
    dec->first = false;
    dec->stored_values[dec->current] = dec->stored_value;
    if (dec->stored_value == NAN_LONG) {
      dec->end_of_stream = true;
      return false;
    }
    *value = getDoubleRepresentation(dec->stored_value);
    return true;
  } else {
    if (!chimp128_next_value(dec))
      return false;
    if (dec->end_of_stream)
      return false;
    *value = getDoubleRepresentation(dec->stored_value);
    return true;
  }
}

/**
 * Finalizes the decoding process.
 *
 * This implementation does not require any special actions on finish.
 *
 * @param dec Pointer to the decoder.
 * @return true on success.
 */
bool chimp128_float_decoder_finish(Chimp128FloatDecoder *dec) { return true; }

/**
 * Destroys the CHIMP128 float decoder and frees any allocated memory.
 *
 * @param dec Pointer to the decoder to destroy.
 */
void chimp128_float_decoder_destroy(Chimp128FloatDecoder *dec) {
  if (!dec)
    return;
  if (dec->stored_values)
    free(dec->stored_values);
  free(dec);
}
