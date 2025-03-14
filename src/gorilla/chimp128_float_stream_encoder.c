// chimp128_float_encoder.c

#include "gorilla/bit_writer.h"
#include "gorilla/float_stream_common.h"
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

// Use the standard NAN from math.h for the sentinel.
#ifndef NAN
#define NAN (0.0 / 0.0)
#endif

// CHIMP128 uses two lookup tables that are identical in length (64 entries)
// to the Java version.
static const unsigned short chimp128_leadingRound[64] = {
    0,  0,  0,  0,  0,  0,  0,  0,  8,  8,  8,  8,  12, 12, 12, 12,
    16, 16, 18, 18, 20, 20, 22, 22, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24};

static const unsigned short chimp128_leadingRepresentation[64] = {
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 5, 5,
    6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7};

// CHIMP128 encoder state structure.
typedef struct {
  FlushCallback flush_cb;
  void *flush_ctx;
  BitWriter bw; // Underlying bit writer.

  bool first;
  uint64_t
      *stored_values;  // Array of previous values (length = previous_values)
  int previous_values; // How many previous values to remember
  int previous_values_log2; // floor(log2(previous_values))
  int threshold;            // Computed as 6 + previous_values_log2
  int set_lsb;              // (1 << (threshold + 1)) - 1
  int *indices; // Array of size (1 << (threshold + 1)); maps key -> last index
  int index;    // Total count of values processed
  int current;  // Circular index into stored_values
  int flag_one_size;        // previous_values_log2 + 11
  int flag_zero_size;       // previous_values_log2 + 2
  int stored_leading_zeros; // Sentinel value; 65 means “reset”
  int size;                 // Total number of bits written (for statistics)
} Chimp128FloatEncoder;

/**
 * Creates a new CHIMP128 float encoder.
 *
 * @param flush_cb The flush callback for the BitWriter.
 * @param flush_ctx The flush callback context.
 * @param previous_values Number of previous values to use (e.g. 128 for
 * CHIMP128).
 * @return A pointer to a new Chimp128FloatEncoder, or NULL on error.
 */
Chimp128FloatEncoder *chimp128_float_encoder_create(FlushCallback flush_cb,
                                                    void *flush_ctx,
                                                    int previous_values) {
  if (!flush_cb || previous_values <= 0)
    return NULL;

  Chimp128FloatEncoder *enc = malloc(sizeof(Chimp128FloatEncoder));
  if (!enc)
    return NULL;

  enc->flush_cb = flush_cb;
  enc->flush_ctx = flush_ctx;
  bitwriter_init(&enc->bw, flush_cb, flush_ctx);

  enc->first = true;
  enc->previous_values = previous_values;
  enc->stored_values = malloc(previous_values * sizeof(uint64_t));
  if (!enc->stored_values) {
    free(enc);
    return NULL;
  }
  enc->previous_values_log2 = (int)(log(previous_values) / log(2));
  enc->threshold = 6 + enc->previous_values_log2;
  enc->set_lsb = (1 << (enc->threshold + 1)) - 1;
  int indices_size = 1 << (enc->threshold + 1);
  enc->indices = malloc(indices_size * sizeof(int));
  if (!enc->indices) {
    free(enc->stored_values);
    free(enc);
    return NULL;
  }
  // Initialize indices to zero.
  for (int i = 0; i < indices_size; i++) {
    enc->indices[i] = 0;
  }
  enc->index = 0;
  enc->current = 0;
  enc->flag_zero_size = enc->previous_values_log2 + 2;
  enc->flag_one_size = enc->previous_values_log2 + 11;
  enc->stored_leading_zeros = 65; // Sentinel value.
  enc->size = 0;
  return enc;
}

/**
 * Internal helper to write the first value.
 */
static bool chimp128_write_first(Chimp128FloatEncoder *enc, uint64_t value) {
  enc->first = false;
  enc->stored_values[enc->current] = value;
  if (!bitwriter_write(&enc->bw, value, 64))
    return false;
  enc->size += 64;
  int key = (int)(value & enc->set_lsb);
  enc->indices[key] = enc->index;
  return true;
}

/**
 * Internal helper that implements CHIMP128 compression for a new value.
 */
static bool chimp128_compress_value(Chimp128FloatEncoder *enc, uint64_t value) {
  int key = (int)(value & enc->set_lsb);
  uint64_t xor_val;
  int previousIndex;
  int trailingZeros = 0;
  int currIndex = enc->indices[key];

  if ((enc->index - currIndex) < enc->previous_values) {
    uint64_t tempXor =
        value ^ enc->stored_values[currIndex % enc->previous_values];
    trailingZeros = getTrailingZeroBits(tempXor);
    if (trailingZeros > enc->threshold) {
      previousIndex = currIndex % enc->previous_values;
      xor_val = tempXor;
    } else {
      previousIndex = enc->index % enc->previous_values;
      xor_val = enc->stored_values[previousIndex] ^ value;
    }
  } else {
    previousIndex = enc->index % enc->previous_values;
    xor_val = enc->stored_values[previousIndex] ^ value;
  }

  if (xor_val == 0) {
    // Write the previousIndex using flag_zero_size bits.
    if (!bitwriter_write(&enc->bw, (uint64_t)previousIndex,
                         enc->flag_zero_size))
      return false;
    enc->size += enc->flag_zero_size;
    enc->stored_leading_zeros = 65;
  } else {
    int normal_lz = getLeadingZeroBits(xor_val);
    int leadingZeros = chimp128_leadingRound[normal_lz];
    if (trailingZeros > enc->threshold) {
      int significant_bits = 64 - leadingZeros - trailingZeros;
      // Compute a compound header value:
      // 512 * (previous_values + previousIndex) + 64 * leadingRepresentation +
      // significant_bits.
      int header = 512 * (enc->previous_values + previousIndex) +
                   64 * chimp128_leadingRepresentation[leadingZeros] +
                   significant_bits;
      if (!bitwriter_write(&enc->bw, (uint64_t)header, enc->flag_one_size))
        return false;
      if (!bitwriter_write(&enc->bw, xor_val >> trailingZeros,
                           significant_bits))
        return false;
      enc->size += enc->flag_one_size + significant_bits;
      enc->stored_leading_zeros = 65;
    } else if (leadingZeros == enc->stored_leading_zeros) {
      // Write control value "2" in 2 bits.
      if (!bitwriter_write(&enc->bw, 2, 2))
        return false;
      int significant_bits = 64 - leadingZeros;
      if (!bitwriter_write(&enc->bw, xor_val, significant_bits))
        return false;
      enc->size += 2 + significant_bits;
    } else {
      enc->stored_leading_zeros = leadingZeros;
      int significant_bits = 64 - leadingZeros;
      // Write control value: 24 + leadingRepresentation in 5 bits.
      if (!bitwriter_write(
              &enc->bw, 24 + chimp128_leadingRepresentation[leadingZeros], 5))
        return false;
      if (!bitwriter_write(&enc->bw, xor_val, significant_bits))
        return false;
      enc->size += 5 + significant_bits;
    }
  }
  enc->current = (enc->current + 1) % enc->previous_values;
  enc->stored_values[enc->current] = value;
  enc->index++;
  enc->indices[key] = enc->index;
  return true;
}

/**
 * Adds a new double value to the CHIMP128 encoder.
 *
 * The double is converted to its 64-bit representation and compressed.
 */
bool chimp128_float_encoder_add_value(Chimp128FloatEncoder *enc, double value) {
  if (!enc)
    return false;
  uint64_t as_bits = getUint64Representation(value);
  if (enc->first) {
    return chimp128_write_first(enc, as_bits);
  } else {
    return chimp128_compress_value(enc, as_bits);
  }
}

/**
 * Finishes the encoding process.
 *
 * According to the reference, a sentinel NaN is added followed by one extra 0
 * bit, and then the BitWriter is flushed.
 */
bool chimp128_float_encoder_finish(Chimp128FloatEncoder *enc) {
  if (!enc)
    return false;
  // Add sentinel NaN.
  if (!chimp128_float_encoder_add_value(enc, NAN))
    return false;
  // Write one extra bit (false).
  if (!bitwriter_write(&enc->bw, 0, 1))
    return false;
  return bitwriter_flush(&enc->bw);
}

/**
 * Destroys the CHIMP128 encoder and frees all allocated memory.
 */
void chimp128_float_encoder_destroy(Chimp128FloatEncoder *enc) {
  if (!enc)
    return;
  if (enc->stored_values)
    free(enc->stored_values);
  if (enc->indices)
    free(enc->indices);
  free(enc);
}
