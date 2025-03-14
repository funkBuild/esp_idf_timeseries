// chimp_float_encoder.c

#include "gorilla/bit_writer.h"
#include "gorilla/float_stream_common.h"
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define THRESHOLD 6

// For CHIMP, we replicate the Java arrays used for rounding the leading zeros
// and the representation for leading zeros.
static const unsigned short leadingRound[64] = {
    0,  0,  0,  0,  0,  0,  0,  0,  8,  8,  8,  8,  12, 12, 12, 12,
    16, 16, 18, 18, 20, 20, 22, 22, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24};

static const unsigned short leadingRepresentation[64] = {
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 5, 5,
    6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7};

// CHIMP Float Encoder state
typedef struct {
  FlushCallback flush_cb;
  void *flush_ctx;
  BitWriter bw; // The bit writer for output bits

  bool first;
  uint64_t stored_value;
  int stored_leading_zeros; // Java calls this 'storedLeadingZeros'; 65 is used
                            // as a sentinel
} ChimpFloatEncoder;

/**
 * Create a new CHIMP float encoder.
 */
ChimpFloatEncoder *chimp_float_encoder_create(FlushCallback flush_cb,
                                              void *flush_ctx) {
  if (!flush_cb)
    return NULL;

  ChimpFloatEncoder *enc =
      (ChimpFloatEncoder *)malloc(sizeof(ChimpFloatEncoder));
  if (!enc)
    return NULL;

  enc->flush_cb = flush_cb;
  enc->flush_ctx = flush_ctx;
  bitwriter_init(&enc->bw, flush_cb, flush_ctx);

  enc->first = true;
  enc->stored_value = 0;
  // We use '65' as a sentinel for "not set" or "very large".
  // The Java code uses Integer.MAX_VALUE, but 65 is enough as we handle up to
  // 64 bits only.
  enc->stored_leading_zeros = 65;

  return enc;
}

// Forward declaration for internal usage
static bool chimp_compress_value(ChimpFloatEncoder *enc, uint64_t value);

/**
 * Add a double value to the CHIMP float encoder.
 */
bool chimp_float_encoder_add_value(ChimpFloatEncoder *enc, double value) {
  if (!enc)
    return false;

  uint64_t as_bits = getUint64Representation(value);

  if (enc->first) {
    // Write the first value in full (64 bits)
    if (!bitwriter_write(&enc->bw, as_bits, 64)) {
      return false;
    }
    enc->stored_value = as_bits;
    enc->first = false;
    // According to the reference, we do not set stored_leading_zeros to 65
    // here explicitly. The logic only updates it after subsequent calls.
    return true;
  } else {
    return chimp_compress_value(enc, as_bits);
  }
}

/**
 * Internal helper that implements the CHIMP compression logic for each
 * non-first value.
 */
static bool chimp_compress_value(ChimpFloatEncoder *enc, uint64_t value) {
  uint64_t xor_val = enc->stored_value ^ value;

  if (xor_val == 0) {
    // "00" => same as old value
    // In Java: out.writeBit(false); out.writeBit(false);
    // This is 2 bits => 00
    if (!bitwriter_write(&enc->bw, 0b00, 2)) {
      return false;
    }
    // The reference sets storedLeadingZeros = 65
    enc->stored_leading_zeros = 65;
  } else {
    // Get the "rounded" leading zeros from table
    int normal_lz = getLeadingZeroBits(xor_val);
    int tz = getTrailingZeroBits(xor_val);

    // "leadingZeros" in CHIMP is actually the rounded version
    int lz = leadingRound[normal_lz];

    // Now we decide the control bits
    // 1) If trailingZeros > THRESHOLD => "01"
    if (tz > THRESHOLD) {
      // "01"
      if (!bitwriter_write(&enc->bw, 0b01, 2)) {
        return false;
      }

      int significant_bits = 64 - lz - tz;
      // Next 3 bits => leadingRepresentation[lz]
      if (!bitwriter_write(&enc->bw, leadingRepresentation[lz], 3)) {
        return false;
      }
      // Next 6 bits => significant_bits
      if (!bitwriter_write(&enc->bw, significant_bits, 6)) {
        return false;
      }
      // Then the meaningful bits: (xor_val >> tz) for 'significant_bits' bits
      if (!bitwriter_write(&enc->bw, (xor_val >> tz), significant_bits)) {
        return false;
      }

      // After using "01", storedLeadingZeros is set to 65 in the reference
      enc->stored_leading_zeros = 65;
    }
    // 2) else if lz == stored_leading_zeros => "10"
    else if (lz == enc->stored_leading_zeros) {
      // "10"
      if (!bitwriter_write(&enc->bw, 0b10, 2)) {
        return false;
      }
      // We write (64 - lz) bits of XOR
      int significant_bits = 64 - lz;
      if (!bitwriter_write(&enc->bw, xor_val, significant_bits)) {
        return false;
      }
    }
    // 3) else => "11"
    else {
      // "11"
      if (!bitwriter_write(&enc->bw, 0b11, 2)) {
        return false;
      }
      // Then write 3 bits => leadingRepresentation[lz]
      if (!bitwriter_write(&enc->bw, leadingRepresentation[lz], 3)) {
        return false;
      }
      // We write (64 - lz) bits of XOR
      int significant_bits = 64 - lz;
      if (!bitwriter_write(&enc->bw, xor_val, significant_bits)) {
        return false;
      }
      // Update storedLeadingZeros
      enc->stored_leading_zeros = lz;
    }
  }

  enc->stored_value = value;
  return true;
}

/**
 * Finish encoding.
 * Per the Java reference, we add a sentinel NaN, then write one more '0' bit,
 * then flush.
 */
bool chimp_float_encoder_finish(ChimpFloatEncoder *enc) {
  if (!enc)
    return false;

  // Add sentinel NaN
  chimp_float_encoder_add_value(enc, NAN);

  // Write one extra bit (false)
  if (!bitwriter_write(&enc->bw, 0, 1)) {
    return false;
  }

  // Flush any remaining bits
  return bitwriter_flush(&enc->bw);
}

/**
 * Destroy the encoder and free its memory.
 */
void chimp_float_encoder_destroy(ChimpFloatEncoder *enc) {
  if (!enc)
    return;
  free(enc);
}
