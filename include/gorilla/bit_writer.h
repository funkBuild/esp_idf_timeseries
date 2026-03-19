#ifndef BITWRITER_H
#define BITWRITER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "gorilla/gorilla_stream_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// Define the buffer size in bytes. 64 bytes reduces flush callback frequency.
#ifndef BITWRITER_BUFFER_SIZE
#define BITWRITER_BUFFER_SIZE 64
#endif

// BitWriter struct holds the buffer and state for writing bits.
// Uses a 64-bit accumulator for word-level bit packing (O(1) per write).
typedef struct {
  // Buffer to accumulate bytes (size defined by BITWRITER_BUFFER_SIZE)
  uint8_t buffer[BITWRITER_BUFFER_SIZE];
  // Number of complete bytes currently in the buffer.
  int byte_count;
  // 64-bit accumulator; valid bits occupy the MSB positions.
  // bits_in_acc tracks how many MSB bits are valid (0..63).
  uint64_t acc;
  int bits_in_acc;
  // Callback function to flush the buffer.
  FlushCallback flush_cb;
  // Context to pass to the flush callback.
  void *flush_ctx;
} BitWriter;

/* Hot path: drain complete bytes from the accumulator into the buffer.
 * Defined inline here for zero-call-overhead in the encoder hot loop. */
static inline bool bw_drain(BitWriter *bw) {
  while (bw->bits_in_acc >= 8) {
    if (bw->byte_count == BITWRITER_BUFFER_SIZE) {
      if (!bw->flush_cb(bw->buffer, BITWRITER_BUFFER_SIZE, bw->flush_ctx))
        return false;
      bw->byte_count = 0;
    }
    bw->buffer[bw->byte_count++] = (uint8_t)(bw->acc >> 56);
    bw->acc <<= 8;
    bw->bits_in_acc -= 8;
  }
  return true;
}

static inline bool bitwriter_write(BitWriter *bw, uint64_t bits, int nbits) {
  if (nbits == 0)
    return true;
  if (nbits < 0 || nbits > 64)
    return false;
  uint64_t value = (nbits == 64) ? bits : (bits & ((1ULL << nbits) - 1));
  int free_bits = 64 - bw->bits_in_acc;
  if (nbits <= free_bits) {
    bw->acc |= (value << (free_bits - nbits));
    bw->bits_in_acc += nbits;
  } else {
    bw->acc |= (value >> (nbits - free_bits));
    bw->bits_in_acc = 64;
    if (!bw_drain(bw))
      return false;
    int remaining = nbits - free_bits;
    bw->acc = (remaining < 64) ? (value << (64 - remaining)) : value;
    bw->bits_in_acc = remaining;
  }
  return bw_drain(bw);
}

// Initializes the BitWriter by setting counters to zero and storing the flush
// callback.
void bitwriter_init(BitWriter *bw, FlushCallback flush_cb, void *flush_ctx);

// Flushes any accumulated complete or partial bytes in the buffer.
// The partial byte is padded with zeros on the right.
// Returns true on success, false if the flush callback fails.
bool bitwriter_flush(BitWriter *bw);

#ifdef __cplusplus
}
#endif

#endif /* BITWRITER_H */
