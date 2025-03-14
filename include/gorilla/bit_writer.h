#ifndef BITWRITER_H
#define BITWRITER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Define the buffer size in bytes. This can be changed as needed.
#ifndef BITWRITER_BUFFER_SIZE
#define BITWRITER_BUFFER_SIZE 16
#endif

// Callback type used to flush the contents of the BitWriter buffer.
// It should return true on success and false on failure.
typedef bool (*FlushCallback)(const uint8_t *data, size_t len, void *ctx);

// BitWriter struct holds the buffer and state for writing bits.
typedef struct {
  // Buffer to accumulate bytes (size defined by BITWRITER_BUFFER_SIZE)
  uint8_t buffer[BITWRITER_BUFFER_SIZE];
  // Number of complete bytes currently in the buffer.
  int byte_count;
  // Number of bits already written in the current (partial) byte (0 to 7).
  int bit_index;
  // Callback function to flush the buffer.
  FlushCallback flush_cb;
  // Context to pass to the flush callback.
  void *flush_ctx;
} BitWriter;

// Initializes the BitWriter by setting counters to zero and storing the flush
// callback.
void bitwriter_init(BitWriter *bw, FlushCallback flush_cb, void *flush_ctx);

// Writes 'nbits' (up to 64) from the lower bits of 'bits' into the BitWriter.
// Bits are written from most-significant to least-significant.
// Returns true on success, false if the flush callback fails.
bool bitwriter_write(BitWriter *bw, uint64_t bits, int nbits);

// Flushes any accumulated complete or partial bytes in the buffer.
// The partial byte is padded with zeros on the right.
// Returns true on success, false if the flush callback fails.
bool bitwriter_flush(BitWriter *bw);

#ifdef __cplusplus
}
#endif

#endif /* BITWRITER_H */
