#ifndef BIT_READER_H
#define BIT_READER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

// FillCallback: Called to fill the BitReader’s buffer.
typedef bool (*FillCallback)(void *ctx, uint8_t *buffer, size_t len,
                             size_t *filled);

// BitReader structure: manages bit-level input from a fill callback.
typedef struct {
  uint8_t buffer[64]; // Internal buffer for up to 64 bytes (512 bits)
  int buf_size;       // Number of bytes currently available in the buffer.
  int byte_index;     // Current index into the buffer.
  int bit_index;      // Bit offset in the current byte (0..7).
  FillCallback fill_cb;
  void *fill_ctx;
} BitReader;

// Initialize the BitReader.
void bitreader_init(BitReader *br, FillCallback fill_cb, void *fill_ctx);

// Returns the number of bits available in the BitReader.
static inline int bitreader_available(const BitReader *br) {
  return ((br->buf_size - br->byte_index) * 8) - br->bit_index;
}

// Refill the BitReader’s buffer.
// Moves any unread bytes to the beginning and then calls fill_cb
// to load additional data.
bool bitreader_refill(BitReader *br);

// Ensure that at least nbits are available in the BitReader.
bool bitreader_ensure(BitReader *br, int nbits);

// Read the next nbits (1 to 64) from the BitReader and store the result in
// *out. Bits are read in the order they were written (MSB first).
bool bitreader_read(BitReader *br, int nbits, uint64_t *out);

// Peek at the next nbits without consuming them.
bool bitreader_peek(BitReader *br, int nbits, uint64_t *out);

#ifdef __cplusplus
}
#endif

#endif /* BIT_READER_H */
