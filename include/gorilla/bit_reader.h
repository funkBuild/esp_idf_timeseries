#ifndef BIT_READER_H
#define BIT_READER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

// FillCallback: Called to fill the BitReader's buffer.
typedef bool (*FillCallback)(void *ctx, uint8_t *buffer, size_t len,
                             size_t *filled);

// BitReader structure: manages bit-level input using a 64-bit window register.
// The window holds the next bits to be read in its MSB positions.
typedef struct {
  uint64_t window;    // bit window, MSB-first: next bit to read = bit 63
  int window_bits;    // number of valid bits in window (in MSB positions)
  uint8_t buffer[64]; // backing buffer from fill_cb
  int buf_size;       // bytes available in buffer
  int buf_byte;       // current read position in buffer
  FillCallback fill_cb;
  void *fill_ctx;
  // Direct pointer mode (zero-copy when data is already in RAM)
  const uint8_t *direct_data;
  size_t direct_size;
  size_t direct_offset;
} BitReader;

// Initialize the BitReader with a fill callback.
void bitreader_init(BitReader *br, FillCallback fill_cb, void *fill_ctx);

// Initialize the BitReader in direct pointer mode (zero-copy).
void bitreader_init_direct(BitReader *br, const uint8_t *data, size_t size);

// Returns the number of bits available in the BitReader (window + buffer).
static inline int bitreader_available(const BitReader *br) {
  return br->window_bits + (br->buf_size - br->buf_byte) * 8;
}

static inline bool br_next_byte(BitReader *br, uint8_t *b) {
  if (br->direct_data) {
    if (br->direct_offset >= br->direct_size)
      return false;
    *b = br->direct_data[br->direct_offset++];
    return true;
  }
  if (br->buf_byte >= br->buf_size) {
    size_t filled = 0;
    if (!br->fill_cb(br->fill_ctx, br->buffer, 64, &filled))
      return false;
    if (filled == 0)
      return false;
    br->buf_size = (int)filled;
    br->buf_byte = 0;
  }
  *b = br->buffer[br->buf_byte++];
  return true;
}

static inline bool br_load_byte(BitReader *br) {
  uint8_t b;
  if (!br_next_byte(br, &b))
    return false;
  br->window |= ((uint64_t)b << (56 - br->window_bits));
  br->window_bits += 8;
  return true;
}

static inline bool br_fill_window(BitReader *br, int nbits) {
  while (br->window_bits < nbits && br->window_bits <= 56) {
    if (!br_load_byte(br))
      return br->window_bits >= nbits;
  }
  return br->window_bits >= nbits;
}

static inline bool bitreader_read(BitReader *br, int nbits, uint64_t *out) {
  if (nbits <= 0 || nbits > 64)
    return false;
  if (!br_fill_window(br, nbits)) {
    int have = br->window_bits;
    int need = nbits - have;
    if (have <= 0)
      return false;
    uint64_t high_part = br->window >> (64 - have);
    br->window = 0;
    br->window_bits = 0;
    if (!br_load_byte(br))
      return false;
    uint64_t low_part = br->window >> (64 - need);
    br->window <<= need;
    br->window_bits -= need;
    *out = (high_part << need) | low_part;
    return true;
  }
  if (nbits == 64) {
    *out = br->window;
    br->window = 0;
  } else {
    *out = br->window >> (64 - nbits);
    br->window <<= nbits;
  }
  br->window_bits -= nbits;
  return true;
}

// Refill the BitReader's buffer from the fill callback.
bool bitreader_refill(BitReader *br);

// Ensure that at least nbits are available in the BitReader.
bool bitreader_ensure(BitReader *br, int nbits);

// Peek at the next nbits without consuming them.
bool bitreader_peek(BitReader *br, int nbits, uint64_t *out);

#ifdef __cplusplus
}
#endif

#endif /* BIT_READER_H */
