#include "gorilla/bit_reader.h"

void bitreader_init(BitReader *br, FillCallback fill_cb, void *fill_ctx) {
  br->buf_size = 0;
  br->byte_index = 0;
  br->bit_index = 0;
  br->fill_cb = fill_cb;
  br->fill_ctx = fill_ctx;
}

bool bitreader_refill(BitReader *br) {
  int unread = br->buf_size - br->byte_index;
  // Always move unread bytes down to index 0, whether bit_index is 0 or not
  if (unread > 0) {
    memmove(br->buffer, br->buffer + br->byte_index, unread);
  }

  br->buf_size = unread;
  br->byte_index = 0;
  // br->bit_index remains as-is because we might be in the middle of a byte

  int to_read = 64 - br->buf_size;
  size_t filled = 0;
  if (to_read > 0) {
    if (!br->fill_cb(br->fill_ctx, br->buffer + br->buf_size, to_read, &filled))
      return false;
    if (filled == 0) {
      // No more data available
      return false;
    }
    br->buf_size += (int)filled;
  }
  return true;
}

bool bitreader_ensure(BitReader *br, int nbits) {
  while (bitreader_available(br) < nbits) {
    if (!bitreader_refill(br))
      return false;
  }
  return true;
}

bool bitreader_read(BitReader *br, int nbits, uint64_t *out) {
  if (nbits <= 0 || nbits > 64)
    return false;
  if (!bitreader_ensure(br, nbits))
    return false;

  uint64_t result = 0;
  int bits_to_go = nbits;
  while (bits_to_go > 0) {
    uint8_t cur = br->buffer[br->byte_index];
    int available = 8 - br->bit_index;
    int take = (bits_to_go < available) ? bits_to_go : available;
    int shift = available - take;
    uint8_t part = (cur >> shift) & ((1U << take) - 1);
    result = (result << take) | part;
    bits_to_go -= take;
    br->bit_index += take;
    if (br->bit_index == 8) {
      br->bit_index = 0;
      br->byte_index++;
    }
  }
  *out = result;
  return true;
}

bool bitreader_peek(BitReader *br, int nbits, uint64_t *out) {
  BitReader copy = *br; // Copy current state.
  return bitreader_read(&copy, nbits, out);
}
