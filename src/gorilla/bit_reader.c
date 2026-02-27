#include "gorilla/bit_reader.h"
#include <string.h>

void bitreader_init(BitReader *br, FillCallback fill_cb, void *fill_ctx) {
  br->window = 0;
  br->window_bits = 0;
  br->buf_size = 0;
  br->buf_byte = 0;
  br->fill_cb = fill_cb;
  br->fill_ctx = fill_ctx;
}

bool bitreader_refill(BitReader *br) {
  int unread = br->buf_size - br->buf_byte;
  if (unread > 0) {
    memmove(br->buffer, br->buffer + br->buf_byte, unread);
  }
  br->buf_size = unread;
  br->buf_byte = 0;

  size_t filled = 0;
  int to_read = 64 - br->buf_size;
  if (to_read > 0) {
    if (!br->fill_cb(br->fill_ctx, br->buffer + br->buf_size, to_read,
                     &filled))
      return filled > 0;
    br->buf_size += (int)filled;
  }
  return br->buf_size > br->buf_byte;
}

bool bitreader_ensure(BitReader *br, int nbits) {
  return br_fill_window(br, nbits);
}

bool bitreader_peek(BitReader *br, int nbits, uint64_t *out) {
  BitReader copy = *br;
  return bitreader_read(&copy, nbits, out);
}
