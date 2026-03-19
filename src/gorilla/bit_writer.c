#include "gorilla/bit_writer.h"

void bitwriter_init(BitWriter *bw, FlushCallback flush_cb, void *flush_ctx) {
  bw->byte_count = 0;
  bw->acc = 0;
  bw->bits_in_acc = 0;
  bw->flush_cb = flush_cb;
  bw->flush_ctx = flush_ctx;
}

bool bitwriter_flush(BitWriter *bw) {
  // First drain any complete bytes still in the accumulator
  if (!bw_drain(bw))
    return false;

  // Handle the remaining partial byte (0..7 bits)
  if (bw->bits_in_acc > 0) {
    // If the buffer is full, flush it before appending the partial byte
    if (bw->byte_count == BITWRITER_BUFFER_SIZE) {
      if (!bw->flush_cb(bw->buffer, BITWRITER_BUFFER_SIZE, bw->flush_ctx))
        return false;
      bw->byte_count = 0;
    }
    // Top bw->bits_in_acc bits of acc are valid; pad remaining bits with 0
    bw->buffer[bw->byte_count++] = (uint8_t)(bw->acc >> 56);
  }
  int total = bw->byte_count;
  if (total > 0) {
    if (!bw->flush_cb(bw->buffer, total, bw->flush_ctx))
      return false;
  }
  bw->byte_count = 0;
  bw->acc = 0;
  bw->bits_in_acc = 0;
  return true;
}
