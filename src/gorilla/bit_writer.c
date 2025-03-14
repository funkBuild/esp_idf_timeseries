#include "gorilla/bit_writer.h"

void bitwriter_init(BitWriter *bw, FlushCallback flush_cb, void *flush_ctx) {
  bw->byte_count = 0;
  bw->bit_index = 0;
  bw->flush_cb = flush_cb;
  bw->flush_ctx = flush_ctx;
  // Initialize the first byte to 0.
  bw->buffer[0] = 0;
}

bool bitwriter_write(BitWriter *bw, uint64_t bits, int nbits) {
  // Ensure the number of bits is within valid range (0 to 64)
  if (nbits < 0 || nbits > 64) {
    return false;
  }

  while (nbits > 0) {
    // Determine the number of free bits in the current byte.
    int available = 8 - bw->bit_index;
    // Write as many bits as possible into the current byte.
    int to_write = (nbits < available) ? nbits : available;

    // Extract the top 'to_write' bits from the lower 'nbits' bits of 'bits'.
    // (bits >> (nbits - to_write)) isolates the correct chunk in the LSBs.
    uint8_t chunk =
        (uint8_t)((bits >> (nbits - to_write)) & ((1U << to_write) - 1));
    // Shift the chunk so it occupies the correct position in the current byte.
    chunk <<= (available - to_write);
    // Insert the chunk into the current byte.
    bw->buffer[bw->byte_count] |= chunk;

    // Update the bit index and decrease the remaining bit count.
    bw->bit_index += to_write;
    nbits -= to_write;

    // If the current byte is full, move to the next byte.
    if (bw->bit_index == 8) {
      bw->byte_count++;
      bw->bit_index = 0;
      // If not at the end of the buffer, clear the new byte.
      if (bw->byte_count < BITWRITER_BUFFER_SIZE) {
        bw->buffer[bw->byte_count] = 0;
      }
      // If the buffer is full, flush it using the provided callback.
      if (bw->byte_count == BITWRITER_BUFFER_SIZE) {
        if (!bw->flush_cb(bw->buffer, BITWRITER_BUFFER_SIZE, bw->flush_ctx))
          return false;
        // Reset the buffer after a successful flush.
        bw->byte_count = 0;
        bw->bit_index = 0;
        bw->buffer[0] = 0;
      }
    }
  }
  return true;
}

bool bitwriter_flush(BitWriter *bw) {
  // Determine the total number of bytes to flush (complete bytes plus a partial
  // one if present).
  int total_bytes = bw->byte_count;
  if (bw->bit_index > 0)
    total_bytes++;

  if (total_bytes > 0) {
    if (!bw->flush_cb(bw->buffer, total_bytes, bw->flush_ctx))
      return false;
  }
  // Reset the writer state.
  bw->byte_count = 0;
  bw->bit_index = 0;
  bw->buffer[0] = 0;
  return true;
}
