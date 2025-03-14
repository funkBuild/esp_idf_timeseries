#include "gorilla/simple8b_stream_decoder.h"
#include "gorilla/gorilla_stream_decoder.h" // now defines gorilla_decoder_stream_t
#include "gorilla/gorilla_stream_types.h" // for gorilla_decoder_fill_cb, etc.

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h> // Optional: for debugging
#include <stdlib.h>
#include <string.h>

/* --- Internal Definitions --- */

/* A selector describes one Simple8b packing format */
typedef struct {
  uint8_t selector; // selector value (0..15)
  uint64_t n;       // number of values that can be packed with this selector
  uint64_t bits;    // number of bits per value (0 means a run of ones)
} Simple8bSelector;

/* The selectors in the order used by the algorithm (as in the C++ code) */
static const Simple8bSelector selectors[] = {
    {0, 240, 0}, {1, 120, 0}, {2, 60, 1},  {3, 30, 2}, {4, 20, 3},  {5, 15, 4},
    {6, 12, 5},  {7, 10, 6},  {8, 8, 7},   {9, 7, 8},  {10, 6, 10}, {11, 5, 12},
    {12, 4, 15}, {13, 3, 20}, {14, 2, 30}, {15, 1, 60}};
#define NUM_SELECTORS (sizeof(selectors) / sizeof(selectors[0]))

/* --- Simple8b Decoder Structure --- */

/*
  This version does not allocate a pending[] buffer.
  Instead, it stores the decoding state for the current 64-bit word.
*/
struct Simple8bStreamDecoder {
  gorilla_decoder_stream_t
      decoder; // Underlying decoder state (fill callback and context)
  // Decoding state for the current block:
  uint8_t current_selector; // The selector index (0..15)
  uint64_t current_n;       // Number of values in this block.
  uint64_t current_bits;    // Bits per value in this block.
  uint64_t current_payload; // The lower 60 bits of the word.
  size_t current_index;     // Next value index to decode (0-based).
};

/* --- Helper Functions for Bit Reading --- */

/*
 * read_word
 *
 * Reads a 64-bit word by invoking the fill callback.
 * A temporary 8-byte buffer is allocated on the stack and filled.
 */
static bool read_word(Simple8bStreamDecoder *decoder, uint64_t *word) {
  uint8_t buffer[8];
  size_t filled = 0;
  if (!decoder->decoder.fill_cb(decoder->decoder.fill_ctx, buffer,
                                sizeof(buffer), &filled)) {
    printf("Failed to fill buffer\n");
    return false;
  }
  if (filled < sizeof(buffer)) {
    printf("Failed to read full word\n");
    return false;
  }
  memcpy(word, buffer, sizeof(buffer));
  return true;
}

/*
 * load_block
 *
 * Given a 64-bit word, this function extracts the selector and payload and
 * initializes the decoder's state so that subsequent calls to get a value will
 * decode one value at a time.
 */
static bool load_block(Simple8bStreamDecoder *decoder, uint64_t word) {
  // Extract the top 4 bits as the selector index.
  uint8_t selector_index = word >> 60;
  if (selector_index >= NUM_SELECTORS) {
    return false; // Invalid selector.
  }
  const Simple8bSelector *sel = &selectors[selector_index];
  decoder->current_selector = selector_index;
  decoder->current_n = sel->n;
  decoder->current_bits = sel->bits;
  decoder->current_index = 0;
  // Extract the payload (lower 60 bits).
  decoder->current_payload = word & ((1ULL << 60) - 1);
  return true;
}

/* --- Implementation of Public API --- */

/*
 * simple8b_stream_decoder_create
 *
 * Allocates and initializes a new Simple8bStreamDecoder.
 * Note: the bit buffer parameters have been removed.
 */
Simple8bStreamDecoder *
simple8b_stream_decoder_create(gorilla_decoder_fill_cb fill_cb,
                               void *fill_ctx) {
  if (!fill_cb) {
    return NULL;
  }

  Simple8bStreamDecoder *decoder = malloc(sizeof(Simple8bStreamDecoder));
  if (!decoder)
    return NULL;
  memset(decoder, 0, sizeof(Simple8bStreamDecoder));

  /* Initialize the embedded gorilla_decoder_stream_t.
     We assume a stream type of GORILLA_STREAM_TYPE_SIMPLE8B.
     (This constant should be defined in gorilla_stream_types.h.) */
  if (!gorilla_decoder_init(&decoder->decoder, GORILLA_STREAM_TYPE_SIMPLE8B,
                            fill_cb, fill_ctx)) {
    free(decoder);
    return NULL;
  }
  decoder->current_n = 0; // No block loaded yet.
  decoder->current_index = 0;
  return decoder;
}

/*
 * simple8b_stream_decoder_destroy
 *
 * Releases all resources associated with the decoder.
 */
void simple8b_stream_decoder_destroy(Simple8bStreamDecoder *decoder) {
  if (!decoder)
    return;
  free(decoder);
}

/*
 * simple8b_stream_decoder_get
 *
 * Returns the next decoded value. This implementation decodes one value on
 * demand from the current block. When the current block is exhausted, it reads
 * a new 64-bit word and reloads the decoding state.
 */
bool simple8b_stream_decoder_get(Simple8bStreamDecoder *decoder,
                                 uint64_t *value) {
  if (!decoder || !value) {
    printf("Invalid decoder or value");
    return false;
  }

  // If no block is loaded or the current block is exhausted, load a new block.
  if (decoder->current_index >= decoder->current_n) {
    uint64_t word;
    if (!read_word(decoder, &word)) {
      printf("Failed to read word from decoder");
      return false;
    }
    if (!load_block(decoder, word)) {
      printf("Failed to read word from decoder");
      return false;
    }
  }

  // Decode the next value.
  if (decoder->current_bits == 0) {
    *value = 1;
  } else {
    uint64_t mask = (1ULL << decoder->current_bits) - 1;
    size_t shift = decoder->current_index * decoder->current_bits;
    *value = (decoder->current_payload >> shift) & mask;
  }
  decoder->current_index++;
  return true;
}

/*
 * simple8b_stream_decoder_finish
 *
 * Finalizes the decoding process.
 */
bool simple8b_stream_decoder_finish(Simple8bStreamDecoder *decoder) {
  if (!decoder)
    return false;
  return true;
}
