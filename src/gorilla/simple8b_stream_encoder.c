#include "gorilla/simple8b_stream_encoder.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
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

/* --- Helper Functions --- */

/**
 * @brief Check whether values[offset..offset+n-1] can be encoded with the given
 * bits.
 *
 * For selectors with bits==0 we assume that each value must be exactly 1.
 */
static bool can_pack(const uint64_t *values, size_t count, size_t offset,
                     uint64_t n, uint64_t bits) {
  if (count - offset < n)
    return false;
  if (bits == 0) {
    for (size_t i = offset; i < offset + n; i++) {
      if (values[i] != 1)
        return false;
    }
    return true;
  }
  uint64_t max = (1ULL << bits) - 1;
  for (size_t i = offset; i < offset + n; i++) {
    if (values[i] > max)
      return false;
  }
  return true;
}

/**
 * @brief Pack n values (starting at *offset) into a 64-bit word using the
 * specified bits and selector.
 *
 * The 4 most-significant bits store the selector.
 * If bits==0, no bits are stored for each value (they are implicitly 1).
 *
 * After packing, *offset is advanced by n.
 */
static uint64_t pack_values(const uint64_t *values, size_t *offset, uint64_t n,
                            uint64_t bits, uint8_t selector) {
  uint64_t out = selector; // lower 4 bits hold the selector value
  out <<= 60;              // move selector to the top 4 bits
  for (uint64_t i = 0; i < n; i++) {
    if (bits > 0) {
      out |= values[*offset + i] << (i * bits);
    }
    /* else: for bits==0, the value is implicitly 1 and no bits are stored */
  }
  *offset += n;
  return out;
}

/**
 * @brief Flush a prefix of the encoder’s stored values.
 *
 * The flush_all flag distinguishes between a final flush (flush_all == true),
 * where all values up to 'limit' are packed and flushed, and an incremental
 * flush (flush_all == false) where only one packable block is flushed to ensure
 * maximum packing is used without flushing more than necessary.
 *
 * After flushing, any remaining values are shifted to the beginning of the
 * encoder->values array.
 */
static bool flush_prefix(Simple8bStreamEncoder *encoder, size_t limit,
                         bool flush_all) {
  size_t offset = 0;
  if (!flush_all) {
    // Incremental flush: flush a single packable block.
    if (offset < limit) {
      bool packed = false;
      for (size_t i = 0; i < NUM_SELECTORS; i++) {
        const Simple8bSelector *sel = &selectors[i];
        if (can_pack(encoder->values, limit, offset, sel->n, sel->bits)) {
          uint64_t word = pack_values(encoder->values, &offset, sel->n,
                                      sel->bits, sel->selector);
          if (!encoder->flush_cb((const uint8_t *)&word, sizeof(word),
                                 encoder->flush_ctx))
            return false;
          packed = true;
          break;
        }
      }
      if (!packed) {
        const Simple8bSelector *sel =
            &selectors[15]; // fallback for single value
        uint64_t word = pack_values(encoder->values, &offset, sel->n, sel->bits,
                                    sel->selector);
        if (!encoder->flush_cb((const uint8_t *)&word, sizeof(word),
                               encoder->flush_ctx))
          return false;
      }
    }
  } else {
    // Final flush: pack and flush until offset reaches the limit.
    while (offset < limit) {
      bool packed = false;
      for (size_t i = 0; i < NUM_SELECTORS; i++) {
        const Simple8bSelector *sel = &selectors[i];
        if (can_pack(encoder->values, limit, offset, sel->n, sel->bits)) {
          uint64_t word = pack_values(encoder->values, &offset, sel->n,
                                      sel->bits, sel->selector);
          if (!encoder->flush_cb((const uint8_t *)&word, sizeof(word),
                                 encoder->flush_ctx))
            return false;
          packed = true;
          break;
        }
      }
      if (!packed) {
        const Simple8bSelector *sel =
            &selectors[15]; // fallback for single value
        uint64_t word = pack_values(encoder->values, &offset, sel->n, sel->bits,
                                    sel->selector);
        if (!encoder->flush_cb((const uint8_t *)&word, sizeof(word),
                               encoder->flush_ctx))
          return false;
      }
    }
  }
  // After flushing, shift any remaining values to the beginning.
  size_t flushed = offset;
  size_t remaining = encoder->count - flushed;
  if (remaining > 0) {
    memmove(encoder->values, encoder->values + flushed,
            remaining * sizeof(uint64_t));
  }
  encoder->count = remaining;
  return true;
}

/* --- Implementation of Public API --- */

Simple8bStreamEncoder *simple8b_stream_encoder_create(FlushCallback flush_cb,
                                                      void *flush_ctx) {
  if (!flush_cb)
    return NULL;
  Simple8bStreamEncoder *encoder =
      (Simple8bStreamEncoder *)malloc(sizeof(Simple8bStreamEncoder));
  if (!encoder)
    return NULL;
  encoder->flush_cb = flush_cb;
  encoder->flush_ctx = flush_ctx;
  encoder->capacity = 64; // initial capacity
  encoder->count = 0;
  encoder->values = (uint64_t *)malloc(encoder->capacity * sizeof(uint64_t));
  if (!encoder->values) {
    free(encoder);
    return NULL;
  }
  return encoder;
}

void simple8b_stream_encoder_destroy(Simple8bStreamEncoder *encoder) {
  if (!encoder)
    return;
  free(encoder->values);
  free(encoder);
}

/**
 * @brief Add a value to the encoder.
 *
 * The new value is appended into encoder->values. If that causes the total
 * count to equal a selector’s block size, the function uses can_pack to check
 * whether the block is packable. If not, an incremental flush (flushing only
 * one packable block) is performed so that the new value begins a fresh block.
 */
bool simple8b_stream_encoder_add(Simple8bStreamEncoder *encoder,
                                 uint64_t value) {
  if (!encoder)
    return false;

  /* Grow the dynamic array if necessary */
  if (encoder->count >= encoder->capacity) {
    size_t new_capacity = encoder->capacity * 2;
    uint64_t *new_values =
        (uint64_t *)realloc(encoder->values, new_capacity * sizeof(uint64_t));
    if (!new_values)
      return false;
    encoder->values = new_values;
    encoder->capacity = new_capacity;
  }

  /* Append the new value */
  encoder->values[encoder->count++] = value;

  /* Check if we've reached a block size for any selector */
  for (size_t i = 0; i < NUM_SELECTORS; i++) {
    const Simple8bSelector *sel = &selectors[i];
    if (encoder->count == sel->n) {
      /* If the complete block (including the new value) is not packable,
         perform an incremental flush so that the new value will start a new
         block. */
      if (!can_pack(encoder->values, encoder->count, 0, sel->n, sel->bits)) {
        size_t flush_count = encoder->count - 1;
        if (flush_count > 0) {
          if (!flush_prefix(encoder, flush_count, false))
            return false;
        }
      }
      break; // Only the first matching selector is considered.
    }
  }
  return true;
}

/**
 * @brief Flush all currently stored values.
 *
 * This function performs a final flush (flushing all values) by calling
 * flush_prefix with flush_all set to true.
 */
bool simple8b_stream_encoder_flush(Simple8bStreamEncoder *encoder) {
  if (!encoder)
    return false;

  if (encoder->count > 0) {
    if (!flush_prefix(encoder, encoder->count, true))
      return false;
  }
  return true;
}

bool simple8b_stream_encoder_finish(Simple8bStreamEncoder *encoder) {
  /* Flush any remaining values */
  return simple8b_stream_encoder_flush(encoder);
}
