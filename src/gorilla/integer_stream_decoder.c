#include "gorilla/integer_stream_decoder.h"
#include "gorilla/gorilla_stream_decoder.h" // Assumed to exist
#include "gorilla/gorilla_stream_types.h"   // For gorilla_decoder_fill_cb, etc.
#include "gorilla/simple8b_stream_decoder.h" // Assumed to exist
#include "gorilla/zigzag_stream.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Structure definition for the integer stream decoder.
   This structure is intentionally opaque in the header. */
struct IntegerStreamDecoder {
  bool has_first;
  bool has_second;
  uint64_t first_value;
  uint64_t last_value;
  int64_t last_delta;
  Simple8bStreamDecoder *simple8b_decoder;
};

/**
 * @brief Create a new IntegerStreamDecoder.
 *
 * The provided fill callback and its context are passed to the underlying
 * Simple8b stream decoder.
 *
 * @param fill_cb The callback function to fill the decoder.
 * @param fill_ctx The context pointer for the fill callback.
 *
 * @return Pointer to a newly allocated IntegerStreamDecoder on success,
 *         or NULL on error.
 */
IntegerStreamDecoder *
integer_stream_decoder_create(gorilla_decoder_fill_cb fill_cb, void *fill_ctx) {
  if (!fill_cb) {
    return NULL;
  }

  IntegerStreamDecoder *decoder = malloc(sizeof(IntegerStreamDecoder));
  if (!decoder)
    return NULL;
  memset(decoder, 0, sizeof(IntegerStreamDecoder));

  /* Create the underlying simple8b stream decoder using only the fill callback
     and its context. */
  decoder->simple8b_decoder = simple8b_stream_decoder_create(fill_cb, fill_ctx);
  if (!decoder->simple8b_decoder) {
    free(decoder);
    return NULL;
  }
  return decoder;
}

/**
 * @brief Destroy an IntegerStreamDecoder and free all resources.
 *
 * @param decoder The decoder instance to destroy.
 */
void integer_stream_decoder_destroy(IntegerStreamDecoder *decoder) {
  if (!decoder)
    return;
  if (decoder->simple8b_decoder)
    simple8b_stream_decoder_destroy(decoder->simple8b_decoder);
  free(decoder);
}

/**
 * @brief Read the next integer value from the stream.
 *
 * The decoding follows the inverse logic of the integer stream encoder:
 *
 * - If this is the first value, it is interpreted as the absolute value.
 * - If this is the second value, the zigzag‑decoded value is added to the first
 * value.
 * - For subsequent values, the zigzag‑decoded second‑order difference is
 * applied.
 *
 * @param decoder The IntegerStreamDecoder instance.
 * @param value Out parameter to store the decoded integer.
 *
 * @return true on successfully reading a value, false if no further data is
 * available or an error occurred.
 */
bool integer_stream_decoder_get_value(IntegerStreamDecoder *decoder,
                                      uint64_t *value) {
  if (!decoder || !value)
    return false;

  uint64_t encoded;
  // Read the next encoded value from the underlying Simple8b decoder.
  if (!simple8b_stream_decoder_get(decoder->simple8b_decoder, &encoded)) {
    printf("Failed to get value from simple8b decoder\n");
    return false;
  }

  if (!decoder->has_first) {
    // First value is stored absolutely.
    decoder->first_value = encoded;
    decoder->last_value = encoded;
    decoder->has_first = true;
    *value = encoded;
    return true;
  } else if (!decoder->has_second) {
    // Second value: decode the delta relative to the first.
    int64_t delta = zigzag_decode(encoded);
    uint64_t second = decoder->first_value + delta;
    decoder->last_value = second;
    decoder->last_delta = delta;
    decoder->has_second = true;
    *value = second;
    return true;
  } else {
    // Subsequent values: decode the second-order difference.
    int64_t D = zigzag_decode(encoded);
    decoder->last_delta += D;
    decoder->last_value += decoder->last_delta;
    *value = decoder->last_value;
    return true;
  }
}

/**
 * @brief Finalize the integer stream decoder.
 *
 * This function calls finish on the underlying Simple8b decoder to ensure that
 * all data has been consumed correctly.
 *
 * @param decoder The IntegerStreamDecoder instance.
 *
 * @return true on successful finalization, false on error.
 */
bool integer_stream_decoder_finish(IntegerStreamDecoder *decoder) {
  if (!decoder)
    return false;
  return simple8b_stream_decoder_finish(decoder->simple8b_decoder);
}
