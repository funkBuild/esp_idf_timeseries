#ifndef STRING_STREAM_DECODER_H
#define STRING_STREAM_DECODER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "gorilla/gorilla_stream_types.h"

/**
 * Opaque handle for the string stream decoder.
 */
typedef struct StringStreamDecoder StringStreamDecoder;

/**
 * @brief Create a new string stream decoder using zlib (inflate).
 *
 * @param fill_cb   Callback to pull compressed data from your storage.
 * @param fill_ctx  Context pointer passed to fill_cb.
 *
 * @return New StringStreamDecoder*, or NULL on error.
 */
StringStreamDecoder *string_stream_decoder_create(FillCallback fill_cb,
                                                  void *fill_ctx);

/**
 * @brief Destroy a string stream decoder.
 *
 * If not already finished, this will also end the zlib stream, freeing
 * resources.
 *
 * @param decoder Pointer to the decoder to destroy.
 */
void string_stream_decoder_destroy(StringStreamDecoder *decoder);

/**
 * @brief Read one string from the compressed stream.
 *
 * 1. Reads a 4-byte little-endian length prefix.
 * 2. Allocates a buffer of that length.
 * 3. Reads the string bytes into that buffer.
 * 4. Returns the pointer and length to the caller.
 *
 * The caller is responsible for `free()`ing `*out_data`.
 *
 * @param decoder       Pointer to the StringStreamDecoder.
 * @param out_data      Out-parameter for the newly allocated string bytes.
 * @param out_length    Out-parameter for the length in bytes.
 *
 * @return true if a string was successfully read, false if no data or an error.
 */
bool string_stream_decoder_get_value(StringStreamDecoder *decoder,
                                     uint8_t **out_data, size_t *out_length);

/**
 * @brief Mark the decoder as finished and verify the zlib stream is fully
 * consumed.
 *
 * If any compressed data remains unread, or if there's a zlib error, this may
 * fail.
 *
 * @param decoder Pointer to the StringStreamDecoder.
 * @return true on success, false on error.
 */
bool string_stream_decoder_finish(StringStreamDecoder *decoder);

#endif /* STRING_STREAM_DECODER_H */
