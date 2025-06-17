#ifndef STRING_STREAM_ENCODER_H
#define STRING_STREAM_ENCODER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Callback signature for flushing compressed data.
 *
 * \param data     Pointer to compressed data to flush.
 * \param len      Length of data in bytes.
 * \param context  User-defined pointer passed at creation time.
 *
 * \return true on success, false on error.
 */
typedef bool (*FlushCallback)(const uint8_t *data, size_t len, void *context);

/**
 * @brief Opaque handle for the incremental string encoder.
 */
typedef struct StringStreamEncoder StringStreamEncoder;

/**
 * @brief Create a new string stream encoder using zlib.
 *
 * @param flush_cb   Callback used for flushing compressed data.
 * @param flush_ctx  Context pointer passed to the callback.
 *
 * @return Pointer to a newly allocated StringStreamEncoder, or NULL on failure.
 */
StringStreamEncoder *string_stream_encoder_create(FlushCallback flush_cb,
                                                  void *flush_ctx);

/**
 * @brief Destroy an existing string stream encoder.
 *
 * Frees internal buffers and ends the zlib deflate context.
 *
 * @param encoder Pointer to the StringStreamEncoder to destroy.
 */
void string_stream_encoder_destroy(StringStreamEncoder *encoder);

/**
 * @brief Add one string to be compressed.
 *
 * Internally, this will:
 *  1. Write a 4-byte length prefix (little-endian).
 *  2. Write the raw string data itself.
 *  3. Both parts are subject to compression by zlib.
 *
 * @param encoder Pointer to the StringStreamEncoder.
 * @param data    Pointer to the raw string bytes.
 * @param length  Length of the string in bytes.
 *
 * @return true on success, false on failure (e.g., if encoder is finished).
 */
bool string_stream_encoder_add_value(StringStreamEncoder *encoder,
                                     const char *data, size_t length);

/**
 * @brief Flush the current compression buffer with a zlib sync flush.
 *
 * This ensures all compressed data so far is delivered to the flush callback.
 * The zlib stream remains open, so subsequent calls to @ref
 * string_stream_encoder_add_value can still benefit from the existing history.
 *
 * @param encoder Pointer to the StringStreamEncoder.
 * @return true on success, false on error.
 */
bool string_stream_encoder_flush(StringStreamEncoder *encoder);

/**
 * @brief Finalize the compression stream.
 *
 * This will deflate all remaining input until Z_STREAM_END is reached, call the
 * flush callback with any leftover compressed data, and mark the encoder as
 * finished. Once finished, no further calls to @ref
 * string_stream_encoder_add_value are allowed.
 *
 * @param encoder Pointer to the StringStreamEncoder.
 * @return true on success, false on error.
 */
bool string_stream_encoder_finish(StringStreamEncoder *encoder);

#ifdef __cplusplus
}
#endif

#endif /* STRING_STREAM_ENCODER_H */
