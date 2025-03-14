#ifndef TIMESERIES_COMPRESSION_H
#define TIMESERIES_COMPRESSION_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Compress a buffer of data using the configured compression library.
 *
 * @param[in]  in_data    Pointer to input (uncompressed) data buffer
 * @param[in]  in_size    Size of the input data in bytes
 * @param[out] out_data   On success, newly allocated buffer containing
 * compressed data
 * @param[out] out_size   On success, size of the compressed data in bytes
 *
 * @return true on success, false otherwise
 *
 * @note The caller is responsible for freeing `*out_data` when done.
 */
bool timeseries_compression_compress(const uint8_t *in_data, size_t in_size,
                                     uint8_t **out_data, size_t *out_size);

/**
 * @brief Decompress data using zlib.
 *
 * The default implementation below assumes the first 4 bytes of the
 * compressed block store the uncompressed size (as a 32-bit value).
 * Then the rest of `in_data` is the actual zlib-compressed bytes.
 *
 * @param in_data    Pointer to compressed data buffer
 * @param in_size    Size of the compressed data buffer
 * @param out_data   Output pointer to newly allocated uncompressed data
 * @param out_size   Size of the uncompressed data
 * @return true on success, false on failure
 *
 * @note Caller must free(*out_data) when done.
 */
bool timeseries_compression_decompress(const uint8_t *in_data, size_t in_size,
                                       uint8_t **out_data, size_t *out_size);

#ifdef __cplusplus
}
#endif

#endif // TIMESERIES_COMPRESSION_H
