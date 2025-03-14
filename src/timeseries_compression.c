#include "timeseries_compression.h"
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#define CHUNK_SIZE 16384

/**
 * @brief Implementation using zlib under the hood.
 *        You can replace this with a different library if needed.
 */
bool timeseries_compression_compress(const uint8_t *in_data, size_t in_size,
                                     uint8_t **out_data, size_t *out_size) {
  if (!in_data || !out_data || !out_size) {
    return false;
  }

  // 1) Ask zlib for the upper bound of compressed data size
  uLongf max_compressed_size = compressBound((uLong)in_size);

  // 2) Allocate a buffer large enough to hold the worst‐case compressed data
  uint8_t *temp = (uint8_t *)malloc(max_compressed_size);
  if (!temp) {
    return false;
  }

  // 3) Perform compression with zlib
  uLongf actual_compressed_size = max_compressed_size;
  int zerr = compress2(temp, &actual_compressed_size, in_data, (uLong)in_size,
                       Z_BEST_COMPRESSION);

  if (zerr != Z_OK) {
    // On failure, free and return false
    free(temp);
    return false;
  }

  // 4) Success: return the compressed data buffer & size
  *out_data = temp;
  *out_size = (size_t)actual_compressed_size;

  return true;
}

bool timeseries_compression_decompress(const uint8_t *in_data, size_t in_size,
                                       uint8_t **out_data, size_t *out_size) {
  if (!in_data || !out_data || !out_size) {
    return false;
  }

  // Set up the inflation stream.
  z_stream strm;
  memset(&strm, 0, sizeof(strm));
  strm.next_in = (Bytef *)in_data;
  strm.avail_in = in_size;

  int ret = inflateInit(&strm);
  if (ret != Z_OK) {
    // Initialization error.
    return false;
  }

  // Prepare an initial output buffer.
  size_t buffer_capacity = CHUNK_SIZE;
  uint8_t *buffer = (uint8_t *)malloc(buffer_capacity);
  if (!buffer) {
    inflateEnd(&strm);
    return false;
  }

  size_t total_output = 0;
  int inflate_ret;

  // Loop until the stream is fully decompressed.
  do {
    // If needed, enlarge the output buffer.
    if (total_output + CHUNK_SIZE > buffer_capacity) {
      size_t new_capacity = buffer_capacity * 2;
      uint8_t *new_buffer = (uint8_t *)realloc(buffer, new_capacity);
      if (!new_buffer) {
        free(buffer);
        inflateEnd(&strm);
        return false;
      }
      buffer = new_buffer;
      buffer_capacity = new_capacity;
    }

    // Set the next output pointer.
    strm.next_out = buffer + total_output;
    strm.avail_out = (uInt)(buffer_capacity - total_output);

    inflate_ret = inflate(&strm, Z_NO_FLUSH);

    if (inflate_ret != Z_OK && inflate_ret != Z_STREAM_END) {
      // Decompression error.
      free(buffer);
      inflateEnd(&strm);
      return false;
    }

    // Increase total output by the number of bytes just decompressed.
    total_output = buffer_capacity - strm.avail_out;

  } while (inflate_ret != Z_STREAM_END);

  // Clean up.
  inflateEnd(&strm);

  // Optionally shrink the buffer to the actual size.
  uint8_t *final_buffer = (uint8_t *)realloc(buffer, total_output);
  if (final_buffer != NULL) {
    buffer = final_buffer;
  }

  *out_data = buffer;
  *out_size = total_output;
  return true;
}