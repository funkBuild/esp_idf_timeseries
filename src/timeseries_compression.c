#include "timeseries_compression.h"
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#define CHUNK_SIZE 16384

/**
 * @brief Implementation using zlib under the hood.
 *
 * Uses deflateInit2/inflateInit2 with reduced memory settings suitable
 * for embedded targets (smaller window and memory level).
 */
bool timeseries_compression_compress(const uint8_t *in_data, size_t in_size,
                                     uint8_t **out_data, size_t *out_size) {
  if (!in_data || !out_data || !out_size) {
    return false;
  }

  z_stream strm;
  memset(&strm, 0, sizeof(strm));

  // Use reduced memory settings for embedded:
  // windowBits=12 (4KB window), memLevel=4 (~16KB internal state)
  int ret = deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                         12, 4, Z_DEFAULT_STRATEGY);
  if (ret != Z_OK) {
    return false;
  }

  // Allocate output buffer with generous upper bound
  size_t max_out = deflateBound(&strm, (uLong)in_size);
  uint8_t *temp = (uint8_t *)malloc(max_out);
  if (!temp) {
    deflateEnd(&strm);
    return false;
  }

  strm.next_in = (Bytef *)in_data;
  strm.avail_in = (uInt)in_size;
  strm.next_out = temp;
  strm.avail_out = (uInt)max_out;

  ret = deflate(&strm, Z_FINISH);
  if (ret != Z_STREAM_END) {
    deflateEnd(&strm);
    free(temp);
    return false;
  }

  size_t compressed_size = strm.total_out;
  deflateEnd(&strm);

  // Shrink buffer to actual size
  uint8_t *shrunk = (uint8_t *)realloc(temp, compressed_size);
  if (shrunk) {
    temp = shrunk;
  }

  *out_data = temp;
  *out_size = compressed_size;
  return true;
}

bool timeseries_compression_decompress(const uint8_t *in_data, size_t in_size,
                                       uint8_t **out_data, size_t *out_size) {
  if (!in_data || !out_data || !out_size) {
    return false;
  }

  z_stream strm;
  memset(&strm, 0, sizeof(strm));
  strm.next_in = (Bytef *)in_data;
  strm.avail_in = in_size;

  // Use windowBits=12 to match the compressor
  int ret = inflateInit2(&strm, 12);
  if (ret != Z_OK) {
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

  do {
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

    strm.next_out = buffer + total_output;
    strm.avail_out = (uInt)(buffer_capacity - total_output);

    inflate_ret = inflate(&strm, Z_NO_FLUSH);

    if (inflate_ret != Z_OK && inflate_ret != Z_STREAM_END) {
      free(buffer);
      inflateEnd(&strm);
      return false;
    }

    total_output = buffer_capacity - strm.avail_out;

  } while (inflate_ret != Z_STREAM_END);

  inflateEnd(&strm);

  uint8_t *final_buffer = (uint8_t *)realloc(buffer, total_output);
  if (final_buffer != NULL) {
    buffer = final_buffer;
  }

  *out_data = buffer;
  *out_size = total_output;
  return true;
}
