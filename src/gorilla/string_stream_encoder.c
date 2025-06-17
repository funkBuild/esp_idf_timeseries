#include "gorilla/string_stream_encoder.h"
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

/* A small buffer to hold compressed bytes before flushing. */
#define STRING_STREAM_CHUNK_SIZE 128

/**
 * @brief Internal structure encapsulating the zlib state and flush callback.
 */
struct StringStreamEncoder {
  FlushCallback flush_cb;
  void *flush_ctx;
  z_stream zstrm;
  bool finished;

  uint8_t out_buffer[STRING_STREAM_CHUNK_SIZE];
};

/**
 * @brief Convert a 32-bit value to little-endian byte order.
 *
 * If your platform is guaranteed little-endian, you can omit or define as a
 * no-op. If you prefer big-endian on the wire, invert this function
 * accordingly.
 */
static uint32_t to_little_endian_u32(uint32_t x) {
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
  // On little-endian systems, no conversion is necessary.
  return x;
#else
  // On big-endian systems, swap the bytes.
  return ((x & 0x000000FFU) << 24) | ((x & 0x0000FF00U) << 8) |
         ((x & 0x00FF0000U) >> 8) | ((x & 0xFF000000U) >> 24);
#endif
}
/**
 * @brief Helper to flush the current compressed output buffer.
 *
 * @param enc Pointer to the StringStreamEncoder.
 * @return true on success, false on callback error.
 */
static bool flush_deflate_output(StringStreamEncoder *enc) {
  /* Calculate how many bytes of compressed data were just produced. */
  size_t have = STRING_STREAM_CHUNK_SIZE - enc->zstrm.avail_out;
  if (have > 0) {
    if (!enc->flush_cb(enc->out_buffer, have, enc->flush_ctx)) {
      return false;
    }
  }

  /* Reset the buffer for the next pass. */
  enc->zstrm.avail_out = STRING_STREAM_CHUNK_SIZE;
  enc->zstrm.next_out = enc->out_buffer;
  return true;
}

StringStreamEncoder *string_stream_encoder_create(FlushCallback flush_cb,
                                                  void *flush_ctx) {
  if (!flush_cb) {
    return NULL;
  }

  StringStreamEncoder *enc = (StringStreamEncoder *)calloc(1, sizeof(*enc));
  if (!enc) {
    return NULL;
  }

  enc->flush_cb = flush_cb;
  enc->flush_ctx = flush_ctx;

  /* Prepare zlib's deflate state. Use minimal memory level to reduce RAM usage.
   */
  enc->zstrm.zalloc = Z_NULL;
  enc->zstrm.zfree = Z_NULL;
  enc->zstrm.opaque = Z_NULL;

  /*
   * deflateInit2 parameters:
   *   - compression level: Z_DEFAULT_COMPRESSION (or pick 1..9)
   *   - method: Z_DEFLATED
   *   - windowBits: 15 for zlib header, or -15 for raw deflate
   *   - memoryLevel: 1 (lowest memory usage) ... 9 (highest)
   *   - strategy: Z_DEFAULT_STRATEGY
   */
  int ret = deflateInit2(&enc->zstrm, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                         15, /* +16 if you want gzip, or -15 for raw deflate. */
                         1,  /* minimal memory usage */
                         Z_DEFAULT_STRATEGY);
  if (ret != Z_OK) {
    free(enc);
    return NULL;
  }

  /* Set up the output buffer for deflate. */
  enc->zstrm.avail_out = STRING_STREAM_CHUNK_SIZE;
  enc->zstrm.next_out = enc->out_buffer;

  return enc;
}

void string_stream_encoder_destroy(StringStreamEncoder *enc) {
  if (!enc) {
    return;
  }
  /* If the user never called finish, we still end deflate here. */
  deflateEnd(&enc->zstrm);
  free(enc);
}

bool string_stream_encoder_add_value(StringStreamEncoder *enc, const char *data,
                                     size_t length) {
  if (!enc || enc->finished || !data) {
    return false;
  }

  /* 1) Write a 4-byte length prefix in little-endian format. */
  uint32_t len_le = to_little_endian_u32((uint32_t)length);

  enc->zstrm.avail_in = sizeof(len_le);
  enc->zstrm.next_in = (Bytef *)&len_le;

  while (enc->zstrm.avail_in > 0) {
    int ret = deflate(&enc->zstrm, Z_NO_FLUSH);
    if (ret == Z_STREAM_ERROR) {
      return false; /* Something went wrong inside zlib. */
    }
    if (!flush_deflate_output(enc)) {
      return false; /* Callback failed. */
    }
  }

  /* 2) Write the actual string bytes. */
  enc->zstrm.avail_in = (uInt)length;
  enc->zstrm.next_in = (Bytef *)data;

  while (enc->zstrm.avail_in > 0) {
    int ret = deflate(&enc->zstrm, Z_NO_FLUSH);
    if (ret == Z_STREAM_ERROR) {
      return false;
    }
    if (!flush_deflate_output(enc)) {
      return false;
    }
  }

  return true;
}

bool string_stream_encoder_flush(StringStreamEncoder *enc) {
  if (!enc || enc->finished) {
    return false;
  }

  /* Force a sync flush so all current data is emitted. */
  int ret;
  do {
    ret = deflate(&enc->zstrm, Z_SYNC_FLUSH);
    if (ret == Z_STREAM_ERROR) {
      return false;
    }
    if (!flush_deflate_output(enc)) {
      return false;
    }
  } while (enc->zstrm.avail_out == 0); /* If buffer was full, keep going. */

  return true;
}

bool string_stream_encoder_finish(StringStreamEncoder *enc) {
  if (!enc || enc->finished) {
    return false;
  }
  enc->finished = true;

  /* Drain the compressor until we reach the end. */
  int ret;
  do {
    ret = deflate(&enc->zstrm, Z_FINISH);
    if (ret == Z_STREAM_ERROR) {
      return false;
    }
    if (!flush_deflate_output(enc)) {
      return false;
    }
  } while (ret != Z_STREAM_END);

  /* We do not call deflateEnd() here so that destroy can still be invoked. */
  return true;
}
