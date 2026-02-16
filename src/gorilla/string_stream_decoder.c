#include "gorilla/string_stream_decoder.h"

#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#include "esp_log.h"

static const char *TAG = "STRING_DECODER";

/**
 * The size of the chunk we'll request from the fill callback and feed to zlib.
 * The smaller the buffer, the less memory usage but the more callbacks.
 */
#define STRING_DECODER_IN_CHUNK_SIZE 128

/**
 * Internal structure for our decoder:
 *   - We store the zlib `z_stream` for inflating data.
 *   - We store the user-supplied fill callback for reading compressed bytes.
 *   - We maintain an input buffer to feed to zlib.
 */
struct StringStreamDecoder {
  z_stream zstrm;       /* Zlib stream state. */
  FillCallback fill_cb; /* Callback to fetch compressed data. */
  void *fill_ctx;       /* Context pointer for fill_cb. */

  bool finished;     /* If the user called finish or if we hit Z_STREAM_END. */
  bool stream_ended; /* True if inflate() returned Z_STREAM_END. */

  uint8_t in_buffer[STRING_DECODER_IN_CHUNK_SIZE];
  size_t in_buffer_size; /* How many bytes are currently in in_buffer. */
  size_t in_buffer_pos;  /* Current read position within in_buffer. */
};

/* Forward declarations for internal helper functions. */
static bool refill_input_buffer(StringStreamDecoder *dec);
static bool read_uncompressed_bytes(StringStreamDecoder *dec, uint8_t *dst,
                                    size_t len);
static uint32_t from_little_endian_u32(const uint8_t *bytes);

StringStreamDecoder *string_stream_decoder_create(FillCallback fill_cb,
                                                  void *fill_ctx) {
  if (!fill_cb) {
    return NULL;
  }

  StringStreamDecoder *dec = (StringStreamDecoder *)calloc(1, sizeof(*dec));
  if (!dec) {
    return NULL;
  }
  dec->fill_cb = fill_cb;
  dec->fill_ctx = fill_ctx;

  /* Initialize zlib for raw zlib or standard zlib stream (choose windowBits).
   */
  dec->zstrm.zalloc = Z_NULL;
  dec->zstrm.zfree = Z_NULL;
  dec->zstrm.opaque = Z_NULL;

  /*
   * Typically, to match a zlib-based encoder, use `windowBits = 15` if
   * the encoder used 15 as well. If the encoder used raw deflate with
   * `windowBits = -15`, do so here as well. Also ensure memoryLevel
   * matches your usage constraints.
   */
  int ret = inflateInit2(&dec->zstrm, 15);
  if (ret != Z_OK) {
    free(dec);
    return NULL;
  }

  dec->in_buffer_size = 0; /* Nothing in the buffer yet. */
  dec->in_buffer_pos = 0;
  dec->finished = false;
  dec->stream_ended = false;
  return dec;
}

void string_stream_decoder_destroy(StringStreamDecoder *dec) {
  if (!dec) {
    return;
  }
  /* End zlib (if not already ended by finish). */
  inflateEnd(&dec->zstrm);
  free(dec);
}

/**
 * @brief Reads exactly `len` uncompressed bytes from the zlib stream.
 *
 * This function calls inflate() repeatedly. If zlib needs more input, we call
 * the fill callback to get more compressed data. We continue until we have
 * produced `len` uncompressed bytes or until we reach the end of the stream.
 */
static bool read_uncompressed_bytes(StringStreamDecoder *dec, uint8_t *dst,
                                    size_t len) {
  size_t total_read = 0;
  while (total_read < len) {
    /* If we have no compressed bytes left to feed zlib, try to fill the buffer.
     */
    if (dec->zstrm.avail_in == 0 && !dec->stream_ended) {
      if (!refill_input_buffer(dec)) {
        ESP_LOGW(TAG, "Failed to refill input buffer");
      }
      dec->zstrm.next_in = dec->in_buffer + dec->in_buffer_pos;
      dec->zstrm.avail_in = (uInt)(dec->in_buffer_size - dec->in_buffer_pos);
    }

    dec->zstrm.next_out = dst + total_read;
    dec->zstrm.avail_out = (uInt)(len - total_read);

    /* Ask zlib to inflate. */
    int ret = inflate(&dec->zstrm, Z_NO_FLUSH);
    if (ret == Z_STREAM_END) {
      dec->stream_ended = true;
      /* We might still have produced some data in this call. */
    } else if (ret != Z_OK && ret != Z_BUF_ERROR && ret != Z_STREAM_END) {
      ESP_LOGE(TAG, "inflate() failed: %d", ret);
      return false;
    }

    /* How many bytes did we actually produce in this call? */
    size_t bytes_produced = (len - total_read) - dec->zstrm.avail_out;
    total_read += bytes_produced;

    /* How many input bytes did we consume? */
    size_t bytes_consumed =
        (dec->in_buffer_size - dec->in_buffer_pos) - dec->zstrm.avail_in;
    dec->in_buffer_pos += bytes_consumed;

    /* If we've reached the end of the compressed stream (Z_STREAM_END) but
       haven't read all `len` uncompressed bytes, it's an error (truncated
       data). */
    if (dec->stream_ended && total_read < len) {
      ESP_LOGW(TAG, "Truncated data: wanted %u bytes, got %u",
               (unsigned int)len, (unsigned int)total_read);
      return false;
    }

    /* Detect zero-progress: no bytes produced and no bytes consumed means
       the stream is stuck (truncated input). Break to avoid infinite loop. */
    if (bytes_produced == 0 && bytes_consumed == 0 && !dec->stream_ended) {
      ESP_LOGE(TAG, "inflate() made no progress, likely truncated stream");
      return false;
    }

    /* If we've consumed the entire in_buffer, reset it. */
    if (dec->in_buffer_pos >= dec->in_buffer_size) {
      dec->in_buffer_pos = 0;
      dec->in_buffer_size = 0;
    }
  }
  return true;
}

/**
 * @brief Attempt to refill in_buffer by calling fill_cb.
 *
 * @return true if we read > 0 bytes, false if we read 0 (EOF) or an error
 * occurred.
 */
static bool refill_input_buffer(StringStreamDecoder *dec) {
  if (dec->stream_ended) {
    return false; /* End of stream reached already. */
  }

  /* Move leftover data to the front of the buffer, if any. */
  if (dec->in_buffer_pos < dec->in_buffer_size) {
    size_t leftover = dec->in_buffer_size - dec->in_buffer_pos;
    memmove(dec->in_buffer, dec->in_buffer + dec->in_buffer_pos, leftover);
    dec->in_buffer_size = leftover;
    dec->in_buffer_pos = 0;
  } else {
    dec->in_buffer_size = 0;
    dec->in_buffer_pos = 0;
  }

  /* Request more compressed data via the fill callback. */
  size_t space = STRING_DECODER_IN_CHUNK_SIZE - dec->in_buffer_size;
  if (space == 0) {
    /* This shouldn't happen, but handle it gracefully. */
    return false;
  }

  size_t filled = 0;
  bool success = dec->fill_cb(
      dec->fill_ctx, dec->in_buffer + dec->in_buffer_size, space, &filled);

  if (!success || filled == 0) {
    /* Callback signaled end-of-data or an error. */
    return false;
  }

  dec->in_buffer_size += filled;
  return true;
}

/* Helper to interpret a 4-byte little-endian prefix as a 32-bit integer. */
static uint32_t from_little_endian_u32(const uint8_t *bytes) {
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
  // On little-endian systems, we can simply copy the bytes.
  uint32_t x;
  memcpy(&x, bytes, sizeof(x));
  return x;
#else
  // On big-endian systems, reconstruct the integer.
  return ((uint32_t)bytes[0]) | (((uint32_t)bytes[1]) << 8) |
         (((uint32_t)bytes[2]) << 16) | (((uint32_t)bytes[3]) << 24);
#endif
}

bool string_stream_decoder_get_value(StringStreamDecoder *dec,
                                     uint8_t **out_data, size_t *out_length) {

  if (!dec || !out_data || !out_length) {
    ESP_LOGE(TAG, "Invalid args: dec=%p out_data=%p out_length=%p",
             (void *)dec, (void *)out_data, (void *)out_length);
    return false;
  }

  if (dec->finished) {
    ESP_LOGE(TAG, "dec->finished is true");
    return false;
  }

  /* First, read the 4-byte length prefix. */
  uint8_t prefix[4];
  if (!read_uncompressed_bytes(dec, prefix, 4)) {
    ESP_LOGE(TAG, "Failed to read 4-byte prefix");
    return false; /* Probably no more data or an error. */
  }

  /* Convert from little-endian. */
  uint32_t length = from_little_endian_u32(prefix);
  if (length == 0) {
    /* An empty string is valid, just return zero-length. */
    *out_data = NULL;
    *out_length = 0;
    return true;
  }

  /* Allocate a buffer to hold the string data. */
  uint8_t *buf = (uint8_t *)malloc(length);
  if (!buf) {
    ESP_LOGE(TAG, "OOM for string data of length %u", (unsigned int)length);
    return false;
  }

  /* Now read the `length` bytes of string content. */
  if (!read_uncompressed_bytes(dec, buf, length)) {
    ESP_LOGE(TAG, "Failed to read %u bytes of string data",
             (unsigned int)length);
    free(buf);
    return false;
  }

  /* Success! Return them to the caller. */
  *out_data = buf;
  *out_length = length;
  return true;
}

bool string_stream_decoder_finish(StringStreamDecoder *dec) {
  if (!dec) {
    return false;
  }
  /* If we've already finished, do nothing. */
  if (dec->finished) {
    return true;
  }

  dec->finished = true;

  /*
   * We can attempt to read until Z_STREAM_END if not reached yet,
   * to ensure the compressed stream is fully consumed. If you want
   * strict checking that no extra data remains, do that here.
   */

  /* If inflate has already returned Z_STREAM_END, we consider it good. */
  if (!dec->stream_ended) {
    /* Try to keep inflating until we hit the end or run out of compressed data.
     */
    while (!dec->stream_ended) {
      if (dec->zstrm.avail_in == 0 && !refill_input_buffer(dec)) {
        /* No more data from fill_cb, so if we haven't hit stream_ended yet,
           the data might be truncated. For some use cases, you might treat
           this as an error. */
        break;
      }
      dec->zstrm.next_in = dec->in_buffer + dec->in_buffer_pos;
      dec->zstrm.avail_in = (uInt)(dec->in_buffer_size - dec->in_buffer_pos);

      uint8_t dummy[32];
      dec->zstrm.next_out = dummy;
      dec->zstrm.avail_out = sizeof(dummy);

      int ret = inflate(&dec->zstrm, Z_NO_FLUSH);
      if (ret == Z_STREAM_END) {
        dec->stream_ended = true;
      } else if (ret != Z_OK && ret != Z_BUF_ERROR && ret != Z_STREAM_END) {
        /* Real error in stream. */
        return false;
      }

      size_t consumed =
          (dec->in_buffer_size - dec->in_buffer_pos) - dec->zstrm.avail_in;
      dec->in_buffer_pos += consumed;
      if (dec->in_buffer_pos >= dec->in_buffer_size) {
        dec->in_buffer_pos = 0;
        dec->in_buffer_size = 0;
      }
    }
  }

  /* Return true if we ended cleanly (or at least tried to). */
  return true;
}
