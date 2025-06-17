#include "gorilla/gorilla_stream_encoder.h"
#include "gorilla/boolean_stream_encoder.h"
#include "gorilla/float_stream_encoder.h" // New float encoder interface.
#include "gorilla/gorilla_stream_types.h"
#include "gorilla/integer_stream_encoder.h" // Existing integer encoder interface.
#include "gorilla/string_stream_encoder.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "esp_log.h"

const char *TAG = "GorillaStreamEncoder";

/*
 * The original flush callback type (from gorilla_stream_flush_cb)
 * is defined as:
 *     typedef bool (*gorilla_stream_flush_cb)(void *context, const uint8_t
 * *data, size_t len);
 *
 * The encoder libraries (both integer and float) expect a flush callback of
 * type FlushCallback: typedef bool (*FlushCallback)(const uint8_t *data, size_t
 * len, void *context);
 *
 * To bridge this difference, we define an adapter structure and callback.
 */

/* Adapter structure to hold the original flush callback and context */
typedef struct {
  gorilla_stream_flush_cb orig_flush_cb;
  void *orig_flush_ctx;
} FlushAdapter;

/* Adapter callback: receives (data, len, context) and calls the original flush
 * callback with swapped parameters */
static bool flush_adapter_cb(const uint8_t *data, size_t len, void *ctx) {
  FlushAdapter *adapter = (FlushAdapter *)ctx;
  return adapter->orig_flush_cb(adapter->orig_flush_ctx, data, len);
}

/* Wrapper structure for integer encoder */
typedef struct {
  IntegerStreamEncoder *encoder;
  FlushAdapter *adapter;
} IntegerEncoderWrapper;

/* Wrapper structure for float encoder */
typedef struct {
  FloatStreamEncoder *encoder;
  FlushAdapter *adapter;
} FloatEncoderWrapper;

typedef struct {
  BooleanStreamEncoder *encoder;
  FlushAdapter *adapter;
} BooleanEncoderWrapper;

typedef struct {
  StringStreamEncoder *encoder;
  FlushAdapter *adapter;
} StringEncoderWrapper;

/**
 * @brief Initialize the Gorilla stream.
 *
 * This function sets up the internal state. The bit buffer parameters are
 * ignored, as all data is written immediately via the flush callback.
 *
 * For integer streams (GORILLA_STREAM_INT) and floating-point streams
 * (GORILLA_STREAM_FLOAT), an instance of the corresponding encoder is created
 * using a flush adapter.
 */
bool gorilla_stream_init(gorilla_stream_t *stream,
                         gorilla_stream_type_t stream_type,
                         uint8_t *bit_buffer,    /* Ignored */
                         size_t bit_buffer_size, /* Ignored */
                         gorilla_stream_flush_cb flush_cb, void *flush_ctx) {
  if (!stream || !flush_cb) {
    return false;
  }

  stream->stream_type = stream_type;
  stream->flush_cb = flush_cb;
  stream->flush_ctx = flush_ctx;
  stream->encoder_impl = NULL;

  /* For non-integer/float streams, we no longer use a bit buffer. */
  if (stream_type == GORILLA_STREAM_INT) {
    /* Allocate and set up the flush adapter */
    FlushAdapter *adapter = (FlushAdapter *)malloc(sizeof(FlushAdapter));
    if (!adapter)
      return false;
    adapter->orig_flush_cb = flush_cb;
    adapter->orig_flush_ctx = flush_ctx;

    /* Create the integer stream encoder using the adapter callback */
    IntegerStreamEncoder *enc =
        integer_stream_encoder_create(flush_adapter_cb, adapter);
    if (!enc) {
      free(adapter);
      return false;
    }

    /* Wrap the integer encoder and adapter into a single structure */
    IntegerEncoderWrapper *wrapper =
        (IntegerEncoderWrapper *)malloc(sizeof(IntegerEncoderWrapper));
    if (!wrapper) {
      integer_stream_encoder_destroy(enc);
      free(adapter);
      return false;
    }
    wrapper->encoder = enc;
    wrapper->adapter = adapter;
    stream->encoder_impl = wrapper;
  } else if (stream_type == GORILLA_STREAM_FLOAT) {
    /* Allocate and set up the flush adapter for float encoding */
    FlushAdapter *adapter = (FlushAdapter *)malloc(sizeof(FlushAdapter));
    if (!adapter)
      return false;
    adapter->orig_flush_cb = flush_cb;
    adapter->orig_flush_ctx = flush_ctx;

    /* Create the float stream encoder using the adapter callback */
    FloatStreamEncoder *enc =
        float_stream_encoder_create(flush_adapter_cb, adapter);
    if (!enc) {
      free(adapter);
      return false;
    }

    /* Wrap the float encoder and adapter into a single structure */
    FloatEncoderWrapper *wrapper =
        (FloatEncoderWrapper *)malloc(sizeof(FloatEncoderWrapper));
    if (!wrapper) {
      float_stream_encoder_destroy(enc);
      free(adapter);
      return false;
    }
    wrapper->encoder = enc;
    wrapper->adapter = adapter;
    stream->encoder_impl = wrapper;
  } else if (stream_type == GORILLA_STREAM_BOOL) {
    // New branch for boolean streams.
    FlushAdapter *adapter = malloc(sizeof(FlushAdapter));
    if (!adapter)
      return false;
    adapter->orig_flush_cb = flush_cb;
    adapter->orig_flush_ctx = flush_ctx;
    BooleanStreamEncoder *enc =
        boolean_stream_encoder_create(flush_adapter_cb, adapter);
    if (!enc) {
      free(adapter);
      return false;
    }
    BooleanEncoderWrapper *wrapper = malloc(sizeof(BooleanEncoderWrapper));
    if (!wrapper) {
      boolean_stream_encoder_destroy(enc);
      free(adapter);
      return false;
    }
    wrapper->encoder = enc;
    wrapper->adapter = adapter;
    stream->encoder_impl = wrapper;
  } else if (stream_type == GORILLA_STREAM_STRING) {
    FlushAdapter *adapter = (FlushAdapter *)malloc(sizeof(FlushAdapter));

    if (!adapter) {
      ESP_LOGE(TAG, "Failed to allocate memory for flush adapter");
      return false;
    }

    adapter->orig_flush_cb = flush_cb;
    adapter->orig_flush_ctx = flush_ctx;

    StringStreamEncoder *enc =
        string_stream_encoder_create(flush_adapter_cb, adapter);
    if (!enc) {
      ESP_LOGE(TAG, "Failed to create string stream encoder");
      free(adapter);
      return false;
    }

    StringEncoderWrapper *wrapper =
        (StringEncoderWrapper *)malloc(sizeof(StringEncoderWrapper));
    if (!wrapper) {
      ESP_LOGE(TAG, "Failed to allocate memory for string encoder wrapper");
      string_stream_encoder_destroy(enc);
      free(adapter);
      return false;
    }
    wrapper->encoder = enc;
    wrapper->adapter = adapter;
    stream->encoder_impl = wrapper;
  }

  return true;
}

/**
 * @brief Add an integer value to the Gorilla stream.
 *
 * For streams of type GORILLA_STREAM_INT, the integer encoder is used.
 * For other stream types, the raw 64-bit value is immediately flushed.
 */
bool gorilla_stream_add_timestamp(gorilla_stream_t *stream, uint64_t value) {
  if (!stream) {
    return false;
  }

  if (stream->stream_type == GORILLA_STREAM_INT) {
    IntegerEncoderWrapper *wrapper =
        (IntegerEncoderWrapper *)stream->encoder_impl;
    return integer_stream_encoder_add_value(wrapper->encoder, value);
  } else {
    /* For non-integer streams, immediately flush the raw value. */
    return stream->flush_cb(stream->flush_ctx, (const uint8_t *)&value,
                            sizeof(value));
  }
}

/**
 * @brief Add a floating-point value to the Gorilla stream.
 *
 * For streams of type GORILLA_STREAM_FLOAT, the float encoder is used.
 * For other stream types, the raw 64-bit representation of the double is
 * immediately flushed.
 */
bool gorilla_stream_add_float(gorilla_stream_t *stream, double value) {
  if (!stream) {
    return false;
  }

  if (stream->stream_type == GORILLA_STREAM_FLOAT) {
    FloatEncoderWrapper *wrapper = (FloatEncoderWrapper *)stream->encoder_impl;
    return float_stream_encoder_add_value(wrapper->encoder, value);
  } else {
    /* For non-float streams, flush the raw 64-bit representation of the double.
     */
    uint64_t raw = 0;
    memcpy(&raw, &value, sizeof(double));
    return stream->flush_cb(stream->flush_ctx, (const uint8_t *)&raw,
                            sizeof(raw));
  }
}

/**
 * @brief Add a boolean value to the Gorilla stream.
 *
 * For streams of type GORILLA_STREAM_BOOL, the boolean encoder is used.
 * For non-boolean streams, the raw value is flushed as a full byte (0 or 1).
 */
bool gorilla_stream_add_boolean(gorilla_stream_t *stream, bool value) {
  if (!stream) {
    return false;
  }
  if (stream->stream_type == GORILLA_STREAM_BOOL) {
    BooleanEncoderWrapper *wrapper =
        (BooleanEncoderWrapper *)stream->encoder_impl;
    return boolean_stream_encoder_add_value(wrapper->encoder, value);
  } else {
    uint8_t raw = value ? 1 : 0;
    return stream->flush_cb(stream->flush_ctx, &raw, sizeof(raw));
  }
}

bool gorilla_stream_add_string(gorilla_stream_t *stream, const char *data,
                               size_t length) {
  if (!stream) {
    return false;
  }
  if (stream->stream_type == GORILLA_STREAM_STRING) {
    StringEncoderWrapper *wrapper =
        (StringEncoderWrapper *)stream->encoder_impl;
    return string_stream_encoder_add_value(wrapper->encoder, data, length);
  } else {
    /* Fallback: immediately flush raw string if it's not a string stream. */
    return stream->flush_cb(stream->flush_ctx, (const uint8_t *)data, length);
  }
}

/**
 * @brief Finalize the Gorilla stream.
 *
 * For integer and float streams, the finish call is delegated to the
 * corresponding encoding library. For other types, no additional flushing is
 * necessary.
 */
bool gorilla_stream_finish(gorilla_stream_t *stream) {
  if (!stream) {
    return false;
  }

  if (stream->stream_type == GORILLA_STREAM_INT) {
    IntegerEncoderWrapper *wrapper =
        (IntegerEncoderWrapper *)stream->encoder_impl;
    return integer_stream_encoder_finish(wrapper->encoder);
  } else if (stream->stream_type == GORILLA_STREAM_FLOAT) {
    FloatEncoderWrapper *wrapper = (FloatEncoderWrapper *)stream->encoder_impl;
    return float_stream_encoder_finish(wrapper->encoder);
  } else if (stream->stream_type == GORILLA_STREAM_BOOL) {
    BooleanEncoderWrapper *wrapper =
        (BooleanEncoderWrapper *)stream->encoder_impl;
    return boolean_stream_encoder_finish(wrapper->encoder);
  } else if (stream->stream_type == GORILLA_STREAM_STRING) {
    StringEncoderWrapper *wrapper =
        (StringEncoderWrapper *)stream->encoder_impl;
    return string_stream_encoder_finish(wrapper->encoder);
  }

  return true;
}

/**
 * @brief Deinitialize the Gorilla stream.
 *104
 * For integer and float streams, this also destroys the corresponding encoding
 * library instance and frees the flush adapter.
 */
void gorilla_stream_deinit(gorilla_stream_t *stream) {
  if (!stream) {
    return;
  }

  if (stream->stream_type == GORILLA_STREAM_INT) {
    IntegerEncoderWrapper *wrapper =
        (IntegerEncoderWrapper *)stream->encoder_impl;
    if (wrapper) {
      if (wrapper->encoder) {
        integer_stream_encoder_destroy(wrapper->encoder);
      }
      if (wrapper->adapter) {
        free(wrapper->adapter);
      }
      free(wrapper);
      stream->encoder_impl = NULL;
    }
  } else if (stream->stream_type == GORILLA_STREAM_FLOAT) {
    FloatEncoderWrapper *wrapper = (FloatEncoderWrapper *)stream->encoder_impl;
    if (wrapper) {
      if (wrapper->encoder) {
        float_stream_encoder_destroy(wrapper->encoder);
      }
      if (wrapper->adapter) {
        free(wrapper->adapter);
      }
      free(wrapper);
      stream->encoder_impl = NULL;
    }
  } else if (stream->stream_type == GORILLA_STREAM_BOOL) {
    BooleanEncoderWrapper *wrapper =
        (BooleanEncoderWrapper *)stream->encoder_impl;
    if (wrapper) {
      if (wrapper->encoder) {
        boolean_stream_encoder_destroy(wrapper->encoder);
      }
      if (wrapper->adapter) {
        free(wrapper->adapter);
      }
      free(wrapper);
      stream->encoder_impl = NULL;
    }
  } else if (stream->stream_type == GORILLA_STREAM_STRING) {
    StringEncoderWrapper *wrapper =
        (StringEncoderWrapper *)stream->encoder_impl;
    if (wrapper) {
      if (wrapper->encoder) {
        string_stream_encoder_destroy(wrapper->encoder);
      }
      if (wrapper->adapter) {
        free(wrapper->adapter);
      }
      free(wrapper);
      stream->encoder_impl = NULL;
    }
  }
}
