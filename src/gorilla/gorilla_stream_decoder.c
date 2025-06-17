#include "gorilla/gorilla_stream_decoder.h"
#include "gorilla/bit_reader.h"
#include "gorilla/boolean_stream_decoder.h"
#include "gorilla/float_stream_decoder.h" // New float decoder interface.
#include "gorilla/gorilla_stream_types.h"
#include "gorilla/integer_stream_decoder.h" // Existing integer decoder interface.
#include "gorilla/string_stream_decoder.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "esp_log.h"

static const char *TAG = "GORILLA_DECODER";

bool gorilla_decoder_init(gorilla_decoder_stream_t *decoder,
                          gorilla_stream_type_t stream_type,
                          gorilla_decoder_fill_cb fill_cb, void *fill_ctx) {
  if (!decoder || !fill_cb) {
    return false;
  }

  decoder->stream_type = stream_type;
  decoder->fill_cb = fill_cb;
  decoder->fill_ctx = fill_ctx;
  decoder->decoder_impl = NULL;

  if (stream_type == GORILLA_STREAM_INT) {
    decoder->decoder_impl =
        (void *)integer_stream_decoder_create(fill_cb, fill_ctx);
    if (!decoder->decoder_impl) {
      return false;
    }
  } else if (stream_type == GORILLA_STREAM_FLOAT) {
    decoder->decoder_impl =
        (void *)float_stream_decoder_create(fill_cb, fill_ctx);
    if (!decoder->decoder_impl) {
      return false;
    }
  } else if (stream_type == GORILLA_STREAM_BOOL) {
    decoder->decoder_impl =
        (void *)boolean_stream_decoder_create(fill_cb, fill_ctx);
    if (!decoder->decoder_impl) {
      return false;
    }
  } else if (stream_type == GORILLA_STREAM_STRING) {
    decoder->decoder_impl =
        (void *)string_stream_decoder_create(fill_cb, fill_ctx);
    if (!decoder->decoder_impl) {
      return false;
    }
  }
  return true;
}

bool gorilla_decoder_get_timestamp(gorilla_decoder_stream_t *decoder,
                                   uint64_t *timestamp) {
  if (!decoder || !timestamp) {
    return false;
  }

  if (decoder->stream_type == GORILLA_STREAM_INT &&
      decoder->decoder_impl != NULL) {
    return integer_stream_decoder_get_value(
        (IntegerStreamDecoder *)decoder->decoder_impl, timestamp);
  }

  // Fallback: if not an integer stream, return an error.
  return false;
}

bool gorilla_decoder_get_float(gorilla_decoder_stream_t *decoder,
                               double *value) {
  if (!decoder || !value) {
    return false;
  }

  if (decoder->stream_type == GORILLA_STREAM_FLOAT &&
      decoder->decoder_impl != NULL) {
    return float_stream_decoder_get_value(
        (FloatStreamDecoder *)decoder->decoder_impl, value);
  }

  // Fallback: if not a float stream, return an error.
  return false;
}

bool gorilla_decoder_get_boolean(gorilla_decoder_stream_t *decoder,
                                 bool *value) {
  if (!decoder || !value) {
    return false;
  }

  if (decoder->stream_type == GORILLA_STREAM_BOOL &&
      decoder->decoder_impl != NULL) {
    return boolean_stream_decoder_get_value(
        (BooleanStreamDecoder *)decoder->decoder_impl, value);
  }
  // Fallback: if not a boolean stream, return an error.
  return false;
}

bool gorilla_decoder_get_string(gorilla_decoder_stream_t *decoder,
                                uint8_t **out_data, size_t *out_length) {

  if (!decoder) {
    ESP_LOGE(TAG, "Decoder is NULL");
  }

  if (!out_data) {
    ESP_LOGE(TAG, "out_data is NULL");
  }

  if (!out_length) {
    ESP_LOGE(TAG, "out_length is NULL");
  }

  if (!decoder || !out_data || !out_length) {
    return false;
  }
  if (decoder->stream_type == GORILLA_STREAM_STRING && decoder->decoder_impl) {
    return string_stream_decoder_get_value(
        (StringStreamDecoder *)decoder->decoder_impl, out_data, out_length);
  }
  return false;
}

bool gorilla_decoder_finish(gorilla_decoder_stream_t *decoder) {
  if (!decoder) {
    return false;
  }
  if (decoder->stream_type == GORILLA_STREAM_INT &&
      decoder->decoder_impl != NULL) {
    return integer_stream_decoder_finish(
        (IntegerStreamDecoder *)decoder->decoder_impl);
  } else if (decoder->stream_type == GORILLA_STREAM_FLOAT &&
             decoder->decoder_impl != NULL) {
    return float_stream_decoder_finish(
        (FloatStreamDecoder *)decoder->decoder_impl);
  } else if (decoder->stream_type == GORILLA_STREAM_BOOL &&
             decoder->decoder_impl != NULL) {
    return boolean_stream_decoder_finish(
        (BooleanStreamDecoder *)decoder->decoder_impl);
  } else if (decoder->stream_type == GORILLA_STREAM_STRING &&
             decoder->decoder_impl) {
    return string_stream_decoder_finish(
        (StringStreamDecoder *)decoder->decoder_impl);
  }
  return true;
}

void gorilla_decoder_deinit(gorilla_decoder_stream_t *decoder) {
  if (!decoder) {
    return;
  }
  if (decoder->stream_type == GORILLA_STREAM_INT &&
      decoder->decoder_impl != NULL) {
    integer_stream_decoder_destroy(
        (IntegerStreamDecoder *)decoder->decoder_impl);
    decoder->decoder_impl = NULL;
  } else if (decoder->stream_type == GORILLA_STREAM_FLOAT &&
             decoder->decoder_impl != NULL) {
    float_stream_decoder_destroy((FloatStreamDecoder *)decoder->decoder_impl);
    decoder->decoder_impl = NULL;
  } else if (decoder->stream_type == GORILLA_STREAM_BOOL &&
             decoder->decoder_impl != NULL) {
    boolean_stream_decoder_destroy(
        (BooleanStreamDecoder *)decoder->decoder_impl);
    decoder->decoder_impl = NULL;
  } else if (decoder->stream_type == GORILLA_STREAM_STRING) {
    string_stream_decoder_destroy((StringStreamDecoder *)decoder->decoder_impl);
  }
}
