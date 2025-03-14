#include "gorilla/integer_stream_encoder.h"
#include "gorilla/zigzag_stream.h"
#include <stdlib.h>

IntegerStreamEncoder *integer_stream_encoder_create(FlushCallback flush_cb,
                                                    void *flush_ctx) {
  if (!flush_cb)
    return NULL;

  IntegerStreamEncoder *encoder =
      (IntegerStreamEncoder *)malloc(sizeof(IntegerStreamEncoder));
  if (!encoder)
    return NULL;

  encoder->flush_cb = flush_cb;
  encoder->flush_ctx = flush_ctx;
  encoder->has_first = false;
  encoder->has_second = false;
  encoder->first_value = 0;
  encoder->last_value = 0;
  encoder->last_delta = 0;

  encoder->simple8b_encoder =
      simple8b_stream_encoder_create(flush_cb, flush_ctx);
  if (!encoder->simple8b_encoder) {
    free(encoder);
    return NULL;
  }
  return encoder;
}

void integer_stream_encoder_destroy(IntegerStreamEncoder *encoder) {
  if (!encoder)
    return;

  if (encoder->simple8b_encoder)
    simple8b_stream_encoder_destroy(encoder->simple8b_encoder);
  free(encoder);
}

bool integer_stream_encoder_add_value(IntegerStreamEncoder *encoder,
                                      uint64_t value) {
  if (!encoder)
    return false;

  // First value: store absolutely.
  if (!encoder->has_first) {
    encoder->first_value = value;
    encoder->has_first = true;

    if (!simple8b_stream_encoder_add(encoder->simple8b_encoder,
                                     encoder->first_value))
      return false;
  }
  // Second value: compute delta relative to the first.
  else if (!encoder->has_second) {
    int64_t delta = (int64_t)(value - encoder->first_value);
    encoder->last_delta = delta;
    encoder->last_value = value;
    encoder->has_second = true;
    uint64_t encoded_delta = zigzag_encode(delta);

    if (!simple8b_stream_encoder_add(encoder->simple8b_encoder, encoded_delta))
      return false;
  }
  // Subsequent values: compute second-order difference.
  else {
    int64_t D = (int64_t)(value - encoder->last_value) - encoder->last_delta;
    encoder->last_delta += D;
    encoder->last_value = value;
    uint64_t encoded_D = zigzag_encode(D);

    if (!simple8b_stream_encoder_add(encoder->simple8b_encoder, encoded_D))
      return false;
  }
  return true;
}

bool integer_stream_encoder_flush(IntegerStreamEncoder *encoder) {
  if (!encoder)
    return false;
  return simple8b_stream_encoder_flush(encoder->simple8b_encoder);
}

bool integer_stream_encoder_finish(IntegerStreamEncoder *encoder) {
  if (!encoder)
    return false;
  return simple8b_stream_encoder_finish(encoder->simple8b_encoder);
}
