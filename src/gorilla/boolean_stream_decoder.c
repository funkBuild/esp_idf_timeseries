#include "gorilla/boolean_stream_decoder.h"
#include <stdlib.h>

BooleanStreamDecoder *boolean_stream_decoder_create(FillCallback fill_cb,
                                                    void *fill_ctx) {
  if (!fill_cb)
    return NULL;
  BooleanStreamDecoder *dec =
      (BooleanStreamDecoder *)malloc(sizeof(BooleanStreamDecoder));
  if (!dec)
    return NULL;
  // Initialize the internal BitReader with the fill callback.
  bitreader_init(&dec->br, fill_cb, fill_ctx);
  return dec;
}

bool boolean_stream_decoder_get_value(BooleanStreamDecoder *dec, bool *value) {
  if (!dec || !value)
    return false;
  uint64_t bit;
  // Read one bit from the BitReader.
  if (!bitreader_read(&dec->br, 1, &bit))
    return false;
  *value = (bit != 0);
  return true;
}

bool boolean_stream_decoder_finish(BooleanStreamDecoder *dec) {
  // No additional cleanup is needed.
  return true;
}

void boolean_stream_decoder_destroy(BooleanStreamDecoder *dec) {
  if (dec)
    free(dec);
}
