#ifndef GORILLA_STREAM_DECODER_H
#define GORILLA_STREAM_DECODER_H

#include "gorilla/float_stream_decoder.h"
#include "gorilla/gorilla_stream_types.h"
#include "gorilla/integer_stream_decoder.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Opaque type for the Gorilla decoder stream.
 *
 * This structure holds the stream type, the fill callback (and its context),
 * and a pointer to the underlying decoder implementation. For integer streams,
 * decoder_impl points to an IntegerStreamDecoder; for floating-point streams,
 * it points to a FloatStreamDecoder.
 */
typedef struct {
  gorilla_stream_type_t stream_type;
  gorilla_decoder_fill_cb fill_cb;
  void *fill_ctx;
  void *decoder_impl;
} gorilla_decoder_stream_t;

/**
 * @brief Initialize the Gorilla decoder stream.
 *
 * Sets up the internal state and creates the appropriate underlying decoder
 * based on the specified stream type (e.g. GORILLA_STREAM_INT or
 * GORILLA_STREAM_FLOAT).
 *
 * @param decoder    Pointer to the decoder stream state to initialize.
 * @param stream_type The type of stream (e.g. GORILLA_STREAM_INT or
 * GORILLA_STREAM_FLOAT).
 * @param fill_cb    Callback function to fill data for decoding.
 * @param fill_ctx   User-supplied context for the fill callback.
 *
 * @return true on success; false on error.
 */
bool gorilla_decoder_init(gorilla_decoder_stream_t *decoder,
                          gorilla_stream_type_t stream_type,
                          gorilla_decoder_fill_cb fill_cb, void *fill_ctx);

/**
 * @brief Retrieve the next integer value from the Gorilla decoder stream.
 *
 * For streams of type GORILLA_STREAM_INT, this function uses the underlying
 * integer decoder to obtain the next value.
 *
 * @param decoder   Pointer to the decoder stream.
 * @param timestamp Out parameter to store the next decoded integer value.
 *
 * @return true if a value was successfully decoded; false on error or
 * end-of-data.
 */
bool gorilla_decoder_get_timestamp(gorilla_decoder_stream_t *decoder,
                                   uint64_t *timestamp);

/**
 * @brief Retrieve the next floating-point value from the Gorilla decoder
 * stream.
 *
 * For streams of type GORILLA_STREAM_FLOAT, this function uses the underlying
 * float decoder to obtain the next value.
 *
 * @param decoder Pointer to the decoder stream.
 * @param value   Out parameter to store the next decoded double value.
 *
 * @return true if a value was successfully decoded; false on error or
 * end-of-data.
 */
bool gorilla_decoder_get_float(gorilla_decoder_stream_t *decoder,
                               double *value);

bool gorilla_decoder_get_boolean(gorilla_decoder_stream_t *decoder,
                                 bool *value);
bool gorilla_decoder_get_string(gorilla_decoder_stream_t *decoder,
                                uint8_t **out_data, size_t *out_length);

/**
 * @brief Finalize the decoding process.
 *
 * If an underlying decoder was created, this function finalizes it.
 *
 * @param decoder Pointer to the decoder stream.
 *
 * @return true if finalization succeeded; false otherwise.
 */
bool gorilla_decoder_finish(gorilla_decoder_stream_t *decoder);

/**
 * @brief Deinitialize the Gorilla decoder stream.
 *
 * Cleans up any resources allocated during decoding.
 *
 * @param decoder Pointer to the decoder stream.
 */
void gorilla_decoder_deinit(gorilla_decoder_stream_t *decoder);

#ifdef __cplusplus
}
#endif

#endif /* GORILLA_STREAM_DECODER_H */
