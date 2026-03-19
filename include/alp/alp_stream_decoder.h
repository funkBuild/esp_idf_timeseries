#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ALP_STREAM_BLOCK_SIZE 128

/**
 * @brief Streaming ALP decoder that decodes values one block at a time.
 *
 * Instead of decoding all values upfront into a large array, this decoder
 * lazily decodes blocks of 128 values as they are consumed. This reduces
 * peak heap usage from O(N) to O(1) per iterator.
 *
 * The caller must keep the compressed data buffer alive for the lifetime
 * of the decoder (the decoder reads directly from it).
 */
typedef struct {
    const uint8_t *pos;       /* current read position in compressed stream */
    const uint8_t *end;       /* end of compressed data */
    int64_t running;          /* delta carry across blocks */
    uint16_t num_blocks;      /* total blocks in stream */
    uint16_t blocks_decoded;  /* blocks decoded so far */
    bool is_float;            /* true = ALP float, false = ALP int */

    /* Current decoded block cache (heap-allocated, ALP_STREAM_BLOCK_SIZE * 8 bytes).
     * Cast to double* for float streams, int64_t* for int streams. */
    void *block_buf;
    uint16_t block_count;     /* values in current decoded block */
    uint16_t block_offset;    /* next value index to return (0..block_count-1) */

    uint8_t *scratch;         /* heap-allocated scratch for float FFOR decode */
} alp_stream_decoder_t;

/**
 * @brief Initialize the streaming ALP decoder.
 *
 * Parses the stream header and prepares for block-by-block decoding.
 * Does NOT decode any values — first block is decoded on first next() call.
 *
 * @param dec       Decoder state (caller-owned, e.g. stack or struct field)
 * @param data      Pointer to compressed ALP data (must remain valid)
 * @param len       Length of compressed data in bytes
 * @param is_float  true for ALP float streams, false for ALP int streams
 * @return true on success, false if header is invalid
 */
bool alp_stream_decoder_init(alp_stream_decoder_t *dec,
                              const uint8_t *data, size_t len,
                              bool is_float);

/**
 * @brief Release resources owned by the streaming decoder.
 *
 * Frees the internal scratch buffer allocated during init.  Must be called
 * when the decoder is no longer needed.  Safe to call on a zero-initialised
 * or already-deinited decoder (no-op).
 */
void alp_stream_decoder_deinit(alp_stream_decoder_t *dec);

/**
 * @brief Get the next decoded float value.
 * Automatically decodes the next block when current one is exhausted.
 * @return true if a value was returned, false if no more data
 */
bool alp_stream_decoder_next_float(alp_stream_decoder_t *dec, double *out);

/**
 * @brief Get the next decoded int64 value.
 * Automatically decodes the next block when current one is exhausted.
 * @return true if a value was returned, false if no more data
 */
bool alp_stream_decoder_next_int(alp_stream_decoder_t *dec, int64_t *out);

/**
 * @brief Skip `count` values in the stream.
 * Used for time-range pruning — advances past values without returning them.
 * @return true if all values were skipped, false if stream ended early
 */
bool alp_stream_decoder_skip(alp_stream_decoder_t *dec, size_t count);

#ifdef __cplusplus
}
#endif
