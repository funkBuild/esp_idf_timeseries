#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ALP_INT_MAGIC      0x414C5002U
#define ALP_INT_BLOCK_SIZE 128

/*
 * Encode `count` int64_t values using FFOR (Frame-of-Reference) bit-packing.
 *
 * Allocates the output buffer; caller must free(*out).
 * Returns the number of bytes written, or 0 on failure.
 *
 * Compression is loss-less. Every value round-trips exactly.
 *
 * Stream format:
 *   [16 bytes] stream header: magic(4) | count(4) | nblocks(2) | tail(2) | pad(8)
 *   For each block:
 *     [8 bytes] bh0: bw(8) | pad(24) | block_count(16) | pad(16)
 *     [8 bytes] bh1: for_base (int64, little-endian)
 *     [packed_words * 8 bytes] FFOR-packed deltas (each value stored as value - for_base)
 *
 * where for_base = min(block values), bw = bits needed for (max - min).
 */
size_t alp_encode_int(const int64_t *values, size_t count, uint8_t **out);

/*
 * Decode `count` int64_t values from `data`.
 * Returns true on success, false on malformed input or truncated data.
 */
bool alp_decode_int(const uint8_t *data, size_t data_len,
                    int64_t *out, size_t count);

#ifdef __cplusplus
}
#endif
