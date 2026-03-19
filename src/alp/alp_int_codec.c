#include "alp/alp_int_codec.h"
#include "alp_ffor.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>

/* Compute the number of bits needed to represent the range [min_val, max_val]. */
static uint8_t int_required_bw(int64_t min_val, int64_t max_val) {
    if (max_val <= min_val) return 0;
    /* Subtract in unsigned to avoid signed overflow UB when
     * min_val=INT64_MIN and max_val=INT64_MAX. */
    uint64_t range = (uint64_t)max_val - (uint64_t)min_val;
    return (uint8_t)(64 - __builtin_clzll(range));
}

/*
 * Encode one block of `count` delta values into `out_buf`.
 * No scratch needed — FFOR pack writes directly into out_buf+16.
 * Returns bytes written (always 16 + packed_words*8, never 0).
 */
__attribute__((optimize("O3")))
static size_t encode_block_int(const int64_t *deltas, size_t count,
                                uint8_t *out_buf) {
    /* Pass 1: find min and max */
    int64_t min_val = INT64_MAX, max_val = INT64_MIN;
    for (size_t i = 0; i < count; i++) {
        if (deltas[i] < min_val) min_val = deltas[i];
        if (deltas[i] > max_val) max_val = deltas[i];
    }

    uint8_t bw = int_required_bw(min_val, max_val);
    size_t packed_words = alp_ffor_packed_words(count, bw);

    /* Block header */
    uint64_t bh0 = (uint64_t)bw | ((uint64_t)count << 32);
    uint64_t bh1;
    memcpy(&bh1, &min_val, 8);
    memcpy(out_buf,     &bh0, 8);
    memcpy(out_buf + 8, &bh1, 8);

    /* Pass 2: FFOR pack directly into output (always 8-byte aligned) */
    alp_ffor_pack(deltas, count, min_val, bw, (uint64_t *)(out_buf + 16));

    return 16 + packed_words * 8;
}

/*
 * Delta-FFOR encoder.
 *
 * Stream format (20-byte header):
 *   magic(4) | count(4) | nblocks(2) | tail(2) | anchor(8)
 *
 * anchor = values[0].  Blocks contain deltas: delta[i] = values[i] - values[i-1],
 * with delta[0] of the first block = 0 (anchor is stored in the header).
 * Cross-block continuity: the first delta of block N is relative to the last
 * value of block N-1, computed by the encoder before blocking.
 */
__attribute__((optimize("O3")))
size_t alp_encode_int(const int64_t *values, size_t count, uint8_t **out) {
    if (!count || !values || !out) return 0;

    size_t num_blocks = (count + ALP_INT_BLOCK_SIZE - 1) / ALP_INT_BLOCK_SIZE;

    /* Allocate delta scratch — same size as input.  We compute deltas into this
     * buffer to avoid mutating the caller's array. */
    int64_t *deltas = (int64_t *)malloc(count * sizeof(int64_t));
    if (!deltas) return 0;

    int64_t anchor = values[0];
    deltas[0] = 0; /* first delta is 0; anchor stored in stream header */
    for (size_t i = 1; i < count; i++) {
        deltas[i] = values[i] - values[i - 1];
    }

    /* Conservative upper bound per block:
     *   header (16) + worst-case packed data (ALP_INT_BLOCK_SIZE * 8 bytes for bw=64)
     * Plus stream header (20 bytes). */
    size_t max_out = 20 + num_blocks * (16 + (ALP_INT_BLOCK_SIZE + 1) * 8);
    uint8_t *buf = (uint8_t *)malloc(max_out);
    if (!buf) { free(deltas); return 0; }

    /* Stream header (20 bytes) */
    uint8_t *p = buf;
    uint32_t magic = ALP_INT_MAGIC;
    uint32_t total = (uint32_t)count;
    uint16_t nb    = (uint16_t)num_blocks;
    uint16_t tail  = (uint16_t)(count % ALP_INT_BLOCK_SIZE);
    if (tail == 0 && count > 0) tail = ALP_INT_BLOCK_SIZE;

    memcpy(p, &magic,  4); p += 4;
    memcpy(p, &total,  4); p += 4;
    memcpy(p, &nb,     2); p += 2;
    memcpy(p, &tail,   2); p += 2;
    memcpy(p, &anchor, 8); p += 8;

    /* Encode each block of deltas */
    for (size_t b = 0; b < num_blocks; b++) {
        size_t offset = b * ALP_INT_BLOCK_SIZE;
        size_t bcount = ALP_INT_BLOCK_SIZE;
        if (offset + bcount > count) bcount = count - offset;

        size_t written = encode_block_int(deltas + offset, bcount, p);
        p += written;
    }

    free(deltas);

    size_t total_bytes = (size_t)(p - buf);
    uint8_t *compact = (uint8_t *)realloc(buf, total_bytes);
    *out = compact ? compact : buf;
    return total_bytes;
}

/*
 * Delta-FFOR decoder.
 *
 * Unpacks FFOR-packed deltas per block, then applies a running prefix-sum
 * starting from the anchor value stored in the stream header.
 */
__attribute__((optimize("O3")))
bool alp_decode_int(const uint8_t * restrict data, size_t data_len,
                    int64_t * restrict out, size_t count) {
    if (!data || !out || count == 0) return false;

    const uint8_t *p   = data;
    const uint8_t *end = data + data_len;

    /* Stream header (20 bytes):
     * magic(4) + total_values(4) + num_blocks(2) + tail(2) + anchor(8) */
    if (p + 20 > end) return false;

    uint32_t magic;
    memcpy(&magic, p, 4); p += 4;
    if (magic != ALP_INT_MAGIC) return false;

    uint32_t total_values;
    memcpy(&total_values, p, 4); p += 4;

    uint16_t num_blocks;
    memcpy(&num_blocks, p, 2); p += 2;

    p += 2; /* tail */

    int64_t anchor;
    memcpy(&anchor, p, 8); p += 8;

    int64_t *scratch = NULL;  /* heap-allocated only for partial last block */
    uint64_t *aligned_buf = NULL; /* heap-allocated for alignment */

    size_t out_idx = 0;

    for (uint16_t block = 0; block < num_blocks && out_idx < count; block++) {
        if (p + 16 > end) goto fail;

        uint64_t bh0; memcpy(&bh0, p, 8); p += 8;
        uint64_t bh1; memcpy(&bh1, p, 8); p += 8;

        uint8_t  bw          = (uint8_t)(bh0 & 0xFF);
        if (bw > 64) goto fail;
        uint16_t block_count = (uint16_t)((bh0 >> 32) & 0xFFFF);

        int64_t for_base;
        memcpy(&for_base, &bh1, 8);

        if (block_count == 0 || block_count > ALP_INT_BLOCK_SIZE) goto fail;

        size_t packed_words = alp_ffor_packed_words(block_count, bw);
        if (packed_words * 8 > (size_t)(end - p)) goto fail;

        size_t to_decode = block_count;
        if (to_decode > count - out_idx) to_decode = count - out_idx;

        /* p may not be 8-byte aligned (stream header is 20 bytes + 16-byte
         * block headers), so memcpy into an aligned buffer before calling
         * alp_ffor_unpack which dereferences uint64_t pointers directly. */
        const uint64_t *packed_ptr;
        if (packed_words > 0 && ((uintptr_t)p & 7u) != 0) {
            if (!aligned_buf) {
                aligned_buf = (uint64_t *)malloc((ALP_INT_BLOCK_SIZE + 1) * sizeof(uint64_t));
                if (!aligned_buf) goto fail;
            }
            memcpy(aligned_buf, p, packed_words * 8);
            packed_ptr = aligned_buf;
        } else {
            packed_ptr = (const uint64_t *)p;
        }

        if (to_decode == block_count) {
            /* Full block: unpack deltas directly into output */
            alp_ffor_unpack(packed_ptr, block_count,
                            for_base, bw, out + out_idx);
        } else {
            /* Partial block: heap-allocate scratch on first need */
            if (!scratch) {
                scratch = (int64_t *)malloc(ALP_INT_BLOCK_SIZE * sizeof(int64_t));
                if (!scratch) goto fail;
            }
            alp_ffor_unpack(packed_ptr, block_count,
                            for_base, bw, scratch);
            memcpy(out + out_idx, scratch, to_decode * sizeof(int64_t));
        }

        p += packed_words * 8;
        out_idx += to_decode;
    }

    free(scratch);
    free(aligned_buf);

    /* Prefix-sum: convert deltas back to absolute values.
     * out[0] is delta=0, so out[0] = anchor + 0 = anchor.
     * out[i] = out[i-1] + delta[i]. */
    if (out_idx > 0) {
        out[0] += anchor;
        for (size_t i = 1; i < out_idx; i++) {
            out[i] += out[i - 1];
        }
    }

    return (out_idx == count);

fail:
    free(scratch);
    free(aligned_buf);
    return false;
}
