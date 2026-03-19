#include "alp/alp_decoder.h"
#include "alp/alp_encoder.h"
#include "alp/alp_constants.h"
#include "alp_ffor.h"
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <stdint.h>
#include <limits.h>

/*
 * Scratch buffer layout (single allocation):
 *
 *   [ decoded_ints  | packed_data    | exc_positions ]
 *   [ 128 × int64  | 129 × uint64   | 128 × uint16  ]
 *   [ 1024 B        | 1032 B         | 256 B          ]
 *
 * exc_values reuses packed_data's memory — FFOR unpack is complete before
 * exception data is read, so the two never overlap in time. Saves 1024 bytes.
 */
#define SCRATCH_INT_SZ   (ALP_BLOCK_SIZE * sizeof(int64_t))   /* 1024 */
#define SCRATCH_PACK_SZ  ((ALP_BLOCK_SIZE + 1) * sizeof(uint64_t))  /* 1032 */
#define SCRATCH_POS_SZ   (ALP_BLOCK_SIZE * sizeof(uint16_t))  /*  256 */
#define SCRATCH_TOTAL    (SCRATCH_INT_SZ + SCRATCH_PACK_SZ + SCRATCH_POS_SZ)

__attribute__((optimize("O3")))
bool alp_decode(const uint8_t * restrict data, size_t data_len,
                double * restrict out, size_t count) {
    if (!data || !out || count == 0) return false;

    const uint8_t *p   = data;
    const uint8_t *end = data + data_len;

    /* ------------------------------------------------------------------ */
    /* Stream header (24 bytes)                                            */
    /* ------------------------------------------------------------------ */
    if (p + 24 > end) return false;

    uint32_t magic;
    memcpy(&magic, p, 4); p += 4;
    if (magic != ALP_MAGIC) return false;

    uint32_t total_values;
    memcpy(&total_values, p, 4); p += 4;

    uint16_t num_blocks;
    memcpy(&num_blocks, p, 2); p += 2;

    p += 2; /* tail_count — we use block_count from each block header */

    uint8_t scheme = *p++;
    p += 3; /* padding */

    int64_t anchor;
    memcpy(&anchor, p, 8); p += 8;

    if (scheme != ALP_SCHEME_ALP) return false;

    /* ------------------------------------------------------------------ */
    /* Heap scratch for all temporary buffers (2304 B — avoids blowing   */
    /* 4096-byte FreeRTOS task stacks)                                    */
    /* ------------------------------------------------------------------ */
    uint8_t *scratch = (uint8_t *)malloc(SCRATCH_TOTAL);
    if (!scratch) return false;

    int64_t  *decoded_ints  = (int64_t  *)(scratch);
    uint64_t *packed_data   = (uint64_t *)(scratch + SCRATCH_INT_SZ);
    uint16_t *exc_positions = (uint16_t *)(scratch + SCRATCH_INT_SZ + SCRATCH_PACK_SZ);
    /* exc_values reuses packed_data — safe because FFOR unpack is finished
       before exception data is read from the stream. */
    uint64_t *exc_values    = packed_data;

    size_t out_idx = 0;
    int64_t running = anchor;

    for (uint16_t block = 0; block < num_blocks && out_idx < count; block++) {
        /* ---------------------------------------------------------------- */
        /* Block header (16 bytes)                                          */
        /* ---------------------------------------------------------------- */
        if (p + 16 > end) goto fail;

        uint64_t bh0; memcpy(&bh0, p, 8); p += 8;
        uint64_t bh1; memcpy(&bh1, p, 8); p += 8;

        uint8_t  exp             = (uint8_t) (bh0        & 0xFF);
        uint8_t  fac             = (uint8_t) ((bh0 >>  8) & 0xFF);
        if (exp > 18) exp = 18;
        if (fac > 18) fac = 18;
        uint8_t  bw              = (uint8_t) ((bh0 >> 16) & 0xFF);
        if (bw > 64) goto fail;
        uint16_t exception_count = (uint16_t)((bh0 >> 32) & 0xFFFF);
        uint16_t block_count     = (uint16_t)((bh0 >> 48) & 0xFFFF);

        int64_t for_base;
        memcpy(&for_base, &bh1, 8);

        if (block_count == 0 || block_count > ALP_BLOCK_SIZE) goto fail;
        if (exception_count > block_count) goto fail;

        /* ---------------------------------------------------------------- */
        /* FFOR-packed data                                                 */
        /* ---------------------------------------------------------------- */
        size_t packed_words = alp_ffor_packed_words(block_count, bw);
        if (packed_words * 8 > (size_t)(end - p)) goto fail;
        memcpy(packed_data, p, packed_words * 8);
        p += packed_words * 8;

        alp_ffor_unpack(packed_data, block_count, for_base, bw, decoded_ints);

        /* ---------------------------------------------------------------- */
        /* Delta decode: prefix-sum with running carry across blocks.       */
        /* Track min/max for the int32 fast-path check below.               */
        /* ---------------------------------------------------------------- */
        decoded_ints[0] += running;
        int64_t blk_min = decoded_ints[0], blk_max = decoded_ints[0];
        for (size_t i = 1; i < block_count; i++) {
            decoded_ints[i] += decoded_ints[i - 1];
            if (decoded_ints[i] < blk_min) blk_min = decoded_ints[i];
            if (decoded_ints[i] > blk_max) blk_max = decoded_ints[i];
        }
        running = decoded_ints[block_count - 1];

        /* ---------------------------------------------------------------- */
        /* Exception data                                                   */
        /* ---------------------------------------------------------------- */
        if (exception_count > 0) {
            size_t pos_words = ((size_t)exception_count * 2 + 7) / 8;
            if (pos_words * 8 > (size_t)(end - p)) goto fail;

            for (uint16_t i = 0; i < exception_count; i++) {
                size_t word_idx = i / 4;
                size_t off      = (size_t)(i % 4) * 16;
                uint64_t w;
                memcpy(&w, p + word_idx * 8, 8);
                exc_positions[i] = (uint16_t)((w >> off) & 0xFFFF);
            }
            p += pos_words * 8;

            if ((size_t)exception_count * 8 > (size_t)(end - p)) goto fail;
            memcpy(exc_values, p, (size_t)exception_count * 8);
            p += (size_t)exception_count * 8;
        }

        /* ---------------------------------------------------------------- */
        /* Reconstruct doubles                                              */
        /* ---------------------------------------------------------------- */

        /* Precompute block-level scale factor: one multiply per value instead
         * of one multiply + one divide.  fac and exp are block constants so
         * this division happens once per block, not once per value.          */
        double scale = ALP_SCALE_FACTORS[fac] / ALP_SCALE_FACTORS[exp];

        /* How many values from this block will actually land in `out` */
        size_t to_decode = block_count;
        if (to_decode > count - out_idx)
            to_decode = count - out_idx;

        if (__builtin_expect(exception_count == 0, 1)) {
            /* Fast path: no exceptions.
             * On Xtensa LX7, int64→double uses software __fixdfdi (~20-50 cycles).
             * int32→double uses hardware FCVT.D.W (~4 cycles).
             * Use int32 cast when all post-prefix-sum values fit in int32 range. */
            bool use_i32 = (blk_min >= (int64_t)INT32_MIN) &&
                           (blk_max <= (int64_t)INT32_MAX);
            if (use_i32) {
#pragma GCC ivdep
                for (size_t i = 0; i < to_decode; i++) {
                    out[out_idx + i] = (double)(int32_t)decoded_ints[i] * scale;
                }
            } else {
#pragma GCC ivdep
                for (size_t i = 0; i < to_decode; i++) {
                    out[out_idx + i] = (double)decoded_ints[i] * scale;
                }
            }
            out_idx += to_decode;
        } else {
            /* Slow path: exception substitution required */
            uint16_t exc_idx = 0;
            for (size_t i = 0; i < to_decode; i++) {
                if (exc_idx < exception_count && exc_positions[exc_idx] == (uint16_t)i) {
                    memcpy(&out[out_idx], &exc_values[exc_idx], 8);
                    exc_idx++;
                } else {
                    out[out_idx] = (double)decoded_ints[i] * scale;
                }
                out_idx++;
            }
        }
    }

    free(scratch);
    return (out_idx == count);

fail:
    free(scratch);
    return false;
}
