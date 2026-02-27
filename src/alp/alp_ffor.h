#pragma once
#include <stdint.h>
#include <stddef.h>
#include <string.h>

/* Returns number of uint64_t words needed to pack `count` values at `bw` bits each */
static inline size_t alp_ffor_packed_words(size_t count, uint8_t bw) {
    if (bw == 0) return 0;
    return ((size_t)count * bw + 63) / 64;
}

/* Pack `count` int64 values (relative to `base`) at `bw` bits each into `out` */
static inline void alp_ffor_pack(const int64_t *values, size_t count,
                                  int64_t base, uint8_t bw, uint64_t *out) {
    if (bw == 0) return;
    if (bw == 64) {
        for (size_t i = 0; i < count; i++) {
            out[i] = (uint64_t)(values[i] - base);
        }
        return;
    }
    /* Fast paths for power-of-2 widths mirror the unpack fast paths.
     * Cases 8/16/32 write independent elements so #pragma GCC ivdep is safe.
     * Cases 1/2/4 use |= into the same word so ivdep is omitted. */
    switch (bw) {
        case 1: {
            size_t n_words = alp_ffor_packed_words(count, 1);
            memset(out, 0, n_words * 8);
            for (size_t i = 0; i < count; i++) {
                uint64_t delta = (uint64_t)(values[i] - base) & 1ULL;
                out[i >> 6] |= delta << (i & 63u);
            }
            return;
        }
        case 2: {
            size_t n_words = alp_ffor_packed_words(count, 2);
            memset(out, 0, n_words * 8);
            for (size_t i = 0; i < count; i++) {
                uint64_t delta = (uint64_t)(values[i] - base) & 3ULL;
                out[i >> 5] |= delta << ((i & 31u) * 2u);
            }
            return;
        }
        case 4: {
            size_t n_words = alp_ffor_packed_words(count, 4);
            memset(out, 0, n_words * 8);
            for (size_t i = 0; i < count; i++) {
                uint64_t delta = (uint64_t)(values[i] - base) & 0xFULL;
                out[i >> 4] |= delta << ((i & 15u) * 4u);
            }
            return;
        }
        default:
            break;
    }
    size_t n_words = alp_ffor_packed_words(count, bw);
    memset(out, 0, n_words * sizeof(uint64_t));
    uint64_t mask = (bw < 64) ? ((1ULL << bw) - 1) : ~(uint64_t)0;
    size_t   word_idx = 0;
    unsigned bit_off  = 0;
    for (size_t i = 0; i < count; i++) {
        uint64_t val = (uint64_t)(values[i] - base) & mask;
        out[word_idx] |= val << bit_off;
        unsigned new_off = bit_off + bw;
        if (new_off > 64) {
            out[word_idx + 1] |= val >> (64u - bit_off);
            word_idx++;
            bit_off = new_off - 64;
        } else if (new_off == 64) {
            word_idx++;
            bit_off = 0;
        } else {
            bit_off = new_off;
        }
    }
}

/* Unpack `count` int64 values from FFOR-packed `in` (at `bw` bits, base `base`) into `out`.
 *
 * Fast paths for power-of-2 widths (1, 2, 4, 8, 16, 32) exploit word-aligned
 * packing to avoid per-element bit arithmetic.  Values are stored as unsigned
 * deltas from base; we cast to the appropriate unsigned type then add the
 * (possibly negative) signed base.
 */
static inline void alp_ffor_unpack(const uint64_t *in, size_t count,
                                    int64_t base, uint8_t bw, int64_t *out) {
    switch (bw) {
        case 0:
            /* All values equal base */
#pragma GCC ivdep
            for (size_t i = 0; i < count; i++) out[i] = base;
            return;

        case 1: {
            /* 64 values per word */
            const uint64_t *src = in;
            size_t i = 0;
            while (i + 64 <= count) {
                uint64_t w = *src++;
#pragma GCC ivdep
                for (unsigned b = 0; b < 64; b++, i++)
                    out[i] = (int64_t)((w >> b) & 1u) + base;
            }
            if (i < count) {
                uint64_t w = *src;
                for (; i < count; i++)
                    out[i] = (int64_t)((w >> (i & 63u)) & 1u) + base;
            }
            return;
        }

        case 2: {
            /* 32 values per word */
            const uint64_t *src = in;
            size_t i = 0;
            while (i + 32 <= count) {
                uint64_t w = *src++;
#pragma GCC ivdep
                for (unsigned b = 0; b < 64; b += 2, i++)
                    out[i] = (int64_t)((w >> b) & 3u) + base;
            }
            if (i < count) {
                uint64_t w = *src;
                for (; i < count; i++)
                    out[i] = (int64_t)((w >> ((i & 31u) * 2u)) & 3u) + base;
            }
            return;
        }

        case 4: {
            /* 16 values per word */
            const uint64_t *src = in;
            size_t i = 0;
            while (i + 16 <= count) {
                uint64_t w = *src++;
#pragma GCC ivdep
                for (unsigned b = 0; b < 64; b += 4, i++)
                    out[i] = (int64_t)((w >> b) & 0xFu) + base;
            }
            if (i < count) {
                uint64_t w = *src;
                for (; i < count; i++)
                    out[i] = (int64_t)((w >> ((i & 15u) * 4u)) & 0xFu) + base;
            }
            return;
        }

        case 8: {
            /* 8 values per word — cast to byte array, unsigned deltas */
            const uint8_t *bytes = (const uint8_t *)in;
#pragma GCC ivdep
            for (size_t i = 0; i < count; i++)
                out[i] = (int64_t)bytes[i] + base;
            return;
        }

        case 16: {
            /* 4 values per word — cast to uint16_t array, unsigned deltas */
            const uint16_t *shorts = (const uint16_t *)in;
#pragma GCC ivdep
            for (size_t i = 0; i < count; i++)
                out[i] = (int64_t)shorts[i] + base;
            return;
        }

        case 32: {
            /* 2 values per word — cast to uint32_t array, unsigned deltas */
            const uint32_t *words = (const uint32_t *)in;
#pragma GCC ivdep
            for (size_t i = 0; i < count; i++)
                out[i] = (int64_t)words[i] + base;
            return;
        }

        case 64:
#pragma GCC ivdep
            for (size_t i = 0; i < count; i++) out[i] = (int64_t)in[i] + base;
            return;

        default:
            break;
    }

    /* Generic path for odd bit widths (3, 5, 6, 7, 9, …, 63).
     * Track word_idx and bit_off incrementally to avoid >> 6 and & 63 per
     * iteration.  bit_off is maintained in [0, 63]; word_idx advances when
     * a value spans two words (bit_off + bw >= 64 after extraction). */
    uint64_t mask = (1ULL << bw) - 1;
    size_t   word_idx = 0;
    unsigned bit_off  = 0;
    for (size_t i = 0; i < count; i++) {
        uint64_t val = (in[word_idx] >> bit_off) & mask;
        unsigned new_off = bit_off + bw;
        if (new_off > 64) {
            /* Value spans two words: take (64-bit_off) bits from current word,
             * remaining (bw - (64-bit_off)) = (new_off-64) bits from next.   */
            val |= (in[word_idx + 1] << (64u - bit_off)) & mask;
            word_idx++;
            bit_off = new_off - 64;
        } else if (new_off == 64) {
            word_idx++;
            bit_off = 0;
        } else {
            bit_off = new_off;
        }
        out[i] = (int64_t)val + base;
    }
}
