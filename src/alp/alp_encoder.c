#include "alp/alp_encoder.h"
#include "alp/alp_constants.h"
#include "alp_ffor.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Integer divisor table: (int64_t)ALP_SCALE_FACTORS[i] — avoids double->int64 cast in hot path */
static const int64_t FACT_INT_ARR[19] = {
    1LL, 10LL, 100LL, 1000LL, 10000LL, 100000LL, 1000000LL, 10000000LL,
    100000000LL, 1000000000LL, 10000000000LL, 100000000000LL,
    1000000000000LL, 10000000000000LL, 100000000000000LL,
    1000000000000000LL, 10000000000000000LL, 100000000000000000LL,
    1000000000000000000LL
};

/* Compute the number of bits needed to represent the range [0, range] */
static uint8_t required_bw(int64_t min_val, int64_t max_val) {
    if (max_val <= min_val) return 0;
    /* Subtract in unsigned to avoid signed overflow UB. */
    uint64_t range = (uint64_t)max_val - (uint64_t)min_val;
    return (uint8_t)(64 - __builtin_clzll(range));
}

/*
 * Fast inline encoder — precomputed scale factors, no isfinite check.
 * fact_e  = ALP_SCALE_FACTORS[exp]
 * fac_div = FACT_INT_ARR[fac]  (integer divisor)
 * inv_scale = ALP_SCALE_FACTORS[fac] / ALP_SCALE_FACTORS[exp]  (round-trip scale)
 *
 * The caller must guarantee v is finite before calling this.
 */
static inline bool try_encode_fast(double v, double fact_e, int64_t fac_div,
                                    double inv_scale, int64_t *out) {
    double scaled  = v * fact_e;
    double rounded = round(scaled);
    if (rounded > (double)ALP_MAX_SAFE_INT || rounded < (double)ALP_MIN_SAFE_INT)
        return false;
    int64_t ri      = (int64_t)rounded;
    int64_t encoded = ri / fac_div;
    /* Round-trip check: encoded * inv_scale should equal v exactly */
    if ((double)encoded * inv_scale != v) return false;
    *out = encoded;
    return true;
}

/*
 * find_best_exp_fac — two-phase search for the best (exp, fac) pair.
 *
 * Phase 1: Quick scan over exp 0..9, fac 0..min(exp,4) with only 8 evenly-
 *          spaced samples.  If a 0-exception pair is found, go straight to
 *          Phase 3.  Note the best candidate found so far.
 *
 *          Optimization 1: After Phase 1, if best has >50% exceptions on the
 *          8-sample probe, skip Phase 2 entirely — data has no decimal
 *          structure and Phase 2 would waste ~15ms without benefit.
 *
 * Phase 2: Full-range scan (exp 0..18, fac 0..exp) using up to 128 samples,
 *          with early abandon if exceptions already exceed current best after
 *          25% of the sample.  Only reached when Phase 1 found nothing perfect
 *          AND Phase 1 best had <=50% exceptions.
 *
 * Phase 3: Validate winner on 32 samples before committing.
 */
__attribute__((optimize("O3")))
static void find_best_exp_fac(const double *values, size_t count,
                               uint8_t *best_exp, uint8_t *best_fac) {

    /* -----------------------------------------------------------------------
     * Common sample setup
     * -------------------------------------------------------------------- */
    size_t sample128 = (count < 128) ? count : 128;
    size_t step128   = count / sample128;
    if (step128 == 0) step128 = 1;

    size_t sample8 = (count < 8) ? count : 8;
    size_t step8   = count / sample8;
    if (step8 == 0) step8 = 1;

    size_t sample32 = (count < 32) ? count : 32;
    size_t step32   = count / sample32;
    if (step32 == 0) step32 = 1;

    size_t best_exceptions = sample128 + 1;
    *best_exp = 0;
    *best_fac = 0;

    int64_t dummy;

    /* -----------------------------------------------------------------------
     * Phase 1: quick scan — exp 0..9, fac 0..min(exp,4), 8 samples
     * -------------------------------------------------------------------- */
#define PHASE1_MAX_EXP 9
#define PHASE1_MAX_FAC 4

    bool   found_perfect      = false;
    size_t p1_best_exceptions = sample8 + 1;

    for (uint8_t e = 0; e <= PHASE1_MAX_EXP && !found_perfect; e++) {
        double   fact_e   = ALP_SCALE_FACTORS[e];
        uint8_t  f_limit  = (e < PHASE1_MAX_FAC) ? e : PHASE1_MAX_FAC;
        for (uint8_t f = 0; f <= f_limit && !found_perfect; f++) {
            int64_t  fac_div  = FACT_INT_ARR[f];
            double   inv_scale = ALP_SCALE_FACTORS[f] / fact_e;
            size_t   exceptions = 0;
            for (size_t i = 0; i < sample8; i++) {
                double v = values[i * step8];
                /* Phase 1 only runs with values we need to check isfinite on
                 * once; treat non-finite as exception */
                if (!isfinite(v) ||
                    !try_encode_fast(v, fact_e, fac_div, inv_scale, &dummy)) {
                    exceptions++;
                }
            }
            if (exceptions < p1_best_exceptions) {
                p1_best_exceptions = exceptions;
                best_exceptions    = exceptions;
                *best_exp = e;
                *best_fac = f;
                if (exceptions == 0) {
                    found_perfect = true;
                }
            }
        }
    }

    /* -----------------------------------------------------------------------
     * Optimization 1: Fast bail-out for non-compressible data.
     *
     * If the best Phase 1 pair has >50% exceptions on the 8-sample probe,
     * the data has no decimal structure.  Phase 2 would exhaust all 190 pairs
     * over 128 samples and take ~15ms without improving the result.  Skip it.
     *
     * The exception path in encode_block handles arbitrary exception rates
     * correctly (values stored as raw IEEE-754), so correctness is maintained.
     * -------------------------------------------------------------------- */
    if (!found_perfect) {
        size_t threshold = sample8 / 2;  /* 50% of the 8-sample probe */
        if (p1_best_exceptions > threshold) {
            /* Data has no decimal structure — skip Phase 2 entirely.
             * best_exp/best_fac already set to Phase 1 best. */
            goto phase3;
        }
    }

    /* -----------------------------------------------------------------------
     * Phase 2: full-range scan with early abandon — only when no perfect pair
     * was found in Phase 1.  Uses the Phase 1 best as the initial threshold.
     * -------------------------------------------------------------------- */
    if (!found_perfect) {
        /* Scale best_exceptions from 8-sample domain to 128-sample domain */
        size_t scaled_best = (best_exceptions * sample128 + sample8 - 1) / sample8;
        if (scaled_best > sample128) scaled_best = sample128;
        /* Use scaled threshold but don't let it be tighter than what we know */
        size_t p2_best = scaled_best;

        size_t abandon_at = sample128 / 4;  /* abandon after 25% of sample */

        for (uint8_t e = 0; e <= ALP_MAX_EXP; e++) {
            double   fact_e   = ALP_SCALE_FACTORS[e];
            for (uint8_t f = 0; f <= e; f++) {
                int64_t  fac_div   = FACT_INT_ARR[f];
                double   inv_scale = ALP_SCALE_FACTORS[f] / fact_e;
                size_t   exceptions = 0;
                bool     abandoned  = false;
                for (size_t i = 0; i < sample128; i++) {
                    double v = values[i * step128];
                    if (!isfinite(v) ||
                        !try_encode_fast(v, fact_e, fac_div, inv_scale, &dummy)) {
                        exceptions++;
                        /* Early abandon: after 25% of samples, if exceptions
                         * already exceed best, this pair cannot win */
                        if (i >= abandon_at && exceptions >= p2_best) {
                            abandoned = true;
                            break;
                        }
                    }
                }
                if (!abandoned && exceptions < p2_best) {
                    p2_best = exceptions;
                    *best_exp = e;
                    *best_fac = f;
                    if (exceptions == 0) {
                        found_perfect = true;
                        goto phase3;
                    }
                }
            }
        }
        /* Update best_exceptions with what Phase 2 found */
        best_exceptions = p2_best;
    }

phase3:
    ; /* goto target — Phase 3 validation was dead code and has been removed */
}

/*
 * Delta-FFOR ALP encoder.
 *
 * Stream format (24-byte header):
 *   magic(4) | count(4) | nblocks(2) | tail(2) | scheme(1) | pad(3) | anchor(8)
 *
 * All values are scaled to int64 via (exp, fac), then delta-encoded across the
 * entire stream.  anchor = encoded[0].  Exception positions (values that don't
 * round-trip) are stored as raw IEEE-754 in each block; their encoded slots are
 * set to the previous value so that delta = 0 at exception positions.
 *
 * Block binary layout (unchanged from non-delta version):
 *   [8 bytes] bh0: exp(8) | fac(8) | bw(8) | pad(8) | exc_count(16) | block_count(16)
 *   [8 bytes] bh1: for_base (int64_t, FFOR min of deltas)
 *   [packed_words * 8 bytes] FFOR-packed delta integers
 *   if exc_count > 0:
 *     [pos_words * 8 bytes] packed exception positions (4x uint16 per word)
 *     [exc_count * 8 bytes] exception raw IEEE-754 bit patterns
 */
__attribute__((optimize("O3")))
size_t alp_encode(const double *values, size_t count, uint8_t **out) {
    if (!count || !values || !out) return 0;

    uint8_t exp, fac;
    find_best_exp_fac(values, count, &exp, &fac);

    size_t num_blocks = (count + ALP_BLOCK_SIZE - 1) / ALP_BLOCK_SIZE;

    /* --- Global scaling + exception detection --- */
    int64_t *encoded = (int64_t *)malloc(count * sizeof(int64_t));
    bool    *is_exc  = (bool *)calloc(count, sizeof(bool));
    if (!encoded || !is_exc) { free(encoded); free(is_exc); return 0; }

    double  fact_e    = ALP_SCALE_FACTORS[exp];
    int64_t fac_div   = FACT_INT_ARR[fac];
    double  inv_scale = ALP_SCALE_FACTORS[fac] / fact_e;

    bool all_finite = true;
    for (size_t i = 0; i < count; i++) {
        if (!isfinite(values[i])) { all_finite = false; break; }
    }

    /* Pass 1: scale + range check */
    if (all_finite) {
        for (size_t i = 0; i < count; i++) {
            double scaled  = values[i] * fact_e;
            double rounded = round(scaled);
            if (rounded > (double)ALP_MAX_SAFE_INT ||
                rounded < (double)ALP_MIN_SAFE_INT) {
                is_exc[i] = true;
                encoded[i] = 0;
            } else {
                encoded[i] = (int64_t)rounded / fac_div;
            }
        }
    } else {
        for (size_t i = 0; i < count; i++) {
            double scaled  = values[i] * fact_e;
            double rounded = round(scaled);
            if (!isfinite(values[i]) ||
                rounded > (double)ALP_MAX_SAFE_INT ||
                rounded < (double)ALP_MIN_SAFE_INT) {
                is_exc[i] = true;
                encoded[i] = 0;
            } else {
                encoded[i] = (int64_t)rounded / fac_div;
            }
        }
    }

    /* Pass 2: round-trip verify */
    for (size_t i = 0; i < count; i++) {
        if (is_exc[i]) continue;
        if ((double)encoded[i] * inv_scale != values[i]) {
            is_exc[i] = true;
        }
    }

    /* Set exception placeholders to previous encoded value so delta = 0 */
    for (size_t i = 0; i < count; i++) {
        if (is_exc[i]) {
            encoded[i] = (i > 0) ? encoded[i - 1] : 0;
        }
    }

    /* Compute deltas in-place (backward pass to avoid overwrite) */
    int64_t anchor = encoded[0];
    for (size_t i = count - 1; i > 0; i--) {
        encoded[i] -= encoded[i - 1];
    }
    encoded[0] = 0; /* first delta is 0; anchor stored in stream header */

    /* --- Allocate output buffer --- */
    size_t max_out = 24 + num_blocks * (16 + ALP_BLOCK_SIZE * 8 + 256 + ALP_BLOCK_SIZE * 8 + 8);
    uint8_t *buf = (uint8_t *)malloc(max_out);
    if (!buf) { free(encoded); free(is_exc); return 0; }

    /* Stream header (24 bytes):
     *   magic(4) | total(4) | nb(2) | tail(2) | scheme(1) | pad(3) | anchor(8)
     */
    uint8_t *p = buf;
    uint32_t magic  = ALP_MAGIC;
    uint32_t total  = (uint32_t)count;
    uint16_t nb     = (uint16_t)num_blocks;
    uint16_t tail   = (uint16_t)(count % ALP_BLOCK_SIZE);
    if (tail == 0 && count > 0) tail = ALP_BLOCK_SIZE;
    uint8_t  scheme = ALP_SCHEME_ALP;

    memcpy(p, &magic,  4); p += 4;
    memcpy(p, &total,  4); p += 4;
    memcpy(p, &nb,     2); p += 2;
    memcpy(p, &tail,   2); p += 2;
    *p++ = scheme;
    memset(p, 0, 3);       p += 3;
    memcpy(p, &anchor, 8); p += 8;

    /* Scratch for FFOR packing + exception collection (reused across blocks).
     * Single allocation: packed[129] | exc_val[128] | exc_pos[128] */
    size_t scratch_size = (ALP_BLOCK_SIZE + 1 + ALP_BLOCK_SIZE) * sizeof(uint64_t)
                        + ALP_BLOCK_SIZE * sizeof(uint16_t);
    uint64_t *packed = (uint64_t *)malloc(scratch_size);
    if (!packed) { free(buf); free(encoded); free(is_exc); return 0; }
    uint64_t *exc_val = packed + (ALP_BLOCK_SIZE + 1);
    uint16_t *exc_pos = (uint16_t *)(exc_val + ALP_BLOCK_SIZE);

    /* --- Encode each block of deltas --- */
    for (size_t b = 0; b < num_blocks; b++) {
        size_t offset = b * ALP_BLOCK_SIZE;
        size_t bcount = ALP_BLOCK_SIZE;
        if (offset + bcount > count) bcount = count - offset;

        const int64_t *blk_deltas = encoded + offset;
        const bool    *blk_exc    = is_exc  + offset;
        const double  *blk_vals   = values  + offset;

        /* Find min/max of deltas in this block */
        int64_t min_val = INT64_MAX, max_val = INT64_MIN;
        for (size_t i = 0; i < bcount; i++) {
            if (blk_deltas[i] < min_val) min_val = blk_deltas[i];
            if (blk_deltas[i] > max_val) max_val = blk_deltas[i];
        }
        if (min_val == INT64_MAX) { min_val = 0; max_val = 0; }

        uint8_t bw = required_bw(min_val, max_val);
        size_t packed_words = alp_ffor_packed_words(bcount, bw);

        /* FFOR pack the deltas */
        alp_ffor_pack(blk_deltas, bcount, min_val, bw, packed);

        /* Collect exceptions for this block */
        uint16_t exc_count = 0;
        for (size_t i = 0; i < bcount; i++) {
            if (blk_exc[i]) {
                exc_pos[exc_count] = (uint16_t)i;
                uint64_t bits;
                memcpy(&bits, &blk_vals[i], 8);
                exc_val[exc_count] = bits;
                exc_count++;
            }
        }

        /* Block header (16 bytes) */
        uint64_t bh0 = (uint64_t)exp
                     | ((uint64_t)fac       <<  8)
                     | ((uint64_t)bw        << 16)
                     | ((uint64_t)0         << 24)
                     | ((uint64_t)exc_count << 32)
                     | ((uint64_t)bcount    << 48);

        int64_t for_base = min_val;
        uint64_t bh1;
        memcpy(&bh1, &for_base, 8);

        memcpy(p, &bh0, 8); p += 8;
        memcpy(p, &bh1, 8); p += 8;

        /* FFOR packed data */
        memcpy(p, packed, packed_words * 8);
        p += packed_words * 8;

        /* Exception positions — packed 4 per uint64_t word (16 bits each) */
        if (exc_count > 0) {
            size_t pos_words = ((size_t)exc_count * 2 + 7) / 8;
            memset(p, 0, pos_words * 8);
            for (uint16_t i = 0; i < exc_count; i++) {
                size_t word_idx = i / 4;
                size_t off = (size_t)(i % 4) * 16;
                ((uint64_t *)p)[word_idx] |= ((uint64_t)exc_pos[i]) << off;
            }
            p += pos_words * 8;
            memcpy(p, exc_val, (size_t)exc_count * 8);
            p += (size_t)exc_count * 8;
        }
    }

    free(packed);
    free(encoded);
    free(is_exc);

    size_t total_bytes = (size_t)(p - buf);
    uint8_t *compact = (uint8_t *)realloc(buf, total_bytes);
    *out = compact ? compact : buf;
    return total_bytes;
}
