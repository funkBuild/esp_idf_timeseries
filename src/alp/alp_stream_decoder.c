#include "alp/alp_stream_decoder.h"
#include "alp/alp_encoder.h"
#include "alp/alp_int_codec.h"
#include "alp/alp_constants.h"
#include "alp_ffor.h"

#include <stdlib.h>
#include <string.h>

/*
 * Scratch buffer for FFOR unpack + exception handling.
 * Reuses packed_data space for exc_values (same as alp_decoder.c).
 */
#define SCRATCH_INT_SZ   (ALP_STREAM_BLOCK_SIZE * sizeof(int64_t))  /* 1024 */
#define SCRATCH_PACK_SZ  ((ALP_STREAM_BLOCK_SIZE + 1) * sizeof(uint64_t)) /* 1032 */
#define SCRATCH_POS_SZ   (ALP_STREAM_BLOCK_SIZE * sizeof(uint16_t)) /*  256 */
#define SCRATCH_TOTAL    (SCRATCH_INT_SZ + SCRATCH_PACK_SZ + SCRATCH_POS_SZ)

/* ------------------------------------------------------------------ */
/* Float ALP: decode one block into dec->block_buf (as double[])      */
/* ------------------------------------------------------------------ */
static bool decode_next_float_block(alp_stream_decoder_t *dec) {
    if (dec->blocks_decoded >= dec->num_blocks)
        return false;

    const uint8_t *p = dec->pos;
    const uint8_t *end = dec->end;

    /* Block header (16 bytes) */
    if (p + 16 > end) return false;

    uint64_t bh0; memcpy(&bh0, p, 8); p += 8;
    uint64_t bh1; memcpy(&bh1, p, 8); p += 8;

    uint8_t  exp             = (uint8_t) (bh0        & 0xFF);
    uint8_t  fac             = (uint8_t) ((bh0 >>  8) & 0xFF);
    uint8_t  bw              = (uint8_t) ((bh0 >> 16) & 0xFF);
    if (bw > 64) return false;
    uint16_t exception_count = (uint16_t)((bh0 >> 32) & 0xFFFF);
    uint16_t block_count     = (uint16_t)((bh0 >> 48) & 0xFFFF);

    int64_t for_base;
    memcpy(&for_base, &bh1, 8);

    if (block_count == 0 || block_count > ALP_STREAM_BLOCK_SIZE)
        return false;
    if (exception_count > block_count)
        return false;

    /* Heap scratch cached in struct (2304 bytes, allocated once in init) */
    uint8_t *scratch = dec->scratch;
    if (!scratch) return false;
    int64_t  *decoded_ints  = (int64_t  *)(scratch);
    uint64_t *packed_data   = (uint64_t *)(scratch + SCRATCH_INT_SZ);
    uint16_t *exc_positions = (uint16_t *)(scratch + SCRATCH_INT_SZ + SCRATCH_PACK_SZ);
    uint64_t *exc_values    = packed_data; /* reuse after FFOR unpack */

    /* FFOR-packed data */
    size_t packed_words = alp_ffor_packed_words(block_count, bw);
    if (packed_words * 8 > (size_t)(end - p)) return false;
    memcpy(packed_data, p, packed_words * 8);
    p += packed_words * 8;

    alp_ffor_unpack(packed_data, block_count, for_base, bw, decoded_ints);

    /* Delta decode with running carry */
    decoded_ints[0] += dec->running;
    for (size_t i = 1; i < block_count; i++)
        decoded_ints[i] += decoded_ints[i - 1];
    dec->running = decoded_ints[block_count - 1];

    /* Exception data */
    if (exception_count > 0) {
        size_t pos_words = ((size_t)exception_count * 2 + 7) / 8;
        if (pos_words * 8 > (size_t)(end - p)) return false;

        for (uint16_t i = 0; i < exception_count; i++) {
            size_t word_idx = i / 4;
            size_t off = (size_t)(i % 4) * 16;
            uint64_t w;
            memcpy(&w, p + word_idx * 8, 8);
            exc_positions[i] = (uint16_t)((w >> off) & 0xFFFF);
        }
        p += pos_words * 8;

        if ((size_t)exception_count * 8 > (size_t)(end - p)) return false;
        memcpy(exc_values, p, (size_t)exception_count * 8);
        p += (size_t)exception_count * 8;
    }

    /* Reconstruct doubles */
    if (exp > 18) exp = 18;
    if (fac > 18) fac = 18;
    double scale = ALP_SCALE_FACTORS[fac] / ALP_SCALE_FACTORS[exp];
    double *out_f64 = (double *)dec->block_buf;

    if (exception_count == 0) {
        for (size_t i = 0; i < block_count; i++)
            out_f64[i] = (double)decoded_ints[i] * scale;
    } else {
        uint16_t exc_idx = 0;
        for (size_t i = 0; i < block_count; i++) {
            if (exc_idx < exception_count && exc_positions[exc_idx] == (uint16_t)i) {
                memcpy(&out_f64[i], &exc_values[exc_idx], 8);
                exc_idx++;
            } else {
                out_f64[i] = (double)decoded_ints[i] * scale;
            }
        }
    }

    dec->block_count = block_count;
    dec->block_offset = 0;
    dec->blocks_decoded++;
    dec->pos = p;
    return true;
}

/* ------------------------------------------------------------------ */
/* Int ALP: decode one block into dec->block_buf (as int64_t[])       */
/* ------------------------------------------------------------------ */
static bool decode_next_int_block(alp_stream_decoder_t *dec) {
    if (dec->blocks_decoded >= dec->num_blocks)
        return false;

    const uint8_t *p = dec->pos;
    const uint8_t *end = dec->end;

    /* Block header (16 bytes) */
    if (p + 16 > end) return false;

    uint64_t bh0; memcpy(&bh0, p, 8); p += 8;
    uint64_t bh1; memcpy(&bh1, p, 8); p += 8;

    uint8_t  bw          = (uint8_t)(bh0 & 0xFF);
    if (bw > 64) return false;
    uint16_t block_count = (uint16_t)((bh0 >> 32) & 0xFFFF);

    int64_t for_base;
    memcpy(&for_base, &bh1, 8);

    if (block_count == 0 || block_count > ALP_STREAM_BLOCK_SIZE)
        return false;

    /* FFOR-packed data — copy into aligned scratch before unpacking.
     * The stream pointer p may not be 8-byte aligned (20-byte stream header
     * + 16-byte block headers), and alp_ffor_unpack dereferences uint64_t*. */
    size_t packed_words = alp_ffor_packed_words(block_count, bw);
    if (packed_words * 8 > (size_t)(end - p)) return false;

    int64_t *out_i64 = (int64_t *)dec->block_buf;
    if (packed_words > 0) {
        uint64_t *aligned = (uint64_t *)dec->scratch;
        memcpy(aligned, p, packed_words * 8);
        alp_ffor_unpack(aligned, block_count, for_base, bw, out_i64);
    } else {
        alp_ffor_unpack((const uint64_t *)p, block_count, for_base, bw, out_i64);
    }
    p += packed_words * 8;

    /* Delta decode with running carry (prefix-sum) */
    out_i64[0] += dec->running;
    for (size_t i = 1; i < block_count; i++)
        out_i64[i] += out_i64[i - 1];
    dec->running = out_i64[block_count - 1];

    dec->block_count = block_count;
    dec->block_offset = 0;
    dec->blocks_decoded++;
    dec->pos = p;
    return true;
}

/* ------------------------------------------------------------------ */
/* Public API                                                         */
/* ------------------------------------------------------------------ */

bool alp_stream_decoder_init(alp_stream_decoder_t *dec,
                              const uint8_t *data, size_t len,
                              bool is_float) {
    if (!dec || !data || len == 0)
        return false;

    memset(dec, 0, sizeof(*dec));
    dec->is_float = is_float;
    dec->end = data + len;

    const uint8_t *p = data;

    if (is_float) {
        /* ALP float stream header: 24 bytes */
        if (len < 24) return false;

        uint32_t magic;
        memcpy(&magic, p, 4); p += 4;
        if (magic != ALP_MAGIC) return false;

        p += 4; /* total_values */

        uint16_t num_blocks;
        memcpy(&num_blocks, p, 2); p += 2;
        dec->num_blocks = num_blocks;

        p += 2; /* tail_count */
        uint8_t scheme = *p++;
        p += 3; /* padding */

        int64_t anchor;
        memcpy(&anchor, p, 8); p += 8;

        if (scheme != ALP_SCHEME_ALP) return false;

        dec->running = anchor;
    } else {
        /* ALP int stream header: 20 bytes
         * magic(4) + total_values(4) + num_blocks(2) + tail(2) + anchor(8) */
        if (len < 20) return false;

        uint32_t magic;
        memcpy(&magic, p, 4); p += 4;
        if (magic != ALP_INT_MAGIC) return false;

        p += 4; /* total_values */

        uint16_t num_blocks;
        memcpy(&num_blocks, p, 2); p += 2;
        dec->num_blocks = num_blocks;

        p += 2; /* tail */

        int64_t anchor;
        memcpy(&anchor, p, 8); p += 8;

        dec->running = anchor;
    }

    dec->pos = p;
    dec->blocks_decoded = 0;
    dec->block_count = 0;
    dec->block_offset = 0;

    /* Allocate block buffer for decoded values (both float and int paths) */
    dec->block_buf = malloc(ALP_STREAM_BLOCK_SIZE * sizeof(double));
    if (!dec->block_buf) return false;

    /* Allocate scratch buffer for FFOR decode.
     * Float path needs full SCRATCH_TOTAL (decoded_ints + packed_data + exc_positions).
     * Int path needs only packed_data region for alignment copy before FFOR unpack. */
    size_t scratch_sz = is_float ? SCRATCH_TOTAL : SCRATCH_PACK_SZ;
    dec->scratch = (uint8_t *)malloc(scratch_sz);
    if (!dec->scratch) {
        free(dec->block_buf);
        dec->block_buf = NULL;
        return false;
    }

    return true;
}

void alp_stream_decoder_deinit(alp_stream_decoder_t *dec) {
    if (!dec) return;
    free(dec->block_buf);
    dec->block_buf = NULL;
    free(dec->scratch);
    dec->scratch = NULL;
}

bool alp_stream_decoder_next_float(alp_stream_decoder_t *dec, double *out) {
    if (!dec || !out || !dec->is_float)
        return false;

    /* Decode next block if current one is exhausted */
    if (dec->block_offset >= dec->block_count) {
        if (!decode_next_float_block(dec))
            return false;
    }

    *out = ((double *)dec->block_buf)[dec->block_offset++];
    return true;
}

bool alp_stream_decoder_next_int(alp_stream_decoder_t *dec, int64_t *out) {
    if (!dec || !out || dec->is_float)
        return false;

    /* Decode next block if current one is exhausted */
    if (dec->block_offset >= dec->block_count) {
        if (!decode_next_int_block(dec))
            return false;
    }

    *out = ((int64_t *)dec->block_buf)[dec->block_offset++];
    return true;
}

bool alp_stream_decoder_skip(alp_stream_decoder_t *dec, size_t count) {
    if (!dec) return false;

    for (size_t i = 0; i < count; i++) {
        /* Decode next block if current one is exhausted */
        if (dec->block_offset >= dec->block_count) {
            bool ok = dec->is_float ? decode_next_float_block(dec)
                                    : decode_next_int_block(dec);
            if (!ok) return false;
        }
        dec->block_offset++;
    }
    return true;
}
