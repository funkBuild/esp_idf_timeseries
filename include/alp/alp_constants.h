#pragma once

/*
 * Shared ALP scaling table.
 *
 * ALP_SCALE_FACTORS[i] = 10^i for i in [0, 18].
 *
 * Previously duplicated as FACT_ARR[] and FRAC_ARR[] in alp_encoder.c,
 * alp_decoder.c, and alp_stream_decoder.c.  Both arrays were identical;
 * this single table replaces them all.
 */
static const double ALP_SCALE_FACTORS[19] = {
    1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,
    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
};
