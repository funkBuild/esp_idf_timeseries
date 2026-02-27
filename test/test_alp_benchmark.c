/**
 * @file test_alp_benchmark.c
 * @brief ALP encoder/decoder correctness tests and benchmark vs Gorilla float encoder.
 *
 * Patterns tested (512 values each = 4 full ALP blocks of 128):
 *   A - Slow drift:     20.0 + i * 0.01  (temperature-like, 2 decimal places)
 *   B - Integer float:  (double)(i % 100)
 *   C - Constant:       42.5
 *   D - Step function:  20.0 + (i / 10) * 0.5
 *   E - Random:         seeded pseudo-random doubles in [0, 100)
 *   F - High precision: sin(i * 0.01) * 50.0 + 50.0
 */

#include "unity.h"
#include "esp_timer.h"
#include "alp/alp_encoder.h"
#include "alp/alp_decoder.h"
#include "alp/alp_int_codec.h"
#include "gorilla/float_stream_encoder.h"
#include "gorilla/float_stream_decoder.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>

/* =========================================================================
 * Constants
 * ========================================================================= */
#define BENCH_N   512   /* 4 full ALP blocks */
#define NUM_PATS  6

/* =========================================================================
 * Gorilla buffer helpers (matching the pattern from timeseries_perf_test.c)
 * ========================================================================= */

typedef struct {
    uint8_t *buf;
    size_t   size;
    size_t   cap;
} gorilla_collect_ctx_t;

static bool gorilla_collect_flush(const uint8_t *data, size_t len, void *ctx_) {
    gorilla_collect_ctx_t *ctx = (gorilla_collect_ctx_t *)ctx_;
    if (ctx->size + len > ctx->cap) {
        size_t new_cap = ctx->cap * 2 + len + 256;
        uint8_t *nb = (uint8_t *)realloc(ctx->buf, new_cap);
        if (!nb) return false;
        ctx->buf = nb;
        ctx->cap = new_cap;
    }
    memcpy(ctx->buf + ctx->size, data, len);
    ctx->size += len;
    return true;
}

typedef struct {
    const uint8_t *data;
    size_t         size;
    size_t         offset;
} gorilla_read_ctx_t;

/* FillCallback signature: bool (*)(void *ctx, uint8_t *buf, size_t max_len, size_t *filled) */
static bool gorilla_read_fill(void *ctx_, uint8_t *buffer, size_t max_len, size_t *filled) {
    gorilla_read_ctx_t *ctx = (gorilla_read_ctx_t *)ctx_;
    size_t remaining = ctx->size - ctx->offset;
    size_t to_read   = (remaining < max_len) ? remaining : max_len;
    if (to_read > 0) {
        memcpy(buffer, ctx->data + ctx->offset, to_read);
        ctx->offset += to_read;
    }
    *filled = to_read;
    return true;
}

/* =========================================================================
 * Data pattern generators
 * ========================================================================= */

static void gen_slow_drift(double *v, size_t n) {
    for (size_t i = 0; i < n; i++) v[i] = 20.0 + (double)i * 0.01;
}

static void gen_integer_float(double *v, size_t n) {
    for (size_t i = 0; i < n; i++) v[i] = (double)(i % 100);
}

static void gen_constant(double *v, size_t n) {
    for (size_t i = 0; i < n; i++) v[i] = 42.5;
}

static void gen_step_function(double *v, size_t n) {
    for (size_t i = 0; i < n; i++) v[i] = 20.0 + (double)(i / 10) * 0.5;
}

/* Simple LCG — seeded to 42 for reproducibility */
static void gen_random(double *v, size_t n) {
    uint64_t state = 42;
    for (size_t i = 0; i < n; i++) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        /* Map to [0, 100) with 4 decimal places */
        v[i] = (double)(state >> 32) / (double)(1ULL << 32) * 100.0;
    }
}

static void gen_high_precision(double *v, size_t n) {
    for (size_t i = 0; i < n; i++) {
        v[i] = sin((double)i * 0.01) * 50.0 + 50.0;
    }
}

/* =========================================================================
 * ALP encode + decode helper
 * Returns true on round-trip success for all values.
 * enc_us / dec_us / compressed_bytes are output parameters.
 * ========================================================================= */
static bool alp_roundtrip(const double *values, size_t count,
                           int64_t *enc_us_out, int64_t *dec_us_out,
                           size_t *compressed_bytes_out) {
    /* Warmup pass: prime I-cache before timed run */
    {
        uint8_t *dummy = NULL;
        size_t nb = alp_encode(values, count, &dummy);
        if (dummy && nb > 0) {
            double *tmp = (double *)malloc(count * sizeof(double));
            if (tmp) { alp_decode(dummy, nb, tmp, count); free(tmp); }
            free(dummy);
        }
    }

    uint8_t *compressed = NULL;

    int64_t t0 = esp_timer_get_time();
    size_t nbytes = alp_encode(values, count, &compressed);
    int64_t t1 = esp_timer_get_time();

    if (nbytes == 0 || !compressed) return false;

    *enc_us_out        = t1 - t0;
    *compressed_bytes_out = nbytes;

    double *decoded = (double *)malloc(count * sizeof(double));
    if (!decoded) { free(compressed); return false; }

    int64_t t2 = esp_timer_get_time();
    bool ok = alp_decode(compressed, nbytes, decoded, count);
    int64_t t3 = esp_timer_get_time();

    *dec_us_out = t3 - t2;

    /* Bit-exact round-trip verification */
    if (ok) {
        for (size_t i = 0; i < count; i++) {
            uint64_t orig, dec;
            memcpy(&orig, &values[i], 8);
            memcpy(&dec,  &decoded[i], 8);
            if (orig != dec) { ok = false; break; }
        }
    }

    free(decoded);
    free(compressed);
    return ok;
}

/* =========================================================================
 * Gorilla encode + decode helper
 * Returns true on success; correctness verified by bit-exact comparison.
 * ========================================================================= */
static bool gorilla_roundtrip(const double *values, size_t count,
                               int64_t *enc_us_out, int64_t *dec_us_out,
                               size_t *compressed_bytes_out) {
    /* Warmup pass: prime I-cache before timed run */
    {
        gorilla_collect_ctx_t wctx0 = {0};
        FloatStreamEncoder *we = float_stream_encoder_create(gorilla_collect_flush, &wctx0);
        if (we) {
            for (size_t i = 0; i < count; i++)
                float_stream_encoder_add_value(we, values[i]);
            float_stream_encoder_finish(we);
            float_stream_encoder_destroy(we);
            if (wctx0.buf && wctx0.size > 0) {
                gorilla_read_ctx_t rctx0 = { .data = wctx0.buf, .size = wctx0.size, .offset = 0 };
                FloatStreamDecoder *wd = float_stream_decoder_create((FillCallback)gorilla_read_fill, &rctx0);
                if (wd) {
                    double tmp;
                    for (size_t i = 0; i < count; i++) float_stream_decoder_get_value(wd, &tmp);
                    float_stream_decoder_destroy(wd);
                }
            }
            free(wctx0.buf);
        }
    }

    gorilla_collect_ctx_t wctx = {0};

    FloatStreamEncoder *enc =
        float_stream_encoder_create(gorilla_collect_flush, &wctx);
    if (!enc) return false;

    int64_t t0 = esp_timer_get_time();
    for (size_t i = 0; i < count; i++) {
        if (!float_stream_encoder_add_value(enc, values[i])) {
            float_stream_encoder_destroy(enc);
            free(wctx.buf);
            return false;
        }
    }
    if (!float_stream_encoder_finish(enc)) {
        float_stream_encoder_destroy(enc);
        free(wctx.buf);
        return false;
    }
    int64_t t1 = esp_timer_get_time();
    float_stream_encoder_destroy(enc);

    *enc_us_out           = t1 - t0;
    *compressed_bytes_out = wctx.size;

    /* Decode */
    gorilla_read_ctx_t rctx = {
        .data   = wctx.buf,
        .size   = wctx.size,
        .offset = 0,
    };

    FloatStreamDecoder *dec =
        float_stream_decoder_create((FillCallback)gorilla_read_fill, &rctx);
    if (!dec) { free(wctx.buf); return false; }

    double *decoded = (double *)malloc(count * sizeof(double));
    if (!decoded) {
        float_stream_decoder_destroy(dec);
        free(wctx.buf);
        return false;
    }

    bool ok = true;
    int64_t t2 = esp_timer_get_time();
    for (size_t i = 0; i < count; i++) {
        if (!float_stream_decoder_get_value(dec, &decoded[i])) {
            ok = false;
            break;
        }
    }
    int64_t t3 = esp_timer_get_time();
    *dec_us_out = t3 - t2;

    /* Gorilla is a lossy-capable codec but should be bit-exact for finite
     * doubles without NaN/Inf edge cases; do bit-exact comparison. */
    if (ok) {
        for (size_t i = 0; i < count; i++) {
            uint64_t orig, d;
            memcpy(&orig, &values[i], 8);
            memcpy(&d,    &decoded[i], 8);
            if (orig != d) { ok = false; break; }
        }
    }

    float_stream_decoder_destroy(dec);
    free(decoded);
    free(wctx.buf);
    return ok;
}

/* =========================================================================
 * Individual correctness tests
 * ========================================================================= */

TEST_CASE("alp: correctness slow_drift", "[alp][correctness]") {
    double *v = (double *)malloc(BENCH_N * sizeof(double));
    TEST_ASSERT_NOT_NULL(v);
    gen_slow_drift(v, BENCH_N);

    int64_t eu, du;
    size_t nb;
    TEST_ASSERT_TRUE_MESSAGE(alp_roundtrip(v, BENCH_N, &eu, &du, &nb),
                             "ALP round-trip failed for slow_drift");
    free(v);
}

TEST_CASE("alp: correctness integer_float", "[alp][correctness]") {
    double *v = (double *)malloc(BENCH_N * sizeof(double));
    TEST_ASSERT_NOT_NULL(v);
    gen_integer_float(v, BENCH_N);

    int64_t eu, du;
    size_t nb;
    TEST_ASSERT_TRUE_MESSAGE(alp_roundtrip(v, BENCH_N, &eu, &du, &nb),
                             "ALP round-trip failed for integer_float");
    free(v);
}

TEST_CASE("alp: correctness constant", "[alp][correctness]") {
    double *v = (double *)malloc(BENCH_N * sizeof(double));
    TEST_ASSERT_NOT_NULL(v);
    gen_constant(v, BENCH_N);

    int64_t eu, du;
    size_t nb;
    TEST_ASSERT_TRUE_MESSAGE(alp_roundtrip(v, BENCH_N, &eu, &du, &nb),
                             "ALP round-trip failed for constant");
    free(v);
}

TEST_CASE("alp: correctness step_function", "[alp][correctness]") {
    double *v = (double *)malloc(BENCH_N * sizeof(double));
    TEST_ASSERT_NOT_NULL(v);
    gen_step_function(v, BENCH_N);

    int64_t eu, du;
    size_t nb;
    TEST_ASSERT_TRUE_MESSAGE(alp_roundtrip(v, BENCH_N, &eu, &du, &nb),
                             "ALP round-trip failed for step_function");
    free(v);
}

TEST_CASE("alp: correctness random", "[alp][correctness]") {
    double *v = (double *)malloc(BENCH_N * sizeof(double));
    TEST_ASSERT_NOT_NULL(v);
    gen_random(v, BENCH_N);

    int64_t eu, du;
    size_t nb;
    TEST_ASSERT_TRUE_MESSAGE(alp_roundtrip(v, BENCH_N, &eu, &du, &nb),
                             "ALP round-trip failed for random");
    free(v);
}

TEST_CASE("alp: correctness high_precision", "[alp][correctness]") {
    double *v = (double *)malloc(BENCH_N * sizeof(double));
    TEST_ASSERT_NOT_NULL(v);
    gen_high_precision(v, BENCH_N);

    int64_t eu, du;
    size_t nb;
    TEST_ASSERT_TRUE_MESSAGE(alp_roundtrip(v, BENCH_N, &eu, &du, &nb),
                             "ALP round-trip failed for high_precision");
    free(v);
}

/* =========================================================================
 * Combined benchmark test — prints comparison table
 * ========================================================================= */

TEST_CASE("alp: benchmark vs gorilla", "[alp][benchmark]") {
    typedef void (*gen_fn)(double *, size_t);

    static const char *names[NUM_PATS] = {
        "Slow drift",
        "Integer float",
        "Constant",
        "Step function",
        "Random",
        "High precision",
    };

    static gen_fn generators[NUM_PATS] = {
        gen_slow_drift,
        gen_integer_float,
        gen_constant,
        gen_step_function,
        gen_random,
        gen_high_precision,
    };

    double *values = (double *)malloc(BENCH_N * sizeof(double));
    TEST_ASSERT_NOT_NULL(values);

    /* ------------------------------------------------------------------ */
    /* Print table header                                                  */
    /* ------------------------------------------------------------------ */
    printf("\n");
    printf("=== ALP vs Gorilla Benchmark (%d values each) ===\n", BENCH_N);
    printf("%-16s | %9s | %10s | %10s | %13s | %14s | %14s | %9s\n",
           "Pattern",
           "ALP bytes", "ALP enc us", "ALP dec us",
           "Gorilla bytes", "Gorilla enc us", "Gorilla dec us",
           "ALP ratio");
    printf("%-16s-+-%9s-+-%10s-+-%10s-+-%13s-+-%14s-+-%14s-+-%9s\n",
           "----------------",
           "---------", "----------", "----------",
           "-------------", "--------------", "--------------",
           "---------");

    size_t raw_bytes = BENCH_N * sizeof(double);

    for (int p = 0; p < NUM_PATS; p++) {
        generators[p](values, BENCH_N);

        int64_t alp_enc_us  = 0;
        int64_t alp_dec_us  = 0;
        size_t  alp_bytes   = 0;
        bool    alp_ok      = alp_roundtrip(values, BENCH_N,
                                             &alp_enc_us, &alp_dec_us,
                                             &alp_bytes);

        int64_t gor_enc_us  = 0;
        int64_t gor_dec_us  = 0;
        size_t  gor_bytes   = 0;
        bool    gor_ok      = gorilla_roundtrip(values, BENCH_N,
                                                 &gor_enc_us, &gor_dec_us,
                                                 &gor_bytes);

        double alp_ratio = (alp_bytes > 0) ?
                           (double)raw_bytes / (double)alp_bytes : 0.0;

        printf("%-16s | %9zu | %10lld | %10lld | %13zu | %14lld | %14lld | %7.2fx\n",
               names[p],
               alp_bytes,
               (long long)alp_enc_us,
               (long long)alp_dec_us,
               gor_bytes,
               (long long)gor_enc_us,
               (long long)gor_dec_us,
               alp_ratio);

        /* Correctness assertions — both codecs must round-trip exactly */
        TEST_ASSERT_TRUE_MESSAGE(alp_ok,
                                 "ALP round-trip failed in benchmark");
        TEST_ASSERT_TRUE_MESSAGE(gor_ok,
                                 "Gorilla round-trip failed in benchmark");
    }

    printf("\n(raw uncompressed = %zu bytes per pattern)\n", raw_bytes);
    printf("\n");

    free(values);
}

/* =========================================================================
 * Integer ALP generators
 * ========================================================================= */

static void gen_int_counter(int64_t *v, size_t n) {
    for (size_t i = 0; i < n; i++) v[i] = (int64_t)i;
}
static void gen_int_adc(int64_t *v, size_t n) {
    /* Simulated 12-bit ADC: range ~400 counts around 1800 */
    for (size_t i = 0; i < n; i++) v[i] = 1800 + (int64_t)(i % 400);
}
static void gen_int_timestamps(int64_t *v, size_t n) {
    /* Millisecond timestamps: large base, delta=100ms */
    for (size_t i = 0; i < n; i++) v[i] = 1700000000000LL + (int64_t)i * 100LL;
}
static void gen_int_stride7(int64_t *v, size_t n) {
    for (size_t i = 0; i < n; i++) v[i] = (int64_t)i * 7LL;
}
static void gen_int_random16(int64_t *v, size_t n) {
    uint64_t state = 99;
    for (size_t i = 0; i < n; i++) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        v[i] = (int64_t)(state >> 48); /* 16-bit range */
    }
}
static void gen_int_constant(int64_t *v, size_t n) {
    for (size_t i = 0; i < n; i++) v[i] = 42LL;
}

/* =========================================================================
 * Integer ALP round-trip helper
 * ========================================================================= */

static bool alp_int_roundtrip(const int64_t *values, size_t count,
                               int64_t *enc_us_out, int64_t *dec_us_out,
                               size_t *bytes_out) {
    /* Warmup pass: prime I-cache before timed run */
    {
        uint8_t *dummy = NULL;
        size_t nb = alp_encode_int(values, count, &dummy);
        if (dummy && nb > 0) {
            int64_t *tmp = (int64_t *)malloc(count * sizeof(int64_t));
            if (tmp) { alp_decode_int(dummy, nb, tmp, count); free(tmp); }
            free(dummy);
        }
    }

    uint8_t *compressed = NULL;
    int64_t t0 = esp_timer_get_time();
    size_t nbytes = alp_encode_int(values, count, &compressed);
    int64_t t1 = esp_timer_get_time();
    if (nbytes == 0 || !compressed) return false;
    *enc_us_out = t1 - t0;
    *bytes_out  = nbytes;

    int64_t *decoded = (int64_t *)malloc(count * sizeof(int64_t));
    if (!decoded) { free(compressed); return false; }

    int64_t t2 = esp_timer_get_time();
    bool ok = alp_decode_int(compressed, nbytes, decoded, count);
    int64_t t3 = esp_timer_get_time();
    *dec_us_out = t3 - t2;

    if (ok) {
        for (size_t i = 0; i < count; i++) {
            if (values[i] != decoded[i]) { ok = false; break; }
        }
    }
    free(decoded);
    free(compressed);
    return ok;
}

/* =========================================================================
 * Integer ALP benchmark test
 * ========================================================================= */

TEST_CASE("alp_int: benchmark vs float alp", "[alp][alp_int][benchmark]") {
    typedef void (*gen_int_fn)(int64_t *, size_t);

    static const char *names[] = {
        "Counter",
        "ADC sensor",
        "Timestamps",
        "Stride-7",
        "Random16",
        "Constant",
    };
    static gen_int_fn generators[] = {
        gen_int_counter,
        gen_int_adc,
        gen_int_timestamps,
        gen_int_stride7,
        gen_int_random16,
        gen_int_constant,
    };
    const int NUM_INT_PATS = 6;

    int64_t *ivalues = (int64_t *)malloc(BENCH_N * sizeof(int64_t));
    double  *fvalues = (double  *)malloc(BENCH_N * sizeof(double));
    TEST_ASSERT_NOT_NULL(ivalues);
    TEST_ASSERT_NOT_NULL(fvalues);

    printf("\n");
    printf("=== Integer ALP vs Float ALP Benchmark (%d values each) ===\n", BENCH_N);
    printf("%-14s | %10s | %10s | %10s | %11s | %11s | %11s\n",
           "Pattern",
           "IntALP B", "IntALP enc", "IntALP dec",
           "FltALP B", "FltALP enc", "FltALP dec");
    printf("%-14s-+-%10s-+-%10s-+-%10s-+-%11s-+-%11s-+-%11s\n",
           "--------------",
           "----------","----------","----------",
           "-----------","-----------","-----------");

    for (int p = 0; p < NUM_INT_PATS; p++) {
        generators[p](ivalues, BENCH_N);

        /* Integer ALP */
        int64_t ie = 0, id = 0; size_t ib = 0;
        bool iok = alp_int_roundtrip(ivalues, BENCH_N, &ie, &id, &ib);

        /* Float ALP: cast int64->double, encode, decode, cast back */
        for (size_t i = 0; i < BENCH_N; i++) fvalues[i] = (double)ivalues[i];
        int64_t fe = 0, fd = 0; size_t fb = 0;
        bool fok = alp_roundtrip(fvalues, BENCH_N, &fe, &fd, &fb);

        printf("%-14s | %10zu | %10lld | %10lld | %11zu | %11lld | %11lld %s\n",
               names[p], ib, (long long)ie, (long long)id,
               fb, (long long)fe, (long long)fd,
               (!iok ? "INT_FAIL" : !fok ? "FLT_FAIL" : ""));

        TEST_ASSERT_TRUE_MESSAGE(iok, "Integer ALP round-trip failed");
    }
    printf("\n(raw uncompressed = %zu bytes)\n\n", BENCH_N * sizeof(int64_t));

    free(ivalues);
    free(fvalues);
}
