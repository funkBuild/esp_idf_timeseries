/**
 * @file timeseries_string_stream_encoder_test.c
 * @brief Tests for string_stream_encoder, including the NULL data pointer bug
 *        that occurred during compaction when string fields read from flash
 *        had NULL str pointers with length 0 (e.g., empty firmware version
 *        strings).
 */

#include "unity.h"
#include "gorilla/string_stream_encoder.h"
#include "gorilla/string_stream_decoder.h"
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

// ============================================================================
// Helpers: in-memory buffer for encoder flush / decoder fill round-trip
// ============================================================================

#define TEST_BUF_SIZE 4096

typedef struct {
    uint8_t buf[TEST_BUF_SIZE];
    size_t  len;   /* bytes written so far */
    size_t  pos;   /* read cursor for decoder */
} test_buffer_t;

static bool flush_to_buffer(const uint8_t *data, size_t len, void *ctx) {
    test_buffer_t *tb = (test_buffer_t *)ctx;
    if (tb->len + len > TEST_BUF_SIZE) {
        return false;
    }
    memcpy(tb->buf + tb->len, data, len);
    tb->len += len;
    return true;
}

static bool fill_from_buffer(void *ctx, uint8_t *buffer, size_t max_len,
                             size_t *filled) {
    test_buffer_t *tb = (test_buffer_t *)ctx;
    size_t remaining = tb->len - tb->pos;
    if (remaining == 0) {
        *filled = 0;
        return false;
    }
    size_t to_copy = remaining < max_len ? remaining : max_len;
    memcpy(buffer, tb->buf + tb->pos, to_copy);
    tb->pos += to_copy;
    *filled = to_copy;
    return true;
}

// ============================================================================
// Bug regression: NULL data pointer must be treated as empty string
// ============================================================================

TEST_CASE("string_stream_encoder: NULL data with length 0 succeeds",
          "[string_encoder][regression]") {
    test_buffer_t tb = {0};

    StringStreamEncoder *enc = string_stream_encoder_create(flush_to_buffer, &tb);
    TEST_ASSERT_NOT_NULL(enc);

    /* This is the bug scenario: during compaction a string field read from
       flash could have a NULL str pointer with length 0.  The old guard
       `if (!data) return false` rejected this.  After the fix it must
       succeed. */
    TEST_ASSERT_TRUE_MESSAGE(
        string_stream_encoder_add_value(enc, NULL, 0),
        "Adding NULL data with length 0 should succeed (was the bug)");

    /* A normal string should still work after the NULL entry. */
    const char *normal = "firmware_v2.1";
    TEST_ASSERT_TRUE(
        string_stream_encoder_add_value(enc, normal, strlen(normal)));

    /* Finalize the stream. */
    TEST_ASSERT_TRUE(string_stream_encoder_finish(enc));

    string_stream_encoder_destroy(enc);

    /* Verify the compressed buffer is non-empty. */
    TEST_ASSERT_GREATER_THAN_UINT32(0, (uint32_t)tb.len);
}

TEST_CASE("string_stream_encoder: NULL data round-trip through decoder",
          "[string_encoder][regression][roundtrip]") {
    test_buffer_t tb = {0};

    /* ---------- encode ---------- */
    StringStreamEncoder *enc = string_stream_encoder_create(flush_to_buffer, &tb);
    TEST_ASSERT_NOT_NULL(enc);

    /* First value: NULL pointer (empty/missing string from flash). */
    TEST_ASSERT_TRUE(string_stream_encoder_add_value(enc, NULL, 0));

    /* Second value: a normal string. */
    const char *normal = "hello";
    TEST_ASSERT_TRUE(
        string_stream_encoder_add_value(enc, normal, strlen(normal)));

    TEST_ASSERT_TRUE(string_stream_encoder_finish(enc));
    string_stream_encoder_destroy(enc);

    /* ---------- decode ---------- */
    tb.pos = 0; /* rewind for reading */

    StringStreamDecoder *dec =
        string_stream_decoder_create(fill_from_buffer, &tb);
    TEST_ASSERT_NOT_NULL(dec);

    /* First decoded value should be an empty string (length 0). */
    uint8_t *out_data = NULL;
    size_t   out_len  = 999; /* sentinel */
    TEST_ASSERT_TRUE(string_stream_decoder_get_value(dec, &out_data, &out_len));
    TEST_ASSERT_EQUAL_UINT32(0, (uint32_t)out_len);
    /* out_data may be NULL or a zero-length allocation; either is fine. */
    free(out_data);

    /* Second decoded value should match the normal string. */
    out_data = NULL;
    out_len  = 0;
    TEST_ASSERT_TRUE(string_stream_decoder_get_value(dec, &out_data, &out_len));
    TEST_ASSERT_EQUAL_UINT32(strlen(normal), (uint32_t)out_len);
    TEST_ASSERT_NOT_NULL(out_data);
    TEST_ASSERT_EQUAL_MEMORY(normal, out_data, strlen(normal));
    free(out_data);

    string_stream_decoder_destroy(dec);
}

// ============================================================================
// Basic encoder sanity checks
// ============================================================================

TEST_CASE("string_stream_encoder: create with NULL callback returns NULL",
          "[string_encoder][error]") {
    StringStreamEncoder *enc = string_stream_encoder_create(NULL, NULL);
    TEST_ASSERT_NULL(enc);
}

TEST_CASE("string_stream_encoder: add_value on NULL encoder returns false",
          "[string_encoder][error]") {
    TEST_ASSERT_FALSE(string_stream_encoder_add_value(NULL, "x", 1));
}

TEST_CASE("string_stream_encoder: add_value after finish returns false",
          "[string_encoder][error]") {
    test_buffer_t tb = {0};
    StringStreamEncoder *enc = string_stream_encoder_create(flush_to_buffer, &tb);
    TEST_ASSERT_NOT_NULL(enc);

    TEST_ASSERT_TRUE(string_stream_encoder_finish(enc));
    TEST_ASSERT_FALSE(string_stream_encoder_add_value(enc, "x", 1));

    string_stream_encoder_destroy(enc);
}
