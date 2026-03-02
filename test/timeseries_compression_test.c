/**
 * @file timeseries_compression_test.c
 * @brief Direct tests for timeseries_compression_compress/decompress round-trip.
 */

#include "timeseries_compression.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// ============================================================================
// NULL parameter handling
// ============================================================================

TEST_CASE("compression: compress NULL in_data returns false",
          "[compression][error]") {
  uint8_t *out = NULL;
  size_t out_size = 0;
  TEST_ASSERT_FALSE(timeseries_compression_compress(NULL, 100, &out, &out_size));
}

TEST_CASE("compression: compress NULL out_data returns false",
          "[compression][error]") {
  uint8_t data[] = {1, 2, 3};
  size_t out_size = 0;
  TEST_ASSERT_FALSE(
      timeseries_compression_compress(data, sizeof(data), NULL, &out_size));
}

TEST_CASE("compression: compress NULL out_size returns false",
          "[compression][error]") {
  uint8_t data[] = {1, 2, 3};
  uint8_t *out = NULL;
  TEST_ASSERT_FALSE(
      timeseries_compression_compress(data, sizeof(data), &out, NULL));
}

TEST_CASE("compression: decompress NULL in_data returns false",
          "[compression][error]") {
  uint8_t *out = NULL;
  size_t out_size = 0;
  TEST_ASSERT_FALSE(
      timeseries_compression_decompress(NULL, 100, &out, &out_size));
}

TEST_CASE("compression: decompress NULL out_data returns false",
          "[compression][error]") {
  uint8_t data[] = {1, 2, 3};
  size_t out_size = 0;
  TEST_ASSERT_FALSE(
      timeseries_compression_decompress(data, sizeof(data), NULL, &out_size));
}

TEST_CASE("compression: decompress NULL out_size returns false",
          "[compression][error]") {
  uint8_t data[] = {1, 2, 3};
  uint8_t *out = NULL;
  TEST_ASSERT_FALSE(
      timeseries_compression_decompress(data, sizeof(data), &out, NULL));
}

// ============================================================================
// Round-trip tests
// ============================================================================

TEST_CASE("compression: round-trip small data", "[compression][roundtrip]") {
  uint8_t input[] = "Hello, timeseries compression!";
  size_t input_size = sizeof(input); // includes null terminator

  uint8_t *compressed = NULL;
  size_t compressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_compress(input, input_size,
                                                   &compressed, &compressed_size));
  TEST_ASSERT_NOT_NULL(compressed);
  TEST_ASSERT_GREATER_THAN(0, compressed_size);

  uint8_t *decompressed = NULL;
  size_t decompressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_decompress(
      compressed, compressed_size, &decompressed, &decompressed_size));
  TEST_ASSERT_NOT_NULL(decompressed);
  TEST_ASSERT_EQUAL(input_size, decompressed_size);
  TEST_ASSERT_EQUAL_MEMORY(input, decompressed, input_size);

  free(compressed);
  free(decompressed);
}

TEST_CASE("compression: round-trip 1 byte", "[compression][roundtrip]") {
  uint8_t input[] = {0x42};

  uint8_t *compressed = NULL;
  size_t compressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_compress(input, 1, &compressed,
                                                   &compressed_size));
  TEST_ASSERT_NOT_NULL(compressed);

  uint8_t *decompressed = NULL;
  size_t decompressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_decompress(
      compressed, compressed_size, &decompressed, &decompressed_size));
  TEST_ASSERT_EQUAL(1, decompressed_size);
  TEST_ASSERT_EQUAL_UINT8(0x42, decompressed[0]);

  free(compressed);
  free(decompressed);
}

TEST_CASE("compression: round-trip 4KB data", "[compression][roundtrip]") {
  size_t size = 4096;
  uint8_t *input = malloc(size);
  TEST_ASSERT_NOT_NULL(input);

  // Fill with a pattern that compresses well
  for (size_t i = 0; i < size; i++) {
    input[i] = (uint8_t)(i & 0xFF);
  }

  uint8_t *compressed = NULL;
  size_t compressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_compress(input, size, &compressed,
                                                   &compressed_size));
  TEST_ASSERT_NOT_NULL(compressed);

  uint8_t *decompressed = NULL;
  size_t decompressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_decompress(
      compressed, compressed_size, &decompressed, &decompressed_size));
  TEST_ASSERT_EQUAL(size, decompressed_size);
  TEST_ASSERT_EQUAL_MEMORY(input, decompressed, size);

  free(input);
  free(compressed);
  free(decompressed);
}

TEST_CASE("compression: round-trip 16KB data", "[compression][roundtrip]") {
  size_t size = 16 * 1024;
  uint8_t *input = malloc(size);
  TEST_ASSERT_NOT_NULL(input);

  // Fill with pseudo-random data (less compressible)
  uint32_t state = 12345;
  for (size_t i = 0; i < size; i++) {
    state = state * 1103515245 + 12345;
    input[i] = (uint8_t)(state >> 16);
  }

  uint8_t *compressed = NULL;
  size_t compressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_compress(input, size, &compressed,
                                                   &compressed_size));
  TEST_ASSERT_NOT_NULL(compressed);

  uint8_t *decompressed = NULL;
  size_t decompressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_decompress(
      compressed, compressed_size, &decompressed, &decompressed_size));
  TEST_ASSERT_EQUAL(size, decompressed_size);
  TEST_ASSERT_EQUAL_MEMORY(input, decompressed, size);

  free(input);
  free(compressed);
  free(decompressed);
}

TEST_CASE("compression: round-trip all zeros", "[compression][roundtrip]") {
  size_t size = 1024;
  uint8_t *input = calloc(1, size);
  TEST_ASSERT_NOT_NULL(input);

  uint8_t *compressed = NULL;
  size_t compressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_compress(input, size, &compressed,
                                                   &compressed_size));
  TEST_ASSERT_NOT_NULL(compressed);
  // All-zeros should compress very well
  TEST_ASSERT_LESS_THAN(size, compressed_size);

  uint8_t *decompressed = NULL;
  size_t decompressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_decompress(
      compressed, compressed_size, &decompressed, &decompressed_size));
  TEST_ASSERT_EQUAL(size, decompressed_size);
  TEST_ASSERT_EQUAL_MEMORY(input, decompressed, size);

  free(input);
  free(compressed);
  free(decompressed);
}

TEST_CASE("compression: round-trip all 0xFF bytes", "[compression][roundtrip]") {
  size_t size = 1024;
  uint8_t *input = malloc(size);
  TEST_ASSERT_NOT_NULL(input);
  memset(input, 0xFF, size);

  uint8_t *compressed = NULL;
  size_t compressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_compress(input, size, &compressed,
                                                   &compressed_size));
  TEST_ASSERT_NOT_NULL(compressed);

  uint8_t *decompressed = NULL;
  size_t decompressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_decompress(
      compressed, compressed_size, &decompressed, &decompressed_size));
  TEST_ASSERT_EQUAL(size, decompressed_size);
  TEST_ASSERT_EQUAL_MEMORY(input, decompressed, size);

  free(input);
  free(compressed);
  free(decompressed);
}

// ============================================================================
// Corrupted data handling
// ============================================================================

TEST_CASE("compression: decompress corrupted data returns false",
          "[compression][error]") {
  // Random garbage that is not valid zlib
  uint8_t garbage[] = {0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04};
  uint8_t *out = NULL;
  size_t out_size = 0;
  TEST_ASSERT_FALSE(timeseries_compression_decompress(garbage, sizeof(garbage),
                                                      &out, &out_size));
}

TEST_CASE("compression: decompress truncated data returns false",
          "[compression][error]") {
  // First compress something valid
  uint8_t input[] = "This is a test string for truncation testing.";
  uint8_t *compressed = NULL;
  size_t compressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_compress(
      input, sizeof(input), &compressed, &compressed_size));
  TEST_ASSERT_NOT_NULL(compressed);
  TEST_ASSERT_GREATER_THAN(2, compressed_size);

  // Truncate by half
  uint8_t *out = NULL;
  size_t out_size = 0;
  TEST_ASSERT_FALSE(timeseries_compression_decompress(
      compressed, compressed_size / 2, &out, &out_size));

  free(compressed);
}

// ============================================================================
// Edge cases
// ============================================================================

TEST_CASE("compression: compressed size smaller than input for repetitive data",
          "[compression][property]") {
  // Highly compressible: repeated pattern
  size_t size = 4096;
  uint8_t *input = malloc(size);
  TEST_ASSERT_NOT_NULL(input);
  for (size_t i = 0; i < size; i++) {
    input[i] = 'A';
  }

  uint8_t *compressed = NULL;
  size_t compressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_compress(input, size, &compressed,
                                                   &compressed_size));
  TEST_ASSERT_NOT_NULL(compressed);
  TEST_ASSERT_LESS_THAN(size, compressed_size);

  free(input);
  free(compressed);
}

TEST_CASE("compression: round-trip simulated timeseries data",
          "[compression][roundtrip]") {
  // Simulate what compaction actually compresses: arrays of timestamps + values
  size_t num_points = 100;
  size_t size = num_points * (sizeof(uint64_t) + sizeof(double));
  uint8_t *input = malloc(size);
  TEST_ASSERT_NOT_NULL(input);

  // Fill with realistic timestamp+value patterns
  uint64_t ts = 1000000;
  double val = 25.0;
  uint8_t *ptr = input;
  for (size_t i = 0; i < num_points; i++) {
    memcpy(ptr, &ts, sizeof(ts));
    ptr += sizeof(ts);
    memcpy(ptr, &val, sizeof(val));
    ptr += sizeof(val);
    ts += 60000; // 1-minute intervals
    val += 0.1;  // slowly increasing
  }

  uint8_t *compressed = NULL;
  size_t compressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_compress(input, size, &compressed,
                                                   &compressed_size));

  uint8_t *decompressed = NULL;
  size_t decompressed_size = 0;
  TEST_ASSERT_TRUE(timeseries_compression_decompress(
      compressed, compressed_size, &decompressed, &decompressed_size));
  TEST_ASSERT_EQUAL(size, decompressed_size);
  TEST_ASSERT_EQUAL_MEMORY(input, decompressed, size);

  free(input);
  free(compressed);
  free(decompressed);
}
