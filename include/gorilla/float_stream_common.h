#ifndef FLOAT_STREAM_COMMON_H
#define FLOAT_STREAM_COMMON_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

// --- Utility Functions ---
// Convert a double into its 64-bit representation.
static inline uint64_t getUint64Representation(double value) {
  uint64_t result;
  memcpy(&result, &value, sizeof(uint64_t));
  return result;
}

// Convert a 64-bit representation into a double.
static inline double getDoubleRepresentation(uint64_t rep) {
  double value;
  memcpy(&value, &rep, sizeof(uint64_t));
  return value;
}

// Count the number of leading zero bits in a 64-bit integer.
static inline int getLeadingZeroBits(uint64_t x) {
#if defined(__GNUC__)
  return x ? __builtin_clzll(x) : 64;
#else
  int count = 0;
  uint64_t mask = 1ULL << 63;
  while (mask && !(x & mask)) {
    count++;
    mask >>= 1;
  }
  return count;
#endif
}

// Count the number of trailing zero bits in a 64-bit integer.
static inline int getTrailingZeroBits(uint64_t x) {
#if defined(__GNUC__)
  return x ? __builtin_ctzll(x) : 64;
#else
  int count = 0;
  while (x && !(x & 1ULL)) {
    count++;
    x >>= 1;
  }
  return count;
#endif
}

#endif // FLOAT_STREAM_COMMON_H
