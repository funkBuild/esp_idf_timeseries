#ifndef ZIGZAG_STREAM_H
#define ZIGZAG_STREAM_H

#include <stdint.h>

/**
 * @brief ZigZag encode a signed 64-bit integer.
 */
static inline uint64_t zigzag_encode(int64_t value) {
  return ((uint64_t)value << 1) ^ ((uint64_t)(value >> 63));
}

/**
 * @brief ZigZag decode a 64-bit encoded integer.
 */
static inline int64_t zigzag_decode(uint64_t value) {
  return (int64_t)((value >> 1) ^ -((int64_t)(value & 1)));
}

#endif // ZIGZAG_STREAM_H
