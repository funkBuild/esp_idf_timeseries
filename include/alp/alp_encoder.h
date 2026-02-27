#pragma once
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#define ALP_BLOCK_SIZE   128   /* values per block */
#define ALP_MAGIC        0x414C5001U
#define ALP_MAX_EXP      18
#define ALP_RD_MAX_DICT  8
#define ALP_SCHEME_ALP   0
#define ALP_MAX_SAFE_INT ((int64_t)((1ULL << 53)))
#define ALP_MIN_SAFE_INT (-(int64_t)((1ULL << 53)))

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Encode `count` doubles into a malloc'd buffer.
 * Returns compressed byte count. `*out` must be freed by caller.
 * Returns 0 on failure.
 */
size_t alp_encode(const double *values, size_t count, uint8_t **out);

#ifdef __cplusplus
}
#endif
