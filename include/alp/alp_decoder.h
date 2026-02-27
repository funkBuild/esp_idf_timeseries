#pragma once
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Decode ALP-compressed data back to doubles.
 * `out` must be pre-allocated with at least `count` doubles.
 * Returns true on success.
 */
bool alp_decode(const uint8_t *data, size_t data_len, double *out, size_t count);

#ifdef __cplusplus
}
#endif
