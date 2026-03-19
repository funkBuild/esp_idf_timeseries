#ifndef TIMESERIES_FLASH_UTILS_H
#define TIMESERIES_FLASH_UTILS_H

#include "esp_partition.h"
#include "timeseries_internal.h"
#include <stdbool.h>
#include <stdint.h>

/**
 * Round up x to the next multiple of 4K (4096).
 */
uint32_t tsdb_round_up_4k(uint32_t x);

/**
 * Safely rewrite a page header with an updated page_size field.
 *
 * On NOR flash, writes can only clear bits (1->0). If the new page_size has
 * bits set that are clear in the old page_size, a plain write would silently
 * corrupt the value (hardware ANDs old & new). This function detects that case
 * and falls back to erase-and-rewrite of the first 4K sector.
 */
bool tsdb_safe_rewrite_page_header(const esp_partition_t *part,
                                   uint32_t page_offset,
                                   timeseries_page_header_t *hdr,
                                   uint32_t old_page_size);

/**
 * Write a single byte to flash using a 4-byte-aligned read-modify-write.
 * Required because esp_partition_write needs size to be a multiple of 4.
 */
esp_err_t tsdb_flash_write_byte(const esp_partition_t *part, uint32_t offset,
                                uint8_t value);

#endif // TIMESERIES_FLASH_UTILS_H
