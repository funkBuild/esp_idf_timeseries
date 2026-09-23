#pragma once
// Host stub: RAM-backed partition. Reads are provided by the test.
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>   /* IDF headers pull these in transitively */
#include <inttypes.h>
#include "esp_err.h"
typedef uint32_t esp_partition_mmap_handle_t;
typedef struct { uint32_t address; uint32_t size; const char* label; } esp_partition_t;
esp_err_t esp_partition_read(const esp_partition_t* p, size_t off, void* dst, size_t len);
esp_err_t esp_partition_write(const esp_partition_t* p, size_t off, const void* src, size_t len);
esp_err_t esp_partition_erase_range(const esp_partition_t* p, size_t off, size_t len);
void esp_partition_munmap(esp_partition_mmap_handle_t h);
