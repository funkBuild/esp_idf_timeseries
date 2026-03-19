#include "timeseries_flash_utils.h"

#include "esp_log.h"
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "FLASH_UTILS";

uint32_t tsdb_round_up_4k(uint32_t x) {
  const uint32_t align = 4096;
  return ((x + align - 1) / align) * align;
}

esp_err_t tsdb_flash_write_byte(const esp_partition_t *part, uint32_t offset,
                                uint8_t value) {
  uint32_t aligned = offset & ~3U;
  uint32_t pos = offset - aligned;
  uint8_t buf[4];
  esp_err_t err = esp_partition_read(part, aligned, buf, sizeof(buf));
  if (err != ESP_OK) {
    return err;
  }

  // NOR flash can only clear bits (1->0), never set them (0->1)
  if ((buf[pos] & value) != value) {
    ESP_LOGE(TAG, "NOR violation: trying to set bits at offset 0x%08" PRIx32,
             (uint32_t)offset);
    return ESP_ERR_INVALID_ARG;
  }

  buf[pos] = value;
  return esp_partition_write(part, aligned, buf, sizeof(buf));
}

bool tsdb_safe_rewrite_page_header(const esp_partition_t *part,
                                   uint32_t page_offset,
                                   timeseries_page_header_t *hdr,
                                   uint32_t old_page_size) {
  // Read the full old header from flash for NOR constraint check
  timeseries_page_header_t old_hdr;
  if (esp_partition_read(part, page_offset, &old_hdr, sizeof(old_hdr)) != ESP_OK) {
    ESP_LOGE(TAG, "Failed reading old header for NOR check @0x%" PRIx32, page_offset);
    return false;
  }

  // Check NOR flash constraint across entire header: every byte must satisfy
  // (old & new) == new, meaning we only clear bits, never set them.
  bool can_write_directly = true;
  const uint8_t *old_bytes = (const uint8_t *)&old_hdr;
  const uint8_t *new_bytes = (const uint8_t *)hdr;
  for (size_t i = 0; i < sizeof(old_hdr); i++) {
    if ((old_bytes[i] & new_bytes[i]) != new_bytes[i]) {
      can_write_directly = false;
      break;
    }
  }

  if (can_write_directly) {
    return esp_partition_write(part, page_offset, hdr, sizeof(*hdr)) == ESP_OK;
  }

  // Bit-flip violation: must erase first sector and rewrite
  ESP_LOGW(TAG, "header rewrite requires sector erase @0x%" PRIx32, page_offset);

  uint8_t *sector_buf = malloc(4096);
  if (!sector_buf) {
    ESP_LOGE(TAG, "OOM allocating sector buffer for header rewrite");
    return false;
  }

  // Read the entire first sector
  if (esp_partition_read(part, page_offset, sector_buf, 4096) != ESP_OK) {
    ESP_LOGE(TAG, "Failed reading sector for header rewrite");
    free(sector_buf);
    return false;
  }

  // Patch the header in the buffer
  memcpy(sector_buf, hdr, sizeof(*hdr));

  // Verify sector alignment before erasing
  if (page_offset & 0xFFF) {
    ESP_LOGE(TAG, "page_offset 0x%08" PRIx32 " not sector-aligned", (uint32_t)page_offset);
    free(sector_buf);
    return false;
  }

  // Erase the first sector
  if (esp_partition_erase_range(part, page_offset, 4096) != ESP_OK) {
    ESP_LOGE(TAG, "Failed erasing sector for header rewrite");
    free(sector_buf);
    return false;
  }

  // Write the sector back -- retry on failure since data is lost if we don't
  esp_err_t err = ESP_FAIL;
  for (int retry = 0; retry < 3; retry++) {
    err = esp_partition_write(part, page_offset, sector_buf, 4096);
    if (err == ESP_OK) {
      break;
    }
    ESP_LOGE(TAG, "Failed rewriting sector after erase (attempt %d/3)", retry + 1);
  }
  free(sector_buf);
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "CRITICAL: sector data lost after erase @0x%" PRIx32, page_offset);
    return false;
  }

  return true;
}
