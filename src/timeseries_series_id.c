

/**
 * Calculate series ID using MD5 hash of measurement, sorted tags, and field name
 *
 * @param measurement_name The measurement name
 * @param tag_keys Array of tag keys
 * @param tag_values Array of tag values
 * @param num_tags Number of tags
 * @param field_name The field name
 * @param series_id Output buffer for 16-byte MD5 hash
 * @return true on success, false on buffer overflow
 */
#include "timeseries_series_id.h"

#include <stdio.h>
#include <stdbool.h>
#include "esp_log.h"
#include "mbedtls/md5.h"

static const char* TAG = "timeseries_series_id";

bool calculate_series_id(const char* measurement_name, const char* const* tag_keys, const char* const* tag_values,
                         size_t num_tags, const char* field_name, unsigned char series_id[16]) {
  char buffer[256];
  memset(buffer, 0, sizeof(buffer));
  size_t offset = 0;

  // Add measurement name
  offset += snprintf(buffer + offset, sizeof(buffer) - offset, "%s", measurement_name);

  // Sort tags by key for consistent series ID generation
  size_t* tag_indices = NULL;
  if (num_tags > 0) {
    tag_indices = calloc(num_tags, sizeof(size_t));
    if (!tag_indices) {
      ESP_LOGE(TAG, "Failed to allocate memory for tag sorting");
      return false;
    }

    // Initialize indices
    for (size_t i = 0; i < num_tags; i++) {
      tag_indices[i] = i;
    }

    // Simple bubble sort for tag keys (usually small number of tags)
    for (size_t i = 0; i < num_tags - 1; i++) {
      for (size_t j = 0; j < num_tags - i - 1; j++) {
        if (strcmp(tag_keys[tag_indices[j]], tag_keys[tag_indices[j + 1]]) > 0) {
          size_t temp = tag_indices[j];
          tag_indices[j] = tag_indices[j + 1];
          tag_indices[j + 1] = temp;
        }
      }
    }

    // Add sorted tags to buffer
    for (size_t t = 0; t < num_tags; t++) {
      size_t idx = tag_indices[t];
      offset += snprintf(buffer + offset, sizeof(buffer) - offset, ":%s:%s", tag_keys[idx], tag_values[idx]);
      if (offset >= sizeof(buffer)) {
        ESP_LOGE(TAG, "MD5 input buffer overflow for field '%s'", field_name);
        free(tag_indices);
        return false;
      }
    }

    free(tag_indices);
  }

  // Add field name
  offset += snprintf(buffer + offset, sizeof(buffer) - offset, ":%s", field_name);
  if (offset >= sizeof(buffer)) {
    ESP_LOGE(TAG, "MD5 buffer overflow (field_name too long?)");
    return false;
  }

  // Calculate MD5 hash
  mbedtls_md5((const unsigned char*)buffer, strlen(buffer), series_id);

  return true;
}