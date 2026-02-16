/**
 * Calculate series ID using MD5 hash of measurement, sorted tags, and field name
 *
 * @param measurement_name The measurement name
 * @param tag_keys Array of tag keys
 * @param tag_values Array of tag values
 * @param num_tags Number of tags
 * @param field_name The field name
 * @param series_id Output buffer for 16-byte MD5 hash
 * @return true on success, false on error
 */
#include "timeseries_series_id.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include "esp_log.h"
#include "mbedtls/md.h"

static const char* TAG = "timeseries_series_id";

bool calculate_series_id(const char* measurement_name, const char* const* tag_keys, const char* const* tag_values,
                         size_t num_tags, const char* field_name, unsigned char series_id[16]) {
  static const char colon = ':';

  const mbedtls_md_info_t* md_info = mbedtls_md_info_from_type(MBEDTLS_MD_MD5);
  if (!md_info) {
    ESP_LOGE(TAG, "MD5 not available");
    return false;
  }

  mbedtls_md_context_t ctx;
  mbedtls_md_init(&ctx);

  if (mbedtls_md_setup(&ctx, md_info, 0) != 0 || mbedtls_md_starts(&ctx) != 0) {
    ESP_LOGE(TAG, "MD5 init failed");
    mbedtls_md_free(&ctx);
    return false;
  }

  /* Feed measurement name */
  mbedtls_md_update(&ctx, (const unsigned char*)measurement_name, strlen(measurement_name));

  /* Sort tags by key for consistent series ID generation */
  size_t* tag_indices = NULL;
  if (num_tags > 0) {
    tag_indices = calloc(num_tags, sizeof(size_t));
    if (!tag_indices) {
      ESP_LOGE(TAG, "Failed to allocate memory for tag sorting");
      mbedtls_md_free(&ctx);
      return false;
    }

    for (size_t i = 0; i < num_tags; i++) {
      tag_indices[i] = i;
    }

    for (size_t i = 0; i < num_tags - 1; i++) {
      for (size_t j = 0; j < num_tags - i - 1; j++) {
        if (strcmp(tag_keys[tag_indices[j]], tag_keys[tag_indices[j + 1]]) > 0) {
          size_t temp = tag_indices[j];
          tag_indices[j] = tag_indices[j + 1];
          tag_indices[j + 1] = temp;
        }
      }
    }

    /* Feed sorted tags: ":key:value" for each */
    for (size_t t = 0; t < num_tags; t++) {
      size_t idx = tag_indices[t];
      mbedtls_md_update(&ctx, (const unsigned char*)&colon, 1);
      mbedtls_md_update(&ctx, (const unsigned char*)tag_keys[idx], strlen(tag_keys[idx]));
      mbedtls_md_update(&ctx, (const unsigned char*)&colon, 1);
      mbedtls_md_update(&ctx, (const unsigned char*)tag_values[idx], strlen(tag_values[idx]));
    }

    free(tag_indices);
  }

  /* Feed field name: ":field_name" */
  mbedtls_md_update(&ctx, (const unsigned char*)&colon, 1);
  mbedtls_md_update(&ctx, (const unsigned char*)field_name, strlen(field_name));

  mbedtls_md_finish(&ctx, series_id);
  mbedtls_md_free(&ctx);

  return true;
}
