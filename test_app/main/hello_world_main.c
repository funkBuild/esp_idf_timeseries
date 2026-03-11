#include "unity.h"
#include "esp_heap_caps.h"
#include "esp_log.h"
#include <inttypes.h>

void setUp(void) {
  ESP_LOGI("HEAP", "%" PRIu32 " free, %" PRIu32 " largest",
           (uint32_t)heap_caps_get_free_size(MALLOC_CAP_DEFAULT),
           (uint32_t)heap_caps_get_largest_free_block(MALLOC_CAP_DEFAULT));
}

void tearDown(void) {}

void app_main(void) {
  ESP_LOGI("test_main", "Free heap at start: %" PRIu32 " bytes, largest block: %" PRIu32 " bytes",
           (uint32_t)heap_caps_get_free_size(MALLOC_CAP_DEFAULT),
           (uint32_t)heap_caps_get_largest_free_block(MALLOC_CAP_DEFAULT));

  UNITY_BEGIN();
  unity_run_all_tests();
  UNITY_END();
}