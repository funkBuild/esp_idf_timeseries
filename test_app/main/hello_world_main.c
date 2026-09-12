#include <stdlib.h>
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
  const char *filter = NULL;
#if CONFIG_IDF_TARGET_LINUX
  filter = getenv("TIMESERIES_TEST_FILTER");
#elif defined(TIMESERIES_TEST_TAG)
  filter = TIMESERIES_TEST_TAG;
#endif
  if (filter && *filter) {
    unity_run_tests_by_tag(filter, false);
  } else {
    unity_run_all_tests();
  }
  int failures = UNITY_END();

#if CONFIG_IDF_TARGET_LINUX
  // Host build (tools/host_test): app_main RETURNING does not end the process on the linux
  // target -- the FreeRTOS scheduler keeps running and the process hangs. LeakSanitizer only
  // reports at process exit, so without this the suite yields NO leak data at all (not zero).
  exit(failures ? 1 : 0);
#else
  (void)failures;
#endif
}
