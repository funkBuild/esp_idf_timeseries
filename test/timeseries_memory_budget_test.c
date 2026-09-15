#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include <stdlib.h>

extern timeseries_db_t* timeseries_get_db_handle(void);
#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_fail_calloc(size_t, size_t);
bool tsdb_audit_calloc_failed(void);
#endif

static void memory_cache_workload(bool fail_cache) {
  timeseries_deinit();
#if CONFIG_IDF_TARGET_LINUX
  // Fail the optional cache itself, not a required database allocation.
  if (fail_cache) tsdb_audit_fail_calloc(TSDB_L0_USAGE_CACHE_SIZE,
      sizeof(*((timeseries_db_t*)0)->l0_usage_cache));
#endif
  TEST_ASSERT_TRUE(timeseries_init());
  timeseries_db_t* db = timeseries_get_db_handle();
  TEST_ASSERT_NOT_NULL(db);
  if (fail_cache) TEST_ASSERT_NULL(db->l0_usage_cache);
  else TEST_ASSERT_NOT_NULL(db->l0_usage_cache);
  TEST_ASSERT_TRUE(timeseries_clear_all());
  const char* field = "value";
  for (uint64_t ts = 1; ts <= 5; ts++) {
    timeseries_field_value_t value = {.type=TIMESERIES_FIELD_TYPE_INT,.data.int_val=(int64_t)ts};
    timeseries_insert_data_t in = {.measurement_name="memory",.field_names=&field,.num_fields=1,
      .num_points=1,.timestamps_ms=&ts,.field_values=&value};
    TEST_ASSERT_TRUE(timeseries_insert(&in));
    timeseries_query_t q = {.measurement_name="memory",.start_ms=1,.end_ms=5};
    timeseries_query_result_t result={0};
    TEST_ASSERT_TRUE(timeseries_query(&q,&result));
    TEST_ASSERT_EQUAL(ts,result.num_points);
    timeseries_query_free_result(&result);
  }
  // Clear and reuse cannot retain cached occupancy from the old page sequence.
  TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_query_t q = {.measurement_name="memory"};
  timeseries_query_result_t result={0};
  TEST_ASSERT_TRUE(timeseries_query(&q,&result));
  TEST_ASSERT_EQUAL(0,result.num_points);
  timeseries_query_free_result(&result);
  timeseries_deinit();
  TEST_ASSERT_NULL(db->l0_usage_cache);
}
TEST_CASE("memory: occupancy cache lives on heap and retires with database", "[memory_budget]") {
  memory_cache_workload(false);
}
#if CONFIG_IDF_TARGET_LINUX
TEST_CASE("memory: cache OOM retains insert and query correctness", "[memory_budget]") {
  memory_cache_workload(true);
}
#endif
