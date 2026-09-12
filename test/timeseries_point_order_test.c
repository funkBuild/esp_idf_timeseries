#include "unity.h"
#include "timeseries_data.h"

TEST_CASE("point order: full timestamp width and count boundaries", "[seventh][point_order]") {
  const uint64_t ts[] = {UINT64_MAX, 0, UINT64_MAX-1, 0, 1, UINT64_MAX};
  const size_t expected[] = {3, 4, 2, 5};
  tsdb_point_order_t order;
  TEST_ASSERT_TRUE(tsdb_point_order_init(ts, 6, &order));
  bool ok = order.source_count == 6 && order.count == 4 && order.indices;
  for (size_t i = 0; ok && i < 4; i++) ok = order.indices[i] == expected[i];
  tsdb_point_order_deinit(&order);
  TEST_ASSERT_TRUE(ok);
  TEST_ASSERT_EQUAL(0, order.count);
  TEST_ASSERT_NULL(order.indices);
  TEST_ASSERT_FALSE(tsdb_point_order_init(ts, SIZE_MAX, &order));
  TEST_ASSERT_NULL(order.indices);
  TEST_ASSERT_FALSE(tsdb_point_order_init(NULL, 1, &order));
  TEST_ASSERT_FALSE(tsdb_point_order_init(ts, 1, NULL));
  TEST_ASSERT_TRUE(tsdb_point_order_init(ts, 0, &order));
  TEST_ASSERT_EQUAL(0, order.count);
  TEST_ASSERT_NULL(order.indices);
  tsdb_point_order_deinit(&order);
  tsdb_point_order_deinit(&order);
}

TEST_CASE("point order: odd and even batch lengths match last-occurrence model", "[seventh][point_order]") {
  uint64_t ts[65];
  uint32_t state = 98765;
  bool ok = true;
  for (size_t n = 1; n <= 65 && ok; n++) {
    for (unsigned pattern = 0; pattern < 4 && ok; pattern++) {
      for (size_t i = 0; i < n; i++) {
        state = state*1664525u + 1013904223u;
        size_t key = pattern == 0 ? i : pattern == 1 ? n-i : pattern == 2 ? i/3 : state%17;
        ts[i] = UINT64_MAX - key;
      }
      tsdb_point_order_t order;
      if (!tsdb_point_order_init(ts, n, &order)) { ok = false; break; }
      size_t unique = 0;
      for (size_t i = 0; i < n; i++) {
        bool last = true;
        for (size_t j = i+1; j < n; j++) if (ts[j] == ts[i]) last = false;
        if (last) unique++;
      }
      ok = order.count == unique && order.source_count == n;
      for (size_t i = 0; ok && i < order.count; i++) {
        size_t index = order.indices ? order.indices[i] : i;
        if (index >= n) { ok = false; break; }
        if (i && ts[order.indices ? order.indices[i-1] : i-1] >= ts[index]) ok = false;
        for (size_t j = index+1; j < n; j++) if (ts[j] == ts[index]) ok = false;
      }
      tsdb_point_order_deinit(&order);
    }
  }
  TEST_ASSERT_TRUE(ok);
}
