#include "timeseries_data.h"
#include <stdlib.h>
#include <string.h>

static bool index_less(const uint64_t *ts, size_t a, size_t b) {
  return ts[a] < ts[b] || (ts[a] == ts[b] && a < b);
}
static void sift(size_t *indices, const uint64_t *ts, size_t root, size_t n) {
  size_t value = indices[root];
  while (root < n / 2) {
    size_t child = root * 2 + 1;
    if (child + 1 < n && index_less(ts, indices[child], indices[child+1])) child++;
    if (!index_less(ts, value, indices[child])) break;
    indices[root] = indices[child]; root = child;
  }
  indices[root] = value;
}

bool tsdb_point_order_init(const uint64_t *timestamps, size_t count, tsdb_point_order_t *order) {
  if (!order) return false;
  memset(order, 0, sizeof(*order));
  if (!timestamps || count > SIZE_MAX / sizeof(*timestamps) || count > SIZE_MAX / sizeof(size_t)) return false;
  order->source_count = order->count = count;
  bool decreasing = false;
  size_t repeated = 0;
  for (size_t i = 1; i < count; i++) {
    if (timestamps[i] < timestamps[i-1]) decreasing = true;
    else if (timestamps[i] == timestamps[i-1]) repeated++;
  }
  if (!decreasing && !repeated) return true;
  // Ordered runs need one index per distinct timestamp, even if the input
  // contains millions of copies. Unordered input needs a full permutation.
  size_t *indices = malloc((decreasing ? count : count - repeated) * sizeof(*indices));
  if (!indices) return false;
  if (!decreasing) {
    size_t used = 0;
    for (size_t i = 0; i < count; i++) {
      if (used && timestamps[i] == timestamps[indices[used-1]]) indices[used-1] = i;
      else indices[used++] = i;
    }
    order->indices = indices;
    order->count = used;
    return true;
  } else {
    for (size_t i = 0; i < count; i++) indices[i] = i;
    // Heap sort bounds stack use; original index breaks ties deterministically.
    for (size_t i = count / 2; i; i--) sift(indices, timestamps, i-1, count);
    for (size_t end = count; end > 1;) {
      --end;
      size_t tmp = indices[0]; indices[0] = indices[end]; indices[end] = tmp;
      sift(indices, timestamps, 0, end);
    }
  }
  size_t used = 0;
  for (size_t i = 0; i < count; i++) {
    if (used && timestamps[indices[i]] == timestamps[indices[used-1]]) indices[used-1] = indices[i];
    else indices[used++] = indices[i];
  }
  order->indices = indices;
  order->count = used;
  return true;
}

void tsdb_point_order_deinit(tsdb_point_order_t *order) {
  if (!order) return;
  free(order->indices);
  memset(order, 0, sizeof(*order));
}
