#include "timeseries_multi_iterator.h"
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "esp_log.h"

static const char* TAG = "timeseries_multi_iter";

static bool field_value_deep_copy(timeseries_field_value_t* dst, const timeseries_field_value_t* src) {
  dst->type = src->type;
  if (src->type == TIMESERIES_FIELD_TYPE_STRING) {
    size_t len = src->data.string_val.length;
    if (!src->data.string_val.str || len == 0) {
      dst->data.string_val.str = NULL;
      dst->data.string_val.length = 0;
      return true;
    }
    char* copy = (char*)malloc(len + 1);
    if (!copy) {
      ESP_LOGE(TAG, "OOM duplicating string value");
      return false;
    }
    memcpy(copy, src->data.string_val.str, len);
    copy[len] = '\0';
    dst->data.string_val.str = copy;
    dst->data.string_val.length = len;
  } else {
    dst->data = src->data; /* plain scalar, shallow copy OK */
  }
  return true;
}

static void heap_swap(size_t* heap, size_t i, size_t j) {
  size_t tmp = heap[i];
  heap[i] = heap[j];
  heap[j] = tmp;
}

/**
 * @brief Compare two sub-iterators by timestamp ascending
 */
static bool heap_less(const timeseries_multi_points_iterator_t* multi, size_t a_idx, size_t b_idx) {
  const multi_points_sub_iter_t* A = &multi->subs[a_idx];
  const multi_points_sub_iter_t* B = &multi->subs[b_idx];
  // ascending by current_ts
  return A->current_ts < B->current_ts;
}

static void heap_sift_up(timeseries_multi_points_iterator_t* multi, size_t pos) {
  while (pos > 0) {
    size_t parent = (pos - 1) / 2;
    if (heap_less(multi, multi->heap[pos], multi->heap[parent])) {
      heap_swap(multi->heap, pos, parent);
      pos = parent;
    } else {
      break;
    }
  }
}

static void heap_sift_down(timeseries_multi_points_iterator_t* multi, size_t pos) {
  size_t size = multi->heap_size;
  while (true) {
    size_t left = 2 * pos + 1;
    size_t right = 2 * pos + 2;
    size_t smallest = pos;
    if (left < size && heap_less(multi, multi->heap[left], multi->heap[smallest])) {
      smallest = left;
    }
    if (right < size && heap_less(multi, multi->heap[right], multi->heap[smallest])) {
      smallest = right;
    }
    if (smallest == pos) {
      break;
    }
    heap_swap(multi->heap, pos, smallest);
    pos = smallest;
  }
}

/**
 * @brief Helper: read the next point from sub-iter i
 * and update multi->subs[i]. If exhausted, mark valid=false.
 */
static bool multi_iter_advance_sub(timeseries_multi_points_iterator_t* multi, size_t i) {
  multi_points_sub_iter_t* sub = &multi->subs[i];
  uint64_t ts = 0;
  timeseries_field_value_t val;

  if (!timeseries_points_iterator_next_timestamp(sub->sub_iter, &ts)) {
    sub->valid = false;
    return false;
  }

  if (!timeseries_points_iterator_next_value(sub->sub_iter, &val)) {
    sub->valid = false;
    return false;
  }

  // Free string memory if needed
  /* Save old value for later free */
  timeseries_field_value_t old = sub->current_val;

  sub->current_ts = ts;
  sub->current_val = val; /* new value now installed */

  if (old.type == TIMESERIES_FIELD_TYPE_STRING) {
    free(old.data.string_val.str); /* safe to free old string */
  }

  return true;
}

/**
 * Init function
 */
bool timeseries_multi_points_iterator_init(timeseries_points_iterator_t** sub_iters, const uint32_t* page_seqs,
                                           size_t sub_count, timeseries_multi_points_iterator_t* out_iter) {
  if (!out_iter || !sub_iters || sub_count == 0) {
    return false;
  }
  memset(out_iter, 0, sizeof(*out_iter));

  // Allocate sub-iterator array
  out_iter->subs = (multi_points_sub_iter_t*)calloc(sub_count, sizeof(multi_points_sub_iter_t));
  if (!out_iter->subs) {
    return false;
  }
  out_iter->heap = (size_t*)calloc(sub_count, sizeof(size_t));
  if (!out_iter->heap) {
    free(out_iter->subs);
    out_iter->subs = NULL;
    return false;
  }

  out_iter->sub_count = sub_count;
  out_iter->heap_size = 0;
  out_iter->valid = true;

  // Initialize each sub-iterator state
  for (size_t i = 0; i < sub_count; i++) {
    multi_points_sub_iter_t* sub = &out_iter->subs[i];
    sub->sub_iter = sub_iters[i];
    sub->page_seq = page_seqs[i];
    sub->valid = true;

    // Attempt to read the first point
    if (!multi_iter_advance_sub(out_iter, i)) {
      // This sub-iterator was empty from the start
      sub->valid = false;
    }
  }

  // Build initial heap of valid subs
  for (size_t i = 0; i < sub_count; i++) {
    if (out_iter->subs[i].valid) {
      out_iter->heap[out_iter->heap_size++] = i;
    }
  }
  // Heapify
  for (ssize_t i = (ssize_t)out_iter->heap_size / 2 - 1; i >= 0; i--) {
    heap_sift_down(out_iter, i);
  }

  return true;
}

/**
 * Next function
 */
bool timeseries_multi_points_iterator_next(timeseries_multi_points_iterator_t* multi_iter, uint64_t* out_ts,
                                           timeseries_field_value_t* out_val) {
  if (!multi_iter || !multi_iter->valid || multi_iter->heap_size == 0) return false;

  /* ---------------- step 1: pick the earliest timestamp ------------- */
  size_t best_sub = multi_iter->heap[0];
  uint64_t smallest_ts = multi_iter->subs[best_sub].current_ts;
  uint32_t best_seq = multi_iter->subs[best_sub].page_seq;

  /* collect all subs with the same ts into popped[]                    */
  size_t popped_cap = multi_iter->heap_size;
  size_t* popped = (size_t*)malloc(popped_cap * sizeof(size_t));
  if (!popped) return false;

  size_t same_cnt = 0;
  while (multi_iter->heap_size && multi_iter->subs[multi_iter->heap[0]].current_ts == smallest_ts) {
    size_t idx = multi_iter->heap[0];
    /* pop */
    multi_iter->heap[0] = multi_iter->heap[--multi_iter->heap_size];
    if (multi_iter->heap_size) heap_sift_down(multi_iter, 0);

    popped[same_cnt++] = idx;
    if (multi_iter->subs[idx].page_seq > best_seq) {
      best_seq = multi_iter->subs[idx].page_seq;
      best_sub = idx;
    }
  }

  /* ---------------- step 2: output the chosen point ----------------- */
  if (out_ts) *out_ts = smallest_ts;
  if (out_val) {
    if (!field_value_deep_copy(out_val, &multi_iter->subs[best_sub].current_val)) {
      free(popped);
      return false; /* OOM */
    }
  }

  /* ---------------- step 3: advance sub-iterators ------------------- */
  for (size_t i = 0; i < same_cnt; ++i) {
    size_t sid = popped[i];
    /* skip the winner for now – we advance it *after* freeing buffer */
    if (sid == best_sub) continue;

    if (multi_iter->subs[sid].valid && multi_iter_advance_sub(multi_iter, sid)) {
      multi_iter->heap[multi_iter->heap_size++] = sid;
      heap_sift_up(multi_iter, multi_iter->heap_size - 1);
    }
  }
  /* finally advance the winner – safe because caller already owns its copy */
  if (multi_iter->subs[best_sub].valid && multi_iter_advance_sub(multi_iter, best_sub)) {
    multi_iter->heap[multi_iter->heap_size++] = best_sub;
    heap_sift_up(multi_iter, multi_iter->heap_size - 1);
  }

  free(popped);
  return true;
}

void timeseries_multi_points_iterator_deinit(timeseries_multi_points_iterator_t* multi_iter) {
  if (!multi_iter) {
    return;
  }

  // Free any string memory
  for (size_t i = 0; i < multi_iter->sub_count; i++) {
    if (multi_iter->subs[i].current_val.type == TIMESERIES_FIELD_TYPE_STRING) {
      free(multi_iter->subs[i].current_val.data.string_val.str);
    }
  }

  if (multi_iter->subs) {
    // We do NOT deinit sub->sub_iter here unless we own them
    // They might be owned externally.
    free(multi_iter->subs);
    multi_iter->subs = NULL;
  }
  if (multi_iter->heap) {
    free(multi_iter->heap);
    multi_iter->heap = NULL;
  }
  memset(multi_iter, 0, sizeof(*multi_iter));
}

bool timeseries_multi_points_iterator_peek(timeseries_multi_points_iterator_t* multi_iter, uint64_t* out_ts,
                                           timeseries_field_value_t* out_val) {
  if (!multi_iter || !multi_iter->valid || multi_iter->heap_size == 0) {
    return false;
  }
  if (!out_ts || !out_val) {
    return false;
  }

  // The top of the heap is the sub-iterator with the smallest current_ts
  size_t top_index = multi_iter->heap[0];
  multi_points_sub_iter_t* sub = &multi_iter->subs[top_index];

  if (!sub->valid) {
    // Shouldn't happen if heap_size > 0, but just in case
    return false;
  }

  *out_ts = sub->current_ts;
  *out_val = sub->current_val;
  return true;
}
