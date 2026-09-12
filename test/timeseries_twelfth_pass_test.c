#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_data.h"
#include "timeseries_page_cache.h"
#include "timeseries_page_cache_snapshot.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "esp_timer.h"
#include "esp_log.h"
#include <stdlib.h>
#include <string.h>

extern timeseries_db_t *timeseries_get_db_handle(void);
static TaskHandle_t retry_owner, concurrent_worker;
static unsigned cas_failures, releases_after_limit;
static SemaphoreHandle_t concurrent_done;
static bool concurrent_progress;
static timeseries_page_header_t header(void);
#if CONFIG_IDF_TARGET_LINUX
static bool grow_before_fallback, growth_completed;
void tsdb_audit_allocation_probe(size_t,bool);
#endif
bool __real_tsdb_snapshot_compare_and_swap(timeseries_db_t *,tsdb_page_cache_snapshot_t *,tsdb_page_cache_snapshot_t *);
bool __wrap_tsdb_snapshot_compare_and_swap(timeseries_db_t *db,tsdb_page_cache_snapshot_t *expected,tsdb_page_cache_snapshot_t *replacement) {
  if(retry_owner==xTaskGetCurrentTaskHandle() && cas_failures<10) {
    cas_failures++;
#if CONFIG_IDF_TARGET_LINUX
    if(grow_before_fallback && cas_failures==10) {
      TaskHandle_t owner=retry_owner; retry_owner=NULL;
      tsdb_page_cache_snapshot_t *live=tsdb_snapshot_acquire_current(db);
      tsdb_page_cache_snapshot_t *growth=tsdb_snapshot_clone(live);
      timeseries_page_header_t h=header(); growth_completed=growth!=NULL;
      for(size_t i=0;i<16 && growth_completed;i++)growth_completed=tsdb_pagecache_batch_add(growth,0x10000+i*4096,&h);
      if(growth_completed)tsdb_snapshot_swap(db,growth); else tsdb_snapshot_release(growth);
      tsdb_snapshot_release(live); retry_owner=owner;
      if(growth_completed)tsdb_audit_allocation_probe(1,false);
    }
#endif
    return false;
  }
  return __real_tsdb_snapshot_compare_and_swap(db,expected,replacement);
}
void __real_tsdb_snapshot_release(tsdb_page_cache_snapshot_t *);
void __wrap_tsdb_snapshot_release(tsdb_page_cache_snapshot_t *snap) {
  if(retry_owner==xTaskGetCurrentTaskHandle() && cas_failures==10 && ++releases_after_limit==2 && concurrent_worker) {
    TaskHandle_t worker=concurrent_worker; concurrent_worker=NULL;
    xTaskNotifyGive(worker);
    concurrent_progress=xSemaphoreTake(concurrent_done,pdMS_TO_TICKS(2000))==pdTRUE;
  }
  __real_tsdb_snapshot_release(snap);
}
static timeseries_page_header_t header(void) {
  timeseries_page_header_t h={.magic_number=TIMESERIES_MAGIC_NUM,.page_type=TIMESERIES_PAGE_TYPE_FIELD_DATA,
    .page_state=TIMESERIES_PAGE_STATE_ACTIVE,.page_size=4096,.sequence_num=1}; return h;
}
static timeseries_db_t *cache_db(void) {
  timeseries_db_t *db=calloc(1,sizeof(*db)); TEST_ASSERT_NOT_NULL(db);
  db->snapshot_mutex=xSemaphoreCreateMutex(); TEST_ASSERT_NOT_NULL(db->snapshot_mutex);
  db->current_snapshot=tsdb_snapshot_create(8); TEST_ASSERT_NOT_NULL(db->current_snapshot);
  return db;
}
static void free_cache_db(timeseries_db_t *db) {
  tsdb_snapshot_release(db->current_snapshot); vSemaphoreDelete(db->snapshot_mutex); free(db);
}
static bool contains(tsdb_page_cache_snapshot_t *s,uint32_t offset) {
  for(size_t i=0;i<s->count;i++)if(s->entries[i].offset==offset)return true;
  return false;
}
typedef struct { timeseries_db_t *db; SemaphoreHandle_t done; bool added; } concurrent_t;
static void add_concurrent(void *arg) {
  concurrent_t *c=arg; ulTaskNotifyTake(pdTRUE,portMAX_DELAY);
  timeseries_page_header_t h=header(); c->added=tsdb_pagecache_add_entry(c->db,0x9000,&h);
  xSemaphoreGive(c->done); vTaskDelete(NULL);
}
static void contention_test(bool remove) {
  timeseries_db_t *db=cache_db(); timeseries_page_header_t h=header();
  TEST_ASSERT_TRUE(tsdb_pagecache_add_entry(db,0x1000,&h));
  // Keep a reader pinned so mutations exercise copy-on-write and its retries.
  tsdb_page_cache_snapshot_t *held=tsdb_snapshot_acquire_current(db);
  concurrent_t c={.db=db,.done=xSemaphoreCreateBinary()}; TEST_ASSERT_NOT_NULL(c.done);
  TEST_ASSERT_EQUAL(pdPASS,xTaskCreate(add_concurrent,"cache_add",8192,&c,tskIDLE_PRIORITY+1,&concurrent_worker));
  retry_owner=xTaskGetCurrentTaskHandle(); cas_failures=releases_after_limit=0;
  concurrent_done=c.done; concurrent_progress=false;
  bool updated=remove ? tsdb_pagecache_remove_entry(db,0x1000) : tsdb_pagecache_add_entry(db,0x5000,&h);
  retry_owner=NULL;
  bool done=concurrent_progress || xSemaphoreTake(c.done,pdMS_TO_TICKS(2000))==pdTRUE;
  tsdb_page_cache_snapshot_t *live=tsdb_snapshot_acquire_current(db);
  bool retained=contains(live,0x9000) && (remove ? !contains(live,0x1000) : contains(live,0x5000));
  tsdb_snapshot_release(live); tsdb_snapshot_release(held); if(done)vSemaphoreDelete(c.done); free_cache_db(db);
  TEST_ASSERT_TRUE(updated); TEST_ASSERT_TRUE(done); TEST_ASSERT_TRUE(concurrent_progress);
  TEST_ASSERT_TRUE(c.added); TEST_ASSERT_TRUE(retained);
}
TEST_CASE("twelfth: add retry fallback retains a concurrent cache update","[twelfth][twelfth_add_race]") { contention_test(false); }
TEST_CASE("twelfth: remove retry fallback retains a concurrent cache update","[twelfth][twelfth_remove_race]") { contention_test(true); }
#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_allocation_probe(size_t,bool);
size_t tsdb_audit_allocation_count(void);
size_t tsdb_audit_largest_allocation(void);
TEST_CASE("twelfth: failed reservation leaves batch and live entries unchanged","[twelfth][twelfth_reservation]") {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db=timeseries_get_db_handle(); atomic_store(&db->compaction_in_progress,true);
  bool stable=true; unsigned failures=0;
  for(size_t fail=1;fail<=6;fail++) {
    tsdb_page_cache_snapshot_t *before=tsdb_snapshot_acquire_current(db);
    tsdb_page_cache_snapshot_t *batch=tsdb_pagecache_begin_batch(db); TEST_ASSERT_NOT_NULL(batch);
    uint32_t offset=UINT32_MAX,size=UINT32_MAX;
    tsdb_audit_allocation_probe(fail,false);
    bool reserved=tsdb_reserve_blank_region(db,4096,batch,&offset,&size);
    tsdb_audit_allocation_probe(0,false);
    tsdb_page_cache_snapshot_t *live=tsdb_snapshot_acquire_current(db);
    if(!reserved) {
      failures++;
      stable &= offset==UINT32_MAX && size==UINT32_MAX && live->count==before->count && batch->count==before->count && !batch->removed_count;
      if(live->count==before->count)stable &= !memcmp(live->entries,before->entries,before->count*sizeof(*before->entries));
      if(batch->count==before->count)stable &= !memcmp(batch->entries,before->entries,before->count*sizeof(*before->entries));
    }
    tsdb_snapshot_release(live); tsdb_snapshot_release(batch); tsdb_snapshot_release(before);
    if(reserved)TEST_ASSERT_TRUE(tsdb_pagecache_remove_entry(db,offset));
  }
  atomic_store(&db->compaction_in_progress,false); timeseries_deinit();
  TEST_ASSERT_GREATER_OR_EQUAL(3,failures); TEST_ASSERT_TRUE(stable);
}
#endif
TEST_CASE("twelfth: zero-sized reservation is rejected without cache changes","[twelfth][twelfth_zero]") {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db=timeseries_get_db_handle();
  tsdb_page_cache_snapshot_t *before=tsdb_snapshot_acquire_current(db);
  uint32_t offset=UINT32_MAX,size=UINT32_MAX;
  bool rejected=!tsdb_reserve_blank_region(db,0,NULL,&offset,&size);
  tsdb_page_cache_snapshot_t *after=tsdb_snapshot_acquire_current(db);
  bool same=after->count==before->count && offset==UINT32_MAX && size==UINT32_MAX;
  tsdb_snapshot_release(before);tsdb_snapshot_release(after);timeseries_deinit();
  TEST_ASSERT_TRUE(rejected);TEST_ASSERT_TRUE(same);
}
static void cache_benchmark(bool pinned) {
  timeseries_db_t *db=cache_db(); timeseries_page_header_t h=header();
  for(size_t i=0;i<1024;i++)TEST_ASSERT_TRUE(tsdb_pagecache_batch_add(db->current_snapshot,(i+1)*8192,&h));
  tsdb_page_cache_snapshot_t *held=tsdb_snapshot_acquire_current(db);
  bool ok=true; esp_log_level_set("*",ESP_LOG_NONE);
#if CONFIG_IDF_TARGET_LINUX
  tsdb_audit_allocation_probe(0,true);
#endif
  int64_t start=esp_timer_get_time();
  for(size_t i=0;i<200 && ok;i++) {
    // Exercise insertion/removal at the beginning, middle and end of the array.
    uint32_t offset=4096+(i%3)*512*8192;
    tsdb_page_cache_snapshot_t *reader=pinned ? tsdb_snapshot_acquire_current(db) : NULL;
    ok=tsdb_pagecache_add_entry(db,offset,&h);
    if(reader) { ok &= reader->count==1024 && !contains(reader,offset); tsdb_snapshot_release(reader); }
    reader=pinned ? tsdb_snapshot_acquire_current(db) : NULL;
    ok=ok && tsdb_pagecache_remove_entry(db,offset);
    if(reader) { ok &= reader->count==1025 && contains(reader,offset); tsdb_snapshot_release(reader); }
  }
  int64_t elapsed=esp_timer_get_time()-start;
#if CONFIG_IDF_TARGET_LINUX
  size_t allocations=tsdb_audit_allocation_count(),largest=tsdb_audit_largest_allocation();tsdb_audit_allocation_probe(0,false);
  printf("TWELFTH_CACHE us=%lld allocations=%zu largest=%zu pinned=%u\n",(long long)elapsed,allocations,largest,(unsigned)pinned);
#else
  printf("TWELFTH_CACHE us=%lld\n",(long long)elapsed);
#endif
  esp_log_level_set("*",ESP_LOG_INFO);
  bool stable=held->count==1024 && db->current_snapshot->count==1024;
  for(size_t i=0;i<1024;i++)stable &= held->entries[i].offset==(i+1)*8192 && db->current_snapshot->entries[i].offset==(i+1)*8192;
  tsdb_snapshot_release(held);free_cache_db(db);
  TEST_ASSERT_TRUE(ok);TEST_ASSERT_TRUE(stable);
#if CONFIG_IDF_TARGET_LINUX
  TEST_ASSERT_LESS_OR_EQUAL(pinned ? 800 : 2,allocations);
  TEST_ASSERT_LESS_OR_EQUAL(1025*sizeof(timeseries_cached_page_t),largest);
#endif
}

TEST_CASE("twelfth: cache add remove benchmark avoids redundant allocations","[twelfth][twelfth_cache_bench]") { cache_benchmark(false); }
TEST_CASE("twelfth: cache benchmark with a reader across every mutation","[twelfth][twelfth_cache_shared]") { cache_benchmark(true); }

#if CONFIG_IDF_TARGET_LINUX
TEST_CASE("twelfth: fallback allocation failure preserves updates and unlocks cache","[twelfth][twelfth_fallback_oom]") {
  timeseries_db_t *db=cache_db(); timeseries_page_header_t h=header();
  TEST_ASSERT_TRUE(tsdb_pagecache_add_entry(db,0x1000,&h));
  tsdb_page_cache_snapshot_t *held=tsdb_snapshot_acquire_current(db);
  cas_failures=releases_after_limit=0; concurrent_worker=NULL;
  grow_before_fallback=true; growth_completed=false; retry_owner=xTaskGetCurrentTaskHandle();
  bool rejected=!tsdb_pagecache_add_entry(db,0x5000,&h);
  retry_owner=NULL; grow_before_fallback=false; tsdb_audit_allocation_probe(0,false);
  tsdb_page_cache_snapshot_t *live=tsdb_snapshot_acquire_current(db);
  bool preserved=live->count==17 && !contains(live,0x5000) && held->count==1;
  for(size_t i=0;i<16;i++)preserved &= contains(live,0x10000+i*4096);
  tsdb_snapshot_release(live);tsdb_snapshot_release(held);
  bool retried=tsdb_pagecache_add_entry(db,0x5000,&h);free_cache_db(db);
  TEST_ASSERT_TRUE(growth_completed);TEST_ASSERT_TRUE(rejected);TEST_ASSERT_TRUE(preserved);TEST_ASSERT_TRUE(retried);
}
#endif

TEST_CASE("twelfth: mutations preserve every pinned snapshot","[twelfth][twelfth_pinned]") {
  timeseries_db_t *db=cache_db(); timeseries_page_header_t h=header();
  TEST_ASSERT_TRUE(tsdb_pagecache_add_entry(db,0x1000,&h));
  tsdb_page_cache_snapshot_t *first=tsdb_snapshot_acquire_current(db);
  h.sequence_num=2; bool updated=tsdb_pagecache_add_entry(db,0x1000,&h);
  tsdb_page_cache_snapshot_t *second=tsdb_snapshot_acquire_current(db);
  bool removed=tsdb_pagecache_remove_entry(db,0x1000);
  bool added=tsdb_pagecache_add_entry(db,0x2000,&h);
  bool stable=first->count==1 && first->entries[0].offset==0x1000 && first->entries[0].header.sequence_num==1 &&
    second->count==1 && second->entries[0].offset==0x1000 && second->entries[0].header.sequence_num==2 &&
    db->current_snapshot->count==1 && db->current_snapshot->entries[0].offset==0x2000;
  tsdb_snapshot_release(first);tsdb_snapshot_release(second);free_cache_db(db);
  TEST_ASSERT_TRUE(updated);TEST_ASSERT_TRUE(removed);TEST_ASSERT_TRUE(added);TEST_ASSERT_TRUE(stable);
}
