#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_compaction.h"
#include "timeseries_page_rewriter.h"
#include "timeseries_iterator.h"
#include "timeseries_revision.h"
#include "timeseries_page_cache_snapshot.h"
#include "timeseries_page_cache.h"
#include "freertos/task.h"
#include "esp_timer.h"
#include "esp_log.h"
#include <stdlib.h>
#include <string.h>

extern timeseries_db_t *timeseries_get_db_handle(void);
static TaskHandle_t write_owner;
static uint32_t fail_write_offset;
static bool write_failed;
static TaskHandle_t copy_measure_owner;
static size_t copy_write_calls;
esp_err_t __real_esp_partition_write(const esp_partition_t *,size_t,const void *,size_t);
esp_err_t __wrap_esp_partition_write(const esp_partition_t *p,size_t offset,const void *data,size_t length) {
  if(copy_measure_owner==xTaskGetCurrentTaskHandle()) copy_write_calls++;
  if(write_owner==xTaskGetCurrentTaskHandle() && offset==fail_write_offset) {
    write_failed=true; write_owner=NULL; return ESP_FAIL;
  }
  return __real_esp_partition_write(p,offset,data,length);
}
#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_allocation_probe(size_t, bool);
size_t tsdb_audit_allocation_count(void);
size_t tsdb_audit_largest_allocation(void);
void tsdb_audit_fail_read_at(size_t, bool);
#endif
static bool insert(size_t count, size_t first) {
  uint64_t *ts = malloc(count*sizeof(*ts));
  timeseries_field_value_t *values = calloc(count, sizeof(*values));
  if (!ts || !values) { free(ts); free(values); return false; }
  for (size_t i=0; i<count; i++) { ts[i]=first+i; values[i].type=TIMESERIES_FIELD_TYPE_INT; values[i].data.int_val=first+i; }
  const char *field="v";
  timeseries_insert_data_t in={.measurement_name="eleventh",.field_names=&field,.field_values=values,
    .num_fields=1,.timestamps_ms=ts,.num_points=count};
  bool ok=timeseries_insert(&in); free(ts); free(values); return ok;
}
static bool matches(size_t count) {
  timeseries_query_t q={.measurement_name="eleventh"}; timeseries_query_result_t r={0};
  bool ok=timeseries_query(&q,&r) && r.num_points==count;
  for(size_t i=0; ok && i<count; i++) ok=r.timestamps[i]==i+1 && r.columns[0].values[i].data.int_val==(int64_t)i+1;
  timeseries_query_free_result(&r); return ok;
}
static timeseries_db_t *prepare(size_t count, size_t chunk, uint32_t *page, timeseries_field_data_header_t *header) {
  TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
  timeseries_db_t *db=timeseries_get_db_handle(); atomic_store(&db->compaction_in_progress,true);
  size_t old=db->chunk_size; db->chunk_size=chunk;
  bool ok=insert(count,1); db->chunk_size=old; TEST_ASSERT_TRUE(ok);
  *page=db->last_l0_page_offset;
  TEST_ASSERT_EQUAL(ESP_OK,esp_partition_read(db->partition,*page+sizeof(timeseries_page_header_t),header,sizeof(*header)));
  return db;
}
static void cleanup(timeseries_db_t *db) { atomic_store(&db->compaction_in_progress,false); timeseries_deinit(); }
static bool same_bytes(timeseries_db_t *db,uint32_t a,uint32_t b,size_t length) {
  uint8_t left[128],right[128];
  for(size_t i=0;i<length;i+=sizeof(left)) {
    size_t n=length-i; if(n>sizeof(left))n=sizeof(left);
    if(esp_partition_read(db->partition,a+i,left,n)!=ESP_OK || esp_partition_read(db->partition,b+i,right,n)!=ESP_OK || memcmp(left,right,n))return false;
  }
  return true;
}
TEST_CASE("eleventh: large record copying requires no record-sized allocation", "[eleventh][eleventh_copy]") {
  uint32_t page; timeseries_field_data_header_t h;
  timeseries_db_t *db=prepare(500,500,&page,&h);
  timeseries_page_rewriter_t w={0};
  TEST_ASSERT_TRUE(timeseries_page_rewriter_start(db,1,sizeof(timeseries_page_header_t)+sizeof(h)+h.record_length,&w));
#if CONFIG_IDF_TARGET_LINUX
  tsdb_audit_allocation_probe(0,true);
#endif
  bool copied=timeseries_page_rewriter_write_field_data(&w,page,sizeof(timeseries_page_header_t),&h);
#if CONFIG_IDF_TARGET_LINUX
  size_t allocations=tsdb_audit_allocation_count(); tsdb_audit_allocation_probe(0,false);
#endif
  bool equal=copied && same_bytes(db,page+sizeof(timeseries_page_header_t),w.base_offset+sizeof(timeseries_page_header_t),sizeof(h)+h.record_length);
  bool finalized=timeseries_page_rewriter_finalize(&w);
  if(!finalized)timeseries_page_rewriter_abort(&w);
  bool queried=matches(500); cleanup(db);
  TEST_ASSERT_TRUE(copied); TEST_ASSERT_TRUE(equal); TEST_ASSERT_TRUE(finalized); TEST_ASSERT_TRUE(queried);
#if CONFIG_IDF_TARGET_LINUX
  TEST_ASSERT_EQUAL(0,allocations);
#endif
}
TEST_CASE("eleventh: page rewrite has bounded scratch and allocation count", "[eleventh][eleventh_memory]") {
  uint32_t page; timeseries_field_data_header_t h;
  timeseries_db_t *db=prepare(64,1,&page,&h);
#if CONFIG_IDF_TARGET_LINUX
  tsdb_audit_allocation_probe(0,true);
#endif
  bool rewritten=timeseries_compact_page_list(db,&page,1);
#if CONFIG_IDF_TARGET_LINUX
  size_t largest=tsdb_audit_largest_allocation(),allocations=tsdb_audit_allocation_count(); tsdb_audit_allocation_probe(0,false);
#endif
  bool queried=matches(64); cleanup(db);
  TEST_ASSERT_TRUE(rewritten); TEST_ASSERT_TRUE(queried);
#if CONFIG_IDF_TARGET_LINUX
  TEST_ASSERT_LESS_OR_EQUAL(4096,largest); TEST_ASSERT_LESS_OR_EQUAL(20,allocations);
#endif
}
TEST_CASE("eleventh: interrupted rewrite stays invisible after reopen", "[eleventh][eleventh_reopen]") {
  uint32_t page; timeseries_field_data_header_t h; timeseries_db_t *db=prepare(1,1,&page,&h);
  uint64_t revision=db->write_revision;
  timeseries_page_rewriter_t w={0}; TEST_ASSERT_TRUE(timeseries_page_rewriter_start(db,1,4096,&w));
  // Model loss of power after the record header, before its columns/trailer.
  TEST_ASSERT_EQUAL(ESP_OK,esp_partition_write(db->partition,w.write_ptr,&h,sizeof(h)));
  cleanup(db); // Deliberately no writer abort: rebuild solely from flash.
  bool reopened=timeseries_init(); db=timeseries_get_db_handle();
  bool restored=reopened && db->write_revision==revision;
  bool inserted=insert(1,2),queried=matches(2);
  TEST_ASSERT_TRUE(timeseries_clear_all()); timeseries_deinit();
  TEST_ASSERT_TRUE(reopened); TEST_ASSERT_TRUE(restored); TEST_ASSERT_TRUE(inserted); TEST_ASSERT_TRUE(queried);
}
TEST_CASE("eleventh: abort after failed start preserves metadata", "[eleventh][eleventh_abort_start]") {
  uint32_t page; timeseries_field_data_header_t h; timeseries_db_t *db=prepare(1,1,&page,&h);
  timeseries_page_header_t before,after;
  TEST_ASSERT_EQUAL(ESP_OK,esp_partition_read(db->partition,0,&before,sizeof(before)));
  timeseries_page_rewriter_t w={0};
  bool rejected=!timeseries_page_rewriter_start(db,1,db->partition->size+4096,&w);
  timeseries_page_rewriter_abort(&w);
  bool preserved=esp_partition_read(db->partition,0,&after,sizeof(after))==ESP_OK && !memcmp(&before,&after,sizeof(before));
  atomic_store(&db->compaction_in_progress,false); TEST_ASSERT_TRUE(timeseries_clear_all()); timeseries_deinit();
  TEST_ASSERT_TRUE(rejected); TEST_ASSERT_TRUE(preserved);
}
TEST_CASE("eleventh: abort after finalize preserves published page", "[eleventh][eleventh_abort_final]") {
  uint32_t page; timeseries_field_data_header_t h; timeseries_db_t *db=prepare(1,1,&page,&h);
  timeseries_page_rewriter_t w={0}; TEST_ASSERT_TRUE(timeseries_page_rewriter_start(db,1,4096,&w));
  TEST_ASSERT_TRUE(timeseries_page_rewriter_write_field_data(&w,page,sizeof(timeseries_page_header_t),&h));
  TEST_ASSERT_TRUE(timeseries_page_rewriter_finalize(&w));
  timeseries_page_rewriter_abort(&w);
  timeseries_page_header_t header;
  bool active=esp_partition_read(db->partition,w.base_offset,&header,sizeof(header))==ESP_OK && header.page_state==TIMESERIES_PAGE_STATE_ACTIVE;
  cleanup(db); TEST_ASSERT_TRUE(active);
}
TEST_CASE("eleventh: wrapped source offset is rejected before copying", "[eleventh][eleventh_bounds]") {
  uint32_t page; timeseries_field_data_header_t h; timeseries_db_t *db=prepare(1,1,&page,&h);
  timeseries_page_rewriter_t w={0}; TEST_ASSERT_TRUE(timeseries_page_rewriter_start(db,1,4096,&w));
  uint32_t initial=w.write_ptr;
  bool rejected=!timeseries_page_rewriter_write_field_data(&w,UINT32_MAX-100,101,&h);
  bool unchanged=w.write_ptr==initial;
  timeseries_page_rewriter_abort(&w); bool queried=matches(1); cleanup(db);
  TEST_ASSERT_TRUE(rejected); TEST_ASSERT_TRUE(unchanged); TEST_ASSERT_TRUE(queried);
}
#if CONFIG_IDF_TARGET_LINUX
TEST_CASE("eleventh: failed record copy cannot be finalized", "[eleventh][eleventh_read_failure]") {
  uint32_t page; timeseries_field_data_header_t h; timeseries_db_t *db=prepare(500,500,&page,&h);
  timeseries_page_rewriter_t w={0}; TEST_ASSERT_TRUE(timeseries_page_rewriter_start(db,1,16384,&w));
  tsdb_audit_fail_read_at(page+sizeof(timeseries_page_header_t),true);
  bool rejected=!timeseries_page_rewriter_write_field_data(&w,page,sizeof(timeseries_page_header_t),&h);
  tsdb_audit_fail_read_at(0,false);
  bool blocked=!timeseries_page_rewriter_finalize(&w);
  timeseries_page_rewriter_abort(&w); bool queried=matches(500); cleanup(db);
  TEST_ASSERT_TRUE(rejected); TEST_ASSERT_TRUE(blocked); TEST_ASSERT_TRUE(queried);
}
#endif

TEST_CASE("eleventh: legacy revision footer crosses copy chunk boundaries", "[eleventh][eleventh_legacy]") {
  uint32_t page; timeseries_field_data_header_t h; timeseries_db_t *db=prepare(1,1,&page,&h);
  uint32_t page_size=db->last_l0_page_size;
  uint8_t *bytes=malloc(page_size); TEST_ASSERT_NOT_NULL(bytes);
  timeseries_page_header_t ph;
  TEST_ASSERT_EQUAL(ESP_OK,esp_partition_read(db->partition,page,&ph,sizeof(ph)));
  const size_t lengths[]={505,511,512,513,1018,1023,1024,1025};
  bool ok=true;
  for(size_t i=0;i<sizeof(lengths)/sizeof(lengths[0]) && ok;i++) {
    memset(bytes,0xff,page_size); memcpy(bytes,&ph,sizeof(ph));
    h.flags |= TSDB_FIELDDATA_FLAG_REVISION; h.record_length=lengths[i]-sizeof(h);
    memcpy(bytes+sizeof(ph),&h,sizeof(h));
    // Opaque legacy payload: the rewriter must preserve every byte independently
    // of the timestamp/value codec, including at footer split boundaries.
    for(size_t j=sizeof(h);j<lengths[i];j++) bytes[sizeof(ph)+j]=(uint8_t)(j*17);
    ok=esp_partition_erase_range(db->partition,page,page_size)==ESP_OK &&
       esp_partition_write(db->partition,page,bytes,page_size)==ESP_OK;
    timeseries_page_rewriter_t w={0};
    ok=ok && timeseries_page_rewriter_start(db,1,sizeof(ph)+lengths[i]+8,&w);
    ok=ok && timeseries_page_rewriter_write_field_data(&w,page,sizeof(ph),&h);
    timeseries_field_data_header_t copied; uint64_t revision=0;
    ok=ok && esp_partition_read(db->partition,w.base_offset+sizeof(ph),&copied,sizeof(copied))==ESP_OK &&
       copied.record_length==h.record_length+8 && !(copied.flags & TSDB_FIELDDATA_FLAG_REVISION) &&
       (copied.flags & TSDB_FIELDDATA_FLAG_REVISION_VECTOR) &&
       same_bytes(db,page+sizeof(ph)+sizeof(h),w.base_offset+sizeof(ph)+sizeof(h),h.record_length) &&
       esp_partition_read(db->partition,w.base_offset+sizeof(ph)+lengths[i],&revision,8)==ESP_OK &&
       revision==tsdb_legacy_revision(ph.sequence_num,page+sizeof(ph)+sizeof(h));
    timeseries_page_rewriter_abort(&w);
  }
  free(bytes); atomic_store(&db->compaction_in_progress,false);
  TEST_ASSERT_TRUE(timeseries_clear_all()); timeseries_deinit(); TEST_ASSERT_TRUE(ok);
}
TEST_CASE("eleventh: failure after a copied chunk prevents retry and publication", "[eleventh][eleventh_write_failure]") {
  uint32_t page; timeseries_field_data_header_t h; timeseries_db_t *db=prepare(500,500,&page,&h);
  timeseries_page_rewriter_t w={0}; TEST_ASSERT_TRUE(timeseries_page_rewriter_start(db,1,16384,&w));
  write_failed=false; fail_write_offset=w.write_ptr+512; write_owner=xTaskGetCurrentTaskHandle();
  bool rejected=!timeseries_page_rewriter_write_field_data(&w,page,sizeof(timeseries_page_header_t),&h);
  write_owner=NULL;
  bool retry_blocked=!timeseries_page_rewriter_write_field_data(&w,page,sizeof(timeseries_page_header_t),&h);
  bool blocked=!timeseries_page_rewriter_finalize(&w);
  timeseries_page_rewriter_abort(&w); bool queried=matches(500); cleanup(db);
  TEST_ASSERT_TRUE(write_failed); TEST_ASSERT_TRUE(rejected); TEST_ASSERT_TRUE(retry_blocked);
  TEST_ASSERT_TRUE(blocked); TEST_ASSERT_TRUE(queried);
}

TEST_CASE("eleventh: batch abort releases the live allocation reservation", "[eleventh][eleventh_batch_abort]") {
  uint32_t page; timeseries_field_data_header_t h; timeseries_db_t *db=prepare(1,1,&page,&h);
  tsdb_page_cache_snapshot_t *live=tsdb_snapshot_acquire_current(db); TEST_ASSERT_NOT_NULL(live);
  size_t before=live->count; tsdb_snapshot_release(live);
  tsdb_page_cache_snapshot_t *batch=tsdb_pagecache_begin_batch(db); TEST_ASSERT_NOT_NULL(batch);
  timeseries_page_rewriter_t w={.batch_snapshot=batch};
  bool started=timeseries_page_rewriter_start(db,1,4096,&w);
  timeseries_page_rewriter_abort(&w);
  live=tsdb_snapshot_acquire_current(db); TEST_ASSERT_NOT_NULL(live);
  bool released=live->count==before && batch->count==before;
  tsdb_snapshot_release(live); tsdb_snapshot_release(batch);
  bool queried=matches(1); cleanup(db);
  TEST_ASSERT_TRUE(started); TEST_ASSERT_TRUE(released); TEST_ASSERT_TRUE(queried);
}
#if CONFIG_IDF_TARGET_LINUX
TEST_CASE("eleventh: failed publication prevents subsequent record writes", "[eleventh][eleventh_publish_failure]") {
  uint32_t page; timeseries_field_data_header_t h; timeseries_db_t *db=prepare(1,1,&page,&h);
  timeseries_page_rewriter_t w={0}; TEST_ASSERT_TRUE(timeseries_page_rewriter_start(db,1,4096,&w));
  TEST_ASSERT_TRUE(timeseries_page_rewriter_write_field_data(&w,page,sizeof(timeseries_page_header_t),&h));
  // A pinned reader forces copy-on-write publication, making its allocation
  // failure reachable even when unshared snapshots can be updated in place.
  tsdb_page_cache_snapshot_t *reader=tsdb_snapshot_acquire_current(db);
  tsdb_audit_allocation_probe(1,false);
  bool rejected=!timeseries_page_rewriter_finalize(&w);
  tsdb_audit_allocation_probe(0,false);
  tsdb_snapshot_release(reader);
  bool blocked=!timeseries_page_rewriter_write_field_data(&w,page,sizeof(timeseries_page_header_t),&h);
  timeseries_page_rewriter_abort(&w); bool queried=matches(1); cleanup(db);
  TEST_ASSERT_TRUE(rejected); TEST_ASSERT_TRUE(blocked); TEST_ASSERT_TRUE(queried);
}
#endif

TEST_CASE("eleventh: large record copy throughput benchmark", "[eleventh][eleventh_copy_bench]") {
  uint32_t page; timeseries_field_data_header_t h; timeseries_db_t *db=prepare(500,500,&page,&h);
  int64_t elapsed=0; size_t writes=0; bool ok=true;
  esp_log_level_set("*",ESP_LOG_NONE);
  for(size_t i=0;i<64 && ok;i++) {
    timeseries_page_rewriter_t w={0};
    ok=timeseries_page_rewriter_start(db,1,sizeof(timeseries_page_header_t)+sizeof(h)+h.record_length,&w);
    copy_write_calls=0; copy_measure_owner=xTaskGetCurrentTaskHandle();
    int64_t start=esp_timer_get_time();
    ok=ok && timeseries_page_rewriter_write_field_data(&w,page,sizeof(timeseries_page_header_t),&h);
    elapsed+=esp_timer_get_time()-start; copy_measure_owner=NULL; writes+=copy_write_calls;
    ok=ok && same_bytes(db,page+sizeof(timeseries_page_header_t),w.base_offset+sizeof(timeseries_page_header_t),sizeof(h)+h.record_length);
    timeseries_page_rewriter_abort(&w);
  }
  esp_log_level_set("*",ESP_LOG_INFO);
  printf("ELEVENTH_COPY us=%lld writes=%zu\n",(long long)elapsed,writes);
  bool queried=matches(500); cleanup(db); TEST_ASSERT_TRUE(ok); TEST_ASSERT_TRUE(queried);
}

TEST_CASE("eleventh: large page rewrite reuses the locked insert buffer", "[eleventh][eleventh_large_rewrite]") {
  uint32_t page; timeseries_field_data_header_t h; timeseries_db_t *db=prepare(500,500,&page,&h);
  TEST_ASSERT_NOT_NULL(db->write_buffer);
  copy_write_calls=0; copy_measure_owner=xTaskGetCurrentTaskHandle();
#if CONFIG_IDF_TARGET_LINUX
  tsdb_audit_allocation_probe(0,true);
#endif
  esp_log_level_set("*",ESP_LOG_NONE);
  int64_t start=esp_timer_get_time();
  bool rewritten=timeseries_compact_page_list(db,&page,1);
  int64_t elapsed=esp_timer_get_time()-start;
  copy_measure_owner=NULL;
#if CONFIG_IDF_TARGET_LINUX
  size_t largest=tsdb_audit_largest_allocation(); tsdb_audit_allocation_probe(0,false);
#endif
  size_t writes=copy_write_calls;
  esp_log_level_set("*",ESP_LOG_INFO);
  printf("ELEVENTH_LARGE us=%lld writes=%zu\n",(long long)elapsed,writes);
  bool queried=matches(500); cleanup(db);
  TEST_ASSERT_TRUE(rewritten); TEST_ASSERT_TRUE(queried); TEST_ASSERT_LESS_OR_EQUAL(4,writes);
#if CONFIG_IDF_TARGET_LINUX
  TEST_ASSERT_LESS_OR_EQUAL(4096,largest);
#endif
}
