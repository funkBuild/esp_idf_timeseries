#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_points_iterator.h"
#include "timeseries_multi_iterator.h"
#include "alp/alp_stream_decoder.h"
#include "alp/alp_int_codec.h"
#include "alp/alp_encoder.h"
#include "esp_timer.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>

static void ram_iter(timeseries_points_iterator_t *it, uint64_t first, uint64_t step,
                     size_t count, int64_t value_bias) {
    memset(it, 0, sizeof(*it));
    it->valid = true;
    it->series_type = TIMESERIES_FIELD_TYPE_INT;
    it->ts_count = count;
    it->ts_array = malloc(count*sizeof(*it->ts_array));
    it->int_array = malloc(count*sizeof(*it->int_array));
    TEST_ASSERT_NOT_NULL(it->ts_array); TEST_ASSERT_NOT_NULL(it->int_array);
    for (size_t i=0; i<count; i++) {
        it->ts_array[i] = first+i*step;
        it->int_array[i] = (int64_t)it->ts_array[i]+value_bias;
    }
}

TEST_CASE("third: merge peek agrees with newest duplicate returned by next", "[third][third_peek]") {
    timeseries_points_iterator_t it[2];
    timeseries_points_iterator_t *ptrs[] = {&it[0], &it[1]};
    uint32_t seq[] = {1,2};
    ram_iter(&it[0], 10, 1, 2, 0);
    ram_iter(&it[1], 10, 1, 2, 100);
    timeseries_multi_points_iterator_t multi;
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_init(ptrs, seq, 2, &multi));
    uint64_t peek_ts, next_ts;
    timeseries_field_value_t peek_val, next_val;
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_peek(&multi, &peek_ts, &peek_val));
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_next(&multi, &next_ts, &next_val));
    timeseries_multi_points_iterator_deinit(&multi);
    for (int i=0; i<2; i++) timeseries_points_iterator_deinit(&it[i]);
    TEST_ASSERT_EQUAL_UINT64(next_ts, peek_ts);
    TEST_ASSERT_EQUAL_INT64(110, next_val.data.int_val);
    TEST_ASSERT_EQUAL_INT64(next_val.data.int_val, peek_val.data.int_val);
}

static void merge_benchmark(int pattern) {
    enum {SUBS=16, POINTS=256};
    timeseries_points_iterator_t *it = calloc(SUBS, sizeof(*it));
    timeseries_points_iterator_t *ptrs[SUBS];
    uint32_t seq[SUBS];
    TEST_ASSERT_NOT_NULL(it);
    for (int s=0; s<SUBS; s++) {
        ptrs[s]=&it[s]; seq[s]=s+1;
        uint64_t first = pattern==0 ? s*POINTS+1 : (pattern==1 ? s+1 : 1);
        ram_iter(&it[s], first, pattern==1 ? SUBS : 1, POINTS, pattern==2 ? s*10000 : 0);
    }
    size_t expected = pattern==2 ? POINTS : SUBS*POINTS;
    int64_t elapsed=0;
    for (int rep=0; rep<20; rep++) {
        for (int s=0; s<SUBS; s++) { it[s].current_idx=0; it[s].valid=true; }
        timeseries_multi_points_iterator_t multi;
        TEST_ASSERT_TRUE(timeseries_multi_points_iterator_init(ptrs, seq, SUBS, &multi));
        int64_t start=esp_timer_get_time();
        size_t count=0; uint64_t ts; timeseries_field_value_t v;
        while (timeseries_multi_points_iterator_next(&multi, &ts, &v)) {
            count++;
            TEST_ASSERT_EQUAL_UINT64(count, ts);
            TEST_ASSERT_EQUAL_INT64((int64_t)count+(pattern==2 ? (SUBS-1)*10000 : 0), v.data.int_val);
        }
        elapsed+=esp_timer_get_time()-start;
        TEST_ASSERT_EQUAL(expected, count);
        timeseries_multi_points_iterator_deinit(&multi);
    }
    printf("THIRD_MERGE pattern=%d points=%zu avg_us=%lld\n",pattern,expected,(long long)(elapsed/20));
    for (int s=0; s<SUBS; s++) timeseries_points_iterator_deinit(&it[s]);
    free(it);
}
TEST_CASE("third: merge ordered records benchmark", "[third][third_merge]") { merge_benchmark(0); }
TEST_CASE("third: merge interleaved records benchmark", "[third][third_merge]") { merge_benchmark(1); }
TEST_CASE("third: merge duplicate records benchmark", "[third][third_merge]") { merge_benchmark(2); }

TEST_CASE("third: unique merge roots preserve strings and NULL outputs", "[third][third_strings]") {
    timeseries_points_iterator_t it[2] = {0};
    timeseries_points_iterator_t *ptrs[]={&it[0],&it[1]};
    uint32_t seq[]={1,2};
    for (int s=0; s<2; s++) {
        it[s].valid=true; it[s].series_type=TIMESERIES_FIELD_TYPE_STRING;
        it[s].ts_count=it[s].string_array_count=2;
        it[s].ts_array=malloc(2*sizeof(uint64_t));
        it[s].string_array=calloc(2,sizeof(string_array_t));
        TEST_ASSERT_NOT_NULL(it[s].ts_array); TEST_ASSERT_NOT_NULL(it[s].string_array);
        for (int i=0; i<2; i++) {
            it[s].ts_array[i]=s*2+i+1;
            it[s].string_array[i].str=strdup(s ? "new" : "old");
            it[s].string_array[i].length=3;
        }
    }
    timeseries_multi_points_iterator_t m;
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_init(ptrs,seq,2,&m));
    timeseries_field_value_t v; uint64_t ts;
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_next(&m,&ts,&v));
    TEST_ASSERT_EQUAL_STRING("old",v.data.string_val.str);
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_next(&m,NULL,NULL));
    TEST_ASSERT_EQUAL_STRING("old",v.data.string_val.str);
    free(v.data.string_val.str);
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_next(&m,&ts,&v));
    TEST_ASSERT_EQUAL_UINT64(3,ts); TEST_ASSERT_EQUAL_STRING("new",v.data.string_val.str);
    timeseries_multi_points_iterator_deinit(&m);
    for (int s=0; s<2; s++) timeseries_points_iterator_deinit(&it[s]);
    TEST_ASSERT_EQUAL_STRING("new",v.data.string_val.str); free(v.data.string_val.str);
}

#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_reset_reads(void);
size_t tsdb_audit_read_bytes(void);
#endif

static void raw_range_test(timeseries_field_type_e type) {
    TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
    timeseries_set_chunk_size(500);
    uint64_t *ts=malloc(500*sizeof(*ts));
    timeseries_field_value_t *vals=calloc(500,sizeof(*vals));
    TEST_ASSERT_NOT_NULL(ts); TEST_ASSERT_NOT_NULL(vals);
    for (int i=0; i<500; i++) {
        ts[i]=i+1; vals[i].type=type;
        if (type==TIMESERIES_FIELD_TYPE_INT) vals[i].data.int_val=i;
        else if (type==TIMESERIES_FIELD_TYPE_FLOAT) vals[i].data.float_val=i+0.5;
        else vals[i].data.bool_val=(i%2)!=0;
    }
    const char *field="v";
    timeseries_insert_data_t ins={.measurement_name="third",.field_names=&field,.num_fields=1,
        .timestamps_ms=ts,.field_values=vals,.num_points=500};
    TEST_ASSERT_TRUE(timeseries_insert(&ins));
    free(ts); free(vals);
    size_t bytes=0; int64_t start=esp_timer_get_time();
    for (int rep=0; rep<20; rep++) {
#if CONFIG_IDF_TARGET_LINUX
        tsdb_audit_reset_reads();
#endif
        timeseries_query_t q={.measurement_name="third",.start_ms=491,.end_ms=500,.limit=10};
        timeseries_query_result_t r={0};
        TEST_ASSERT_TRUE(timeseries_query(&q,&r));
#if CONFIG_IDF_TARGET_LINUX
        bytes=tsdb_audit_read_bytes();
#endif
        TEST_ASSERT_EQUAL(10,r.num_points);
        for (int i=0; i<10; i++) {
            TEST_ASSERT_EQUAL_UINT64(491+i,r.timestamps[i]);
            if (type==TIMESERIES_FIELD_TYPE_INT) TEST_ASSERT_EQUAL_INT64(490+i,r.columns[0].values[i].data.int_val);
            else if (type==TIMESERIES_FIELD_TYPE_FLOAT) TEST_ASSERT_DOUBLE_WITHIN(.0001,490.5+i,r.columns[0].values[i].data.float_val);
            else TEST_ASSERT_EQUAL((i%2)!=0,r.columns[0].values[i].data.bool_val);
        }
        timeseries_query_free_result(&r);
    }
    printf("THIRD_RANGE type=%d avg_us=%lld bytes=%zu\n",type,(long long)((esp_timer_get_time()-start)/20),bytes);
    // An empty range inside the record's bounds, then an inclusive endpoint.
    timeseries_query_t q={.measurement_name="third",.start_ms=490,.end_ms=480};
    timeseries_query_result_t r={0}; TEST_ASSERT_TRUE(timeseries_query(&q,&r));
    TEST_ASSERT_EQUAL(0,r.num_points); timeseries_query_free_result(&r);
    q.start_ms=q.end_ms=500;
    TEST_ASSERT_TRUE(timeseries_query(&q,&r)); TEST_ASSERT_EQUAL(1,r.num_points);
    TEST_ASSERT_EQUAL_UINT64(500,r.timestamps[0]); timeseries_query_free_result(&r);
    timeseries_deinit();
#if CONFIG_IDF_TARGET_LINUX
    // Includes the query's metadata reads. Buffered metadata scans read whole
    // 64 B+ blocks (fewer, larger reads), about 110 B more per scan of this
    // tiny page than exact per-entry reads; the query still scans 4 times.
    TEST_ASSERT_LESS_THAN(type==TIMESERIES_FIELD_TYPE_BOOL ? 8600 : 9000,bytes);
#endif
}
TEST_CASE("third: narrow raw integer record", "[third][third_range]") {raw_range_test(TIMESERIES_FIELD_TYPE_INT);}
TEST_CASE("third: narrow raw float record", "[third][third_range]") {raw_range_test(TIMESERIES_FIELD_TYPE_FLOAT);}
TEST_CASE("third: narrow raw bool record", "[third][third_range]") {raw_range_test(TIMESERIES_FIELD_TYPE_BOOL);}

TEST_CASE("third: ALP skip crosses blocks and preserves the next value", "[third][third_skip]") {
    enum {N=4096};
    int64_t *values=malloc(N*sizeof(*values)); TEST_ASSERT_NOT_NULL(values);
    for (int i=0;i<N;i++) values[i]=(i%2) ? INT64_MAX-i : INT64_MIN+i;
    uint8_t *encoded=NULL; size_t len=0;
    len=alp_encode_int(values,N,&encoded);
    TEST_ASSERT_GREATER_THAN(0,len);
    int64_t elapsed=0;
    for (int rep=0;rep<100;rep++) {
        alp_stream_decoder_t dec;
        TEST_ASSERT_TRUE(alp_stream_decoder_init(&dec,encoded,len,false));
        int64_t start=esp_timer_get_time();
        TEST_ASSERT_TRUE(alp_stream_decoder_skip(&dec,0));
        TEST_ASSERT_TRUE(alp_stream_decoder_skip(&dec,127));
        TEST_ASSERT_TRUE(alp_stream_decoder_skip(&dec,129));
        TEST_ASSERT_TRUE(alp_stream_decoder_skip(&dec,N-257));
        elapsed+=esp_timer_get_time()-start;
        int64_t v; TEST_ASSERT_TRUE(alp_stream_decoder_next_int(&dec,&v));
        TEST_ASSERT_EQUAL_INT64(values[N-1],v);
        TEST_ASSERT_FALSE(alp_stream_decoder_skip(&dec,1));
        alp_stream_decoder_deinit(&dec);
    }
    printf("THIRD_SKIP avg_us=%lld\n",(long long)(elapsed/100));
    free(values);free(encoded);
}

TEST_CASE("third: narrow raw strings release skipped values", "[third][third_string_range]") {
    TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
    uint64_t ts[]={1,2,3,4};
    timeseries_field_value_t vals[]={
        {.type=TIMESERIES_FIELD_TYPE_STRING,.data.string_val={"unused prefix",13}},
        {.type=TIMESERIES_FIELD_TYPE_STRING,.data.string_val={"selected",8}},
        {.type=TIMESERIES_FIELD_TYPE_STRING,.data.string_val={"",0}},
        {.type=TIMESERIES_FIELD_TYPE_STRING,.data.string_val={"unused suffix",13}},
    };
    const char *field="v";
    timeseries_insert_data_t ins={.measurement_name="third_strings",.field_names=&field,.num_fields=1,
        .timestamps_ms=ts,.field_values=vals,.num_points=4};
    TEST_ASSERT_TRUE(timeseries_insert(&ins));
    timeseries_query_t q={.measurement_name="third_strings",.start_ms=2,.end_ms=3};
    timeseries_query_result_t r={0};
    TEST_ASSERT_TRUE(timeseries_query(&q,&r)); TEST_ASSERT_EQUAL(2,r.num_points);
    TEST_ASSERT_EQUAL_STRING("selected",r.columns[0].values[0].data.string_val.str);
    TEST_ASSERT_EQUAL(0,r.columns[0].values[1].data.string_val.length);
    timeseries_query_free_result(&r); timeseries_deinit();
}

TEST_CASE("third: narrowing cannot hide a truncated raw value column", "[third][third_truncated]") {
    TEST_ASSERT_TRUE(timeseries_init()); TEST_ASSERT_TRUE(timeseries_clear_all());
    uint64_t ts[]={1,2,3};
    timeseries_field_value_t vals[3]={{.type=TIMESERIES_FIELD_TYPE_INT},
        {.type=TIMESERIES_FIELD_TYPE_INT},{.type=TIMESERIES_FIELD_TYPE_INT}};
    const char *field="v";
    timeseries_insert_data_t ins={.measurement_name="third_short",.field_names=&field,.num_fields=1,
        .timestamps_ms=ts,.field_values=vals,.num_points=3};
    TEST_ASSERT_TRUE(timeseries_insert(&ins));
    extern timeseries_db_t *timeseries_get_db_handle(void);
    timeseries_db_t *db=timeseries_get_db_handle();
    uint32_t short_len=8;
    size_t offset=db->last_l0_page_offset+sizeof(timeseries_page_header_t)+
                  sizeof(timeseries_field_data_header_t)+sizeof(uint32_t);
    TEST_ASSERT_EQUAL(ESP_OK,esp_partition_write(db->partition,offset,&short_len,sizeof(short_len)));
    timeseries_query_t q={.measurement_name="third_short",.start_ms=1,.end_ms=1};
    timeseries_query_result_t r={0};
    bool ok=timeseries_query(&q,&r);
    timeseries_query_free_result(&r); timeseries_deinit(); TEST_ASSERT_FALSE(ok);
}

TEST_CASE("third: ALP float skip handles block tails and truncated data", "[third][third_float_skip]") {
    double *values=malloc(260*sizeof(*values)); TEST_ASSERT_NOT_NULL(values);
    for (int i=0;i<260;i++) values[i]=i%2 ? i+3.141592653589793 : i*.125;
    uint8_t *encoded=NULL;
    size_t len=alp_encode(values,260,&encoded); TEST_ASSERT_GREATER_THAN(0,len);
    alp_stream_decoder_t dec;
    TEST_ASSERT_TRUE(alp_stream_decoder_init(&dec,encoded,len,true));
    TEST_ASSERT_TRUE(alp_stream_decoder_skip(&dec,127));
    double value; TEST_ASSERT_TRUE(alp_stream_decoder_next_float(&dec,&value));
    TEST_ASSERT_EQUAL_MEMORY(&values[127],&value,sizeof(value));
    TEST_ASSERT_TRUE(alp_stream_decoder_skip(&dec,131));
    TEST_ASSERT_TRUE(alp_stream_decoder_next_float(&dec,&value));
    TEST_ASSERT_EQUAL_MEMORY(&values[259],&value,sizeof(value));
    TEST_ASSERT_FALSE(alp_stream_decoder_skip(&dec,1));
    alp_stream_decoder_deinit(&dec);
    TEST_ASSERT_TRUE(alp_stream_decoder_init(&dec,encoded,len-1,true));
    TEST_ASSERT_FALSE(alp_stream_decoder_skip(&dec,260));
    alp_stream_decoder_deinit(&dec);
    free(encoded); free(values);
}

TEST_CASE("third: equal duplicate priorities give peek and next the same value", "[third][third_equal_priority]") {
    timeseries_points_iterator_t it[3];
    timeseries_points_iterator_t *ptrs[]={&it[0],&it[1],&it[2]};
    uint32_t seq[]={1,2,2};
    for (int i=0;i<3;i++) ram_iter(&it[i],10,1,1,i*100);
    timeseries_multi_points_iterator_t m;
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_init(ptrs,seq,3,&m));
    uint64_t ts; timeseries_field_value_t peek,next;
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_peek(&m,&ts,&peek));
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_next(&m,&ts,&next));
    timeseries_multi_points_iterator_deinit(&m);
    for (int i=0;i<3;i++) timeseries_points_iterator_deinit(&it[i]);
    TEST_ASSERT_EQUAL_INT64(210,next.data.int_val);
    TEST_ASSERT_EQUAL_INT64(next.data.int_val,peek.data.int_val);
}

TEST_CASE("third: duplicate heads split into interleaved streams with different tails", "[third][third_mixed]") {
    timeseries_points_iterator_t *it=calloc(4,sizeof(*it)); TEST_ASSERT_NOT_NULL(it);
    timeseries_points_iterator_t *ptrs[4]; uint32_t seq[]={1,2,3,4};
    for (int s=0;s<4;s++) {ram_iter(&it[s],1,s+1,8,s*100);ptrs[s]=&it[s];}
    timeseries_multi_points_iterator_t m;
    TEST_ASSERT_TRUE(timeseries_multi_points_iterator_init(ptrs,seq,4,&m));
    for (int timestamp=1;timestamp<=29;timestamp++) {
        int winner=-1;
        for (int s=0;s<4;s++)
            if ((timestamp-1)%(s+1)==0 && (timestamp-1)/(s+1)<8) winner=s;
        if (winner<0) continue;
        uint64_t ts; timeseries_field_value_t peek,val;
        TEST_ASSERT_TRUE(timeseries_multi_points_iterator_peek(&m,&ts,&peek));
        TEST_ASSERT_EQUAL_UINT64(timestamp,ts);
        TEST_ASSERT_EQUAL_INT64(timestamp+winner*100,peek.data.int_val);
        TEST_ASSERT_TRUE(timeseries_multi_points_iterator_next(&m,&ts,&val));
        TEST_ASSERT_EQUAL_UINT64(timestamp,ts);
        TEST_ASSERT_EQUAL_INT64(timestamp+winner*100,val.data.int_val);
    }
    TEST_ASSERT_FALSE(timeseries_multi_points_iterator_next(&m,NULL,NULL));
    timeseries_multi_points_iterator_deinit(&m);
    for (int s=0;s<4;s++) timeseries_points_iterator_deinit(&it[s]);
    free(it);
}
