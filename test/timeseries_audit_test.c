#include "unity.h"
#include "timeseries.h"
#include "timeseries_internal.h"
#include "timeseries_points_iterator.h"
#include "timeseries_data.h"
#include "alp/alp_int_codec.h"
#include "alp/alp_stream_decoder.h"
#include <stdlib.h>
#include <string.h>
#include <limits.h>

extern timeseries_db_t *timeseries_get_db_handle(void);

static void reset_db(void) {
    TEST_ASSERT_TRUE(timeseries_init());
    TEST_ASSERT_TRUE(timeseries_clear_all());
}

static bool insert_values(const char *measurement, const char *field,
                          uint64_t *ts, timeseries_field_value_t *vals, size_t n,
                          const char *tag) {
    const char *key = "sensor";
    timeseries_insert_data_t data = {
        .measurement_name = measurement, .field_names = &field,
        .num_fields = 1, .timestamps_ms = ts, .field_values = vals, .num_points = n,
        .tag_keys = tag ? &key : NULL, .tag_values = tag ? &tag : NULL,
        .num_tags = tag ? 1 : 0,
    };
    return timeseries_insert(&data);
}

TEST_CASE("audit: merge grows beyond partially occupied capacity", "[audit][audit_growth]") {
    reset_db();
    uint64_t ts[3][3] = {{1,2,3}, {1,2,4}, {5,6,7}};
    timeseries_field_value_t vals[3] = {{.type=TIMESERIES_FIELD_TYPE_INT},
        {.type=TIMESERIES_FIELD_TYPE_INT}, {.type=TIMESERIES_FIELD_TYPE_INT}};
    const char *fields[] = {"a", "b", "c"};
    for (int i=0; i<3; ++i)
        TEST_ASSERT_TRUE(insert_values("merge", fields[i], ts[i], vals, 3, NULL));
    timeseries_query_t q = {.measurement_name="merge", .field_names=fields, .num_fields=3};
    timeseries_query_result_t r = {0};
    TEST_ASSERT_TRUE(timeseries_query(&q, &r));
    TEST_ASSERT_EQUAL(7, r.num_points);
    for (int i=0; i<7; ++i) TEST_ASSERT_EQUAL_UINT64(i+1, r.timestamps[i]);
    timeseries_query_free_result(&r);
    timeseries_deinit();
}

TEST_CASE("audit: overlapping string series retain owned values", "[audit][audit_strings]") {
    reset_db();
    uint64_t a[] = {1,2}, b[] = {1,3};
    timeseries_field_value_t vals[2] = {
        {.type=TIMESERIES_FIELD_TYPE_STRING, .data.string_val={"hello",5}},
        {.type=TIMESERIES_FIELD_TYPE_STRING, .data.string_val={"world",5}},
    };
    TEST_ASSERT_TRUE(insert_values("strings", "v", a, vals, 2, "a"));
    TEST_ASSERT_TRUE(insert_values("strings", "v", b, vals, 2, "b"));
    timeseries_query_t q = {.measurement_name="strings"};
    timeseries_query_result_t r = {0};
    TEST_ASSERT_TRUE(timeseries_query(&q, &r));
    TEST_ASSERT_EQUAL(3, r.num_points);
    TEST_ASSERT_EQUAL_MEMORY("hello", r.columns[0].values[0].data.string_val.str, 5);
    timeseries_query_free_result(&r);
    timeseries_deinit();
}

TEST_CASE("audit: long measurement cache does not alias prefix", "[audit][audit_name]") {
    reset_db();
    char long_name[65], short_name[64];
    memset(long_name, 'm', 64); long_name[64]=0;
    memset(short_name, 'm', 63); short_name[63]=0;
    uint64_t ts = 1;
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_INT, .data.int_val=42};
    TEST_ASSERT_TRUE(insert_values(long_name, "v", &ts, &val, 1, NULL));
    TEST_ASSERT_TRUE(insert_values(short_name, "v", &ts, &val, 1, NULL));
    timeseries_query_t q = {.measurement_name=short_name};
    timeseries_query_result_t r = {0};
    TEST_ASSERT_TRUE(timeseries_query(&q, &r));
    TEST_ASSERT_EQUAL(1, r.num_points);
    timeseries_query_free_result(&r);
    timeseries_deinit();
}

TEST_CASE("audit: oversized string leaves storage unchanged", "[audit][audit_oversize]") {
    reset_db();
    char *str = malloc(TIMESERIES_FIELD_DATA_PAGE_SIZE);
    TEST_ASSERT_NOT_NULL(str);
    memset(str, 'x', TIMESERIES_FIELD_DATA_PAGE_SIZE);
    uint64_t ts = 1;
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_STRING,
        .data.string_val={str, TIMESERIES_FIELD_DATA_PAGE_SIZE}};
    tsdb_usage_summary_t before, after;
    TEST_ASSERT_TRUE(timeseries_get_usage_summary(&before));
    TEST_ASSERT_FALSE(insert_values("large", "v", &ts, &val, 1, NULL));
    free(str);
    TEST_ASSERT_TRUE(timeseries_get_usage_summary(&after));
    TEST_ASSERT_EQUAL(before.used_space_bytes, after.used_space_bytes);
    timeseries_deinit();
}

TEST_CASE("audit: ALP empty input supports failure cleanup", "[audit][audit_alp_empty]") {
    alp_stream_decoder_t dec;
    memset(&dec, 0xa5, sizeof(dec));
    uint8_t byte = 0;
    TEST_ASSERT_FALSE(alp_stream_decoder_init(&dec, &byte, 0, false));
    alp_stream_decoder_deinit(&dec);
}

TEST_CASE("audit: raw timestamps stay inside declared column", "[audit][audit_ts_length]") {
    reset_db();
    timeseries_db_t *db = timeseries_get_db_handle();
    struct { timeseries_col_data_header_t hdr; int64_t value; } record = {
        .hdr={.ts_len=0, .val_len=8}, .value=42};
    TEST_ASSERT_EQUAL(ESP_OK, esp_partition_write(db->partition, 8192, &record, sizeof(record)));
    timeseries_points_iterator_t it;
    bool ok = timeseries_points_iterator_init(db, 8192, sizeof(record), 1,
        TIMESERIES_FIELD_TYPE_INT, 0xff, &it);
    timeseries_points_iterator_deinit(&it);
    TEST_ASSERT_FALSE(ok);
    timeseries_deinit();
}

TEST_CASE("audit: ALP int64 extremes round trip", "[audit][audit_alp_extremes]") {
    int64_t vals[260], out[260];
    for (int i=0; i<260; ++i) vals[i] = i%3==0 ? INT64_MIN : i%3==1 ? INT64_MAX : 0;
    uint8_t *encoded = NULL;
    size_t len = alp_encode_int(vals, 260, &encoded);
    TEST_ASSERT_NOT_NULL(encoded);
    TEST_ASSERT_TRUE(alp_decode_int(encoded, len, out, 260));
    TEST_ASSERT_EQUAL_MEMORY(vals, out, sizeof(vals));
    alp_stream_decoder_t dec;
    TEST_ASSERT_TRUE(alp_stream_decoder_init(&dec, encoded, len, false));
    for (int i=0; i<260; ++i) {
        int64_t v;
        TEST_ASSERT_TRUE(alp_stream_decoder_next_int(&dec, &v));
        TEST_ASSERT_EQUAL_INT64(vals[i], v);
    }
    alp_stream_decoder_deinit(&dec);
    free(encoded);
}

TEST_CASE("audit: missing insert tag arrays are rejected", "[audit][audit_null_tags]") {
    reset_db();
    const char *field = "v";
    uint64_t ts = 1;
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_INT};
    timeseries_insert_data_t data = {.measurement_name="null_tags",
        .num_tags=1, .field_names=&field, .num_fields=1,
        .timestamps_ms=&ts, .field_values=&val, .num_points=1};
    TEST_ASSERT_FALSE(timeseries_insert(&data));
    timeseries_deinit();
}

TEST_CASE("audit: missing query field arrays are rejected", "[audit][audit_null_fields]") {
    reset_db();
    uint64_t ts = 1;
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_INT};
    TEST_ASSERT_TRUE(insert_values("null_fields", "v", &ts, &val, 1, NULL));
    timeseries_query_t q = {.measurement_name="null_fields", .num_fields=1};
    timeseries_query_result_t r = {0};
    TEST_ASSERT_FALSE(timeseries_query(&q, &r));
    timeseries_query_free_result(&r);
    timeseries_deinit();
}

TEST_CASE("audit: wrapped string size is rejected before copy", "[audit][audit_string_wrap]") {
    reset_db();
    char str = 'x';
    uint64_t ts = 1;
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_STRING,
        .data.string_val={&str, SIZE_MAX}};
    TEST_ASSERT_FALSE(insert_values("wrap", "v", &ts, &val, 1, NULL));
    timeseries_deinit();
}

TEST_CASE("audit: query limit counts points after time filtering", "[audit][audit_limit]") {
    reset_db();
    uint64_t a[] = {1,2,3}, b[] = {4,5,6};
    timeseries_field_value_t vals[3] = {{.type=TIMESERIES_FIELD_TYPE_INT},
        {.type=TIMESERIES_FIELD_TYPE_INT}, {.type=TIMESERIES_FIELD_TYPE_INT}};
    TEST_ASSERT_TRUE(insert_values("limit", "v", a, vals, 3, NULL));
    TEST_ASSERT_TRUE(insert_values("limit", "v", b, vals, 3, NULL));
    timeseries_query_t q = {.measurement_name="limit", .start_ms=3, .limit=3};
    timeseries_query_result_t r = {0};
    TEST_ASSERT_TRUE(timeseries_query(&q, &r));
    TEST_ASSERT_EQUAL(3, r.num_points);
    for (int i=0; i<3; ++i) TEST_ASSERT_EQUAL_UINT64(i+3, r.timestamps[i]);
    timeseries_query_free_result(&r);
    timeseries_deinit();
}

TEST_CASE("audit: duplicate timestamps select the latest record", "[audit][audit_latest]") {
    reset_db();
    uint64_t ts=1000;
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_INT, .data.int_val=1};
    TEST_ASSERT_TRUE(insert_values("latest", "v", &ts, &val, 1, NULL));
    val.data.int_val=2;
    TEST_ASSERT_TRUE(insert_values("latest", "v", &ts, &val, 1, NULL));
    timeseries_query_t q = {.measurement_name="latest"};
    for (int pass=0; pass<2; pass++) {
        q.rollup_interval = pass ? 1000 : 0;
        q.aggregate_method = TSDB_AGGREGATION_LAST;
        timeseries_query_result_t r = {0};
        TEST_ASSERT_TRUE(timeseries_query(&q, &r));
        TEST_ASSERT_EQUAL(1, r.num_points);
        TEST_ASSERT_EQUAL_INT64(2, r.columns[0].values[0].data.int_val);
        timeseries_query_free_result(&r);
    }
    timeseries_deinit();
}

#if CONFIG_IDF_TARGET_LINUX
void tsdb_audit_fail_calloc(size_t count, size_t size);
bool tsdb_audit_calloc_failed(void);
void tsdb_audit_recycle_on_read(TaskHandle_t task, size_t offset);

typedef struct {
    SemaphoreHandle_t done;
    bool cleared;
} recycle_context_t;
static void recycle_query_page(void *arg) {
    recycle_context_t *ctx = arg;
    ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
    ctx->cleared = timeseries_clear_all();
    if (ctx->cleared) {
        timeseries_db_t *db = timeseries_get_db_handle();
        ctx->cleared = esp_partition_erase_range(db->partition, 8192, 8192) == ESP_OK;
    }
    xSemaphoreGive(ctx->done);
    vTaskDelete(NULL);
}

TEST_CASE("audit: query protects discovered records from reuse", "[audit][audit_recycle]") {
    reset_db();
    uint64_t ts = 1;
    timeseries_field_value_t val = {.type=TIMESERIES_FIELD_TYPE_INT, .data.int_val=42};
    TEST_ASSERT_TRUE(insert_values("recycle", "v", &ts, &val, 1, NULL));
    timeseries_db_t *db = timeseries_get_db_handle();
    size_t offset = db->last_l0_page_offset + sizeof(timeseries_page_header_t) +
                    sizeof(timeseries_field_data_header_t);
    recycle_context_t ctx = {.done=xSemaphoreCreateBinary()};
    TEST_ASSERT_NOT_NULL(ctx.done);
    TaskHandle_t writer;
    TEST_ASSERT_EQUAL(pdPASS, xTaskCreate(recycle_query_page, "audit_recycle", 8192,
                                         &ctx, tskIDLE_PRIORITY+1, &writer));
    tsdb_audit_recycle_on_read(writer, offset);
    timeseries_query_t q = {.measurement_name="recycle"};
    timeseries_query_result_t r = {0};
    bool ok = timeseries_query(&q, &r);
    tsdb_audit_recycle_on_read(NULL, 0);
    bool done = xSemaphoreTake(ctx.done, pdMS_TO_TICKS(1000)) == pdTRUE;
    size_t count = r.num_points;
    int64_t value = count ? r.columns[0].values[0].data.int_val : 0;
    timeseries_query_free_result(&r);
    TEST_ASSERT_TRUE(done);
    vSemaphoreDelete(ctx.done);
    timeseries_deinit();
    TEST_ASSERT_TRUE(ctx.cleared);
    TEST_ASSERT_TRUE(ok);
    TEST_ASSERT_EQUAL(1, count);
    TEST_ASSERT_EQUAL_INT64(42, value);
}

TEST_CASE("audit: query reports result-column allocation failure", "[audit][audit_oom]") {
    reset_db();
    uint64_t ts[] = {1,2,3};
    timeseries_field_value_t vals[3] = {{.type=TIMESERIES_FIELD_TYPE_INT},
        {.type=TIMESERIES_FIELD_TYPE_INT}, {.type=TIMESERIES_FIELD_TYPE_INT}};
    TEST_ASSERT_TRUE(insert_values("oom", "v", ts, vals, 3, NULL));
    timeseries_query_t q = {.measurement_name="oom"};
    timeseries_query_result_t r = {0};
    tsdb_audit_fail_calloc(3, sizeof(timeseries_field_value_t));
    bool ok = timeseries_query(&q, &r);
    bool injected = tsdb_audit_calloc_failed();
    tsdb_audit_fail_calloc(0, 0);
    timeseries_query_free_result(&r);
    timeseries_deinit();
    TEST_ASSERT_TRUE(injected);
    TEST_ASSERT_FALSE(ok);
}
#endif
