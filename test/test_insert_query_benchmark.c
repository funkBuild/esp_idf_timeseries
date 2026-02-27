/**
 * @file test_insert_query_benchmark.c
 * @brief End-to-end insert & query benchmark for the timeseries DB.
 *
 * Measures real-world performance with two data patterns (counter, centered),
 * three sampling intervals (1min, 5min, 15min), and 7 days of simulated data.
 * Each scenario: clear → insert (one point at a time) → compact → query 24h → query 72h.
 */

#include "unity.h"
#include "esp_timer.h"
#include "timeseries.h"
#include <inttypes.h>
#include <string.h>
#include <stdio.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define BASE_TIMESTAMP_MS  1000000ULL   /* arbitrary, avoids 0 */
#define MS_PER_MINUTE      60000ULL
#define MS_PER_HOUR        3600000ULL
#define MS_PER_DAY         86400000ULL
#define SEVEN_DAYS_MINUTES (7 * 24 * 60)

/* =========================================================================
 * Simple LCG PRNG (same as test_alp_benchmark.c)
 * ========================================================================= */

static uint64_t lcg_state;

static void lcg_seed(uint64_t seed) {
    lcg_state = seed;
}

static uint64_t lcg_rand(void) {
    lcg_state = lcg_state * 6364136223846793005ULL + 1442695040888963407ULL;
    return lcg_state >> 32;
}

/* =========================================================================
 * Data generators
 * ========================================================================= */

typedef double (*gen_fn_t)(size_t i, double prev);

/* Counter: monotonically increasing with jitter, ~25 ± 5 per step */
static double gen_counter(size_t i, double prev) {
    if (i == 0) return 0.0;
    return prev + 25.0 + (double)((int)(lcg_rand() % 11) - 5);
}

/* Centered: mean-reverting random walk around 25.0, range ~[10,40] */
static double gen_centered(size_t i, double prev) {
    if (i == 0) return 25.0;
    /* Random float in [-3, +3) */
    double noise = ((double)(lcg_rand() % 6001) / 1000.0) - 3.0;
    double val = prev + (25.0 - prev) * 0.1 + noise;
    if (val < 10.0) val = 10.0;
    if (val > 40.0) val = 40.0;
    return val;
}

/* =========================================================================
 * Scenario result
 * ========================================================================= */

typedef struct {
    const char *stream_name;
    uint32_t    interval_minutes;
    size_t      num_points;
    int64_t     insert_ms;
    int64_t     compact_ms;
    int64_t     query_24h_ms;
    int64_t     query_72h_ms;
    size_t      query_24h_pts;
    size_t      query_72h_pts;
} scenario_result_t;

/* =========================================================================
 * Run a single scenario
 * ========================================================================= */

static scenario_result_t run_scenario(const char *stream_name,
                                      uint32_t interval_minutes,
                                      gen_fn_t generate) {
    scenario_result_t r;
    memset(&r, 0, sizeof(r));
    r.stream_name = stream_name;
    r.interval_minutes = interval_minutes;

    uint64_t interval_ms = (uint64_t)interval_minutes * MS_PER_MINUTE;
    size_t num_points = SEVEN_DAYS_MINUTES / interval_minutes;
    r.num_points = num_points;

    /* 1. Clear */
    TEST_ASSERT_TRUE_MESSAGE(timeseries_clear_all(), "clear_all failed");

    /* 2. Insert — one point at a time */
    const char *field_names[] = {"value"};

    lcg_seed(42);  /* reproducible per scenario */
    double prev = 0.0;

    int64_t t_insert_start = esp_timer_get_time();

    for (size_t i = 0; i < num_points; i++) {
        double val = generate(i, prev);
        prev = val;

        uint64_t ts = BASE_TIMESTAMP_MS + i * interval_ms;
        timeseries_field_value_t fv = {
            .type = TIMESERIES_FIELD_TYPE_FLOAT,
            .data.float_val = val,
        };
        timeseries_insert_data_t ins = {
            .measurement_name = "bench",
            .tag_keys = NULL,
            .tag_values = NULL,
            .num_tags = 0,
            .field_names = field_names,
            .field_values = &fv,
            .num_fields = 1,
            .timestamps_ms = &ts,
            .num_points = 1,
        };
        TEST_ASSERT_TRUE_MESSAGE(timeseries_insert(&ins), "insert failed");
    }

    int64_t t_insert_end = esp_timer_get_time();
    r.insert_ms = (t_insert_end - t_insert_start) / 1000;

    /* 3. Compact */
    int64_t t_compact_start = esp_timer_get_time();
    TEST_ASSERT_TRUE_MESSAGE(timeseries_compact_sync(), "compact_sync failed");
    int64_t t_compact_end = esp_timer_get_time();
    r.compact_ms = (t_compact_end - t_compact_start) / 1000;

    /* End timestamp of the dataset */
    uint64_t end_time = BASE_TIMESTAMP_MS + (uint64_t)(num_points - 1) * interval_ms;

    /* 4. Query 24h */
    {
        timeseries_query_t q;
        memset(&q, 0, sizeof(q));
        q.measurement_name = "bench";
        q.start_ms = (int64_t)(end_time - 24 * MS_PER_HOUR);
        q.end_ms = (int64_t)end_time;
        q.rollup_interval = (uint32_t)MS_PER_HOUR;  /* 1 hour buckets */
        q.aggregate_method = TSDB_AGGREGATION_AVG;

        timeseries_query_result_t result;
        memset(&result, 0, sizeof(result));

        int64_t t0 = esp_timer_get_time();
        TEST_ASSERT_TRUE_MESSAGE(timeseries_query(&q, &result), "query 24h failed");
        int64_t t1 = esp_timer_get_time();

        r.query_24h_ms = (t1 - t0) / 1000;
        r.query_24h_pts = result.num_points;
        timeseries_query_free_result(&result);
    }

    /* 5. Query 72h */
    {
        timeseries_query_t q;
        memset(&q, 0, sizeof(q));
        q.measurement_name = "bench";
        q.start_ms = (int64_t)(end_time - 72 * MS_PER_HOUR);
        q.end_ms = (int64_t)end_time;
        q.rollup_interval = (uint32_t)MS_PER_HOUR;  /* 1 hour buckets */
        q.aggregate_method = TSDB_AGGREGATION_AVG;

        timeseries_query_result_t result;
        memset(&result, 0, sizeof(result));

        int64_t t0 = esp_timer_get_time();
        TEST_ASSERT_TRUE_MESSAGE(timeseries_query(&q, &result), "query 72h failed");
        int64_t t1 = esp_timer_get_time();

        r.query_72h_ms = (t1 - t0) / 1000;
        r.query_72h_pts = result.num_points;
        timeseries_query_free_result(&result);
    }

    return r;
}

/* =========================================================================
 * Benchmark test
 * ========================================================================= */

TEST_CASE("Insert & Query Benchmark (7 days)", "[benchmark][insert][query]") {
    TEST_ASSERT_TRUE_MESSAGE(timeseries_init(), "timeseries_init failed");

    typedef struct {
        const char *name;
        gen_fn_t    gen;
    } stream_def_t;

    static const stream_def_t streams[] = {
        { "Counter",  gen_counter  },
        { "Centered", gen_centered },
    };

    static const uint32_t intervals[] = { 1, 5, 15 };  /* minutes */

    const size_t num_streams = sizeof(streams) / sizeof(streams[0]);
    const size_t num_intervals = sizeof(intervals) / sizeof(intervals[0]);
    const size_t total = num_streams * num_intervals;

    scenario_result_t results[6];  /* 2 streams * 3 intervals */
    size_t idx = 0;

    for (size_t s = 0; s < num_streams; s++) {
        for (size_t iv = 0; iv < num_intervals; iv++) {
            results[idx] = run_scenario(streams[s].name, intervals[iv], streams[s].gen);
            idx++;
        }
    }

    /* Print results table */
    printf("\n");
    printf("=== Insert & Query Benchmark (7 days) ===\n");
    printf("%-16s | %8s | %7s | %9s | %10s | %7s | %7s | %8s | %8s\n",
           "Stream", "Interval", "Points", "Insert ms", "Compact ms",
           "Q24h ms", "Q72h ms", "Q24h pts", "Q72h pts");
    printf("-----------------+----------+---------+-----------+");
    printf("------------+---------+---------+----------+---------\n");

    for (size_t i = 0; i < total; i++) {
        scenario_result_t *r = &results[i];
        printf("%-16s | %5" PRIu32 "min | %7zu | %9" PRId64 " | %10" PRId64 " | %7" PRId64 " | %7" PRId64 " | %8zu | %8zu\n",
               r->stream_name,
               r->interval_minutes,
               r->num_points,
               r->insert_ms,
               r->compact_ms,
               r->query_24h_ms,
               r->query_72h_ms,
               r->query_24h_pts,
               r->query_72h_pts);
    }
    printf("\n");
}
