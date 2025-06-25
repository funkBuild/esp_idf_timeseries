#pragma once
#include <stddef.h>
#include <stdint.h>
#include "esp_err.h"
#include "timeseries.h"

#ifdef __cplusplus
extern "C" {
#endif

esp_err_t tsdb_query_string_parse(const char* query, timeseries_query_t* out);
void tsdb_query_string_free(timeseries_query_t* q);

#ifdef __cplusplus
}
#endif
