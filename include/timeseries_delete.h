#pragma once

#include <stdbool.h>
#include "timeseries_internal.h"

bool timeseries_delete_by_measurement(timeseries_db_t* db, const char* measurement_name);
bool timeseries_delete_by_measurement_and_field(timeseries_db_t* db, const char* measurement_name,
                                                const char* field_name);