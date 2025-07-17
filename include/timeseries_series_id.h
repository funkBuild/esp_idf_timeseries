#pragma once

#include <string.h>
#include <stdbool.h>

bool calculate_series_id(const char* measurement_name, const char* const* tag_keys, const char* const* tag_values,
                         size_t num_tags, const char* field_name, unsigned char series_id[16]);