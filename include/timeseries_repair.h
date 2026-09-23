#pragma once
#include "timeseries_internal.h"
#include <stdbool.h>

/**
 * @brief Neutralise field-data records torn by an interrupted write.
 *
 * Runs once at startup, after the page cache and sequence counter are loaded
 * and before the revision counter is recovered. Torn records are marked
 * deleted; a torn header that overflows its page becomes a deleted filler (or
 * the page's intact prefix is copied to a new page). Only bits are cleared, so
 * the result stays readable by earlier firmware.
 *
 * @return false only if working memory could not be allocated. Stored content
 *         never makes it fail.
 */
bool tsdb_repair_torn_records(timeseries_db_t *db);
