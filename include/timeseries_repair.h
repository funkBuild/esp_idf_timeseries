#pragma once
#include "timeseries_internal.h"
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

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

/* Init fault record (db->init_faults), read by timeseries_init_with_recovery.
 * Every init attempt starts from a cleared record. A scan that cannot read
 * stored content names the page and keeps scanning, so one attempt reports
 * every bad page it met; a failure unrelated to stored content (allocation)
 * marks the attempt transient, and recovery then never erases. */
static inline void tsdb_init_faults_reset(timeseries_db_t *db) {
  memset(&db->init_faults, 0, sizeof(db->init_faults));
}

static inline void tsdb_init_fault_transient(timeseries_db_t *db) { db->init_faults.transient = true; }

/* The page [offset, offset + size) could not be read. */
static inline void tsdb_init_fault_bad_page(timeseries_db_t *db, uint32_t offset, uint32_t size) {
  tsdb_init_faults_t *f = &db->init_faults;
  for (uint8_t i = 0; i < f->count; i++)
    if (f->pages[i].offset == offset) return;
  if (f->count == TSDB_INIT_MAX_BAD_PAGES) {
    f->overflow = true;
    return;
  }
  f->pages[f->count].offset = offset;
  f->pages[f->count].size = size;
  f->count++;
}
