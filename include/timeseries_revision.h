#pragma once
#include "timeseries_internal.h"

/* Revision trailers follow the existing timestamp/value columns. A scalar
 * trailer is eight bytes. A vector uses ALP or packed (uint16 count, uint64
 * revision) runs, followed by the maximum revision (eight bytes). Startup
 * recovery reads that maximum without decoding or allocating the vector. */
typedef struct {
  uint64_t value;
  uint64_t *values;
} tsdb_revision_t;

static inline uint64_t tsdb_legacy_revision(uint32_t sequence, uint32_t data_offset) {
  return ((uint64_t)sequence << 32) | data_offset;
}
static inline uint64_t tsdb_revision_at(const tsdb_revision_t *r, size_t index) {
  return r->values ? r->values[index] : r->value;
}
/* Revisions are either legacy (page sequence << 32 | offset) or counted up
 * from the recovered maximum, so their high word never exceeds the highest
 * page sequence by more than a small margin. A trailer torn by an interrupted
 * write is erased (0xFF...) from its top byte, far outside that bound. */
#define TSDB_REVISION_SEQUENCE_MARGIN (1u << 16)
static inline bool tsdb_revision_plausible(const timeseries_db_t *db, uint64_t revision) {
  return (revision >> 32) <= (uint64_t)atomic_load(&((timeseries_db_t *)db)->sequence_num) +
                                 TSDB_REVISION_SEQUENCE_MARGIN;
}
bool tsdb_revision_next_locked(timeseries_db_t *db, uint64_t *revision);
bool tsdb_revision_recover(timeseries_db_t *db);
bool tsdb_revision_decode(uint8_t flags, const uint8_t *data, size_t length,
                           uint16_t count, tsdb_revision_t *revision);
bool tsdb_revision_read(timeseries_db_t *db, uint32_t offset, uint32_t length,
                         uint32_t columns_length, uint16_t count, uint8_t flags, tsdb_revision_t *revision);
bool tsdb_revision_encode(const uint64_t *values, size_t count, uint64_t scalar,
                           uint8_t **encoded, size_t *length, uint8_t *flags);
