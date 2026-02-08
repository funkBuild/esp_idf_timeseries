# Metadata Operations Performance Comparison

Date: 2025-11-28
Target: ESP32 (QEMU Emulation)

## Test Environment
- ESP-IDF v5.5.1
- QEMU esp_develop_9.0.0_20240606
- Flash: 4MB (DIO mode)

---

## Results Comparison

### measurement_id_lookup (100 iterations)

| Metric | Baseline (partition_read) | With MMAP | Improvement |
|--------|--------------------------|-----------|-------------|
| Avg time | 11,668 us/op | 8,881 us/op | **24% faster** |
| Min time | 7,760 us | 6,619 us | 15% faster |
| Max time | 20,657 us | 12,886 us | 38% faster |
| Throughput | 85.7 ops/s | 112.6 ops/s | **31% increase** |

### series_type_lookup (100 iterations)

| Metric | Baseline (partition_read) | With MMAP | Improvement |
|--------|--------------------------|-----------|-------------|
| Avg time | 260,522 us/op | 10,053 us/op | **96% faster (26x speedup!)** |
| Min time | 233,040 us | 7,182 us | 32x faster |
| Max time | 380,465 us | 15,320 us | 25x faster |
| Throughput | 3.84 ops/s | 99.5 ops/s | **26x increase** |

### tag_index_lookup (100 iterations, with MMAP)

| Metric | Value |
|--------|-------|
| Avg time | 10,860 us/op |
| Throughput | 92.1 ops/s |

### field_list_lookup (100 iterations, with MMAP)

| Metric | Value |
|--------|-------|
| Avg time | 11,263 us/op |
| Throughput | 88.8 ops/s |

### list_measurements (100 iterations, with MMAP)

| Metric | Value |
|--------|-------|
| Avg time | 11,245 us/op |
| Throughput | 88.9 ops/s |
| Measurements found | 10 |

### list_fields (100 iterations, with MMAP)

| Metric | Value |
|--------|-------|
| Avg time | 12,133 us/op |
| Throughput | 82.4 ops/s |
| Fields found | 6 |

### entity_iterator_scan (50 scans, with MMAP)

| Metric | Value |
|--------|-------|
| Total time | 610,944 us |
| Entries per scan | 123 |
| Avg scan time | 12,219 us |

---

## Key Findings

1. **series_type_lookup improved dramatically**: 260ms -> 10ms (26x speedup)
   - This operation requires scanning all metadata entries
   - Each entry previously needed multiple `esp_partition_read()` calls
   - With mmap, direct memory access eliminates function call overhead

2. **measurement_id_lookup improved moderately**: 11.7ms -> 8.9ms (24% faster)
   - This operation is simpler with fewer entries to scan
   - mmap still provides benefit from eliminating read overhead

3. **All metadata operations now consistent**: ~10-12ms per operation
   - Before mmap, series_type_lookup was 26x slower than measurement_id_lookup
   - After mmap, all operations are within similar performance range

## Implementation Details

Changes made to `src/timeseries_iterator.c`:

1. **`timeseries_entity_iterator_init()`**: Now uses `esp_partition_mmap()` to map
   the metadata page into memory. Falls back to partition reads if mmap fails.

2. **`timeseries_entity_iterator_deinit()`**: Now calls `esp_partition_munmap()`
   to release the mapping.

3. **`timeseries_entity_iterator_next()`**: Uses direct memory access via
   `memcpy()` from mapped pointer when available.

4. **`timeseries_entity_iterator_read_data()`**: Uses direct memory access for
   reading key and value data when mmap is available.

## Files Modified

- `include/timeseries_internal.h` - Added mmap cache structures
- `include/timeseries_iterator.h` - Already had mmap fields in entity iterator
- `src/timeseries_iterator.c` - Implemented mmap support in entity iterator
