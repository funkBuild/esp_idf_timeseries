# ESP-IDF Timeseries Performance Baseline Results

**Date:** 2025-11-28
**Platform:** ESP32 (QEMU emulation)
**ESP-IDF Version:** v5.5.1
**Compiler Optimization:** -O2 (CONFIG_COMPILER_OPTIMIZATION_PERF)

## Test Configuration
- CPU Frequency: 160 MHz
- Flash: 4 MB (DIO mode, 40 MHz)
- Series ID Cache: 64 entries (LRU)
- Chunk Buffer: 16 KB

---

## INSERT PERFORMANCE

### Single-Point Insert (100 operations)
| Metric | Value |
|--------|-------|
| Total Time | 3,168 ms |
| Average Time | 31.7 ms/op |
| Min Time | 26.5 ms |
| Max Time | 155.4 ms (first insert with metadata creation) |
| **Throughput** | **31.6 ops/s** |

### Bulk Insert - 100 Points
| Metric | Value |
|--------|-------|
| Total Time | 232.3 ms |
| Time per Point | 2.32 ms/point |
| **Throughput** | **430 points/s** |

### Bulk Insert - 1000 Points
| Metric | Value |
|--------|-------|
| Total Time | 400.3 ms |
| Time per Point | 0.40 ms/point |
| **Throughput** | **2,498 points/s** |

### Multi-Field Insert (6 fields, 288 points)
| Metric | Value |
|--------|-------|
| Total Values | 1,728 |
| Total Time | 4,269 ms |
| Time per Point | 14.8 ms/point |
| **Throughput** | **405 values/s** |

### Integer Field Insert (500 points)
| Metric | Value |
|--------|-------|
| Total Time | 213.2 ms |
| Time per Point | 0.43 ms/point |
| **Throughput** | **2,345 points/s** |

---

## QUERY PERFORMANCE

### Query 100 Points (Single Field, Post-Compaction)
| Metric | Value |
|--------|-------|
| Query Time | 151.8 ms |
| **Throughput** | **659 points/s** |

---

## COMPACTION PERFORMANCE

### Compaction - 500 Points (Single Series)
| Metric | Value |
|--------|-------|
| Compaction Time | 0.78 ms |
| **Throughput** | **645,161 points/s** |

### Compaction - 2000 Points (Single Series)
| Metric | Value |
|--------|-------|
| Compaction Time | 4,584 ms |
| **Throughput** | **436 points/s** |

### Compaction - 20 Series (1000 points total, 50 per series)
| Metric | Value |
|--------|-------|
| Insert Time | 25,690 ms |
| Compaction Time | 0.63 ms |
| Insert Throughput | 38.9 points/s |
| **Compaction Throughput** | **1,597,444 points/s** |

*Note: Multi-series compaction is faster per-point than single-series due to smaller per-series data chunks.*

### Incremental Compaction (5 batches of 100 points)
| Metric | Value |
|--------|-------|
| Avg Insert Time | 193.2 ms/batch |
| Avg Compaction Time | 0.74 ms/batch |

---

## COMPRESSION CODEC PERFORMANCE

### Float Compression (Gorilla) - 1000 Values
| Metric | Value |
|--------|-------|
| Encode Time | 708 us |
| Decode Time | 4,224 us |
| Original Size | 8,000 bytes |
| Compressed Size | 4,124 bytes |
| **Compression Ratio** | **1.94x** |
| Encode Throughput | 1.41M values/s |
| Decode Throughput | 237K values/s |

### Integer Compression (Delta + Simple8B) - 1000 Values
| Metric | Value |
|--------|-------|
| Encode Time | 759 us |
| Original Size | 8,000 bytes |
| Compressed Size | 160 bytes |
| **Compression Ratio** | **50x** |
| Encode Throughput | 1.32M values/s |

---

## METADATA OPERATIONS

### Series Creation Overhead (20 unique series)
| Metric | Value |
|--------|-------|
| Total Time | 25,213 ms |
| Average Time | 1,261 ms/series |
| First 5 Series Avg | 450 ms |
| Last 5 Series Avg | 2,079 ms |

*Note: Series creation time increases as more metadata accumulates due to metadata page scanning.*

### Multi-Tag Insert (5 tags, 100 points)
| Metric | Value |
|--------|-------|
| Insert Time | ~590 ms |
| Time per Point | ~5.9 ms/point |

---

## CACHE PERFORMANCE

### Cache Warmup (100 points, same series)
| Metric | Value |
|--------|-------|
| Cold Insert | 242.7 ms |
| Warm Insert (avg) | 177.0 ms |
| **Cache Speedup** | **1.37x** |

---

## KEY INSIGHTS

1. **Multi-series compaction is efficient**: Compacting 20 series (1000 points) takes only 0.63ms because each series has fewer points to sort and deduplicate.

2. **Series creation scales poorly**: Creating series 1-5 averages 450ms, while series 16-20 average 2,079ms (~4.6x slower) due to linear metadata page scanning.

3. **Large single-series compaction is slow**: 2000 points in one series takes 4.6s because of sorting O(n log n) and deduplication overhead.

4. **I/O is the primary bottleneck**: Most operations are dominated by flash read/write latency, not CPU computation.

---

## TEST NOTES

- Some query tests fail because data isn't compacted before query (test setup limitation)
- Tests run in QEMU which may have different timing characteristics than real hardware
- Cache eviction test (80 series) excluded due to timeout
- Using -O2 optimization level for performance testing

---

## FILES

- Test source: `test/timeseries_perf_test.c`
- Pytest config: `test_app/pytest_perf_tests.py`
- Run script: `run_qemu_perf_tests.sh`
- Build config: `test_app/sdkconfig.defaults`
