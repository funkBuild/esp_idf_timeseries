# ESP-IDF Timeseries

An embedded time-series database component for ESP-IDF. Stores, compresses, and queries timestamped data directly on SPI flash with no external dependencies beyond the ESP-IDF framework.

## Features

- **Multi-field measurements** with tags for flexible data modelling
- **LSM-style compaction** across multiple levels (L0 uncompressed, L1+ compressed)
- **ALP and Gorilla codecs** for efficient numeric compression on resource-constrained hardware
- **zlib page compression** with reduced memory settings (windowBits=12, memLevel=4) for embedded targets
- **Aggregation queries** with rollup intervals (MIN, MAX, AVG, SUM, COUNT, LAST)
- **Tag-based filtering** on queries
- **Background compaction** via a dedicated FreeRTOS task
- **Data expiration** for automatic cleanup of old data
- **Page cache snapshots** with reference counting for lock-free reads during compaction
- **Configurable via Kconfig** (cache sizes, buffer sizes, chunk limits)

## Supported Field Types

| Type | Enum | C Type |
|------|------|--------|
| Float | `TIMESERIES_FIELD_TYPE_FLOAT` | `double` |
| Integer | `TIMESERIES_FIELD_TYPE_INT` | `int64_t` |
| Boolean | `TIMESERIES_FIELD_TYPE_BOOL` | `bool` |
| String | `TIMESERIES_FIELD_TYPE_STRING` | `char*` + `size_t` |

## Installation

Add as a component dependency in your project's `idf_component.yml` or clone into your `components/` directory:

```
components/
  esp_idf_timeseries/    # this repository
```

### Dependencies

- `esp_partition`
- `mbedtls`
- `esp_timer`
- `freertos`
- `espressif__zlib`

### Partition Table

Create a data partition named `storage` in your `partitions.csv`:

```csv
# Name,   Type, SubType, Offset,  Size
nvs,      data, nvs,     0x9000,  0x6000
phy_init, data, phy,     0xf000,  0x1000
factory,  app,  factory, 0x10000, 1M
storage,  data, 0x99,    ,        14M
```

## Kconfig Options

Configure via `idf.py menuconfig` under **ESP IDF Timeseries Configuration**:

| Option | Default | Description |
|--------|---------|-------------|
| `TIMESERIES_USE_SERIES_ID_CACHE` | `y` | Enable in-memory series ID cache for faster repeated inserts |
| `TIMESERIES_SERIES_ID_CACHE_SIZE` | `64` | Number of cached series IDs (8-256, ~150 bytes each) |
| `TIMESERIES_CACHE_USE_LRU` | `y` | LRU eviction policy (vs round-robin when disabled) |
| `TIMESERIES_ENABLE_CACHE_STATS` | `n` | Track and log cache hit/miss statistics |
| `TIMESERIES_CHUNK_BUFFER_SIZE` | `16` | Initial chunk write buffer size in KB (4-64) |
| `TIMESERIES_ALP_CHUNK_MAX_POINTS` | `1024` | Max points per ALP compaction record (16-65535) |

## API Reference

All public API functions are declared in `timeseries.h`.

### Initialization and Lifecycle

```c
#include "timeseries.h"

// Initialize the database. Must be called before any other function.
// Scans the flash partition and builds the in-memory page cache.
bool timeseries_init(void);

// Deinitialize the database, stop background tasks, and free all resources.
void timeseries_deinit(void);

// Erase all data and metadata from the partition.
bool timeseries_clear_all(void);
```

### Types

#### `timeseries_field_value_t`

A tagged union holding a single field value:

```c
typedef struct {
  timeseries_field_type_e type;
  union {
    double float_val;
    int64_t int_val;
    bool bool_val;
    struct {
      char* str;
      size_t length;
    } string_val;
  } data;
} timeseries_field_value_t;
```

#### `timeseries_insert_data_t`

Describes a batch of data points to insert. Field values are stored in row-major order: `field_values[field_index * num_points + point_index]`.

```c
typedef struct {
  const char* measurement_name;

  const char** tag_keys;       // Optional tag key array
  const char** tag_values;     // Optional tag value array
  size_t num_tags;             // Number of tags (0 for none)

  const char** field_names;    // Array of field name strings
  timeseries_field_value_t* field_values;  // length = num_fields * num_points
  size_t num_fields;

  uint64_t* timestamps_ms;    // Array of timestamps in milliseconds
  size_t num_points;           // Number of data points
} timeseries_insert_data_t;
```

#### `timeseries_aggregation_method_e`

```c
typedef enum {
  TSDB_AGGREGATION_NONE = 0,   // Raw data, no aggregation
  TSDB_AGGREGATION_MIN,        // Minimum value per interval
  TSDB_AGGREGATION_MAX,        // Maximum value per interval
  TSDB_AGGREGATION_AVG,        // Average value per interval
  TSDB_AGGREGATION_LAST,       // Last value per interval
  TSDB_AGGREGATION_LATEST,     // Alias for LAST
  TSDB_AGGREGATION_SUM,        // Sum of values per interval
  TSDB_AGGREGATION_COUNT,      // Count of values per interval
} timeseries_aggregation_method_e;
```

#### `timeseries_query_t`

```c
typedef struct {
  const char* measurement_name;    // Required: measurement to query

  const char** tag_keys;           // Optional: tag filters
  const char** tag_values;
  size_t num_tags;

  const char** field_names;        // Optional: field selection (0 = all fields)
  size_t num_fields;

  int64_t start_ms;                // Time range start (0 = no lower bound)
  int64_t end_ms;                  // Time range end (0 = no upper bound)

  size_t limit;                    // Max points to return (0 = no limit)
  uint32_t rollup_interval;        // Aggregation window in ms (0 = raw data)
  timeseries_aggregation_method_e aggregate_method;
} timeseries_query_t;
```

#### `timeseries_query_result_t`

Columnar result structure returned by queries:

```c
typedef struct {
  uint64_t* timestamps;                      // Array of timestamps (length = num_points)
  size_t num_points;                          // Number of data points (rows)
  timeseries_query_result_column_t* columns;  // Array of columns (length = num_columns)
  size_t num_columns;                         // Number of columns (fields)
} timeseries_query_result_t;

typedef struct {
  char* name;                        // Column/field name
  timeseries_field_type_e type;      // Field type
  timeseries_field_value_t* values;  // Array of values (length = num_points)
} timeseries_query_result_column_t;
```

#### `tsdb_tag_pair_t`

```c
typedef struct {
  char* key;
  char* val;
} tsdb_tag_pair_t;
```

#### `tsdb_usage_summary_t`

```c
typedef struct {
  uint32_t num_pages;
  uint32_t size_bytes;
} tsdb_page_usage_summary_t;

typedef struct {
  tsdb_page_usage_summary_t page_summaries[5];  // Per-level page counts and sizes
  tsdb_page_usage_summary_t metadata_summary;    // Metadata page usage
  uint32_t used_space_bytes;                      // Total used space
  uint32_t total_space_bytes;                     // Total partition size
} tsdb_usage_summary_t;
```

### Insert

```c
// Insert one or more data points for one or more fields.
// Points are batched internally based on chunk_size.
bool timeseries_insert(const timeseries_insert_data_t* data);

// Set the chunk size for large inserts (default: 300 points per chunk).
void timeseries_set_chunk_size(size_t chunk_size);
```

### Query

```c
// Execute a query and populate the result structure.
// Returns true on success (including empty results), false on error.
bool timeseries_query(const timeseries_query_t* query,
                      timeseries_query_result_t* result);

// Free all memory allocated by a query result.
// Safe to call with a zeroed or NULL result.
void timeseries_query_free_result(timeseries_query_result_t* result);
```

### Compaction

```c
// Trigger background compaction (returns immediately).
bool timeseries_compact(void);

// Trigger compaction and block until complete.
bool timeseries_compact_sync(void);

// Force compaction of all levels regardless of page-count thresholds.
// Ensures no L0 pages remain. Useful for benchmarking.
bool timeseries_compact_force_sync(void);
```

### Expiration

```c
// Expire old data based on configured retention policies.
bool timeseries_expire(void);
```

### Metadata Queries

```c
// Get all measurement names. Caller must free each string and the array.
bool timeseries_get_measurements(char*** measurements,
                                 size_t* num_measurements);

// Get all field names for a measurement. Caller must free each string and the array.
bool timeseries_get_fields_for_measurement(const char* measurement_name,
                                           char*** fields,
                                           size_t* num_fields);

// Get all tag key-value pairs for a measurement. Caller must free each
// key/val string and the array.
bool timeseries_get_tags_for_measurement(const char* measurement_name,
                                         tsdb_tag_pair_t** tags,
                                         size_t* num_tags);

// Get storage usage summary across all levels.
bool timeseries_get_usage_summary(tsdb_usage_summary_t* summary);
```

### Delete

```c
// Delete all data for a measurement (all fields, all tags).
bool timeseries_delete_measurement(const char* measurement_name);

// Delete all data for a specific field within a measurement.
bool timeseries_delete_measurement_and_field(const char* measurement_name,
                                             const char* field_name);
```

## Usage Examples

### Basic Insert and Query

```c
#include "timeseries.h"
#include <string.h>

void example_basic(void) {
    timeseries_init();

    // Prepare 3 data points for 1 field
    uint64_t timestamps[] = {1000, 2000, 3000};
    timeseries_field_value_t values[] = {
        {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 22.5},
        {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 23.1},
        {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 22.8},
    };
    const char *fields[] = {"temperature"};

    timeseries_insert_data_t data = {
        .measurement_name = "weather",
        .field_names = fields,
        .field_values = values,
        .num_fields = 1,
        .timestamps_ms = timestamps,
        .num_points = 3,
    };
    timeseries_insert(&data);

    // Query all points
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    query.start_ms = 0;
    query.end_ms = INT64_MAX;

    timeseries_query_result_t result = {0};
    if (timeseries_query(&query, &result)) {
        // result.num_points == 3
        // result.columns[0].name == "temperature"
        // result.columns[0].values[0].data.float_val == 22.5
        timeseries_query_free_result(&result);
    }
}
```

### Multi-Field Insert with Tags

```c
void example_multi_field(void) {
    size_t num_points = 2;

    uint64_t timestamps[] = {1000, 2000};

    // 2 fields x 2 points = 4 values (row-major: field0[pt0], field0[pt1], field1[pt0], field1[pt1])
    timeseries_field_value_t values[] = {
        {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 22.5},  // temp @ t=1000
        {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 23.1},  // temp @ t=2000
        {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 65.0},  // humidity @ t=1000
        {.type = TIMESERIES_FIELD_TYPE_FLOAT, .data.float_val = 63.2},  // humidity @ t=2000
    };

    const char *fields[] = {"temperature", "humidity"};
    const char *tag_keys[] = {"location"};
    const char *tag_values[] = {"office"};

    timeseries_insert_data_t data = {
        .measurement_name = "environment",
        .tag_keys = tag_keys,
        .tag_values = tag_values,
        .num_tags = 1,
        .field_names = fields,
        .field_values = values,
        .num_fields = 2,
        .timestamps_ms = timestamps,
        .num_points = num_points,
    };
    timeseries_insert(&data);
}
```

### Aggregation Query

```c
void example_aggregation(void) {
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "environment";
    query.start_ms = 0;
    query.end_ms = INT64_MAX;
    query.rollup_interval = 3600000;                // 1-hour rollup windows
    query.aggregate_method = TSDB_AGGREGATION_AVG;   // Average per window

    timeseries_query_result_t result = {0};
    if (timeseries_query(&query, &result)) {
        for (size_t i = 0; i < result.num_points; i++) {
            // result.timestamps[i] = start of each 1-hour window
            // result.columns[0].values[i].data.float_val = average temperature
        }
        timeseries_query_free_result(&result);
    }
}
```

### Tag-Filtered Query

```c
void example_tag_filter(void) {
    const char *tag_keys[] = {"location"};
    const char *tag_values[] = {"office"};

    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "environment";
    query.tag_keys = tag_keys;
    query.tag_values = tag_values;
    query.num_tags = 1;
    query.start_ms = 0;
    query.end_ms = INT64_MAX;

    timeseries_query_result_t result = {0};
    if (timeseries_query(&query, &result)) {
        // Only returns data matching location=office
        timeseries_query_free_result(&result);
    }
}
```

### Metadata Inspection

```c
void example_metadata(void) {
    // List all measurements
    char **measurements = NULL;
    size_t count = 0;
    if (timeseries_get_measurements(&measurements, &count)) {
        for (size_t i = 0; i < count; i++) {
            printf("Measurement: %s\n", measurements[i]);
            free(measurements[i]);
        }
        free(measurements);
    }

    // List fields for a measurement
    char **fields = NULL;
    size_t num_fields = 0;
    if (timeseries_get_fields_for_measurement("weather", &fields, &num_fields)) {
        for (size_t i = 0; i < num_fields; i++) {
            printf("Field: %s\n", fields[i]);
            free(fields[i]);
        }
        free(fields);
    }

    // Storage usage
    tsdb_usage_summary_t summary;
    if (timeseries_get_usage_summary(&summary)) {
        printf("Used: %lu / %lu bytes\n",
               (unsigned long)summary.used_space_bytes,
               (unsigned long)summary.total_space_bytes);
    }
}
```

## Architecture

### Storage Layout

Data is stored on a dedicated flash partition using a page-based layout:

- **Metadata pages** (8 KB): store measurement definitions, field indexes, tag indexes, and series ID mappings as key-value entries
- **Field data pages** (8 KB): store timestamped field values, organized by series ID

### Compaction Levels

| Level | Format | Description |
|-------|--------|-------------|
| L0 | Uncompressed | Raw inserts, individual field data records |
| L1 | Gorilla/ALP + zlib | Merged and compressed from L0 pages |
| L2+ | Gorilla/ALP + zlib | Further merged from lower levels |

Compaction is triggered automatically when the L0 page count exceeds a threshold (default: 4 pages). It can also be triggered manually via `timeseries_compact()` or `timeseries_compact_sync()`.

### Compression Codecs

- **Gorilla**: XOR-based encoding for floating-point timestamps and values (legacy, backward-compatible)
- **ALP (Adaptive Lossless integer/float Packing)**: FFOR-based encoding for int64 timestamps and float64/int64 values, significantly faster encode than Gorilla
- **zlib**: Page-level compression with reduced window size for embedded targets

### Concurrency

- A `flash_write_mutex` serializes all write operations (inserts and compaction page claims)
- A `snapshot_mutex` protects page cache snapshot swaps
- Compaction runs on a dedicated FreeRTOS task; `timeseries_compact()` is async, `timeseries_compact_sync()` blocks
- Iterators hold snapshot references, ensuring consistent reads even during concurrent writes

## Building and Testing

### Build

```bash
source ~/esp/esp-idf/export.sh
cd test_app
idf.py build
```

### Run Tests (QEMU)

```bash
cd test_app
bash run_tests.sh
```

### Run Tests (Hardware)

```bash
cd test_app
idf.py -p /dev/ttyUSB0 flash monitor
```

## License

MIT License. See [LICENSE](LICENSE) for details.
