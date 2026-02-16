# Query Test Quick Reference

## Test File: timeseries_query_test.c

Quick reference for all test cases organized by category.

## Running Tests

Run all query tests:
```
test "query"
```

Run specific category:
```
test "[query][error]"      # Error handling tests
test "[query][empty]"      # Empty result tests
test "[query][time]"       # Time range tests
test "[query][fields]"     # Field selection tests
test "[query][tags]"       # Tag filtering tests
test "[query][limit]"      # Limit parameter tests
test "[query][rollup]"     # Rollup interval tests
test "[query][types]"      # Data type tests
test "[query][memory]"     # Memory tests
test "[query][integration]" # Integration tests
```

## Test Categories

### Error Handling [query][error]
- `query: NULL query parameter`
- `query: NULL result parameter`
- `query: NULL measurement name`

### Empty Results [query][empty]
- `query: empty database returns empty result`
- `query: nonexistent measurement returns empty result`
- `query: no matching tags returns empty result` [tags]
- `query: no matching fields returns empty result` [fields]

### Time Range [query][time]
- `query: start_ms equals end_ms returns empty result`
- `query: start_ms greater than end_ms`
- `query: start_ms = 0 includes all data from beginning`
- `query: end_ms = 0 includes all data to end`
- `query: both start_ms and end_ms = 0 returns all data`
- `query: time range with no matching data`
- `query: maximum timestamp values` [boundary]

### Field Selection [query][fields]
- `query: num_fields = 0 returns all fields`
- `query: select single field from multiple`
- `query: select subset of fields`
- `query: duplicate field names in query`

### Tag Filtering [query][tags]
- `query: single tag filter match`
- `query: multiple tag filters (intersection)`
- `query: multiple tag filters partial match`
- `query: no tags filter returns all series`

### Limit Parameter [query][limit]
- `query: limit = 0 returns all data`
- `query: limit = 1 returns single point`
- `query: limit less than available data`
- `query: limit greater than available data`
- `query: very large limit value` [boundary]

### Combined Parameters [query][combined]
- `query: time range + limit combination`
- `query: tags + fields + time range + limit`

### Result Structure [query][result]
- `query: result timestamps are sorted`
- `query: result columns have consistent num_points`
- `query: free result with empty result`
- `query: free result multiple times`

### Multiple Series [query][series]
- `query: multiple series with same measurement different tags`

### Rollup Interval [query][rollup]
- `query: rollup_interval = 0`
- `query: rollup_interval aggregates data`

### Edge Cases [query][edge]
- `query: very long measurement name`
- `query: empty string measurement name`
- `query: single point insertion and retrieval`
- `query: consecutive queries same parameters`
- `query: query after database clear`

### Data Types [query][types]
- `query: float field type`
- `query: int field type`
- `query: bool field type`
- `query: mixed field types in single query`

### Memory Management [query][memory]
- `query: result cleanup with all allocated fields`

### Integration [query][integration]
- `query: insert then query multiple measurements`
- `query: large dataset query performance`

## Helper Functions

### `setup_test()`
Initializes database and clears all data before each test.

### `insert_test_data()`
Inserts test data with configurable parameters:
```c
bool insert_test_data(
    const char *measurement_name,
    const char **tag_keys,
    const char **tag_values,
    size_t num_tags,
    const char **field_names,
    size_t num_fields,
    size_t num_points,
    uint64_t start_timestamp,
    uint64_t timestamp_increment
)
```

Field types alternate based on field index:
- `f % 3 == 0`: FLOAT (value = i * 1.5 + f)
- `f % 3 == 1`: INT (value = i * 10 + f)
- `f % 3 == 2`: BOOL (value = (i + f) % 2 == 0)

### `execute_and_validate_query()`
Executes query and validates result structure integrity.

## Common Test Patterns

### Basic Query Test
```c
TEST_CASE("query: description", "[query][tag]") {
    setup_test();

    // Insert test data
    const char *field_names[] = {"temperature"};
    TEST_ASSERT_TRUE(insert_test_data("weather", NULL, NULL, 0,
                                      field_names, 1, 10, 1000, 1000));

    // Setup query
    timeseries_query_t query;
    memset(&query, 0, sizeof(query));
    query.measurement_name = "weather";
    // ... set other parameters

    // Execute and validate
    timeseries_query_result_t result;
    TEST_ASSERT_TRUE(execute_and_validate_query(&query, &result));

    // Verify results
    TEST_ASSERT_EQUAL(expected_points, result.num_points);

    // Cleanup
    timeseries_query_free_result(&result);
}
```

### Tag Filter Test Pattern
```c
const char *tag_keys[] = {"location", "sensor"};
const char *tag_values[] = {"room1", "temp1"};
TEST_ASSERT_TRUE(insert_test_data("weather", tag_keys, tag_values, 2,
                                  field_names, 1, 10, 1000, 1000));

query.tag_keys = tag_keys;
query.tag_values = tag_values;
query.num_tags = 2;
```

### Time Range Test Pattern
```c
query.start_ms = 5000;
query.end_ms = 15000;

// Verify timestamps in results
for (size_t i = 0; i < result.num_points; i++) {
    TEST_ASSERT_GREATER_OR_EQUAL(query.start_ms, result.timestamps[i]);
    TEST_ASSERT_LESS_OR_EQUAL(query.end_ms, result.timestamps[i]);
}
```

### Field Selection Test Pattern
```c
const char *query_field_names[] = {"temperature", "humidity"};
query.field_names = query_field_names;
query.num_fields = 2;

// Verify only requested fields returned
TEST_ASSERT_EQUAL(2, result.num_columns);
```

## Expected Behaviors

### Empty Results
These should return `success = true` with `num_points = 0`:
- Nonexistent measurement
- No matching tags
- No matching fields
- Time range outside data
- Empty database

### Parameter Validation
These should return `success = false`:
- NULL query pointer
- NULL result pointer
- NULL measurement name

### Time Range Behavior
- `start_ms = 0` means "from beginning"
- `end_ms = 0` means "to end"
- Both = 0 means "all data"
- Inverted range (start > end) returns empty result

### Limit Behavior
- `limit = 0` means "no limit"
- `limit = N` returns at most N aggregated points
- Limit greater than available data returns all available

### Field Selection
- `num_fields = 0` returns all fields for measurement
- Duplicate fields in query handled gracefully
- Non-existent fields return empty result

### Tag Filtering
- `num_tags = 0` returns all series
- Multiple tags use intersection (AND logic)
- Non-matching tags return empty result

## Coverage Gaps

Not covered (require mocking or special setup):
1. Flash read failures during query
2. Corrupt data handling
3. Concurrent query access
4. Memory allocation failure injection
5. Decompression failures

## Bugs to Watch For

1. **NULL measurement name**: Causes crash if not validated early
2. **Inverted deleted flag** (line 704): Check may be backwards
3. **Inverted compressed flag** (line 543): Assignment may be backwards
4. **OOM in find_or_create_row**: Partial rollback issues
5. **alloca on large series lists**: Stack overflow risk

See QUERY_TEST_ANALYSIS.md for detailed bug reports.
