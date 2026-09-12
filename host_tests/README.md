Run the real component and its Unity tests using the repository's ESP-IDF Linux
harness, with ASan, UBSan and LeakSanitizer:

```sh
components/esp_idf_timeseries/host_tests/run_tests.sh '[audit]'
components/esp_idf_timeseries/host_tests/run_tests.sh '[second]'
components/esp_idf_timeseries/host_tests/run_tests.sh '[third]'
components/esp_idf_timeseries/host_tests/run_tests.sh
```

The tagged commands select successive review regressions. `[third]` also prints
merge, narrow-record query, and ALP skip benchmarks.
The untagged command runs the full suite. `[second_scan]` prints a repeatable
16,000-point insert / narrow-query benchmark, including flash read counts.
The runner uses `$IDF_PATH`, defaulting to `$HOME/esp/esp-idf`, and requires the
shared harness prerequisites in `tools/host_test/README.md` (including libbsd).
Generated build files live in `tools/host_test/esp_idf_timeseries_test_app`.

Flash operations and task synchronization use ESP-IDF's Linux implementation.
The local `soc` overlay exposes Linux headers without IDF 5.5.5's chip-only
toolchain hook. Heap-tracing stubs come from the shared overlay; LSan checks
actual allocations. The test-only linker wrappers inject selected allocation failures and task
interleavings during record reads, RAM decoding, and compaction publication. They are inactive outside
their tests and are excluded from device builds.

To reproduce against a saved component tree containing `src/` and `include/`:

```sh
TIMESERIES_AUDIT_SOURCE_DIR=/absolute/saved/component \
  components/esp_idf_timeseries/host_tests/run_tests.sh '[audit_strings]'
```

Some unfixed regressions abort under ASan or hang; run those individually with
an external timeout. Every normal invocation clears the source override unless
the environment variable is explicitly supplied.

The portable regressions also run on ESP32-S3 QEMU:

```sh
cd components/esp_idf_timeseries/test_app
idf.py '-DTIMESERIES_TEST_TAG=[audit]' build
./run_tests.sh --skip-build
```

Use `idf.py -DTIMESERIES_TEST_TAG= build` to restore the full device suite.
The allocation-injection and record-reuse tests are Linux-only; all other audit
tests run on both targets.
