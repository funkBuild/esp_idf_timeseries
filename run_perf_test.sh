#!/bin/bash
. /home/matt/esp/esp-idf/export.sh
cd example
idf.py flash monitor 2>&1 | tee /tmp/batched_flash_perf.log &
PID=$!
sleep 20
kill $PID 2>/dev/null
echo "=== Performance Results with Batched Flash Reads ==="
grep -E "Average query time|Minimum query time|Maximum query time" /tmp/batched_flash_perf.log | tail -3
