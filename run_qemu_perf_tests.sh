#!/bin/bash
# Script to run performance tests in QEMU
# Usage: ./run_qemu_perf_tests.sh [test_filter]
#
# Examples:
#   ./run_qemu_perf_tests.sh                    # Run all tests
#   ./run_qemu_perf_tests.sh "[perf]"           # Run all perf tests
#   ./run_qemu_perf_tests.sh "[perf][insert]"   # Run insert perf tests
#   ./run_qemu_perf_tests.sh "[perf][query]"    # Run query perf tests
#   ./run_qemu_perf_tests.sh "[perf][codec]"    # Run codec perf tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_APP_DIR="${SCRIPT_DIR}/test_app"
LOG_FILE="/tmp/tsdb_perf_test.log"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== ESP-IDF Timeseries Performance Tests ===${NC}"

# Source ESP-IDF environment
if [ -f "$HOME/esp/esp-idf/export.sh" ]; then
    echo "Sourcing ESP-IDF environment..."
    . "$HOME/esp/esp-idf/export.sh"
elif [ -n "$IDF_PATH" ]; then
    echo "ESP-IDF already sourced"
else
    echo -e "${RED}Error: ESP-IDF not found. Please install ESP-IDF first.${NC}"
    exit 1
fi

# Navigate to test_app directory
cd "${TEST_APP_DIR}"

# Build the test application
echo -e "${YELLOW}Building test application...${NC}"
idf.py build

# Check if QEMU is available
if ! command -v qemu-system-xtensa &> /dev/null; then
    echo -e "${YELLOW}Warning: QEMU not found. Running on hardware or using pytest-embedded.${NC}"
    echo "To install QEMU for ESP32, see: https://docs.espressif.com/projects/esp-idf/en/latest/esp32/api-guides/tools/qemu.html"
fi

# Run tests using pytest if available
if command -v pytest &> /dev/null && python3 -c "import pytest_embedded" 2>/dev/null; then
    echo -e "${GREEN}Running tests with pytest-embedded...${NC}"

    # Set test filter if provided
    TEST_FILTER="${1:--k perf}"

    pytest pytest_perf_tests.py \
        --target esp32 \
        --embedded-services qemu \
        --qemu-image-path build/unit_test_test.bin \
        ${TEST_FILTER} \
        -v \
        2>&1 | tee "${LOG_FILE}"

    RESULT=$?
else
    # Fallback: Run directly with QEMU if available
    if command -v qemu-system-xtensa &> /dev/null; then
        echo -e "${YELLOW}Running tests directly with QEMU...${NC}"

        # Create QEMU flash image
        echo "Creating flash image..."
        esptool.py --chip esp32 merge_bin -o build/flash_image.bin \
            --flash_mode dio \
            --flash_size 4MB \
            0x1000 build/bootloader/bootloader.bin \
            0x8000 build/partition_table/partition-table.bin \
            0x10000 build/unit_test_test.bin

        # Run QEMU with timeout
        echo "Starting QEMU (timeout: 300s)..."
        timeout 300 qemu-system-xtensa \
            -nographic \
            -machine esp32 \
            -drive file=build/flash_image.bin,if=mtd,format=raw \
            -serial mon:stdio \
            2>&1 | tee "${LOG_FILE}" &

        QEMU_PID=$!

        # Wait for tests to complete (look for Unity results)
        for i in {1..60}; do
            if grep -q "Tests.*Failures.*Ignored" "${LOG_FILE}" 2>/dev/null; then
                break
            fi
            sleep 5
        done

        # Kill QEMU
        kill $QEMU_PID 2>/dev/null || true

        RESULT=0
    else
        echo -e "${YELLOW}Note: QEMU not available. Using serial monitor for hardware testing.${NC}"
        echo "Flashing and monitoring..."

        timeout 180 idf.py flash monitor 2>&1 | tee "${LOG_FILE}" &
        MONITOR_PID=$!

        # Wait for tests to complete
        sleep 120
        kill $MONITOR_PID 2>/dev/null || true

        RESULT=0
    fi
fi

# Extract and display performance results
echo ""
echo -e "${GREEN}=== Performance Test Results ===${NC}"
echo ""

# Parse performance metrics from log
if [ -f "${LOG_FILE}" ]; then
    echo "--- Insert Performance ---"
    grep "PERF \[.*insert" "${LOG_FILE}" 2>/dev/null || echo "No insert metrics found"

    echo ""
    echo "--- Query Performance ---"
    grep "PERF \[query" "${LOG_FILE}" 2>/dev/null || echo "No query metrics found"

    echo ""
    echo "--- Compaction Performance ---"
    grep "PERF \[compact" "${LOG_FILE}" 2>/dev/null || echo "No compaction metrics found"

    echo ""
    echo "--- Codec Performance ---"
    grep "PERF \[.*encode\|PERF \[.*decode" "${LOG_FILE}" 2>/dev/null || echo "No codec metrics found"

    echo ""
    echo "--- End-to-End Performance ---"
    grep "PERF \[e2e\|PERF \[stress" "${LOG_FILE}" 2>/dev/null || echo "No e2e metrics found"

    echo ""
    echo "--- Unity Test Summary ---"
    grep -E "Tests.*Failures.*Ignored|PASS|FAIL" "${LOG_FILE}" 2>/dev/null | tail -20
fi

echo ""
echo -e "${GREEN}Full log saved to: ${LOG_FILE}${NC}"

exit ${RESULT:-0}
