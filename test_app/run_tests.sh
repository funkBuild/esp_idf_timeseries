#!/bin/bash
#
# QEMU Test Runner for esp_idf_timeseries component tests
#
# Builds the test_app, generates a QEMU flash image, runs tests in QEMU,
# and force-kills QEMU when tests complete (QEMU doesn't exit automatically).
#
# Usage:
#   ./run_tests.sh          # Build + run
#   ./run_tests.sh --skip-build  # Run only (assumes already built)
#
# Exit code: 0 if all tests pass, 1 otherwise.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TIMEOUT_SEC=120
SKIP_BUILD=false
QEMU_PID=""
OUTPUT_FILE=""

for arg in "$@"; do
  case "$arg" in
    --skip-build) SKIP_BUILD=true ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

cleanup() {
  if [ -n "$QEMU_PID" ] && kill -0 "$QEMU_PID" 2>/dev/null; then
    kill -9 "$QEMU_PID" 2>/dev/null || true
    wait "$QEMU_PID" 2>/dev/null || true
  fi
  [ -n "$OUTPUT_FILE" ] && rm -f "$OUTPUT_FILE"
}
trap cleanup EXIT

info()  { echo -e "\033[0;32m[INFO]\033[0m $1"; }
error() { echo -e "\033[0;31m[ERROR]\033[0m $1"; }

# --- Build ---
if [ "$SKIP_BUILD" = false ]; then
  info "Building test_app..."
  idf.py build || { error "Build failed"; exit 1; }
fi

if [ ! -f build/unit_test_test.bin ]; then
  error "build/unit_test_test.bin not found. Run without --skip-build."
  exit 1
fi

# --- Generate QEMU flash image ---
info "Generating QEMU flash image..."

python3 -m esptool --chip esp32s3 merge_bin \
  --output build/qemu_flash.bin \
  --fill-flash-size 16MB \
  --flash_mode dio \
  --flash_freq 80m \
  --flash_size 16MB \
  0x0     build/bootloader/bootloader.bin \
  0x8000  build/partition_table/partition-table.bin \
  0x10000 build/unit_test_test.bin

if [ ! -f build/qemu_efuse.bin ]; then
  dd if=/dev/zero of=build/qemu_efuse.bin bs=1K count=4 2>/dev/null
fi

# --- Find QEMU ---
find_qemu() {
  local QEMU_DIR="$HOME/.espressif/tools/qemu-xtensa"
  if [ -d "$QEMU_DIR" ]; then
    local found
    found=$(find "$QEMU_DIR" -name "qemu-system-xtensa" -type f 2>/dev/null | sort -rV | head -1)
    if [ -n "$found" ] && [ -x "$found" ]; then
      echo "$found"
      return
    fi
  fi
  which qemu-system-xtensa 2>/dev/null || true
}

QEMU_BIN=$(find_qemu)
if [ -z "$QEMU_BIN" ]; then
  error "qemu-system-xtensa not found"
  exit 1
fi
info "Using QEMU: $QEMU_BIN"

# --- Run tests in QEMU ---
OUTPUT_FILE=$(mktemp)
info "Running tests in QEMU (timeout: ${TIMEOUT_SEC}s)..."

$QEMU_BIN \
  -M esp32s3 \
  -drive file=build/qemu_flash.bin,if=mtd,format=raw \
  -drive file=build/qemu_efuse.bin,if=none,format=raw,id=efuse \
  -global driver=esp32s3.efuse,property=drive,value=efuse \
  -serial mon:stdio \
  -nographic \
  -no-reboot \
  > "$OUTPUT_FILE" 2>&1 &
QEMU_PID=$!

# Wait for tests to complete or timeout
SECONDS_WAITED=0
TESTS_DONE=false
while [ $SECONDS_WAITED -lt $TIMEOUT_SEC ]; do
  if ! kill -0 "$QEMU_PID" 2>/dev/null; then
    break
  fi
  # Check if Unity has printed the final summary line
  if grep -q "Tests [0-9]* Failures [0-9]* Ignored" "$OUTPUT_FILE" 2>/dev/null; then
    TESTS_DONE=true
    # Give a moment for any trailing output
    sleep 1
    break
  fi
  sleep 1
  SECONDS_WAITED=$((SECONDS_WAITED + 1))
done

# Force-kill QEMU
if kill -0 "$QEMU_PID" 2>/dev/null; then
  if [ "$TESTS_DONE" = false ]; then
    error "Timeout after ${TIMEOUT_SEC}s — killing QEMU"
  fi
  kill -9 "$QEMU_PID" 2>/dev/null || true
  wait "$QEMU_PID" 2>/dev/null || true
fi
QEMU_PID=""

# --- Parse results ---
echo ""
echo "======== QEMU Test Output ========"
cat "$OUTPUT_FILE"
echo "=================================="
echo ""

if [ "$TESTS_DONE" = false ]; then
  error "Tests did not complete within ${TIMEOUT_SEC}s"
  exit 1
fi

# Extract the summary line: "N Tests M Failures K Ignored"
SUMMARY=$(grep "Tests [0-9]* Failures [0-9]* Ignored" "$OUTPUT_FILE" | tail -1)
FAILURES=$(echo "$SUMMARY" | grep -oP '\d+(?= Failures)')

if [ -z "$FAILURES" ]; then
  error "Could not parse test results"
  exit 1
fi

if [ "$FAILURES" -eq 0 ]; then
  info "All tests passed: $SUMMARY"
  exit 0
else
  error "Test failures: $SUMMARY"
  exit 1
fi
