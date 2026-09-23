#!/usr/bin/env bash
# memsafety 2026-09-23 (PROTO): seeded multi-task churn of esp_idf_timeseries.
# Usage: run_tests.sh [asan|tsan] ; env CHURN_SECONDS (default 60), CHURN_SEEDS
# (default "1 2 3 4 5 6"), CHURN_TASKS (task bitmask, default 31 = all),
# CHURN_DET_INSERTS (deterministic single-task
# metadata phase run before each churn, default 160, 0 = skip), BUILD_ROOT
# (default $TMPDIR or /tmp), TIMESERIES_AUDIT_SOURCE_DIR (build a saved copy of
# the component's src/ + include/ instead, e.g. a pre-fix control).
# Exit 0 = all seeds pass.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../.." && pwd)"
SAN="${1:-asan}"
case "$SAN" in
  asan) export HOST_SAN_C="-fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer -g -ftrivial-auto-var-init=pattern" ;;
  tsan) export HOST_SAN_C="-fsanitize=thread -fno-omit-frame-pointer -g" ;;
  *) echo "usage: $0 [asan|tsan]"; exit 2 ;;
esac
export IDF_PATH="${IDF_PATH:-$HOME/esp/esp-idf}"
source "$IDF_PATH/export.sh" >/dev/null 2>&1
BUILD="${BUILD_ROOT:-${TMPDIR:-/tmp}}/tsdb_memsafety_build_$SAN"
SRC_ARGS=()
if [ -n "${TIMESERIES_AUDIT_SOURCE_DIR:-}" ]; then
  BUILD="${BUILD}_audit_src"
  SRC_ARGS=(-D TIMESERIES_AUDIT_SOURCE_DIR="$TIMESERIES_AUDIT_SOURCE_DIR")
fi
cd "$HERE" || exit 2
idf.py -B "$BUILD" -D SDKCONFIG="$BUILD/sdkconfig" -D IDF_TARGET=linux "${SRC_ARGS[@]}" build >"$BUILD.log" 2>&1 || { tail -40 "$BUILD.log"; exit 1; }
ELF="$(find "$BUILD" -maxdepth 1 -name '*.elf' | head -1)"
export ASAN_OPTIONS="quarantine_size_mb=1024:malloc_fill_byte=190:max_malloc_fill_size=1048576:free_fill_byte=90:detect_stack_use_after_return=1:detect_leaks=0:halt_on_error=1:abort_on_error=0"
export UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=1:suppressions=$REPO/tools/host_test/ubsan.supp"
export TSAN_OPTIONS="suppressions=$REPO/tools/host_test/tsan.supp:history_size=7:halt_on_error=1"
RUNNER=()
[ "$SAN" = tsan ] && RUNNER=(setarch "$(uname -m)" -R)
rc=0
# Default task mix: all five tasks (hb, lua, query, meta, admin). Each process
# first runs the deterministic PROTO-TS-1b batch and race phases (CHURN_NO_RACE=1
# skips both) and the deterministic PROTO-TS-1 phase, then the seeded churn.
export CHURN_TASKS="${CHURN_TASKS:-31}"
# The linux FreeRTOS port blocks SIGTERM, so a hung run is killed with SIGKILL
# (-k) after the timeout's SIGTERM is ignored; it then counts as a failure.
for seed in ${CHURN_SEEDS:-1 2 3 4 5 6}; do
  wd="$(mktemp -d)"; log="$wd/run.log"
  ( cd "$wd" && CHURN_SEED=$seed CHURN_SECONDS=${CHURN_SECONDS:-60} timeout -k 10 $(( ${CHURN_SECONDS:-60} + 300 )) "${RUNNER[@]}" "$ELF" </dev/null >"$log" 2>&1 ) || { rc=1; echo "FAIL seed=$seed: process exited non-zero"; }
  # Sanitizer reports, verbatim
  grep -A40 -E "ERROR: AddressSanitizer|WARNING: ThreadSanitizer|runtime error:" "$log" | head -80
  grep -E "^batch:|^race:|^det:|^churn ops|^flash writes|^PASS|^FAIL" "$log"
  rm -rf "$wd"
done
exit $rc
