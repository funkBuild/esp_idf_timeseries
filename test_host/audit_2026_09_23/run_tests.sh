#!/usr/bin/env bash
# Audit 2026-09-23 repro tests (native gcc, ASan+UBSan). Exit 0 = all pass.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMP="$(cd "$HERE/../.." && pwd)"
REPO="$(cd "$COMP/../.." && pwd)"
OUT="$(mktemp -d "${TMPDIR:-/tmp}/tsdb-audit-XXXXXX")"
trap 'rm -rf "$OUT"' EXIT
python3 - "$REPO" "$COMP" "$OUT" <<'PY'
import sys
from pathlib import Path
repo, comp, out = map(Path, sys.argv[1:])
sys.path.insert(0, str(repo / 'tools/host_test'))
sys.dont_write_bytecode = True
from extract_c_functions import extract
(out / 'iterator_funcs.inc').write_text(extract(comp / 'src/timeseries_iterator.c', [
    'timeseries_page_iterator_init', 'timeseries_page_iterator_next',
    'timeseries_blank_iterator_init', 'timeseries_blank_iterator_deinit',
    'timeseries_blank_iterator_next', 'timeseries_entity_iterator_init']))
PY
CFLAGS=(-std=gnu11 -g -O1 -fsanitize=address,undefined -fno-omit-frame-pointer -Wno-unused-variable
        -I"$OUT" -I"$HERE/stubs" -I"$COMP/include" -I"$COMP/src")
rc=0
gcc "${CFLAGS[@]}" "$HERE/test_page_scan_audit.c" "$COMP/src/timeseries_page_cache.c" \
    "$COMP/src/timeseries_page_cache_snapshot.c" -o "$OUT/test_page_scan_audit" || exit 2
ASAN_OPTIONS=detect_leaks=0 "$OUT/test_page_scan_audit" || rc=1
exit $rc
