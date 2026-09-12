#!/usr/bin/env bash
# Run the component's Unity suite on ESP-IDF Linux with ASan/UBSan/LSan.
# Usage: run_tests.sh [tag], e.g. run_tests.sh '[audit]'. No tag runs all tests.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
python3 "$REPO/tools/host_test/gen_project.py" esp_idf_timeseries
python3 - "$REPO" "$HERE" <<'PY'
from pathlib import Path
import sys
repo, here = map(Path, sys.argv[1:])
project = repo / 'tools/host_test/esp_idf_timeseries_test_app/CMakeLists.txt'
text = project.read_text()
text = text.replace('set(EXTRA_COMPONENT_DIRS\n',
                    f'set(EXTRA_COMPONENT_DIRS\n    "{here}/linux_overlay"\n')
text = text.replace('project(esp_idf_timeseries_linux_test)', '''
# Linux has no chip MMU Kconfig; partition.c still needs the page-size constant.
idf_build_set_property(COMPILE_DEFINITIONS "CONFIG_MMU_PAGE_SIZE=65536" APPEND)
set(COMPONENTS main heap_trace esp_idf_timeseries)
project(esp_idf_timeseries_linux_test)''')
project.write_text(text)
PY
export TIMESERIES_TEST_FILTER="${1:-}"
exec "$REPO/tools/host_test/run.sh" esp_idf_timeseries asan \
    "-DTIMESERIES_AUDIT_SOURCE_DIR=${TIMESERIES_AUDIT_SOURCE_DIR:-}" reconfigure
