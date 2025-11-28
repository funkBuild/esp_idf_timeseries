# SPDX-FileCopyrightText: 2024 Espressif Systems (Shanghai) CO LTD
# SPDX-License-Identifier: Apache-2.0
"""
Pytest configuration for ESP-IDF Timeseries performance tests.
"""

import pytest


def pytest_configure(config):
    """Configure pytest markers for ESP-IDF test targets."""
    config.addinivalue_line(
        "markers", "esp32: mark test to run on ESP32 target"
    )
    config.addinivalue_line(
        "markers", "host_test: mark test to run on host"
    )
    config.addinivalue_line(
        "markers", "qemu: mark test to run in QEMU emulator"
    )
    config.addinivalue_line(
        "markers", "perf: performance test marker"
    )
    config.addinivalue_line(
        "markers", "insert: insert operation performance tests"
    )
    config.addinivalue_line(
        "markers", "query: query operation performance tests"
    )
    config.addinivalue_line(
        "markers", "compaction: compaction performance tests"
    )
    config.addinivalue_line(
        "markers", "codec: compression codec performance tests"
    )
    config.addinivalue_line(
        "markers", "cache: cache performance tests"
    )
    config.addinivalue_line(
        "markers", "e2e: end-to-end workflow tests"
    )
    config.addinivalue_line(
        "markers", "stress: stress tests with large datasets"
    )


@pytest.fixture(scope='session')
def perf_results():
    """Fixture to collect performance results across tests."""
    return {}
