# SPDX-FileCopyrightText: 2024 Espressif Systems (Shanghai) CO LTD
# SPDX-License-Identifier: Apache-2.0
"""
Performance tests for ESP-IDF Timeseries database running in QEMU.

These tests verify that the performance benchmarks run successfully and
capture performance metrics from the test output.
"""

import logging
import re
from typing import Callable, Dict, List, Optional

import pytest
from pytest_embedded_idf.dut import IdfDut
from pytest_embedded_qemu.app import QemuApp
from pytest_embedded_qemu.dut import QemuDut


def parse_perf_metrics(output: str) -> Dict[str, Dict[str, str]]:
    """Parse performance metrics from test output.

    Expected format: PERF [test_name] metric: value unit

    Returns:
        Dict mapping test_name -> {metric: "value unit"}
    """
    metrics = {}
    pattern = r'PERF \[(\w+)\] (\w+): ([\d.]+) (\S+)'

    for match in re.finditer(pattern, output):
        test_name = match.group(1)
        metric = match.group(2)
        value = match.group(3)
        unit = match.group(4)

        if test_name not in metrics:
            metrics[test_name] = {}
        metrics[test_name][metric] = f"{value} {unit}"

    return metrics


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_database_init(app: QemuApp, dut: QemuDut) -> None:
    """Test database initialization performance."""
    dut.expect(r'Database initialized for performance testing', timeout=30)


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_clear_database(app: QemuApp, dut: QemuDut) -> None:
    """Test database clearing to known state."""
    dut.expect(r'Database cleared to empty state', timeout=30)


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_single_point_insert(app: QemuApp, dut: QemuDut) -> None:
    """Test single-point insert performance."""
    output = dut.expect(r'PERF \[single_point_insert\] throughput: ([\d.]+)', timeout=120)
    throughput = float(output.group(1))
    logging.info(f"Single-point insert throughput: {throughput} ops/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_bulk_insert_100(app: QemuApp, dut: QemuDut) -> None:
    """Test bulk insert of 100 points."""
    output = dut.expect(r'PERF \[bulk_insert_100\] throughput: ([\d.]+)', timeout=120)
    throughput = float(output.group(1))
    logging.info(f"Bulk insert (100 pts) throughput: {throughput} points/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_bulk_insert_1000(app: QemuApp, dut: QemuDut) -> None:
    """Test bulk insert of 1000 points."""
    output = dut.expect(r'PERF \[bulk_insert_1000\] throughput: ([\d.]+)', timeout=180)
    throughput = float(output.group(1))
    logging.info(f"Bulk insert (1000 pts) throughput: {throughput} points/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_multi_field_insert(app: QemuApp, dut: QemuDut) -> None:
    """Test multi-field insert (6 fields)."""
    output = dut.expect(r'PERF \[multi_field_insert\] throughput: ([\d.]+)', timeout=180)
    throughput = float(output.group(1))
    logging.info(f"Multi-field insert throughput: {throughput} values/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_query_100_1field(app: QemuApp, dut: QemuDut) -> None:
    """Test query of 100 points with single field."""
    output = dut.expect(r'PERF \[query_100_1field\] throughput: ([\d.]+)', timeout=120)
    throughput = float(output.group(1))
    logging.info(f"Query (100 pts, 1 field) throughput: {throughput} points/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_query_1000_1field(app: QemuApp, dut: QemuDut) -> None:
    """Test query of 1000 points with single field."""
    output = dut.expect(r'PERF \[query_1000_1field\] throughput: ([\d.]+)', timeout=180)
    throughput = float(output.group(1))
    logging.info(f"Query (1000 pts, 1 field) throughput: {throughput} points/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_query_500_6fields(app: QemuApp, dut: QemuDut) -> None:
    """Test query of 500 points with 6 fields."""
    output = dut.expect(r'PERF \[query_500_6fields\] throughput: ([\d.]+)', timeout=180)
    throughput = float(output.group(1))
    logging.info(f"Query (500 pts, 6 fields) throughput: {throughput} values/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_compaction_500(app: QemuApp, dut: QemuDut) -> None:
    """Test compaction of 500 points."""
    output = dut.expect(r'PERF \[compaction_500\] throughput: ([\d.]+)', timeout=180)
    throughput = float(output.group(1))
    logging.info(f"Compaction (500 pts) throughput: {throughput} points/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_compaction_2000(app: QemuApp, dut: QemuDut) -> None:
    """Test compaction of 2000 points."""
    output = dut.expect(r'PERF \[compaction_2000\] throughput: ([\d.]+)', timeout=300)
    throughput = float(output.group(1))
    logging.info(f"Compaction (2000 pts) throughput: {throughput} points/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_float_encode(app: QemuApp, dut: QemuDut) -> None:
    """Test float compression encoding performance."""
    output = dut.expect(r'PERF \[float_encode\] compression_ratio: ([\d.]+)', timeout=120)
    ratio = float(output.group(1))
    logging.info(f"Float compression ratio: {ratio}x")
    assert ratio > 0, "Compression ratio should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_float_decode(app: QemuApp, dut: QemuDut) -> None:
    """Test float compression decoding performance."""
    output = dut.expect(r'PERF \[float_decode\] throughput: ([\d.]+)', timeout=120)
    throughput = float(output.group(1))
    logging.info(f"Float decode throughput: {throughput} values/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_int_encode(app: QemuApp, dut: QemuDut) -> None:
    """Test integer compression encoding performance."""
    output = dut.expect(r'PERF \[int_encode\] compression_ratio: ([\d.]+)', timeout=120)
    ratio = float(output.group(1))
    logging.info(f"Integer compression ratio: {ratio}x")
    assert ratio > 0, "Compression ratio should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_int_decode(app: QemuApp, dut: QemuDut) -> None:
    """Test integer compression decoding performance."""
    output = dut.expect(r'PERF \[int_decode\] throughput: ([\d.]+)', timeout=120)
    throughput = float(output.group(1))
    logging.info(f"Integer decode throughput: {throughput} values/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_e2e_workflow(app: QemuApp, dut: QemuDut) -> None:
    """Test full end-to-end workflow."""
    # Expect all three metrics from e2e test
    dut.expect(r'PERF \[e2e_workflow\] insert_time', timeout=180)
    dut.expect(r'PERF \[e2e_workflow\] compact_time', timeout=60)
    output = dut.expect(r'PERF \[e2e_workflow\] query_throughput: ([\d.]+)', timeout=60)
    throughput = float(output.group(1))
    logging.info(f"E2E query throughput: {throughput} values/s")
    assert throughput > 0, "Throughput should be positive"


@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_perf_stress_test(app: QemuApp, dut: QemuDut) -> None:
    """Test stress scenario with large dataset."""
    output = dut.expect(r'PERF \[stress_test\] insert_throughput: ([\d.]+)', timeout=600)
    throughput = float(output.group(1))
    logging.info(f"Stress test insert throughput: {throughput} values/s")
    assert throughput > 0, "Throughput should be positive"


# Combined test that runs all performance tests and collects metrics
@pytest.mark.esp32
@pytest.mark.host_test
@pytest.mark.qemu
def test_run_all_perf_tests(app: QemuApp, dut: QemuDut) -> None:
    """Run all performance tests and collect metrics.

    This test runs the Unity test framework and waits for all tests to complete,
    collecting performance metrics along the way.
    """
    # Wait for tests to complete (look for Unity summary)
    try:
        # Look for Unity test completion
        output = dut.expect(r'(\d+) Tests (\d+) Failures (\d+) Ignored', timeout=900)
        total = int(output.group(1))
        failures = int(output.group(2))
        ignored = int(output.group(3))

        logging.info(f"Unity Tests: {total} total, {failures} failures, {ignored} ignored")

        # Collect all performance metrics from the output buffer
        full_output = dut.pexpect_proc.before.decode('utf-8', errors='ignore') if dut.pexpect_proc.before else ""
        metrics = parse_perf_metrics(full_output)

        # Log summary of metrics
        logging.info("=== Performance Metrics Summary ===")
        for test_name, test_metrics in metrics.items():
            logging.info(f"\n{test_name}:")
            for metric, value in test_metrics.items():
                logging.info(f"  {metric}: {value}")

        # Assert no test failures
        assert failures == 0, f"Expected 0 failures, got {failures}"

    except Exception as e:
        logging.error(f"Performance test failed: {e}")
        raise
