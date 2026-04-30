#!/usr/bin/env python3
"""E2E verification for Phase 16: Daft Coordinator sidecar + gRPC protocol.

Prerequisites:
  - Ray cluster running
  - StarRocks running with Arrow Flight SQL enabled (port 9408)
  - Coordinator NOT running (this script starts it)

Usage:
  SR_HOST=47.239.57.232 python3.11 tests/verify_phase16_coordinator.py

Environment variables:
  SR_HOST        StarRocks host (default: 127.0.0.1)
  SR_FLIGHT_PORT Arrow Flight SQL port (default: 9408)
  COORD_PORT     Coordinator gRPC port (default: 50051)
  RAY_ADDRESS    Ray cluster address (default: ray://127.0.0.1:10001)
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

import grpc
import pyarrow as pa

# Add parent dir to path so we can import proto modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from starrocks.coordinator.proto import coordinator_pb2, coordinator_pb2_grpc

SR_HOST = os.environ.get("SR_HOST", "127.0.0.1")
SR_FLIGHT_PORT = int(os.environ.get("SR_FLIGHT_PORT", "9408"))
COORD_PORT = int(os.environ.get("COORD_PORT", "50051"))
RAY_ADDRESS = os.environ.get("RAY_ADDRESS", "ray://127.0.0.1:10001")

PASS = 0
FAIL = 0


def log_result(test_name: str, passed: bool, detail: str = "") -> None:
    global PASS, FAIL
    status = "PASS" if passed else "FAIL"
    if passed:
        PASS += 1
    else:
        FAIL += 1
    suffix = f" — {detail}" if detail else ""
    print(f"  [{status}] {test_name}{suffix}")


def start_coordinator() -> subprocess.Popen:
    """Start the Coordinator as a subprocess."""
    cmd = [
        sys.executable, "-m", "starrocks.coordinator",
        "--port", str(COORD_PORT),
        "--ray-address", RAY_ADDRESS,
        "--log-level", "DEBUG",
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=os.path.join(os.path.dirname(__file__), ".."),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Wait for server to start
    time.sleep(3)
    if proc.poll() is not None:
        stdout = proc.stdout.read().decode() if proc.stdout else ""
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        raise RuntimeError(
            f"Coordinator failed to start (exit={proc.returncode}).\n"
            f"stdout: {stdout}\nstderr: {stderr}"
        )
    return proc


def get_stub() -> coordinator_pb2_grpc.DaftCoordinatorStub:
    channel = grpc.insecure_channel(f"localhost:{COORD_PORT}")
    return coordinator_pb2_grpc.DaftCoordinatorStub(channel)


def test_health_check(stub):
    """Test 1: GetStatus returns SERVING."""
    try:
        resp = stub.GetStatus(coordinator_pb2.StatusRequest())
        log_result("GetStatus → SERVING", resp.status == "SERVING", f"status={resp.status}")
    except Exception as e:
        log_result("GetStatus → SERVING", False, str(e))


def test_register_function(stub):
    """Test 2: Register a test function."""
    try:
        resp = stub.RegisterFunction(coordinator_pb2.RegisterFunctionRequest(
            function_name="double_values",
            module_path="tests.test_funcs_phase16",
            callable_name="double_values",
        ))
        log_result("RegisterFunction", resp.success, resp.message)
    except Exception as e:
        log_result("RegisterFunction", False, str(e))


def test_register_bad_module(stub):
    """Test 3: Register with bad module → failure response (not crash)."""
    try:
        resp = stub.RegisterFunction(coordinator_pb2.RegisterFunctionRequest(
            function_name="bad",
            module_path="nonexistent_module_xyz",
            callable_name="func",
        ))
        log_result("RegisterFunction bad module → failure", not resp.success, resp.message)
    except Exception as e:
        log_result("RegisterFunction bad module → failure", False, str(e))


def test_submit_plan(stub):
    """Test 4: Submit a Daft plan with map_batches → get results."""
    try:
        endpoint = f"grpc://{SR_HOST}:{SR_FLIGHT_PORT}"
        request = coordinator_pb2.DaftPlanRequest(
            request_id="e2e-test-1",
            arrow_flight_endpoint=endpoint,
            source_sql="SELECT 1 as v UNION ALL SELECT 2 UNION ALL SELECT 3",
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(function_name="double_values"),
                ),
            ],
        )

        responses = list(stub.SubmitDaftPlan(request))
        # Check we got results, not errors
        errors = [r.error for r in responses if r.HasField("error") and r.WhichOneof("result") == "error"]
        if errors:
            log_result("SubmitDaftPlan map_batches", False, f"errors: {errors}")
            return

        # Decode Arrow IPC batches
        tables = []
        for r in responses:
            if r.WhichOneof("result") == "arrow_ipc_batch":
                reader = pa.ipc.open_stream(r.arrow_ipc_batch)
                tables.append(reader.read_all())

        result = pa.concat_tables(tables)
        values = sorted(result.column("v").to_pylist())
        expected = [2, 4, 6]  # doubled
        log_result("SubmitDaftPlan map_batches", values == expected,
                   f"got={values}, expected={expected}")
    except Exception as e:
        log_result("SubmitDaftPlan map_batches", False, str(e))


def test_submit_plan_bad_function(stub):
    """Test 5: Submit plan with unregistered function → error response."""
    try:
        endpoint = f"grpc://{SR_HOST}:{SR_FLIGHT_PORT}"
        request = coordinator_pb2.DaftPlanRequest(
            request_id="e2e-test-err",
            arrow_flight_endpoint=endpoint,
            source_sql="SELECT 1 as v",
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(function_name="nonexistent_func"),
                ),
            ],
        )

        responses = list(stub.SubmitDaftPlan(request))
        has_error = any(
            r.WhichOneof("result") == "error" for r in responses
        )
        log_result("SubmitDaftPlan bad function → error", has_error,
                   f"responses={len(responses)}")
    except Exception as e:
        log_result("SubmitDaftPlan bad function → error", False, str(e))


def test_status_after_register(stub):
    """Test 6: GetStatus shows registered function count."""
    try:
        resp = stub.GetStatus(coordinator_pb2.StatusRequest())
        # We registered "double_values" earlier
        log_result("GetStatus registered_functions >= 1",
                   resp.registered_functions >= 1,
                   f"count={resp.registered_functions}")
    except Exception as e:
        log_result("GetStatus registered_functions >= 1", False, str(e))


def test_coordinator_shutdown():
    """Test 7: After shutdown, gRPC connection fails."""
    try:
        channel = grpc.insecure_channel(f"localhost:{COORD_PORT}")
        stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
        stub.GetStatus(
            coordinator_pb2.StatusRequest(),
            timeout=2,
        )
        # If we reach here, coordinator is still up (shouldn't be after stop)
        log_result("Post-shutdown connection fails", False, "still connected")
    except grpc.RpcError:
        log_result("Post-shutdown connection fails", True)
    except Exception as e:
        log_result("Post-shutdown connection fails", True, f"error (expected): {type(e).__name__}")


def main():
    global PASS, FAIL

    print("=" * 60)
    print("Phase 16 E2E: Daft Coordinator sidecar + gRPC protocol")
    print("=" * 60)
    print(f"  SR_HOST={SR_HOST}, FLIGHT_PORT={SR_FLIGHT_PORT}")
    print(f"  COORD_PORT={COORD_PORT}, RAY_ADDRESS={RAY_ADDRESS}")
    print()

    # Create the test function module for registration
    _create_test_funcs_module()

    print("Starting Coordinator...")
    proc = start_coordinator()
    print(f"  Coordinator PID={proc.pid}")
    print()

    try:
        stub = get_stub()

        print("Running tests...")
        test_health_check(stub)
        test_register_function(stub)
        test_register_bad_module(stub)
        test_submit_plan(stub)
        test_submit_plan_bad_function(stub)
        test_status_after_register(stub)
    finally:
        print("\nStopping Coordinator...")
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        print(f"  Coordinator stopped (exit={proc.returncode})")

    # Test 7: verify shutdown
    time.sleep(1)
    test_coordinator_shutdown()

    print()
    print("=" * 60)
    total = PASS + FAIL
    print(f"Results: {PASS}/{total} passed, {FAIL}/{total} failed")
    print("=" * 60)
    sys.exit(0 if FAIL == 0 else 1)


def _create_test_funcs_module():
    """Create a test_funcs_phase16.py module in the tests/ directory."""
    module_path = os.path.join(os.path.dirname(__file__), "test_funcs_phase16.py")
    if not os.path.exists(module_path):
        with open(module_path, "w") as f:
            f.write('''\
"""Test functions for Phase 16 E2E verification."""

def double_values(daft_df):
    """Double all values in column 'v'."""
    import daft
    return daft_df.with_column("v", daft.col("v") * 2)
''')


if __name__ == "__main__":
    main()
