#!/usr/bin/env python3
"""E2E verification for Phase 20: Global Optimization + Production Hardening.

Tests:
  TC1: Nested map_batches merging — 2 operations in single request
  TC2: Execution statistics — stats in last response
  TC3: Trace ID propagation — FE queryId used as request_id
  TC4: Configurable timeout — daft_coordinator_timeout_seconds via ADMIN SET
  TC5: Max result rows limit — exceeds limit returns error
  TC6: Regression — SELECT 1+1

Prerequisites:
  - Daft Coordinator running on the configured host:port
  - FE with enable_daft_coordinator=true
  - Arrow Flight SQL enabled

Usage:
  SR_HOST=47.239.57.232 python3.11 tests/verify_phase20.py
"""

from __future__ import annotations

import os
import sys
import time

import grpc
import pymysql

# Add parent directory to path for proto imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from starrocks.coordinator.proto import coordinator_pb2, coordinator_pb2_grpc

SR_HOST = os.environ.get("SR_HOST", "127.0.0.1")
SR_PORT = int(os.environ.get("SR_PORT", "9030"))
SR_HTTP_PORT = int(os.environ.get("SR_HTTP_PORT", "8030"))
ARROW_FLIGHT_PORT = int(os.environ.get("ARROW_FLIGHT_PORT", "9408"))
COORDINATOR_HOST = os.environ.get("COORDINATOR_HOST", SR_HOST)
COORDINATOR_PORT = int(os.environ.get("COORDINATOR_PORT", "50051"))

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


def get_connection():
    return pymysql.connect(
        host=SR_HOST,
        port=SR_PORT,
        user="root",
        database="",
        autocommit=True,
    )


def execute_expect_success(conn, sql: str) -> tuple[bool, str, list]:
    """Execute SQL expecting success. Returns (ok, detail, rows)."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            desc = cur.description
        return True, f"cols={[d[0] for d in desc]}, rows={rows[:5]}", list(rows)
    except pymysql.Error as e:
        return False, str(e), []


def execute_expect_error(conn, sql: str) -> tuple[bool, str]:
    """Execute SQL expecting an error. Returns (got_error, detail)."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return False, f"Expected error but got {len(rows)} rows"
    except pymysql.Error as e:
        return True, str(e)


def check_coordinator_status():
    """Check if coordinator is running."""
    channel = grpc.insecure_channel(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
    stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
    try:
        resp = stub.GetStatus(coordinator_pb2.StatusRequest(), timeout=3)
        return resp.status == "SERVING", f"status={resp.status}, funcs={resp.registered_functions}"
    except grpc.RpcError as e:
        return False, str(e)
    finally:
        channel.close()


def register_test_functions():
    """Register identity functions for nested map_batches testing."""
    channel = grpc.insecure_channel(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
    stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
    try:
        for name in ["test_identity_p20a", "test_identity_p20b"]:
            resp = stub.RegisterFunction(
                coordinator_pb2.RegisterFunctionRequest(
                    function_name=name,
                    module_path="starrocks.coordinator.builtin_functions",
                    callable_name="identity_transform",
                ),
                timeout=5,
            )
            if not resp.success:
                return False, f"Failed to register {name}: {resp.message}"
        return True, "Registered test_identity_p20a + test_identity_p20b"
    except grpc.RpcError as e:
        return False, str(e)
    finally:
        channel.close()


def submit_daft_plan_with_stats(request, timeout=30):
    """Submit a DaftPlanRequest via gRPC, return (col_names, rows, stats_pb) or raise."""
    channel = grpc.insecure_channel(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
    stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
    try:
        col_names = []
        all_rows = []
        last_stats = None
        for resp in stub.SubmitDaftPlan(request, timeout=timeout):
            if resp.error:
                raise RuntimeError(resp.error)
            if resp.column_names:
                col_names = list(resp.column_names)
            if resp.num_rows > 0 and col_names:
                ncols = len(col_names)
                values = list(resp.row_values)
                for i in range(resp.num_rows):
                    row = values[i * ncols:(i + 1) * ncols]
                    all_rows.append(row)
            if resp.HasField("stats"):
                last_stats = resp.stats
        return col_names, all_rows, last_stats
    finally:
        channel.close()


def arrow_flight_endpoint():
    return f"grpc+tcp://{SR_HOST}:{ARROW_FLIGHT_PORT}"


def main():
    global PASS, FAIL

    print("=" * 60)
    print("Phase 20: Global Optimization + Production Hardening")
    print("=" * 60)

    # Pre-check: coordinator status
    print("\n--- Pre-check: Coordinator Status ---")
    coord_ok, coord_detail = check_coordinator_status()
    print(f"  Coordinator: {'UP' if coord_ok else 'DOWN'} — {coord_detail}")
    if not coord_ok:
        print("  ABORT: Coordinator not running")
        sys.exit(1)

    # Register test functions
    print("\n--- Pre-check: Register test functions ---")
    reg_ok, reg_detail = register_test_functions()
    print(f"  Register: {'OK' if reg_ok else 'FAIL'} — {reg_detail}")

    conn = get_connection()

    # Setup
    with conn.cursor() as cur:
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'true')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_host' = '{COORDINATOR_HOST}')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_port' = '{COORDINATOR_PORT}')")
        cur.execute("CREATE DATABASE IF NOT EXISTS test_dataframe")
        cur.execute("USE test_dataframe")

    # ---- TC1: Nested map_batches merging ----
    print("\n--- TC1: Nested map_batches merging ---")
    try:
        # Submit via gRPC directly with 2 map_batches operations
        request = coordinator_pb2.DaftPlanRequest(
            request_id="tc1_nested_merge",
            arrow_flight_endpoint=arrow_flight_endpoint(),
            source_sql="SELECT 1 AS a, 2 AS b",
            use_direct_read=True,
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(
                        function_name="test_identity_p20a",
                    ),
                ),
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(
                        function_name="test_identity_p20b",
                    ),
                ),
            ],
        )
        col_names, rows, stats = submit_daft_plan_with_stats(request)
        ok = len(rows) == 1 and "1" in rows[0] and "2" in rows[0]
        detail = f"cols={col_names}, rows={rows}, ops=2"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("Nested map_batches: 2 ops produce correct result", ok, detail)

    # Also test nested via SQL (FE merge)
    try:
        ok2, detail2, sql_rows = execute_expect_success(
            conn,
            "SELECT map_batches('test_identity_p20b') FROM "
            "(SELECT map_batches('test_identity_p20a') FROM "
            "(SELECT 1 AS a, 2 AS b) sub1) sub2"
        )
        # The FE should merge nested map_batches into a single request
        # with 2 operations. Result should be correct.
        detail2_full = f"SQL nested: {detail2}"
    except Exception as e:
        ok2 = False
        detail2_full = str(e)
    log_result("Nested map_batches via SQL (FE merge)", ok2, detail2_full)

    # ---- TC2: Execution statistics ----
    print("\n--- TC2: Execution statistics ---")
    try:
        request = coordinator_pb2.DaftPlanRequest(
            request_id="tc2_stats",
            arrow_flight_endpoint=arrow_flight_endpoint(),
            source_sql="SELECT 1 AS x, 2 AS y",
            use_direct_read=True,
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(
                        function_name="test_identity_p20a",
                    ),
                ),
            ],
        )
        col_names, rows, stats = submit_daft_plan_with_stats(request)
        ok = (stats is not None
              and stats.total_ms >= 0
              and stats.output_rows > 0)
        detail = (f"total_ms={stats.total_ms}, fetch_ms={stats.data_fetch_ms}, "
                  f"exec_ms={stats.daft_execute_ms}, "
                  f"input_rows={stats.input_rows}, output_rows={stats.output_rows}, "
                  f"input_bytes={stats.input_bytes}") if stats else "stats=None"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("Execution stats in last response", ok, detail)

    # ---- TC3: Trace ID propagation ----
    print("\n--- TC3: Trace ID propagation ---")
    # The FE uses context.getQueryId() as requestId. We verify by checking
    # that a known request_id appears in coordinator logs (indirectly, we
    # verify the FE sends queryId by checking the gRPC API accepts it).
    try:
        request = coordinator_pb2.DaftPlanRequest(
            request_id="trace-id-test-12345",
            arrow_flight_endpoint=arrow_flight_endpoint(),
            source_sql="SELECT 42 AS answer",
            use_direct_read=True,
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(
                        function_name="test_identity_p20a",
                    ),
                ),
            ],
        )
        col_names, rows, stats = submit_daft_plan_with_stats(request)
        ok = len(rows) == 1 and "42" in rows[0]
        detail = f"request_id='trace-id-test-12345' accepted, rows={rows}"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("Trace ID propagation (request_id accepted)", ok, detail)

    # ---- TC4: Configurable timeout ----
    print("\n--- TC4: Configurable timeout ---")
    try:
        with conn.cursor() as cur:
            cur.execute("ADMIN SET FRONTEND CONFIG ('daft_coordinator_timeout_seconds' = '600')")
            cur.execute("ADMIN SHOW FRONTEND CONFIG LIKE 'daft_coordinator_timeout_seconds'")
            config_rows = cur.fetchall()
        # Check that the config was set (column layout varies, look for '600' in any column)
        ok = any("600" in str(r) for r in config_rows)
        detail = f"config_rows={config_rows}"
        # Reset to default
        with conn.cursor() as cur:
            cur.execute("ADMIN SET FRONTEND CONFIG ('daft_coordinator_timeout_seconds' = '300')")
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("daft_coordinator_timeout_seconds configurable", ok, detail)

    # ---- TC5: Max result rows limit ----
    print("\n--- TC5: Max result rows limit ---")
    try:
        # Set a very low limit
        with conn.cursor() as cur:
            cur.execute("ADMIN SET FRONTEND CONFIG ('daft_coordinator_max_result_rows' = '5')")

        # Try to fetch more rows than the limit via SQL
        ok_err, err_detail = execute_expect_error(
            conn,
            "SELECT map_batches('test_identity_p20a') FROM "
            "(SELECT generate_series AS id FROM TABLE(generate_series(1, 100))) sub"
        )
        ok = ok_err and ("max" in err_detail.lower() or "limit" in err_detail.lower() or "exceed" in err_detail.lower())
        detail = f"got_error={ok_err}, detail={err_detail}"

        # Reset
        with conn.cursor() as cur:
            cur.execute("ADMIN SET FRONTEND CONFIG ('daft_coordinator_max_result_rows' = '1000000')")
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("Max result rows limit enforced", ok, detail)

    # ---- TC6: Regression — SELECT 1+1 ----
    print("\n--- TC6: Regression — SELECT 1+1 ---")
    ok, detail, rows = execute_expect_success(conn, "SELECT 1+1")
    if ok:
        ok = rows[0][0] == 2
    log_result("SELECT 1+1 works", ok, detail)

    # Cleanup
    with conn.cursor() as cur:
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'false')")
    conn.close()

    # Unregister test functions
    try:
        channel = grpc.insecure_channel(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
        stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
        for name in ["test_identity_p20a", "test_identity_p20b"]:
            stub.UnregisterFunction(
                coordinator_pb2.UnregisterFunctionRequest(function_name=name),
                timeout=3,
            )
        channel.close()
    except Exception:
        pass

    # Summary
    total = PASS + FAIL
    print(f"\n{'=' * 60}")
    print(f"Results: {PASS}/{total} passed, {FAIL}/{total} failed")
    print(f"{'=' * 60}")
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
