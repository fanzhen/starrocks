#!/usr/bin/env python3
"""E2E verification for Phase 19: Direct Data Channel + Stream Load Write-back.

Tests:
  TC1: Daft native read via daft.read_sql() (direct read flag)
  TC2: Legacy path still works (use_direct_read=false)
  TC3: Stream Load write-back via proto API
  TC4: Full pipeline — filter + map_batches with direct read
  TC5: Regression — SELECT 1+1
  TC6: Large data (10K rows) — verify no OOM on Coordinator

Prerequisites:
  - Daft Coordinator running on the configured host:port
  - FE with enable_daft_coordinator=true
  - Arrow Flight SQL enabled

Usage:
  SR_HOST=47.239.57.232 python3.11 tests/verify_phase19.py
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


def register_identity_function():
    """Register a simple identity function on the coordinator."""
    channel = grpc.insecure_channel(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
    stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
    try:
        resp = stub.RegisterFunction(
            coordinator_pb2.RegisterFunctionRequest(
                function_name="test_identity_p19",
                module_path="starrocks.coordinator.builtin_functions",
                callable_name="identity_transform",
            ),
            timeout=5,
        )
        return resp.success, resp.message
    except grpc.RpcError as e:
        return False, str(e)
    finally:
        channel.close()


def submit_daft_plan(request, timeout=30):
    """Submit a DaftPlanRequest via gRPC, return (col_names, rows) or raise."""
    channel = grpc.insecure_channel(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
    stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
    try:
        col_names = []
        all_rows = []
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
        return col_names, all_rows
    finally:
        channel.close()


def arrow_flight_endpoint():
    """Build the Arrow Flight SQL endpoint URL."""
    return f"grpc+tcp://{SR_HOST}:{ARROW_FLIGHT_PORT}"


def main():
    global PASS, FAIL

    print("=" * 60)
    print("Phase 19: Direct Data Channel + Stream Load Write-back")
    print("=" * 60)

    # Pre-check: coordinator status
    print("\n--- Pre-check: Coordinator Status ---")
    coord_ok, coord_detail = check_coordinator_status()
    print(f"  Coordinator: {'UP' if coord_ok else 'DOWN'} — {coord_detail}")
    if not coord_ok:
        print("  ABORT: Coordinator not running")
        sys.exit(1)

    # Register identity function
    print("\n--- Pre-check: Register identity function ---")
    reg_ok, reg_detail = register_identity_function()
    print(f"  Register: {'OK' if reg_ok else 'FAIL'} — {reg_detail}")

    conn = get_connection()

    # Setup: enable coordinator + create test data
    with conn.cursor() as cur:
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'true')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_host' = '{COORDINATOR_HOST}')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_port' = '{COORDINATOR_PORT}')")
        cur.execute("CREATE DATABASE IF NOT EXISTS test_dataframe")
        cur.execute("USE test_dataframe")

    # ---- TC1: Direct read via daft.read_sql() (gRPC API) ----
    print("\n--- TC1: Direct read via daft.read_sql() ---")
    try:
        request = coordinator_pb2.DaftPlanRequest(
            request_id="tc1_direct_read",
            arrow_flight_endpoint=arrow_flight_endpoint(),
            source_sql="SELECT 1 AS a, 2 AS b",
            use_direct_read=True,
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(
                        function_name="test_identity_p19",
                    ),
                ),
            ],
        )
        col_names, rows = submit_daft_plan(request)
        ok = len(rows) == 1 and "1" in rows[0] and "2" in rows[0]
        detail = f"cols={col_names}, rows={rows}"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("Direct read + map_batches returns correct data", ok, detail)

    # ---- TC2: Legacy path (use_direct_read=false) ----
    print("\n--- TC2: Legacy path (use_direct_read=false) ---")
    try:
        request = coordinator_pb2.DaftPlanRequest(
            request_id="tc2_legacy_path",
            arrow_flight_endpoint=arrow_flight_endpoint(),
            source_sql="SELECT 10 AS x, 20 AS y",
            use_direct_read=False,
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(
                        function_name="test_identity_p19",
                    ),
                ),
            ],
        )
        col_names, rows = submit_daft_plan(request)
        ok = len(rows) == 1 and "10" in rows[0] and "20" in rows[0]
        detail = f"cols={col_names}, rows={rows}"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("Legacy path returns correct data", ok, detail)

    # ---- TC3: Stream Load write-back via proto API ----
    print("\n--- TC3: Stream Load write-back ---")
    # Create target table
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_dataframe.write_back_target")
        cur.execute("""
            CREATE TABLE test_dataframe.write_back_target (
                id INT,
                val VARCHAR(50)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """)
    time.sleep(1)

    try:
        request = coordinator_pb2.DaftPlanRequest(
            request_id="tc3_write_back",
            arrow_flight_endpoint=arrow_flight_endpoint(),
            source_sql="SELECT 1 AS id, 'hello' AS val UNION ALL SELECT 2, 'world'",
            use_direct_read=False,
            operations=[
                coordinator_pb2.DaftOperation(
                    write_back=coordinator_pb2.WriteBackOp(
                        database="test_dataframe",
                        table_name="write_back_target",
                        fe_host=SR_HOST,
                        fe_http_port=SR_HTTP_PORT,
                    ),
                ),
            ],
        )
        col_names, rows = submit_daft_plan(request)
        # write_back returns a status row
        ok_write = len(rows) == 1 and "OK" in str(rows)
        detail_write = f"write_back result: cols={col_names}, rows={rows}"

        # Verify data in target table
        time.sleep(2)
        ok_read, detail_read, read_rows = execute_expect_success(
            conn, "SELECT * FROM test_dataframe.write_back_target ORDER BY id"
        )
        ok = ok_write and ok_read and len(read_rows) == 2
        detail = f"{detail_write}; verify: {detail_read}"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("Write-back: source → Stream Load → verify", ok, detail)

    # ---- TC4: Full pipeline — source table + filter + map_batches with direct read ----
    print("\n--- TC4: Pipeline with direct read + filter ---")
    # Create a source table with 100 rows
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_dataframe.pipeline_src")
        cur.execute("""
            CREATE TABLE test_dataframe.pipeline_src (
                id INT,
                val INT
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """)
        # Insert 100 rows
        values = ", ".join(f"({i}, {i * 10})" for i in range(1, 101))
        cur.execute(f"INSERT INTO test_dataframe.pipeline_src VALUES {values}")
    time.sleep(2)

    try:
        request = coordinator_pb2.DaftPlanRequest(
            request_id="tc4_pipeline",
            arrow_flight_endpoint=arrow_flight_endpoint(),
            source_sql="SELECT * FROM test_dataframe.pipeline_src WHERE id > 50",
            use_direct_read=True,
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(
                        function_name="test_identity_p19",
                    ),
                ),
            ],
        )
        col_names, rows = submit_daft_plan(request)
        ok = len(rows) == 50
        detail = f"expected 50 rows, got {len(rows)}, cols={col_names}"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("Pipeline: filter(id>50) + identity → 50 rows", ok, detail)

    # ---- TC5: Regression — SELECT 1+1 ----
    print("\n--- TC5: Regression — SELECT 1+1 ---")
    ok, detail, rows = execute_expect_success(conn, "SELECT 1+1")
    if ok:
        ok = rows[0][0] == 2
    log_result("SELECT 1+1 works", ok, detail)

    # ---- TC6: Large data (10K rows) via direct read ----
    print("\n--- TC6: Large data (10K rows) direct read ---")
    try:
        # Use generate_series to produce 10K rows without creating a table
        request = coordinator_pb2.DaftPlanRequest(
            request_id="tc6_large_data",
            arrow_flight_endpoint=arrow_flight_endpoint(),
            source_sql="SELECT generate_series AS id FROM TABLE(generate_series(1, 10000))",
            use_direct_read=True,
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(
                        function_name="test_identity_p19",
                    ),
                ),
            ],
        )
        col_names, rows = submit_daft_plan(request, timeout=60)
        ok = len(rows) == 10000
        detail = f"expected 10000 rows, got {len(rows)}"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("10K rows direct read + identity", ok, detail)

    # Cleanup
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_dataframe.write_back_target")
        cur.execute("DROP TABLE IF EXISTS test_dataframe.pipeline_src")
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'false')")
    conn.close()

    # Unregister test function
    try:
        channel = grpc.insecure_channel(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
        stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
        stub.UnregisterFunction(
            coordinator_pb2.UnregisterFunctionRequest(function_name="test_identity_p19"),
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
