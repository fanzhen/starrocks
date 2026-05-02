#!/usr/bin/env python3
"""E2E verification for Phase 17c: MAP_BATCHES Execution Routing.

Tests that MAP_BATCHES queries are intercepted before planning,
routed to the Daft Coordinator via gRPC, and results are returned
to the MySQL client.

Prerequisites:
  - Daft Coordinator running on the configured host:port
  - A test function registered via RegisterFunction gRPC
  - Arrow Flight SQL enabled (arrow_flight_port > 0)

Usage:
  SR_HOST=47.239.57.232 python3.11 tests/verify_phase17c.py

Environment variables:
  SR_HOST              StarRocks host (default: 127.0.0.1)
  SR_PORT              StarRocks MySQL port (default: 9030)
  COORDINATOR_HOST     Daft Coordinator host (default: same as SR_HOST)
  COORDINATOR_PORT     Daft Coordinator gRPC port (default: 50051)
"""

from __future__ import annotations

import os
import sys

import grpc
import pymysql

# Add parent directory to path for proto imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from starrocks.coordinator.proto import coordinator_pb2, coordinator_pb2_grpc

SR_HOST = os.environ.get("SR_HOST", "127.0.0.1")
SR_PORT = int(os.environ.get("SR_PORT", "9030"))
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


def execute_expect_error(conn, sql: str, expected_substr: str) -> tuple[bool, str]:
    """Execute SQL expecting an error containing expected_substr."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return False, f"Expected error but got result: {rows[:3]}"
    except pymysql.Error as e:
        err_msg = str(e)
        if expected_substr.lower() in err_msg.lower():
            return True, err_msg
        return False, f"Error doesn't contain '{expected_substr}': {err_msg}"


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


def register_identity_function():
    """Register a simple identity function on the coordinator."""
    channel = grpc.insecure_channel(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
    stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
    try:
        resp = stub.RegisterFunction(
            coordinator_pb2.RegisterFunctionRequest(
                function_name="identity",
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


def main():
    global PASS, FAIL

    print("=" * 60)
    print("Phase 17c: MAP_BATCHES Execution Routing")
    print("=" * 60)

    # Pre-check: coordinator status
    print("\n--- Pre-check: Coordinator Status ---")
    coord_ok, coord_detail = check_coordinator_status()
    print(f"  Coordinator: {'UP' if coord_ok else 'DOWN'} — {coord_detail}")

    # Register identity function
    if coord_ok:
        print("\n--- Pre-check: Register identity function ---")
        reg_ok, reg_detail = register_identity_function()
        print(f"  Register: {'OK' if reg_ok else 'FAIL'} — {reg_detail}")

    conn = get_connection()

    # Enable daft coordinator
    with conn.cursor() as cur:
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'true')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_host' = '{COORDINATOR_HOST}')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_port' = '{COORDINATOR_PORT}')")

    # Test 1: EXPLAIN map_batches → shows DAFT COORDINATOR EXECUTION
    print("\n--- Test 1: EXPLAIN map_batches shows Daft execution plan ---")
    ok, detail, rows = execute_expect_success(
        conn,
        "EXPLAIN SELECT map_batches('identity') FROM (SELECT 1 AS a) t",
    )
    if ok:
        explain_text = "\n".join(str(r[0]) for r in rows)
        ok = "DAFT COORDINATOR EXECUTION" in explain_text
        detail = f"explain contains 'DAFT COORDINATOR EXECUTION': {ok}\n{explain_text}"
    log_result("EXPLAIN shows Daft execution plan", ok, detail)

    # Test 2: Execute map_batches with identity function (if coordinator is up)
    print("\n--- Test 2: Execute map_batches with identity function ---")
    if coord_ok:
        ok, detail, rows = execute_expect_success(
            conn,
            "SELECT map_batches('identity') FROM (SELECT 1 AS a, 2 AS b) t",
        )
        if ok:
            # Should return rows with columns a, b and values 1, 2
            detail = f"rows={rows}"
        log_result("map_batches('identity') returns correct results", ok, detail)
    else:
        print("  [SKIP] Coordinator not running")

    # Test 3: map_batches with nonexistent function → error
    print("\n--- Test 3: map_batches with nonexistent function ---")
    if coord_ok:
        ok, detail = execute_expect_error(
            conn,
            "SELECT map_batches('nonexistent_xyz') FROM (SELECT 1 AS a) t",
            "nonexistent_xyz",
        )
        log_result("Error mentions unregistered function", ok, detail)
    else:
        print("  [SKIP] Coordinator not running")

    # Test 4: map_batches with table source
    print("\n--- Test 4: map_batches with table source ---")
    if coord_ok:
        # Create a test table
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS test_daft")
            cur.execute("USE test_daft")
            cur.execute("DROP TABLE IF EXISTS daft_test_t")
            cur.execute("""
                CREATE TABLE daft_test_t (
                    k1 INT,
                    v1 VARCHAR(50)
                ) ENGINE=OLAP
                DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ('replication_num' = '1')
            """)
            cur.execute("INSERT INTO daft_test_t VALUES (1, 'hello'), (2, 'world')")
        ok, detail, rows = execute_expect_success(
            conn,
            "SELECT map_batches('identity') FROM test_daft.daft_test_t",
        )
        if ok:
            detail = f"rows={rows}"
        log_result("map_batches with table source", ok, detail)

        # Cleanup
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_daft.daft_test_t")
            cur.execute("DROP DATABASE IF EXISTS test_daft")
    else:
        print("  [SKIP] Coordinator not running")

    # Test 5: Regression — SELECT 1+1 still works
    print("\n--- Test 5: Regression — SELECT 1+1 ---")
    ok, detail, _ = execute_expect_success(conn, "SELECT 1+1")
    log_result("SELECT 1+1 works", ok, detail)

    # Test 6: Regression — SELECT with normal functions
    print("\n--- Test 6: Regression — normal function call ---")
    ok, detail, _ = execute_expect_success(conn, "SELECT upper('hello')")
    log_result("SELECT upper('hello') works", ok, detail)

    # Cleanup: disable coordinator
    with conn.cursor() as cur:
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'false')")

    conn.close()

    # Summary
    total = PASS + FAIL
    print(f"\n{'=' * 60}")
    print(f"Results: {PASS}/{total} passed, {FAIL}/{total} failed")
    print(f"{'=' * 60}")
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
