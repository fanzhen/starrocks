#!/usr/bin/env python3
"""E2E verification for Phase 18: Daft Function Registration via ADMIN Commands.

Tests that ADMIN CREATE/DROP/SHOW DAFT FUNCTION commands work end-to-end
through FE SQL → gRPC → Coordinator.

Prerequisites:
  - Daft Coordinator running on the configured host:port
  - FE with enable_daft_coordinator=true

Usage:
  SR_HOST=47.239.57.232 python3.11 tests/verify_phase18.py

Environment variables:
  SR_HOST              StarRocks host (default: 127.0.0.1)
  SR_PORT              StarRocks MySQL port (default: 9030)
  COORDINATOR_HOST     Daft Coordinator host (default: same as SR_HOST)
  COORDINATOR_PORT     Daft Coordinator gRPC port (default: 50051)
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


def execute_ok(conn, sql: str) -> tuple[bool, str]:
    """Execute SQL expecting OK (no result set needed)."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        return True, "OK"
    except pymysql.Error as e:
        return False, str(e)


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
    print("Phase 18: Daft Function Registration via ADMIN Commands")
    print("=" * 60)

    # Pre-check: coordinator status
    print("\n--- Pre-check: Coordinator Status ---")
    coord_ok, coord_detail = check_coordinator_status()
    print(f"  Coordinator: {'UP' if coord_ok else 'DOWN'} — {coord_detail}")
    if not coord_ok:
        print("  ABORT: Coordinator not running")
        sys.exit(1)

    conn = get_connection()

    # Enable daft coordinator
    with conn.cursor() as cur:
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'true')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_host' = '{COORDINATOR_HOST}')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_port' = '{COORDINATOR_PORT}')")

    # Test 1: ADMIN CREATE DAFT FUNCTION
    print("\n--- Test 1: ADMIN CREATE DAFT FUNCTION ---")
    ok, detail = execute_ok(
        conn,
        "ADMIN CREATE DAFT FUNCTION 'test_identity' "
        "PROPERTIES('module_path'='starrocks.coordinator.builtin_functions', "
        "'callable_name'='identity_transform')",
    )
    log_result("ADMIN CREATE DAFT FUNCTION 'test_identity'", ok, detail)

    # Test 2: ADMIN SHOW DAFT FUNCTIONS — should list test_identity
    print("\n--- Test 2: ADMIN SHOW DAFT FUNCTIONS ---")
    ok, detail, rows = execute_expect_success(conn, "ADMIN SHOW DAFT FUNCTIONS")
    if ok:
        found = any("test_identity" in str(r) for r in rows)
        detail = f"found test_identity: {found}, rows={rows}"
        ok = found
    log_result("SHOW lists test_identity", ok, detail)

    # Test 3: Execute map_batches with registered function
    print("\n--- Test 3: map_batches with test_identity ---")
    ok, detail, rows = execute_expect_success(
        conn,
        "SELECT map_batches('test_identity') FROM (SELECT 1 AS a, 2 AS b) t",
    )
    if ok:
        detail = f"rows={rows}"
    log_result("map_batches('test_identity') returns results", ok, detail)

    # Test 4: ADMIN DROP DAFT FUNCTION
    print("\n--- Test 4: ADMIN DROP DAFT FUNCTION ---")
    ok, detail = execute_ok(conn, "ADMIN DROP DAFT FUNCTION 'test_identity'")
    log_result("ADMIN DROP DAFT FUNCTION 'test_identity'", ok, detail)

    # Test 5: ADMIN SHOW DAFT FUNCTIONS — should be empty (or not contain test_identity)
    print("\n--- Test 5: SHOW after DROP ---")
    ok, detail, rows = execute_expect_success(conn, "ADMIN SHOW DAFT FUNCTIONS")
    if ok:
        found = any("test_identity" in str(r) for r in rows)
        detail = f"test_identity absent: {not found}, rows={rows}"
        ok = not found
    log_result("SHOW no longer lists test_identity", ok, detail)

    # Test 6: map_batches with dropped function → error
    print("\n--- Test 6: map_batches after DROP → error ---")
    ok, detail = execute_expect_error(
        conn,
        "SELECT map_batches('test_identity') FROM (SELECT 1 AS a) t",
        "test_identity",
    )
    log_result("Error mentions unregistered function", ok, detail)

    # Test 7: Regression — SELECT 1+1 still works
    print("\n--- Test 7: Regression — SELECT 1+1 ---")
    ok, detail, _ = execute_expect_success(conn, "SELECT 1+1")
    log_result("SELECT 1+1 works", ok, detail)

    # Cleanup
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
