#!/usr/bin/env python3
"""E2E verification for Phase 21a: Cross-engine predicate pushdown + column pruning.

Tests:
  TC1: Outer WHERE filter pushdown
  TC2: Outer SELECT column pruning
  TC3: Combined WHERE + SELECT + LIMIT
  TC4: Regression — no wrapper (direct map_batches)
  TC5: EXPLAIN shows filter + projection info

Prerequisites:
  - Daft Coordinator running on the configured host:port
  - FE with enable_daft_coordinator=true
  - Arrow Flight SQL enabled

Usage:
  SR_HOST=8.218.233.134 python3.11 tests/verify_phase21a.py
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
    suffix = f" -- {detail}" if detail else ""
    print(f"  [{status}] {test_name}{suffix}")


def get_connection():
    return pymysql.connect(
        host=SR_HOST,
        port=SR_PORT,
        user="root",
        database="",
        autocommit=True,
    )


def execute_sql(conn, sql: str):
    """Execute SQL and return (cols, rows)."""
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
    return cols, rows


def register_identity_function():
    """Register an identity function for testing."""
    channel = grpc.insecure_channel(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
    stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
    try:
        resp = stub.RegisterFunction(
            coordinator_pb2.RegisterFunctionRequest(
                function_name="test_identity_p21a",
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


def cleanup_function():
    """Unregister test function."""
    try:
        channel = grpc.insecure_channel(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
        stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
        stub.UnregisterFunction(
            coordinator_pb2.UnregisterFunctionRequest(function_name="test_identity_p21a"),
            timeout=3,
        )
        channel.close()
    except Exception:
        pass


def main():
    global PASS, FAIL

    print("=" * 60)
    print("Phase 21a: Cross-engine Predicate Pushdown + Column Pruning")
    print("=" * 60)

    # Pre-check: register function
    print("\n--- Pre-check: Register identity function ---")
    ok, msg = register_identity_function()
    print(f"  Register: {'OK' if ok else 'FAIL'} -- {msg}")
    if not ok:
        print("  ABORT: Cannot register function")
        sys.exit(1)

    conn = get_connection()

    # Setup
    with conn.cursor() as cur:
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'true')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_host' = '{COORDINATOR_HOST}')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_port' = '{COORDINATOR_PORT}')")
        cur.execute("CREATE DATABASE IF NOT EXISTS test_dataframe")
        cur.execute("USE test_dataframe")

    # Create test table with data
    print("\n--- Setup: Create test table ---")
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_pushdown")
            cur.execute("""
                CREATE TABLE test_pushdown (
                    id BIGINT,
                    name VARCHAR(50),
                    val DOUBLE
                ) DISTRIBUTED BY HASH(id) BUCKETS 1
            """)
            cur.execute("""
                INSERT INTO test_pushdown VALUES
                (1, 'alice', 10.0),
                (2, 'bob', 20.0),
                (3, 'carol', 30.0),
                (4, 'dave', 40.0),
                (5, 'eve', 50.0),
                (6, 'frank', 60.0),
                (7, 'grace', 70.0),
                (8, 'heidi', 80.0),
                (9, 'ivan', 90.0),
                (10, 'judy', 100.0)
            """)
        print("  OK: test_pushdown created with 10 rows")
    except Exception as e:
        print(f"  FAIL: {e}")
        sys.exit(1)

    # ---- TC1: Outer WHERE filter pushdown ----
    print("\n--- TC1: Outer WHERE filter pushdown ---")
    try:
        cols, rows = execute_sql(
            conn,
            "SELECT * FROM (SELECT map_batches('test_identity_p21a') FROM test_pushdown) s WHERE id > 5"
        )
        # Should return rows with id > 5 (6,7,8,9,10)
        ok = len(rows) == 5
        # Verify all returned ids are > 5
        id_col_idx = cols.index("id") if "id" in cols else 0
        if ok:
            ids = [int(r[id_col_idx]) for r in rows]
            ok = all(i > 5 for i in ids)
        detail = f"rows={len(rows)}, cols={cols}, first_row={rows[0] if rows else 'none'}"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("WHERE id > 5 returns only matching rows", ok, detail)

    # ---- TC2: Outer SELECT column pruning ----
    print("\n--- TC2: Outer SELECT column pruning ---")
    try:
        cols, rows = execute_sql(
            conn,
            "SELECT id, name FROM (SELECT map_batches('test_identity_p21a') FROM test_pushdown) s"
        )
        # Should return only id and name columns, 10 rows
        ok = len(rows) == 10 and len(cols) == 2
        if ok:
            ok = "id" in cols and "name" in cols
        detail = f"rows={len(rows)}, cols={cols}"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("SELECT id, name returns only 2 columns", ok, detail)

    # ---- TC3: Combined WHERE + SELECT + LIMIT ----
    print("\n--- TC3: Combined WHERE + SELECT + LIMIT ---")
    try:
        cols, rows = execute_sql(
            conn,
            "SELECT id FROM (SELECT map_batches('test_identity_p21a') FROM test_pushdown) s "
            "WHERE id > 5 LIMIT 3"
        )
        # Should return <= 3 rows, only id column, all id > 5
        ok = len(rows) <= 3 and len(rows) > 0
        if ok:
            ok = len(cols) == 1 and cols[0] == "id"
        if ok:
            ids = [int(r[0]) for r in rows]
            ok = all(i > 5 for i in ids)
        detail = f"rows={len(rows)}, cols={cols}, data={rows}"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("WHERE + SELECT + LIMIT combined", ok, detail)

    # ---- TC4: Regression — no wrapper (direct map_batches) ----
    print("\n--- TC4: Regression -- direct map_batches (no wrapper) ---")
    try:
        cols, rows = execute_sql(
            conn,
            "SELECT map_batches('test_identity_p21a') FROM test_pushdown"
        )
        ok = len(rows) == 10
        detail = f"rows={len(rows)}, cols={cols}"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("Direct map_batches still works (regression)", ok, detail)

    # ---- TC5: EXPLAIN shows filter + projection info ----
    print("\n--- TC5: EXPLAIN with filter + projection ---")
    try:
        cols, rows = execute_sql(
            conn,
            "EXPLAIN SELECT id FROM (SELECT map_batches('test_identity_p21a') FROM test_pushdown) s "
            "WHERE id > 5"
        )
        explain_text = "\n".join(str(r[0]) for r in rows)
        ok = "filter" in explain_text.lower() and "projection" in explain_text.lower()
        detail = f"explain contains filter+projection: {ok}, text={explain_text[:200]}"
    except Exception as e:
        ok = False
        detail = str(e)
    log_result("EXPLAIN shows filter + projection", ok, detail)

    # Cleanup
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_pushdown")
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'false')")
    conn.close()
    cleanup_function()

    # Summary
    total = PASS + FAIL
    print(f"\n{'=' * 60}")
    print(f"Results: {PASS}/{total} passed, {FAIL}/{total} failed")
    print(f"{'=' * 60}")
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
