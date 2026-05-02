#!/usr/bin/env python3
"""E2E verification for Phase 17b: MAP_BATCHES Syntax + Semantic Analysis.

Tests that MAP_BATCHES('func_name', ...) is intercepted in ExpressionAnalyzer
with proper validation (config gate, argument checks).

Usage:
  SR_HOST=47.239.57.232 python3.11 tests/verify_phase17b.py

Environment variables:
  SR_HOST        StarRocks host (default: 127.0.0.1)
  SR_PORT        StarRocks MySQL port (default: 9030)
"""

from __future__ import annotations

import os
import sys

import pymysql

SR_HOST = os.environ.get("SR_HOST", "127.0.0.1")
SR_PORT = int(os.environ.get("SR_PORT", "9030"))

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


def execute_expect_success(conn, sql: str) -> tuple[bool, str]:
    """Execute SQL expecting success."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return True, str(rows[:5])
    except pymysql.Error as e:
        return False, str(e)


def main():
    global PASS, FAIL

    print("=" * 60)
    print("Phase 17b: MAP_BATCHES Syntax + Semantic Analysis")
    print("=" * 60)

    conn = get_connection()

    # Test 1: EXPLAIN map_batches with enabled config → should succeed
    print("\n--- Test 1: EXPLAIN map_batches with enabled config ---")
    with conn.cursor() as cur:
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'true')")
    ok, detail = execute_expect_success(
        conn,
        "EXPLAIN SELECT map_batches('my_func') FROM (SELECT 1 AS a) t",
    )
    log_result("EXPLAIN map_batches succeeds when enabled", ok, detail)

    # Test 2: EXPLAIN map_batches with disabled config → SemanticException
    print("\n--- Test 2: EXPLAIN map_batches with disabled config ---")
    with conn.cursor() as cur:
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'false')")
    ok, detail = execute_expect_error(
        conn,
        "EXPLAIN SELECT map_batches('my_func') FROM (SELECT 1 AS a) t",
        "enable_daft_coordinator",
    )
    log_result("Error mentions enable_daft_coordinator", ok, detail)

    # Test 3: map_batches() with no arguments → error
    print("\n--- Test 3: map_batches() with no arguments ---")
    with conn.cursor() as cur:
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'true')")
    ok, detail = execute_expect_error(
        conn,
        "EXPLAIN SELECT map_batches() FROM (SELECT 1 AS a) t",
        "at least 1 argument",
    )
    log_result("Error about missing arguments", ok, detail)

    # Test 4: map_batches(123) with non-string first arg → error
    print("\n--- Test 4: map_batches(123) with non-string first arg ---")
    ok, detail = execute_expect_error(
        conn,
        "EXPLAIN SELECT map_batches(123) FROM (SELECT 1 AS a) t",
        "string literal",
    )
    log_result("Error about non-string first arg", ok, detail)

    # Test 5: Regression — SELECT 1+1 still works
    print("\n--- Test 5: Regression — SELECT 1+1 ---")
    ok, detail = execute_expect_success(conn, "SELECT 1+1")
    log_result("SELECT 1+1 works", ok, detail)

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
