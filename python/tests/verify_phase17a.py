#!/usr/bin/env python3
"""E2E verification for Phase 17a: FE gRPC Client + Config + Health Check.

Prerequisites:
  - StarRocks FE running (with Phase 17a code)
  - Coordinator NOT running (this script starts/stops it)

Usage:
  SR_HOST=47.239.57.232 python3.11 tests/verify_phase17a.py

Environment variables:
  SR_HOST        StarRocks host (default: 127.0.0.1)
  SR_PORT        StarRocks MySQL port (default: 9030)
  COORD_PORT     Coordinator gRPC port (default: 50051)
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

import pymysql

SR_HOST = os.environ.get("SR_HOST", "127.0.0.1")
SR_PORT = int(os.environ.get("SR_PORT", "9030"))
COORD_PORT = int(os.environ.get("COORD_PORT", "50051"))

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


def show_daft_coordinator(conn) -> dict:
    """Execute SHOW PROC '/daft_coordinator' and return as dict."""
    with conn.cursor() as cur:
        cur.execute("SHOW PROC '/daft_coordinator'")
        rows = cur.fetchall()
        return {row[0]: row[1] for row in rows}


def start_coordinator() -> subprocess.Popen:
    """Start the Coordinator as a subprocess on the remote or local host."""
    cmd = [
        sys.executable, "-m", "starrocks.coordinator",
        "--port", str(COORD_PORT),
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.path.join(os.path.dirname(__file__), ".."),
    )
    time.sleep(3)  # Wait for coordinator to start
    return proc


def stop_coordinator(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def main():
    global PASS, FAIL

    print("=" * 60)
    print("Phase 17a: FE gRPC Client + Config + Health Check")
    print("=" * 60)

    conn = get_connection()

    # Test 1: SHOW PROC when disabled (default)
    print("\n--- Test 1: SHOW PROC '/daft_coordinator' when disabled ---")
    result = show_daft_coordinator(conn)
    log_result("Status is DISABLED", result.get("Status") == "DISABLED",
               f"got={result.get('Status')}")
    log_result("Host present", "Host" in result, f"got={result}")
    log_result("Port present", "Port" in result, f"got={result}")

    # Test 2: Enable coordinator, but coordinator not running → DOWN
    print("\n--- Test 2: Enable coordinator (no coordinator running) ---")
    with conn.cursor() as cur:
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'true')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_host' = '{SR_HOST}')")
        cur.execute(f"ADMIN SET FRONTEND CONFIG ('daft_coordinator_port' = '{COORD_PORT}')")

    result = show_daft_coordinator(conn)
    log_result("Status is DOWN", result.get("Status") == "DOWN",
               f"got={result.get('Status')}")
    log_result("Error message present", "Error" in result,
               f"got={result.get('Error', 'N/A')}")

    # Test 3: Start coordinator, verify SERVING
    print("\n--- Test 3: Start coordinator, verify SERVING ---")
    coord_proc = None
    try:
        coord_proc = start_coordinator()
        result = show_daft_coordinator(conn)
        log_result("Status is SERVING", result.get("Status") == "SERVING",
                   f"got={result.get('Status')}")
        log_result("RegisteredFunctions present",
                   "RegisteredFunctions" in result,
                   f"got={result.get('RegisteredFunctions', 'N/A')}")
    except Exception as e:
        log_result("Start coordinator", False, str(e))
    finally:
        if coord_proc:
            stop_coordinator(coord_proc)

    # Test 4: Coordinator stopped → back to DOWN
    print("\n--- Test 4: After stopping coordinator → DOWN ---")
    time.sleep(2)  # Wait for coordinator to fully stop
    result = show_daft_coordinator(conn)
    log_result("Status is DOWN after stop", result.get("Status") == "DOWN",
               f"got={result.get('Status')}")

    # Test 5: Disable coordinator → back to DISABLED
    print("\n--- Test 5: Disable coordinator → DISABLED ---")
    with conn.cursor() as cur:
        cur.execute("ADMIN SET FRONTEND CONFIG ('enable_daft_coordinator' = 'false')")
    result = show_daft_coordinator(conn)
    log_result("Status is DISABLED", result.get("Status") == "DISABLED",
               f"got={result.get('Status')}")

    conn.close()

    # Summary
    total = PASS + FAIL
    print(f"\n{'=' * 60}")
    print(f"Results: {PASS}/{total} passed, {FAIL}/{total} failed")
    print(f"{'=' * 60}")
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
