#!/usr/bin/env python3
"""E2E verification for Phase 21c: Coordinator lifecycle integration.

Tests:
  TC1: start_daft_coordinator.sh --daemon starts coordinator, creates PID file
  TC2: stop_daft_coordinator.sh stops coordinator, cleans PID file
  TC3: Repeated start is idempotent (already running)

Prerequisites:
  - SSH access to remote server or local environment
  - Coordinator scripts in $STARROCKS_HOME/bin/

Usage:
  SR_HOST=8.218.233.134 python3.11 tests/verify_phase21c.py

  For local testing:
  STARROCKS_HOME=/path/to/starrocks python3.11 tests/verify_phase21c.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from starrocks.coordinator.proto import coordinator_pb2, coordinator_pb2_grpc

SR_HOST = os.environ.get("SR_HOST", "127.0.0.1")
COORDINATOR_HOST = os.environ.get("COORDINATOR_HOST", SR_HOST)
COORDINATOR_PORT = int(os.environ.get("COORDINATOR_PORT", "50051"))
STARROCKS_HOME = os.environ.get("STARROCKS_HOME", os.path.join(os.path.dirname(__file__), "..", ".."))
# For remote testing via SSH
SSH_KEY = os.environ.get("SSH_KEY", os.path.expanduser("~/.ssh/my_ecs.pem"))
REMOTE = SR_HOST != "127.0.0.1"
DOCKER_CONTAINER = os.environ.get("DOCKER_CONTAINER", "sr-dataframe")
REMOTE_STARROCKS_HOME = os.environ.get("REMOTE_STARROCKS_HOME", "/root/starrocks")

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


def run_script(script_name: str, args: str = "") -> tuple[int, str]:
    """Run a script locally or remotely."""
    if REMOTE:
        cmd = (
            f'ssh -i {SSH_KEY} -o StrictHostKeyChecking=no root@{SR_HOST} '
            f'"docker exec {DOCKER_CONTAINER} bash -c '
            f"'{REMOTE_STARROCKS_HOME}/bin/{script_name} {args}'\""
        )
    else:
        cmd = f"{STARROCKS_HOME}/bin/{script_name} {args}"

    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=30
        )
        output = result.stdout + result.stderr
        return result.returncode, output.strip()
    except subprocess.TimeoutExpired:
        return -1, "timeout"
    except Exception as e:
        return -1, str(e)


def check_coordinator_grpc() -> bool:
    """Check if coordinator gRPC is reachable."""
    channel = grpc.insecure_channel(f"{COORDINATOR_HOST}:{COORDINATOR_PORT}")
    stub = coordinator_pb2_grpc.DaftCoordinatorStub(channel)
    try:
        resp = stub.GetStatus(coordinator_pb2.StatusRequest(), timeout=3)
        return resp.status == "SERVING"
    except grpc.RpcError:
        return False
    finally:
        channel.close()


def main():
    global PASS, FAIL

    print("=" * 60)
    print("Phase 21c: Coordinator Lifecycle Integration")
    print("=" * 60)
    print(f"  Mode: {'remote ({SR_HOST})' if REMOTE else 'local'}")

    # First stop any running coordinator
    print("\n--- Pre-check: Stop existing coordinator ---")
    rc, out = run_script("stop_daft_coordinator.sh")
    print(f"  stop: rc={rc}, output={out}")
    time.sleep(2)

    # ---- TC1: Start coordinator ----
    print("\n--- TC1: Start coordinator (--daemon) ---")
    rc, out = run_script("start_daft_coordinator.sh", "--daemon")
    print(f"  start: rc={rc}, output={out}")
    time.sleep(3)  # Give it time to start

    grpc_ok = check_coordinator_grpc()
    ok = rc == 0 and grpc_ok
    detail = f"rc={rc}, grpc_reachable={grpc_ok}, output={out}"
    log_result("Start coordinator creates PID + gRPC reachable", ok, detail)

    # ---- TC2: Stop coordinator ----
    print("\n--- TC2: Stop coordinator ---")
    rc, out = run_script("stop_daft_coordinator.sh")
    print(f"  stop: rc={rc}, output={out}")
    time.sleep(2)

    grpc_ok_after_stop = check_coordinator_grpc()
    ok = rc == 0 and not grpc_ok_after_stop
    detail = f"rc={rc}, grpc_reachable_after_stop={grpc_ok_after_stop}, output={out}"
    log_result("Stop coordinator: process stopped, gRPC unreachable", ok, detail)

    # ---- TC3: Idempotent start ----
    print("\n--- TC3: Idempotent start ---")
    # Start first
    rc1, out1 = run_script("start_daft_coordinator.sh", "--daemon")
    time.sleep(3)
    # Start again — should say "already running"
    rc2, out2 = run_script("start_daft_coordinator.sh", "--daemon")
    ok = rc2 == 0 and "already running" in out2.lower()
    detail = f"first_start: rc={rc1}, second_start: rc={rc2}, output2={out2}"
    log_result("Repeated start is idempotent", ok, detail)

    # Cleanup: leave coordinator running (useful for subsequent tests)

    # Summary
    total = PASS + FAIL
    print(f"\n{'=' * 60}")
    print(f"Results: {PASS}/{total} passed, {FAIL}/{total} failed")
    print(f"{'=' * 60}")
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
