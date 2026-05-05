#!/usr/bin/env bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Start the Daft Coordinator gRPC sidecar for Daft-on-Ray execution.
#
# Environment variables:
#   DAFT_COORDINATOR_PORT  - gRPC listen port (default: 50051)
#   DAFT_PYTHON            - Python interpreter (default: python3)
#   DAFT_RAY_ADDRESS       - Ray cluster address (optional)
#   DAFT_MAX_WORKERS       - Max gRPC thread pool workers (default: 10)
#   DAFT_LOG_LEVEL         - Logging level (default: INFO)
#
# Usage:
#   bin/start_daft_coordinator.sh             # foreground
#   bin/start_daft_coordinator.sh --daemon    # background with PID file

set -e

CURDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STARROCKS_HOME="$(cd "${CURDIR}/.." && pwd)"

DAFT_COORDINATOR_PORT="${DAFT_COORDINATOR_PORT:-50051}"
DAFT_PYTHON="${DAFT_PYTHON:-python3}"
DAFT_MAX_WORKERS="${DAFT_MAX_WORKERS:-10}"
DAFT_LOG_LEVEL="${DAFT_LOG_LEVEL:-INFO}"

LOG_DIR="${STARROCKS_HOME}/log"
PID_FILE="${STARROCKS_HOME}/log/daft_coordinator.pid"
LOG_FILE="${LOG_DIR}/daft_coordinator.out"

mkdir -p "${LOG_DIR}"

# Build command
CMD="${DAFT_PYTHON} -m starrocks.coordinator.cli --port ${DAFT_COORDINATOR_PORT} --max-workers ${DAFT_MAX_WORKERS} --log-level ${DAFT_LOG_LEVEL}"
if [ -n "${DAFT_RAY_ADDRESS}" ]; then
    CMD="${CMD} --ray-address ${DAFT_RAY_ADDRESS}"
fi

# Ensure PYTHONPATH includes the python directory
export PYTHONPATH="${STARROCKS_HOME}/python:${PYTHONPATH:-}"

# Check if already running
if [ -f "${PID_FILE}" ]; then
    OLD_PID=$(cat "${PID_FILE}")
    if kill -0 "${OLD_PID}" 2>/dev/null; then
        echo "Daft Coordinator is already running (PID ${OLD_PID})"
        exit 0
    else
        echo "Removing stale PID file"
        rm -f "${PID_FILE}"
    fi
fi

if [ "$1" = "--daemon" ]; then
    echo "Starting Daft Coordinator in daemon mode on port ${DAFT_COORDINATOR_PORT}..."
    nohup ${CMD} >> "${LOG_FILE}" 2>&1 &
    echo $! > "${PID_FILE}"
    echo "Daft Coordinator started (PID $(cat "${PID_FILE}"), log: ${LOG_FILE})"
else
    echo "Starting Daft Coordinator on port ${DAFT_COORDINATOR_PORT}..."
    exec ${CMD}
fi
