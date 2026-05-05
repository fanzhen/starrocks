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

# Stop the Daft Coordinator gRPC sidecar.

set -e

CURDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STARROCKS_HOME="$(cd "${CURDIR}/.." && pwd)"

PID_FILE="${STARROCKS_HOME}/log/daft_coordinator.pid"

if [ ! -f "${PID_FILE}" ]; then
    echo "Daft Coordinator PID file not found (${PID_FILE})"
    exit 0
fi

PID=$(cat "${PID_FILE}")

if kill -0 "${PID}" 2>/dev/null; then
    echo "Stopping Daft Coordinator (PID ${PID})..."
    kill -TERM "${PID}"
    # Wait up to 10 seconds for graceful shutdown
    for i in $(seq 1 10); do
        if ! kill -0 "${PID}" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    # Force kill if still running
    if kill -0 "${PID}" 2>/dev/null; then
        echo "Force killing Daft Coordinator (PID ${PID})..."
        kill -9 "${PID}" 2>/dev/null || true
    fi
    echo "Daft Coordinator stopped"
else
    echo "Daft Coordinator is not running (PID ${PID})"
fi

rm -f "${PID_FILE}"
