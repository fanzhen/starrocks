// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.service;

import com.starrocks.common.DaftCoordinatorException;
import com.starrocks.proto.coordinator.DaftCoordinatorGrpc;
import com.starrocks.proto.coordinator.StatusRequest;
import com.starrocks.proto.coordinator.StatusResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class DaftCoordinatorClient {
    private static final Logger LOG = LogManager.getLogger(DaftCoordinatorClient.class);
    private static final long DEADLINE_SECONDS = 5;

    private final String host;
    private final int port;
    private ManagedChannel channel;
    private DaftCoordinatorGrpc.DaftCoordinatorBlockingStub stub;

    public DaftCoordinatorClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private synchronized void ensureChannel() {
        if (channel == null || channel.isShutdown()) {
            channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
            stub = DaftCoordinatorGrpc.newBlockingStub(channel);
        }
    }

    public StatusResponse getStatus() throws DaftCoordinatorException {
        ensureChannel();
        try {
            return stub.withDeadlineAfter(DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .getStatus(StatusRequest.getDefaultInstance());
        } catch (StatusRuntimeException e) {
            LOG.warn("Failed to get Daft Coordinator status: {}", e.getStatus(), e);
            throw new DaftCoordinatorException(
                    "Failed to get Daft Coordinator status: " + e.getStatus(), e);
        }
    }

    public synchronized void close() {
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
            channel = null;
            stub = null;
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
