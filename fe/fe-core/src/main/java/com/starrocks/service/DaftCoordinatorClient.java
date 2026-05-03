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
import com.starrocks.coordinator.proto.DaftCoordinatorGrpc;
import com.starrocks.coordinator.proto.DaftFunctionInfo;
import com.starrocks.coordinator.proto.DaftOperation;
import com.starrocks.coordinator.proto.DaftPlanRequest;
import com.starrocks.coordinator.proto.DaftPlanResponse;
import com.starrocks.coordinator.proto.ListFunctionsRequest;
import com.starrocks.coordinator.proto.ListFunctionsResponse;
import com.starrocks.coordinator.proto.MapBatchesOp;
import com.starrocks.coordinator.proto.RegisterFunctionRequest;
import com.starrocks.coordinator.proto.RegisterFunctionResponse;
import com.starrocks.coordinator.proto.StatusRequest;
import com.starrocks.coordinator.proto.StatusResponse;
import com.starrocks.coordinator.proto.UnregisterFunctionRequest;
import com.starrocks.coordinator.proto.UnregisterFunctionResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DaftCoordinatorClient {
    private static final Logger LOG = LogManager.getLogger(DaftCoordinatorClient.class);
    private static final long DEADLINE_SECONDS = 5;
    private static final long SUBMIT_DEADLINE_SECONDS = 300;

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

    /**
     * Submit a Daft plan to the coordinator and collect text-format results.
     *
     * @param requestId unique request identifier
     * @param sourceSQL the SQL query for the coordinator to pull source data
     * @param arrowFlightEndpoint Arrow Flight endpoint URL (grpc+tcp://host:port)
     * @param functionName the registered function name for map_batches
     * @return DaftQueryResult containing column names and rows
     */
    public DaftQueryResult submitDaftPlan(String requestId, String sourceSQL,
            String arrowFlightEndpoint, String functionName)
            throws DaftCoordinatorException {
        ensureChannel();

        DaftPlanRequest request = DaftPlanRequest.newBuilder()
                .setRequestId(requestId)
                .setSourceSql(sourceSQL)
                .setArrowFlightEndpoint(arrowFlightEndpoint)
                .setUseDirectRead(true)
                .addOperations(DaftOperation.newBuilder()
                        .setMapBatches(MapBatchesOp.newBuilder()
                                .setFunctionName(functionName)
                                .build())
                        .build())
                .build();

        try {
            Iterator<DaftPlanResponse> responses = stub
                    .withDeadlineAfter(SUBMIT_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .submitDaftPlan(request);

            List<String> columnNames = null;
            List<List<String>> rows = new ArrayList<>();

            while (responses.hasNext()) {
                DaftPlanResponse resp = responses.next();

                // Check for error (oneof result case)
                if (resp.getResultCase() == DaftPlanResponse.ResultCase.ERROR) {
                    throw new DaftCoordinatorException(
                            "Daft Coordinator error: " + resp.getError());
                }

                // Collect column names from first response
                if (columnNames == null && resp.getColumnNamesCount() > 0) {
                    columnNames = new ArrayList<>(resp.getColumnNamesList());
                }

                // Collect rows — row_values is flattened (num_rows * num_columns)
                int numRows = resp.getNumRows();
                int numCols = columnNames != null ? columnNames.size() : 0;
                List<String> flatValues = resp.getRowValuesList();

                for (int r = 0; r < numRows; r++) {
                    List<String> row = new ArrayList<>(numCols);
                    for (int c = 0; c < numCols; c++) {
                        int idx = r * numCols + c;
                        row.add(idx < flatValues.size() ? flatValues.get(idx) : "");
                    }
                    rows.add(row);
                }
            }

            if (columnNames == null) {
                columnNames = new ArrayList<>();
            }
            return new DaftQueryResult(columnNames, rows);
        } catch (DaftCoordinatorException e) {
            throw e;
        } catch (StatusRuntimeException e) {
            LOG.warn("submitDaftPlan failed: {}", e.getStatus(), e);
            throw new DaftCoordinatorException(
                    "Daft Coordinator unavailable: " + e.getStatus().getDescription(), e);
        } catch (Exception e) {
            LOG.warn("submitDaftPlan unexpected error", e);
            throw new DaftCoordinatorException(
                    "Daft Coordinator error: " + e.getMessage(), e);
        }
    }

    public void registerFunction(String name, String modulePath, String callableName)
            throws DaftCoordinatorException {
        ensureChannel();
        try {
            RegisterFunctionResponse resp = stub
                    .withDeadlineAfter(DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .registerFunction(RegisterFunctionRequest.newBuilder()
                            .setFunctionName(name)
                            .setModulePath(modulePath)
                            .setCallableName(callableName)
                            .build());
            if (!resp.getSuccess()) {
                throw new DaftCoordinatorException(
                        "Failed to register function: " + resp.getMessage());
            }
        } catch (DaftCoordinatorException e) {
            throw e;
        } catch (StatusRuntimeException e) {
            LOG.warn("registerFunction failed: {}", e.getStatus(), e);
            throw new DaftCoordinatorException(
                    "Daft Coordinator unavailable: " + e.getStatus().getDescription(), e);
        }
    }

    public void unregisterFunction(String name) throws DaftCoordinatorException {
        ensureChannel();
        try {
            UnregisterFunctionResponse resp = stub
                    .withDeadlineAfter(DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .unregisterFunction(UnregisterFunctionRequest.newBuilder()
                            .setFunctionName(name)
                            .build());
            if (!resp.getSuccess()) {
                throw new DaftCoordinatorException(
                        "Failed to unregister function: " + resp.getMessage());
            }
        } catch (DaftCoordinatorException e) {
            throw e;
        } catch (StatusRuntimeException e) {
            LOG.warn("unregisterFunction failed: {}", e.getStatus(), e);
            throw new DaftCoordinatorException(
                    "Daft Coordinator unavailable: " + e.getStatus().getDescription(), e);
        }
    }

    public List<DaftFunctionInfo> listFunctions() throws DaftCoordinatorException {
        ensureChannel();
        try {
            ListFunctionsResponse resp = stub
                    .withDeadlineAfter(DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .listFunctions(ListFunctionsRequest.getDefaultInstance());
            return resp.getFunctionsList();
        } catch (StatusRuntimeException e) {
            LOG.warn("listFunctions failed: {}", e.getStatus(), e);
            throw new DaftCoordinatorException(
                    "Daft Coordinator unavailable: " + e.getStatus().getDescription(), e);
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
