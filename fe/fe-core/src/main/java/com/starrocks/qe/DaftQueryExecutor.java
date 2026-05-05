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

package com.starrocks.qe;

import com.starrocks.catalog.Column;
import com.starrocks.common.Config;
import com.starrocks.common.DaftCoordinatorException;
import com.starrocks.coordinator.proto.ExecutionStats;
import com.starrocks.service.DaftCoordinatorClient;
import com.starrocks.service.DaftQueryResult;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes queries containing map_batches() by routing them to the Daft Coordinator
 * instead of the normal StarRocks execution engine.
 *
 * Supports nested map_batches merging: multiple nested map_batches calls are flattened
 * into a single DaftPlanRequest with multiple MapBatchesOp operations.
 */
public class DaftQueryExecutor {
    private static final Logger LOG = LogManager.getLogger(DaftQueryExecutor.class);

    /**
     * Holds the extracted plan info from a (possibly nested) map_batches query.
     */
    private static class DaftPlanInfo {
        final String sourceSQL;
        final List<String> functionNames;  // in execution order (innermost first)

        DaftPlanInfo(String sourceSQL, List<String> functionNames) {
            this.sourceSQL = sourceSQL;
            this.functionNames = functionNames;
        }
    }

    /**
     * Check if statement AST contains a map_batches function call in the select list.
     */
    public static boolean containsMapBatches(StatementBase stmt) {
        if (!(stmt instanceof QueryStatement)) {
            return false;
        }
        QueryRelation queryRelation = ((QueryStatement) stmt).getQueryRelation();
        if (!(queryRelation instanceof SelectRelation)) {
            return false;
        }
        SelectRelation selectRelation = (SelectRelation) queryRelation;
        return hasMapBatchesInSelect(selectRelation);
    }

    /**
     * Execute a query containing map_batches via Daft Coordinator.
     * Sends results back to client via ShowResultSet.
     */
    public static void execute(ConnectContext context, StatementBase stmt,
            StmtExecutor executor) throws Exception {
        QueryStatement queryStmt = (QueryStatement) stmt;
        SelectRelation selectRelation = (SelectRelation) queryStmt.getQueryRelation();

        // Extract plan info with nested map_batches merging
        DaftPlanInfo planInfo = extractDaftPlan(selectRelation);

        // Build Arrow Flight endpoint
        String arrowFlightEndpoint = buildArrowFlightEndpoint();

        LOG.info("DaftQueryExecutor: functions={}, sourceSQL={}, endpoint={}",
                planInfo.functionNames, planInfo.sourceSQL, arrowFlightEndpoint);

        // Use query ID as trace ID for request correlation
        String requestId = context.getQueryId().toString();

        DaftCoordinatorClient client = new DaftCoordinatorClient(
                Config.daft_coordinator_host, Config.daft_coordinator_port);
        try {
            DaftQueryResult result = client.submitDaftPlan(
                    requestId, planInfo.sourceSQL, arrowFlightEndpoint, planInfo.functionNames);

            // Log execution stats if available
            ExecutionStats stats = result.getStats();
            if (stats != null) {
                LOG.info("DaftQueryExecutor stats [{}]: total={}ms, fetch={}ms, execute={}ms, "
                                + "input_rows={}, output_rows={}, input_bytes={}",
                        requestId, stats.getTotalMs(), stats.getDataFetchMs(),
                        stats.getDaftExecuteMs(), stats.getInputRows(),
                        stats.getOutputRows(), stats.getInputBytes());
            }

            // Build ShowResultSet from results
            ShowResultSetMetaData.Builder metaBuilder = ShowResultSetMetaData.builder();
            for (String colName : result.getColumnNames()) {
                metaBuilder.addColumn(new Column(colName, TypeFactory.createDefaultCatalogString()));
            }
            ShowResultSet resultSet = new ShowResultSet(metaBuilder.build(), result.getRows());

            executor.sendShowResult(resultSet);
        } catch (DaftCoordinatorException e) {
            LOG.warn("DaftQueryExecutor failed", e);
            throw new DaftCoordinatorException("Daft Coordinator execution failed: " + e.getMessage(), e);
        } finally {
            client.close();
        }
    }

    /**
     * Build an EXPLAIN string for map_batches queries.
     */
    public static String buildExplainString(StatementBase stmt) {
        QueryStatement queryStmt = (QueryStatement) stmt;
        SelectRelation selectRelation = (SelectRelation) queryStmt.getQueryRelation();
        DaftPlanInfo planInfo = extractDaftPlan(selectRelation);

        StringBuilder sb = new StringBuilder();
        sb.append("DAFT COORDINATOR EXECUTION\n");
        sb.append("  Functions: ").append(planInfo.functionNames).append("\n");
        sb.append("  Operations: ").append(planInfo.functionNames.size())
                .append(" map_batches\n");
        sb.append("  Source SQL: ").append(planInfo.sourceSQL).append("\n");
        sb.append("  Coordinator: ").append(Config.daft_coordinator_host)
                .append(":").append(Config.daft_coordinator_port).append("\n");
        sb.append("  Timeout: ").append(Config.daft_coordinator_timeout_seconds).append("s\n");
        sb.append("  Max Result Rows: ").append(Config.daft_coordinator_max_result_rows).append("\n");
        try {
            sb.append("  Arrow Flight Endpoint: ").append(buildArrowFlightEndpoint()).append("\n");
        } catch (Exception e) {
            sb.append("  Arrow Flight Endpoint: <not configured>\n");
        }
        return sb.toString();
    }

    /**
     * Extract a DaftPlanInfo by recursively detecting nested map_batches calls.
     * Nested map_batches are flattened into a single plan with multiple operations.
     */
    private static DaftPlanInfo extractDaftPlan(SelectRelation selectRelation) {
        String functionName = extractFunctionNameFromSelect(selectRelation);
        Relation fromRelation = selectRelation.getRelation();

        // Recursive: check if FROM is also a map_batches query
        if (fromRelation instanceof SubqueryRelation) {
            QueryRelation innerQR = ((SubqueryRelation) fromRelation)
                    .getQueryStatement().getQueryRelation();
            if (innerQR instanceof SelectRelation && hasMapBatchesInSelect((SelectRelation) innerQR)) {
                DaftPlanInfo inner = extractDaftPlan((SelectRelation) innerQR);
                List<String> merged = new ArrayList<>(inner.functionNames);
                merged.add(functionName);  // outer executes after inner
                return new DaftPlanInfo(inner.sourceSQL, merged);
            }
        }

        // Base case: FROM clause is regular SQL
        return new DaftPlanInfo(extractSourceSQL(selectRelation), List.of(functionName));
    }

    /**
     * Check if a SelectRelation has a map_batches function call in its select list.
     */
    private static boolean hasMapBatchesInSelect(SelectRelation selectRelation) {
        SelectList selectList = selectRelation.getSelectList();
        if (selectList == null) {
            return false;
        }
        for (SelectListItem item : selectList.getItems()) {
            Expr expr = item.getExpr();
            if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr funcExpr = (FunctionCallExpr) expr;
                if (funcExpr.getFunctionName().equalsIgnoreCase("map_batches")) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String extractFunctionNameFromSelect(SelectRelation selectRelation) {
        for (SelectListItem item : selectRelation.getSelectList().getItems()) {
            Expr expr = item.getExpr();
            if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr funcExpr = (FunctionCallExpr) expr;
                if (funcExpr.getFunctionName().equalsIgnoreCase("map_batches")) {
                    Expr firstArg = funcExpr.getChild(0);
                    return ((StringLiteral) firstArg).getStringValue();
                }
            }
        }
        throw new IllegalStateException("map_batches function not found in select list");
    }

    private static String extractSourceSQL(SelectRelation selectRelation) {
        Relation fromRelation = selectRelation.getRelation();
        if (fromRelation == null) {
            throw new IllegalStateException("map_batches requires a FROM clause");
        }

        if (fromRelation instanceof SubqueryRelation) {
            SubqueryRelation subquery = (SubqueryRelation) fromRelation;
            return AstToSQLBuilder.toSQL(subquery.getQueryStatement());
        }

        String relationSQL = AstToSQLBuilder.toSQL(fromRelation);
        return "SELECT * FROM " + relationSQL;
    }

    private static String buildArrowFlightEndpoint() {
        int arrowFlightPort = Config.arrow_flight_port;
        if (arrowFlightPort <= 0) {
            throw new IllegalStateException(
                    "Arrow Flight port not configured. Set arrow_flight_port in FE config.");
        }
        String host = FrontendOptions.getLocalHostAddress();
        return "grpc+tcp://" + host + ":" + arrowFlightPort;
    }
}
