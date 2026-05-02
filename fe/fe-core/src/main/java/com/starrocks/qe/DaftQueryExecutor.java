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

import java.util.UUID;

/**
 * Executes queries containing map_batches() by routing them to the Daft Coordinator
 * instead of the normal StarRocks execution engine.
 */
public class DaftQueryExecutor {
    private static final Logger LOG = LogManager.getLogger(DaftQueryExecutor.class);

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

    /**
     * Execute a query containing map_batches via Daft Coordinator.
     * Sends results back to client via ShowResultSet.
     */
    public static void execute(ConnectContext context, StatementBase stmt,
            StmtExecutor executor) throws Exception {
        QueryStatement queryStmt = (QueryStatement) stmt;
        SelectRelation selectRelation = (SelectRelation) queryStmt.getQueryRelation();

        // 1. Extract function name from map_batches('name')
        String functionName = extractFunctionName(selectRelation);

        // 2. Extract source SQL from the FROM clause
        String sourceSQL = extractSourceSQL(selectRelation);

        // 3. Build Arrow Flight endpoint
        String arrowFlightEndpoint = buildArrowFlightEndpoint();

        LOG.info("DaftQueryExecutor: func={}, sourceSQL={}, endpoint={}",
                functionName, sourceSQL, arrowFlightEndpoint);

        // 4. Call Daft Coordinator
        DaftCoordinatorClient client = new DaftCoordinatorClient(
                Config.daft_coordinator_host, Config.daft_coordinator_port);
        try {
            String requestId = UUID.randomUUID().toString();
            DaftQueryResult result = client.submitDaftPlan(
                    requestId, sourceSQL, arrowFlightEndpoint, functionName);

            // 5. Build ShowResultSet from results
            ShowResultSetMetaData.Builder metaBuilder = ShowResultSetMetaData.builder();
            for (String colName : result.getColumnNames()) {
                metaBuilder.addColumn(new Column(colName, TypeFactory.createDefaultCatalogString()));
            }
            ShowResultSet resultSet = new ShowResultSet(metaBuilder.build(), result.getRows());

            // 6. Send to client
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
        String functionName = extractFunctionName(selectRelation);
        String sourceSQL = extractSourceSQL(selectRelation);

        StringBuilder sb = new StringBuilder();
        sb.append("DAFT COORDINATOR EXECUTION\n");
        sb.append("  Function: ").append(functionName).append("\n");
        sb.append("  Source SQL: ").append(sourceSQL).append("\n");
        sb.append("  Coordinator: ").append(Config.daft_coordinator_host)
                .append(":").append(Config.daft_coordinator_port).append("\n");
        try {
            sb.append("  Arrow Flight Endpoint: ").append(buildArrowFlightEndpoint()).append("\n");
        } catch (Exception e) {
            sb.append("  Arrow Flight Endpoint: <not configured>\n");
        }
        return sb.toString();
    }

    private static String extractFunctionName(SelectRelation selectRelation) {
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

        // If the FROM clause is a subquery, reconstruct it as a SELECT statement
        if (fromRelation instanceof SubqueryRelation) {
            SubqueryRelation subquery = (SubqueryRelation) fromRelation;
            return AstToSQLBuilder.toSQL(subquery.getQueryStatement());
        }

        // For table references and other relations, wrap in SELECT * FROM
        String relationSQL = AstToSQLBuilder.toSQL(fromRelation);
        return "SELECT * FROM " + relationSQL;
    }

    private static String buildArrowFlightEndpoint() {
        int arrowFlightPort = Config.arrow_flight_port;
        if (arrowFlightPort <= 0) {
            throw new IllegalStateException(
                    "Arrow Flight port not configured. Set arrow_flight_port in FE config.");
        }
        // Use the FE's own address so the coordinator can connect back via Arrow Flight SQL
        String host = FrontendOptions.getLocalHostAddress();
        return "grpc+tcp://" + host + ":" + arrowFlightPort;
    }
}
