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
import com.starrocks.common.FeConstants;
import com.starrocks.coordinator.proto.DaftPlanResponse;
import com.starrocks.coordinator.proto.ExecutionStats;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlEofPacket;
import com.starrocks.mysql.MysqlSerializer;
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
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Executes queries containing map_batches() by routing them to the Daft Coordinator
 * instead of the normal StarRocks execution engine.
 *
 * Supports nested map_batches merging: multiple nested map_batches calls are flattened
 * into a single DaftPlanRequest with multiple MapBatchesOp operations.
 *
 * Supports cross-engine pushdown: outer WHERE, SELECT, and LIMIT clauses wrapping
 * map_batches subqueries are extracted as FilterOp, ProjectionOp, and LimitOp and
 * sent to the Daft Coordinator for execution.
 */
public class DaftQueryExecutor {
    private static final Logger LOG = LogManager.getLogger(DaftQueryExecutor.class);

    /**
     * Post-processing operation extracted from the outer query wrapping map_batches.
     */
    public static class PostOp {
        public enum Type { FILTER, PROJECTION, LIMIT }

        public final Type type;
        public final String filterExprJson;   // for FILTER
        public final List<String> columns;    // for PROJECTION
        public final long limitCount;         // for LIMIT

        private PostOp(Type type, String filterExprJson, List<String> columns, long limitCount) {
            this.type = type;
            this.filterExprJson = filterExprJson;
            this.columns = columns;
            this.limitCount = limitCount;
        }

        static PostOp filter(String exprJson) {
            return new PostOp(Type.FILTER, exprJson, null, 0);
        }

        static PostOp projection(List<String> columns) {
            return new PostOp(Type.PROJECTION, null, columns, 0);
        }

        static PostOp limit(long count) {
            return new PostOp(Type.LIMIT, null, null, count);
        }
    }

    /**
     * Holds the extracted plan info from a (possibly nested) map_batches query.
     */
    private static class DaftPlanInfo {
        final String sourceSQL;
        final List<String> functionNames;  // in execution order (innermost first)
        final List<PostOp> postOps;        // post-processing ops (filter, projection, limit)

        DaftPlanInfo(String sourceSQL, List<String> functionNames, List<PostOp> postOps) {
            this.sourceSQL = sourceSQL;
            this.functionNames = functionNames;
            this.postOps = postOps;
        }

        DaftPlanInfo(String sourceSQL, List<String> functionNames) {
            this(sourceSQL, functionNames, new ArrayList<>());
        }
    }

    /**
     * Check if statement AST contains a map_batches function call in the select list,
     * either directly or wrapped in an outer SELECT/WHERE/LIMIT.
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

        // Direct: SELECT map_batches('f') FROM ...
        if (hasMapBatchesInSelect(selectRelation)) {
            return true;
        }

        // Wrapped: SELECT ... FROM (SELECT map_batches('f') FROM ...) sub [WHERE ...] [LIMIT ...]
        return isMapBatchesWrapper(selectRelation);
    }

    /**
     * Execute a query containing map_batches via Daft Coordinator.
     *
     * Uses streaming mode: each gRPC response batch is immediately sent to
     * the MySQL client as MySQL row packets, so FE only holds one batch
     * (~4096 rows) in memory at a time instead of the full result set.
     */
    public static void execute(ConnectContext context, StatementBase stmt,
            StmtExecutor executor) throws Exception {
        QueryStatement queryStmt = (QueryStatement) stmt;
        SelectRelation selectRelation = (SelectRelation) queryStmt.getQueryRelation();

        // Extract plan info with nested map_batches merging and post-ops
        DaftPlanInfo planInfo = extractDaftPlanWithWrapper(selectRelation);

        // Qualify source SQL with current database so Arrow Flight connection
        // (which has no database context) can resolve table names.
        String sourceSQL = qualifySourceSQL(planInfo.sourceSQL, context.getDatabase());

        // Build Arrow Flight endpoint
        String arrowFlightEndpoint = buildArrowFlightEndpoint();

        LOG.info("DaftQueryExecutor: functions={}, sourceSQL={}, postOps={}, endpoint={}",
                planInfo.functionNames, sourceSQL, planInfo.postOps.size(),
                arrowFlightEndpoint);

        // Use query ID as trace ID for request correlation
        String requestId = context.getQueryId().toString();
        long maxResultRows = Config.daft_coordinator_max_result_rows;

        DaftCoordinatorClient client = new DaftCoordinatorClient(
                Config.daft_coordinator_host, Config.daft_coordinator_port);
        try {
            Iterator<DaftPlanResponse> responses = client.submitDaftPlanStreaming(
                    requestId, sourceSQL, arrowFlightEndpoint,
                    planInfo.functionNames, planInfo.postOps);

            streamResultsToClient(context, executor, responses, maxResultRows, requestId);
        } catch (DaftCoordinatorException e) {
            LOG.warn("DaftQueryExecutor failed", e);
            throw new DaftCoordinatorException("Daft Coordinator execution failed: " + e.getMessage(), e);
        } finally {
            client.close();
        }
    }

    /**
     * Stream gRPC responses directly to the MySQL client one batch at a time.
     * This avoids collecting the full result set in FE heap.
     */
    private static void streamResultsToClient(ConnectContext context, StmtExecutor executor,
            Iterator<DaftPlanResponse> responses, long maxResultRows, String requestId)
            throws IOException, DaftCoordinatorException {
        MysqlSerializer serializer = context.getSerializer();
        MysqlChannel channel = context.getMysqlChannel();

        boolean metadataSent = false;
        long totalRows = 0;
        List<String> columnNames = null;

        while (responses.hasNext()) {
            DaftPlanResponse resp = responses.next();

            // Check for error response
            if (resp.getResultCase() == DaftPlanResponse.ResultCase.ERROR) {
                if (metadataSent) {
                    // Can't send error after metadata — best effort: log and break
                    LOG.error("[{}] gRPC error mid-stream: {}", requestId, resp.getError());
                    break;
                }
                throw new DaftCoordinatorException("Daft Coordinator error: " + resp.getError());
            }

            // Extract column names from first response
            if (columnNames == null && resp.getColumnNamesCount() > 0) {
                columnNames = new ArrayList<>(resp.getColumnNamesList());
            }

            // Send metadata (column definitions) on first data batch
            if (!metadataSent && columnNames != null) {
                ShowResultSetMetaData.Builder metaBuilder = ShowResultSetMetaData.builder();
                for (String colName : columnNames) {
                    metaBuilder.addColumn(new Column(colName, TypeFactory.createDefaultCatalogString()));
                }
                executor.sendMetaData(metaBuilder.build());
                metadataSent = true;
            }

            // Send row data
            int numRows = resp.getNumRows();
            int numCols = columnNames != null ? columnNames.size() : 0;
            List<String> flatValues = resp.getRowValuesList();

            for (int r = 0; r < numRows; r++) {
                serializer.reset();
                for (int c = 0; c < numCols; c++) {
                    int idx = r * numCols + c;
                    String value = idx < flatValues.size() ? flatValues.get(idx) : "";
                    if (value == null || value.equals(FeConstants.NULL_STRING)) {
                        serializer.writeNull();
                    } else {
                        serializer.writeLenEncodedString(value);
                    }
                }
                channel.sendOnePacket(serializer.toByteBuffer());
            }

            totalRows += numRows;

            // Check max result rows limit
            if (maxResultRows > 0 && totalRows > maxResultRows) {
                throw new DaftCoordinatorException(
                        "Daft Coordinator result exceeds maximum rows limit: "
                                + totalRows + " > " + maxResultRows);
            }

            // Log stats from last response
            if (resp.hasStats()) {
                ExecutionStats stats = resp.getStats();
                LOG.info("DaftQueryExecutor stats [{}]: total={}ms, fetch={}ms, execute={}ms, "
                                + "input_rows={}, output_rows={}, input_bytes={}",
                        requestId, stats.getTotalMs(), stats.getDataFetchMs(),
                        stats.getDaftExecuteMs(), stats.getInputRows(),
                        stats.getOutputRows(), stats.getInputBytes());
            }
        }

        // If no data was received, still send empty metadata
        if (!metadataSent) {
            ShowResultSetMetaData.Builder metaBuilder = ShowResultSetMetaData.builder();
            if (columnNames != null) {
                for (String colName : columnNames) {
                    metaBuilder.addColumn(new Column(colName, TypeFactory.createDefaultCatalogString()));
                }
            }
            executor.sendMetaData(metaBuilder.build());
        }

        context.updateReturnRows((int) totalRows);
        context.getState().setEof();
    }

    /**
     * Build an EXPLAIN string for map_batches queries.
     */
    public static String buildExplainString(StatementBase stmt) {
        QueryStatement queryStmt = (QueryStatement) stmt;
        SelectRelation selectRelation = (SelectRelation) queryStmt.getQueryRelation();
        DaftPlanInfo planInfo = extractDaftPlanWithWrapper(selectRelation);

        StringBuilder sb = new StringBuilder();
        sb.append("DAFT COORDINATOR EXECUTION\n");
        sb.append("  Functions: ").append(planInfo.functionNames).append("\n");
        sb.append("  Operations: ").append(planInfo.functionNames.size())
                .append(" map_batches");
        if (!planInfo.postOps.isEmpty()) {
            for (PostOp op : planInfo.postOps) {
                switch (op.type) {
                    case FILTER:
                        sb.append(" + filter(").append(op.filterExprJson).append(")");
                        break;
                    case PROJECTION:
                        sb.append(" + projection(").append(op.columns).append(")");
                        break;
                    case LIMIT:
                        sb.append(" + limit(").append(op.limitCount).append(")");
                        break;
                }
            }
        }
        sb.append("\n");
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
     * Top-level extraction that handles both direct map_batches and wrapped patterns.
     */
    private static DaftPlanInfo extractDaftPlanWithWrapper(SelectRelation selectRelation) {
        // Case 1: Direct map_batches in SELECT list
        if (hasMapBatchesInSelect(selectRelation)) {
            return extractDaftPlan(selectRelation);
        }

        // Case 2: Wrapper pattern — outer SELECT/WHERE/LIMIT around a map_batches subquery
        if (isMapBatchesWrapper(selectRelation)) {
            return extractWrappedDaftPlan(selectRelation);
        }

        throw new IllegalStateException("No map_batches found in query");
    }

    /**
     * Extract plan info from a wrapper query pattern:
     * SELECT [cols] FROM (SELECT map_batches(...) FROM ...) sub [WHERE ...] [LIMIT ...]
     */
    private static DaftPlanInfo extractWrappedDaftPlan(SelectRelation outerSelect) {
        // Get inner map_batches plan from subquery
        SubqueryRelation subqueryRelation = (SubqueryRelation) outerSelect.getRelation();
        QueryRelation innerQR = subqueryRelation.getQueryStatement().getQueryRelation();
        SelectRelation innerSelect = (SelectRelation) innerQR;

        DaftPlanInfo innerPlan = extractDaftPlan(innerSelect);
        List<PostOp> postOps = new ArrayList<>(innerPlan.postOps);

        // Extract filter from WHERE clause
        Expr predicate = outerSelect.getPredicate();
        if (predicate != null) {
            String filterJson = trySerializeFilterExpr(predicate);
            if (filterJson != null) {
                postOps.add(PostOp.filter(filterJson));
            } else {
                LOG.warn("DaftQueryExecutor: unsupported WHERE expression, skipping filter pushdown: {}",
                        predicate);
            }
        }

        // Extract projection from SELECT list (unless it's SELECT *)
        List<String> projCols = extractProjectionColumns(outerSelect);
        if (projCols != null && !projCols.isEmpty()) {
            postOps.add(PostOp.projection(projCols));
        }

        // Extract LIMIT
        if (outerSelect.hasLimit() && outerSelect.getLimit().hasLimit()) {
            postOps.add(PostOp.limit(outerSelect.getLimit().getLimit()));
        }

        return new DaftPlanInfo(innerPlan.sourceSQL, innerPlan.functionNames, postOps);
    }

    /**
     * Check if a SelectRelation is a wrapper around a map_batches subquery.
     * Pattern: SELECT ... FROM (subquery_with_map_batches) alias [WHERE ...] [LIMIT ...]
     */
    private static boolean isMapBatchesWrapper(SelectRelation selectRelation) {
        Relation fromRelation = selectRelation.getRelation();
        if (!(fromRelation instanceof SubqueryRelation)) {
            return false;
        }
        SubqueryRelation subquery = (SubqueryRelation) fromRelation;
        QueryRelation innerQR = subquery.getQueryStatement().getQueryRelation();
        if (!(innerQR instanceof SelectRelation)) {
            return false;
        }
        SelectRelation innerSelect = (SelectRelation) innerQR;
        // Inner must have map_batches directly, or itself be a wrapper
        return hasMapBatchesInSelect(innerSelect) || isMapBatchesWrapper(innerSelect);
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

    /**
     * Try to serialize a filter expression to JSON for the Daft Coordinator.
     * Supports simple binary comparisons: col op literal.
     * Returns null if the expression is too complex to serialize.
     */
    static String trySerializeFilterExpr(Expr expr) {
        if (!(expr instanceof BinaryPredicate)) {
            return null;
        }
        BinaryPredicate bp = (BinaryPredicate) expr;
        BinaryType op = bp.getOp();

        // Only support simple comparison operators
        String opStr;
        switch (op) {
            case EQ:
                opStr = "=";
                break;
            case NE:
                opStr = "!=";
                break;
            case GT:
                opStr = ">";
                break;
            case GE:
                opStr = ">=";
                break;
            case LT:
                opStr = "<";
                break;
            case LE:
                opStr = "<=";
                break;
            default:
                return null;
        }

        Expr left = bp.getChild(0);
        Expr right = bp.getChild(1);

        // Pattern: column op literal
        String colName = extractColumnName(left);
        String valueLiteral = extractLiteralValue(right);

        if (colName != null && valueLiteral != null) {
            return "{\"col\": \"" + escapeJson(colName)
                    + "\", \"op\": \"" + opStr
                    + "\", \"value\": " + valueLiteral + "}";
        }

        // Pattern: literal op column (reversed)
        colName = extractColumnName(right);
        valueLiteral = extractLiteralValue(left);
        if (colName != null && valueLiteral != null) {
            // Reverse the operator: literal op col → col commuted_op literal
            BinaryType commuted = op.commutative();
            String commutedStr;
            switch (commuted) {
                case EQ:
                    commutedStr = "=";
                    break;
                case NE:
                    commutedStr = "!=";
                    break;
                case GT:
                    commutedStr = ">";
                    break;
                case GE:
                    commutedStr = ">=";
                    break;
                case LT:
                    commutedStr = "<";
                    break;
                case LE:
                    commutedStr = "<=";
                    break;
                default:
                    return null;
            }
            return "{\"col\": \"" + escapeJson(colName)
                    + "\", \"op\": \"" + commutedStr
                    + "\", \"value\": " + valueLiteral + "}";
        }

        return null;
    }

    private static String extractColumnName(Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            String col = slotRef.getColumnName();
            if (col != null) {
                return col;
            }
            // Fall back to label
            return slotRef.getLabel();
        }
        return null;
    }

    private static String extractLiteralValue(Expr expr) {
        if (expr instanceof IntLiteral) {
            return String.valueOf(((IntLiteral) expr).getLongValue());
        }
        if (expr instanceof FloatLiteral) {
            return String.valueOf(((FloatLiteral) expr).getValue());
        }
        if (expr instanceof StringLiteral) {
            return "\"" + escapeJson(((StringLiteral) expr).getStringValue()) + "\"";
        }
        if (expr instanceof LiteralExpr) {
            // Generic literal — try numeric first
            try {
                long lv = ((LiteralExpr) expr).getLongValue();
                return String.valueOf(lv);
            } catch (Exception e) {
                // not a long
            }
            return "\"" + escapeJson(((LiteralExpr) expr).getStringValue()) + "\"";
        }
        return null;
    }

    /**
     * Extract column names from the outer SELECT list for projection pushdown.
     * Returns null if SELECT * (no pruning needed) or if columns can't be extracted.
     */
    static List<String> extractProjectionColumns(SelectRelation selectRelation) {
        SelectList selectList = selectRelation.getSelectList();
        if (selectList == null) {
            return null;
        }

        // Check for SELECT * — no projection needed
        for (SelectListItem item : selectList.getItems()) {
            if (item.isStar()) {
                return null;
            }
        }

        List<String> columns = new ArrayList<>();
        for (SelectListItem item : selectList.getItems()) {
            Expr expr = item.getExpr();
            // Only support simple column references
            if (expr instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) expr;
                String col = slotRef.getColumnName();
                if (col == null) {
                    col = slotRef.getLabel();
                }
                if (col != null) {
                    columns.add(col);
                } else {
                    return null;  // Can't determine column name, skip projection
                }
            } else {
                // Complex expression in SELECT — don't push down projection
                return null;
            }
        }
        return columns.isEmpty() ? null : columns;
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Qualify source SQL with database context.
     * Arrow Flight connections don't carry the database context from the client session,
     * so we need to ensure table references are fully qualified.
     */
    private static String qualifySourceSQL(String sourceSQL, String database) {
        if (database == null || database.isEmpty()) {
            return sourceSQL;
        }
        // Simple heuristic: if source SQL is "SELECT * FROM `tablename`",
        // replace with "SELECT * FROM `db`.`tablename`"
        String prefix = "SELECT * FROM ";
        if (sourceSQL.startsWith(prefix)) {
            String tablePart = sourceSQL.substring(prefix.length()).trim();
            // Only qualify if table name doesn't already have a dot (already qualified)
            if (!tablePart.contains(".")) {
                return prefix + "`" + database + "`." + tablePart;
            }
        }
        return sourceSQL;
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
