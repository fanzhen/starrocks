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
package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Bm25SearchOptions;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.FloatType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RewriteToBm25PlanRule extends TransformationRule {

    public RewriteToBm25PlanRule() {
        super(RuleType.TF_BM25_REWRITE_RULE,
                Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getOp();
        if (scanOp.getProjection() == null) {
            return false;
        }
        if (scanOp.getBm25SearchOptions().isEnabled()) {
            return false;
        }
        for (ScalarOperator op : scanOp.getProjection().getColumnRefMap().values()) {
            if (findBm25Call(op) != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getOp();
        OlapTable table = (OlapTable) scanOp.getTable();

        // Find the bm25() call in projection
        CallOperator bm25Call = null;
        ColumnRefOperator bm25OutRef = null;
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry :
                scanOp.getProjection().getColumnRefMap().entrySet()) {
            CallOperator found = findBm25Call(entry.getValue());
            if (found != null) {
                bm25Call = found;
                bm25OutRef = entry.getKey();
                break;
            }
        }

        if (bm25Call == null) {
            return List.of();
        }
        final CallOperator finalBm25Call = bm25Call;

        // Extract column ref from bm25(column, query, ...)
        ScalarOperator firstArg = bm25Call.getChild(0);
        if (!firstArg.isColumnRef()) {
            return List.of(); // First arg must be a column reference
        }
        ColumnRefOperator columnRef = (ColumnRefOperator) firstArg;
        Column column = scanOp.getColRefToColumnMetaMap().get(columnRef);
        if (column == null) {
            return List.of();
        }

        // Check if this column has a GIN index with imp_lib=tantivy
        Index ginIndex = findGinIndex(table, column);
        if (ginIndex == null) {
            return List.of(); // No GIN index, keep batch-local fallback
        }

        // Extract query text (arg 1)
        ScalarOperator queryArg = bm25Call.getChild(1);
        if (!(queryArg instanceof ConstantOperator)) {
            return List.of();
        }
        String queryText = String.valueOf(((ConstantOperator) queryArg).getValue());

        // Extract query_type from the MATCH predicate in WHERE clause or bm25 args
        int queryType = 0; // default: any
        if (bm25Call.getChildren().size() >= 4) {
            ScalarOperator qtArg = bm25Call.getChild(3);
            if (qtArg instanceof ConstantOperator) {
                String qt = String.valueOf(((ConstantOperator) qtArg).getValue());
                switch (qt) {
                    case "all":
                        queryType = 1;
                        break;
                    case "phrase":
                        queryType = 2;
                        break;
                    default:
                        queryType = 0;
                        break;
                }
            }
        }

        // Create virtual column for BM25 score (guard against duplicate adds across repeated optimizations)
        String scoreColumnName = "__bm25_score__";
        Column scoreColumn = new Column(scoreColumnName, FloatType.DOUBLE);
        if (table.getColumn(scoreColumnName) == null) {
            table.addColumn(scoreColumn);
        }

        ColumnRefOperator scoreColRef = context.getColumnRefFactory().create(
                scoreColumnName, FloatType.DOUBLE, true);

        Map<ColumnRefOperator, Column> newColRefToColumnMetaMap = new HashMap<>(scanOp.getColRefToColumnMetaMap());
        newColRefToColumnMetaMap.put(scoreColRef, scoreColumn);

        Map<Column, ColumnRefOperator> newColumnMetaToColRefMap = new HashMap<>(scanOp.getColumnMetaToColRefMap());
        newColumnMetaToColRefMap.put(scoreColumn, scoreColRef);

        // Set BM25 search options
        Bm25SearchOptions opts = new Bm25SearchOptions();
        opts.setQuery(queryText);
        opts.setQueryType(queryType);
        opts.setColumnName(column.getName());
        opts.setBm25SlotId(scoreColRef.getId());
        scanOp.setBm25SearchOptions(opts);

        // Replace bm25() call with the score column ref in projection
        Map<ColumnRefOperator, ScalarOperator> newProjectMap = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry :
                scanOp.getProjection().getColumnRefMap().entrySet()) {
            newProjectMap.put(entry.getKey(),
                    replaceBm25WithColRef(entry.getValue(), finalBm25Call, scoreColRef));
        }
        // Ensure the score column is passed through so the scan node outputs it
        newProjectMap.put(scoreColRef, scoreColRef);

        LogicalOlapScanOperator newScanOp = LogicalOlapScanOperator.builder()
                .withOperator(scanOp)
                .setProjection(new Projection(newProjectMap))
                .setColRefToColumnMetaMap(newColRefToColumnMetaMap)
                .setColumnMetaToColRefMap(newColumnMetaToColRefMap)
                .build();

        return List.of(OptExpression.create(newScanOp));
    }

    private CallOperator findBm25Call(ScalarOperator operator) {
        if (operator instanceof CallOperator) {
            CallOperator call = (CallOperator) operator;
            if ("bm25".equalsIgnoreCase(call.getFnName())) {
                return call;
            }
        }
        for (ScalarOperator child : operator.getChildren()) {
            CallOperator found = findBm25Call(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private ScalarOperator replaceBm25WithColRef(ScalarOperator operator, CallOperator bm25Call,
                                                  ColumnRefOperator scoreColRef) {
        if (operator.equals(bm25Call)) {
            return scoreColRef;
        }
        for (int i = 0; i < operator.getChildren().size(); i++) {
            ScalarOperator child = operator.getChild(i);
            operator.setChild(i, replaceBm25WithColRef(child, bm25Call, scoreColRef));
        }
        return operator;
    }

    private Index findGinIndex(OlapTable table, Column column) {
        for (Index index : table.getIndexes()) {
            if (index.getIndexType() == IndexDef.IndexType.GIN) {
                List<ColumnId> indexColumns = index.getColumns();
                if (indexColumns != null && !indexColumns.isEmpty() &&
                        column.getColumnId().equals(indexColumns.get(0))) {
                    // Check imp_lib=tantivy
                    Map<String, String> props = index.getProperties();
                    if (props != null && "tantivy".equalsIgnoreCase(props.get("imp_lib"))) {
                        return index;
                    }
                }
            }
        }
        return null;
    }
}
