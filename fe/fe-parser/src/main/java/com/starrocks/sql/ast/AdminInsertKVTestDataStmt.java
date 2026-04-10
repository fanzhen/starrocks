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

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

import java.util.List;

// ADMIN INSERT KV_TEST_DATA INTO catalog.db.table COLUMNS (col1, col2) VALUES (v1, v2), (v3, v4);
public class AdminInsertKVTestDataStmt extends StatementBase {
    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<List<String>> rows; // each row is a list of literal string values

    public AdminInsertKVTestDataStmt(String catalogName, String dbName, String tableName,
                                     List<String> columnNames, List<List<String>> rows,
                                     NodePosition pos) {
        super(pos);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.rows = rows;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminInsertKVTestDataStatement(this, context);
    }
}
