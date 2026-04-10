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

// ADMIN REBUILD KV_INDEX ON catalog.db.table INDEX indexName;
public class AdminRebuildKVIndexStmt extends StatementBase {
    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final String indexName;

    public AdminRebuildKVIndexStmt(String catalogName, String dbName, String tableName,
                                   String indexName, NodePosition pos) {
        super(pos);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.indexName = indexName;
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

    public String getIndexName() {
        return indexName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminRebuildKVIndexStatement(this, context);
    }
}
