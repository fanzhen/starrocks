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

package com.starrocks.common;

import com.starrocks.thrift.TBm25SearchOptions;

public class Bm25SearchOptions {
    private String query = "";
    private int queryType = 0; // 0=any, 1=all, 2=phrase
    private String columnName = "";
    private int bm25SlotId = 0;

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int getQueryType() {
        return queryType;
    }

    public void setQueryType(int queryType) {
        this.queryType = queryType;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getBm25SlotId() {
        return bm25SlotId;
    }

    public void setBm25SlotId(int bm25SlotId) {
        this.bm25SlotId = bm25SlotId;
    }

    public boolean isEnabled() {
        return !query.isEmpty();
    }

    public TBm25SearchOptions toThrift() {
        TBm25SearchOptions opts = new TBm25SearchOptions();
        opts.setQuery(query);
        opts.setQuery_type(queryType);
        opts.setColumn_name(columnName);
        opts.setBm25_slot_id(bm25SlotId);
        return opts;
    }
}
