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

import com.starrocks.coordinator.proto.ExecutionStats;

import java.util.List;

/**
 * Holds text-format results from a Daft Coordinator query execution.
 */
public class DaftQueryResult {
    private final List<String> columnNames;
    private final List<List<String>> rows;
    private final ExecutionStats stats;

    public DaftQueryResult(List<String> columnNames, List<List<String>> rows) {
        this(columnNames, rows, null);
    }

    public DaftQueryResult(List<String> columnNames, List<List<String>> rows, ExecutionStats stats) {
        this.columnNames = columnNames;
        this.rows = rows;
        this.stats = stats;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public ExecutionStats getStats() {
        return stats;
    }
}
