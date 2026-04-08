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

package com.starrocks.catalog;

import com.starrocks.common.DdlException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages KV index metadata for external tables (e.g., Paimon).
 * v1: in-memory only, not persisted across FE restarts.
 */
public class KVIndexMetadataManager {

    // key: "catalogName.dbName.tableName" (lowercase)
    private final ConcurrentHashMap<String, List<Index>> indexMap = new ConcurrentHashMap<>();

    private static String makeKey(String catalog, String db, String table) {
        return String.format("%s.%s.%s", catalog, db, table).toLowerCase();
    }

    public synchronized void addIndex(String catalog, String db, String table, Index index)
            throws DdlException {
        String key = makeKey(catalog, db, table);
        List<Index> indexes = indexMap.computeIfAbsent(key, k -> new ArrayList<>());
        for (Index existing : indexes) {
            if (existing.getIndexName().equalsIgnoreCase(index.getIndexName())) {
                throw new DdlException("Index " + index.getIndexName() + " already exists");
            }
        }
        // v1: only one KV index per table
        if (!indexes.isEmpty()) {
            throw new DdlException("Only one KV index per table is supported");
        }
        indexes.add(index);
    }

    public synchronized void dropIndex(String catalog, String db, String table, String indexName)
            throws DdlException {
        String key = makeKey(catalog, db, table);
        List<Index> indexes = indexMap.get(key);
        if (indexes == null || indexes.stream().noneMatch(
                i -> i.getIndexName().equalsIgnoreCase(indexName))) {
            throw new DdlException("Index " + indexName + " does not exist");
        }
        indexes.removeIf(i -> i.getIndexName().equalsIgnoreCase(indexName));
        if (indexes.isEmpty()) {
            indexMap.remove(key);
        }
    }

    public List<Index> getIndexes(String catalog, String db, String table) {
        String key = makeKey(catalog, db, table);
        return indexMap.getOrDefault(key, Collections.emptyList());
    }
}
