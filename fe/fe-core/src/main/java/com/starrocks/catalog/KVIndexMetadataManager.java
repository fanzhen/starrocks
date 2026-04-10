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
import com.starrocks.sql.ast.IndexDef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages KV index metadata for external tables (e.g., Paimon).
 * v1: in-memory only, not persisted across FE restarts.
 */
public class KVIndexMetadataManager {

    public enum BuildState {
        PENDING,
        BUILDING,
        READY,
        FAILED
    }

    public static class KVIndexMeta {
        private final String indexName;
        private final List<ColumnId> columns;
        private final IndexDef.IndexType indexType;
        private final String comment;
        private final Map<String, String> properties;

        private volatile BuildState buildState = BuildState.PENDING;
        private volatile long baseSnapshotId = -1;
        private volatile String sstFilePath;
        private volatile long sstFileSize;
        private volatile long indexedRowCount;
        private volatile String errorMessage;
        private volatile long buildStartTimeMs;
        private volatile long buildEndTimeMs;
        private volatile String manifestPath;
        private volatile String[] columnNames;
        private volatile String[] columnTypes;

        public KVIndexMeta(String indexName, List<ColumnId> columns, IndexDef.IndexType indexType,
                           String comment, Map<String, String> properties) {
            this.indexName = indexName;
            this.columns = columns;
            this.indexType = indexType;
            this.comment = comment;
            this.properties = properties;
        }

        public String getIndexName() {
            return indexName;
        }

        public List<ColumnId> getColumns() {
            return columns;
        }

        public IndexDef.IndexType getIndexType() {
            return indexType;
        }

        public String getComment() {
            return comment;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public BuildState getBuildState() {
            return buildState;
        }

        public void setBuildState(BuildState buildState) {
            this.buildState = buildState;
        }

        public long getBaseSnapshotId() {
            return baseSnapshotId;
        }

        public void setBaseSnapshotId(long baseSnapshotId) {
            this.baseSnapshotId = baseSnapshotId;
        }

        public String getSstFilePath() {
            return sstFilePath;
        }

        public void setSstFilePath(String sstFilePath) {
            this.sstFilePath = sstFilePath;
        }

        public long getSstFileSize() {
            return sstFileSize;
        }

        public void setSstFileSize(long sstFileSize) {
            this.sstFileSize = sstFileSize;
        }

        public long getIndexedRowCount() {
            return indexedRowCount;
        }

        public void setIndexedRowCount(long indexedRowCount) {
            this.indexedRowCount = indexedRowCount;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public long getBuildStartTimeMs() {
            return buildStartTimeMs;
        }

        public void setBuildStartTimeMs(long buildStartTimeMs) {
            this.buildStartTimeMs = buildStartTimeMs;
        }

        public long getBuildEndTimeMs() {
            return buildEndTimeMs;
        }

        public void setBuildEndTimeMs(long buildEndTimeMs) {
            this.buildEndTimeMs = buildEndTimeMs;
        }

        public String getManifestPath() {
            return manifestPath;
        }

        public void setManifestPath(String manifestPath) {
            this.manifestPath = manifestPath;
        }

        public String[] getColumnNames() {
            return columnNames;
        }

        public void setColumnNames(String[] columnNames) {
            this.columnNames = columnNames;
        }

        public String[] getColumnTypes() {
            return columnTypes;
        }

        public void setColumnTypes(String[] columnTypes) {
            this.columnTypes = columnTypes;
        }

        public Index toIndex() {
            return new Index(indexName, columns, indexType, comment, properties);
        }

        public String getStatusString() {
            StringBuilder sb = new StringBuilder();
            sb.append(indexType.name());
            sb.append(" (").append(buildState);
            if (baseSnapshotId >= 0) {
                sb.append(", snapshot=").append(baseSnapshotId);
            }
            if (indexedRowCount > 0) {
                sb.append(", rows=").append(indexedRowCount);
            }
            if (buildState == BuildState.FAILED && errorMessage != null) {
                String msg = errorMessage.length() > 80 ? errorMessage.substring(0, 80) + "..." : errorMessage;
                sb.append(", error=").append(msg);
            }
            sb.append(")");
            return sb.toString();
        }
    }

    // key: "catalogName.dbName.tableName" (lowercase)
    private final ConcurrentHashMap<String, List<KVIndexMeta>> indexMap = new ConcurrentHashMap<>();

    private static String makeKey(String catalog, String db, String table) {
        return String.format("%s.%s.%s", catalog, db, table).toLowerCase();
    }

    public synchronized KVIndexMeta addIndexMeta(String catalog, String db, String table,
                                                  String indexName, List<ColumnId> columns,
                                                  IndexDef.IndexType indexType, String comment,
                                                  Map<String, String> properties) throws DdlException {
        String key = makeKey(catalog, db, table);
        List<KVIndexMeta> metas = indexMap.computeIfAbsent(key, k -> new ArrayList<>());
        for (KVIndexMeta existing : metas) {
            if (existing.getIndexName().equalsIgnoreCase(indexName)) {
                throw new DdlException("Index " + indexName + " already exists");
            }
        }
        // v1: only one KV index per table
        if (!metas.isEmpty()) {
            throw new DdlException("Only one KV index per table is supported");
        }
        KVIndexMeta meta = new KVIndexMeta(indexName, columns, indexType, comment, properties);
        metas.add(meta);
        return meta;
    }

    public synchronized void dropIndex(String catalog, String db, String table, String indexName)
            throws DdlException {
        String key = makeKey(catalog, db, table);
        List<KVIndexMeta> metas = indexMap.get(key);
        if (metas == null || metas.stream().noneMatch(
                m -> m.getIndexName().equalsIgnoreCase(indexName))) {
            throw new DdlException("Index " + indexName + " does not exist");
        }
        metas.removeIf(m -> m.getIndexName().equalsIgnoreCase(indexName));
        if (metas.isEmpty()) {
            indexMap.remove(key);
        }
    }

    public KVIndexMeta getIndexMeta(String catalog, String db, String table, String indexName) {
        String key = makeKey(catalog, db, table);
        List<KVIndexMeta> metas = indexMap.getOrDefault(key, Collections.emptyList());
        for (KVIndexMeta meta : metas) {
            if (meta.getIndexName().equalsIgnoreCase(indexName)) {
                return meta;
            }
        }
        return null;
    }

    public List<KVIndexMeta> getIndexMetas(String catalog, String db, String table) {
        String key = makeKey(catalog, db, table);
        List<KVIndexMeta> metas = indexMap.get(key);
        if (metas == null) {
            return Collections.emptyList();
        }
        synchronized (this) {
            return new ArrayList<>(metas);
        }
    }

    /**
     * Backward-compatible: return Index objects for existing callers.
     */
    public List<Index> getIndexes(String catalog, String db, String table) {
        List<KVIndexMeta> metas = getIndexMetas(catalog, db, table);
        List<Index> indexes = new ArrayList<>();
        for (KVIndexMeta meta : metas) {
            indexes.add(meta.toIndex());
        }
        return indexes;
    }
}
