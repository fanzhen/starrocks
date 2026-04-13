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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.IndexDef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages KV index metadata for external tables (e.g., Paimon).
 * In-memory cache backed by manifest.json files on disk.
 * On first access for a table, loads from manifest if present.
 */
public class KVIndexMetadataManager {

    private static final Logger LOG = LogManager.getLogger(KVIndexMetadataManager.class);

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
        dropIndex(catalog, db, table, indexName, null);
    }

    public synchronized void dropIndex(String catalog, String db, String table, String indexName,
                                        String tableLocation) throws DdlException {
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

        // Delete manifest directory on disk
        if (tableLocation != null) {
            String localPath = stripFilePrefix(tableLocation);
            String indexDir = localPath + "/.starrocks_kv_index/" + indexName;
            deleteDirectory(new File(indexDir));
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
     * Get index metas with on-demand manifest loading.
     * If no metas are cached for this table, attempts to load from manifest.json on disk.
     */
    public List<KVIndexMeta> getIndexMetas(String catalog, String db, String table, String tableLocation) {
        String key = makeKey(catalog, db, table);
        if (!indexMap.containsKey(key) && tableLocation != null) {
            loadFromManifest(tableLocation, catalog, db, table);
        }
        return getIndexMetas(catalog, db, table);
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

    private void loadFromManifest(String tableLocation, String catalog, String db, String table) {
        String localPath = stripFilePrefix(tableLocation);
        String kvIndexDir = localPath + "/.starrocks_kv_index";
        File dir = new File(kvIndexDir);
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        File[] indexDirs = dir.listFiles(File::isDirectory);
        if (indexDirs == null) {
            return;
        }

        for (File indexDir : indexDirs) {
            File manifestFile = new File(indexDir, "manifest.json");
            if (!manifestFile.exists()) {
                continue;
            }

            try {
                KVIndexMeta meta = parseManifest(manifestFile);
                if (meta != null) {
                    meta.setBuildState(BuildState.READY);
                    String key = makeKey(catalog, db, table);
                    synchronized (this) {
                        List<KVIndexMeta> metas = indexMap.computeIfAbsent(key, k -> new ArrayList<>());
                        boolean exists = metas.stream()
                                .anyMatch(m -> m.getIndexName().equalsIgnoreCase(meta.getIndexName()));
                        if (!exists) {
                            metas.add(meta);
                            LOG.info("Loaded KV index '{}' from manifest: {}", meta.getIndexName(), manifestFile);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to parse manifest file: {}", manifestFile, e);
            }
        }
    }

    private KVIndexMeta parseManifest(File manifestFile) throws IOException {
        String json = new String(Files.readAllBytes(manifestFile.toPath()));
        JsonObject manifest = JsonParser.parseString(json).getAsJsonObject();

        String indexName = manifest.get("indexName").getAsString();
        long snapshotId = manifest.has("baseSnapshotId") ? manifest.get("baseSnapshotId").getAsLong() : -1;
        long rowCount = manifest.has("rowCount") ? manifest.get("rowCount").getAsLong() : 0;

        // Parse columns
        JsonArray columnsArr = manifest.getAsJsonArray("columns");
        List<ColumnId> columnIds = new ArrayList<>();
        String[] columnNames = new String[columnsArr.size()];
        String[] columnTypes = new String[columnsArr.size()];
        for (int i = 0; i < columnsArr.size(); i++) {
            JsonObject col = columnsArr.get(i).getAsJsonObject();
            String name = col.get("name").getAsString();
            columnIds.add(ColumnId.create(name));
            columnNames[i] = name;
            columnTypes[i] = col.get("type").getAsString();
        }

        // Parse SST file info
        String sstFilePath = null;
        long sstFileSize = 0;
        if (manifest.has("sstFiles")) {
            JsonArray sstFiles = manifest.getAsJsonArray("sstFiles");
            if (sstFiles.size() > 0) {
                JsonObject sstFile = sstFiles.get(0).getAsJsonObject();
                sstFilePath = sstFile.get("path").getAsString();
                sstFileSize = sstFile.get("size").getAsLong();
            }
        }

        KVIndexMeta meta = new KVIndexMeta(indexName, columnIds, IndexDef.IndexType.KV, "", new HashMap<>());
        meta.setBaseSnapshotId(snapshotId);
        meta.setColumnNames(columnNames);
        meta.setColumnTypes(columnTypes);
        meta.setSstFilePath(sstFilePath);
        meta.setSstFileSize(sstFileSize);
        meta.setIndexedRowCount(rowCount);
        meta.setManifestPath(manifestFile.getAbsolutePath());
        return meta;
    }

    static String stripFilePrefix(String location) {
        String path = location;
        if (path.startsWith("file:")) {
            path = path.substring(5);
            while (path.startsWith("//")) {
                path = path.substring(1);
            }
        }
        return path;
    }

    private static void deleteDirectory(File dir) {
        if (!dir.exists()) {
            return;
        }
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    f.delete();
                }
            }
        }
        dir.delete();
    }
}
