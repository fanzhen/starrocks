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
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Asynchronous KV index build executor.
 * Reads Paimon table data via ReadBuilder, generates SSTable files compatible with BE KVIndexReader,
 * and writes manifest.json to the warehouse directory.
 */
public class KVIndexBuildExecutor {

    private static final Logger LOG = LogManager.getLogger(KVIndexBuildExecutor.class);

    private final ExecutorService executor;

    public KVIndexBuildExecutor() {
        this.executor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "kv-index-builder");
            t.setDaemon(true);
            return t;
        });
    }

    public void submitBuildTask(String catalogName, String dbName, String tableName,
                                String indexName, List<String> valueColumnNames,
                                org.apache.paimon.table.Table paimonTable) {
        executor.submit(() -> {
            try {
                doBuild(catalogName, dbName, tableName, indexName, valueColumnNames, paimonTable);
            } catch (Exception e) {
                LOG.error("KV index build failed: {}.{}.{}/{}", catalogName, dbName, tableName, indexName, e);
                updateStateFailed(catalogName, dbName, tableName, indexName, e.getMessage());
            }
        });
    }

    private void doBuild(String catalogName, String dbName, String tableName,
                         String indexName, List<String> valueColumnNames,
                         org.apache.paimon.table.Table paimonTable) throws Exception {
        KVIndexMetadataManager kvMgr = GlobalStateMgr.getCurrentState().getKVIndexMetadataManager();
        KVIndexMetadataManager.KVIndexMeta meta = kvMgr.getIndexMeta(catalogName, dbName, tableName, indexName);
        if (meta == null) {
            LOG.warn("KV index meta not found for {}.{}.{}/{}, skipping build", catalogName, dbName, tableName, indexName);
            return;
        }

        // 1. Update state to BUILDING
        meta.setBuildState(KVIndexMetadataManager.BuildState.BUILDING);
        meta.setBuildStartTimeMs(System.currentTimeMillis());
        LOG.info("KV index build started: {}.{}.{}/{}", catalogName, dbName, tableName, indexName);

        // 2. Get latest snapshot id
        long snapshotId = -1;
        if (paimonTable instanceof DataTable) {
            DataTable dataTable = (DataTable) paimonTable;
            SnapshotManager snapshotManager = new SnapshotManager(
                    dataTable.fileIO(), dataTable.location());
            Long latestId = snapshotManager.latestSnapshotId();
            if (latestId != null) {
                snapshotId = latestId;
            }
        }

        // 3. Get table schema and resolve column indices
        RowType rowType = paimonTable.rowType();
        List<DataField> allFields = rowType.getFields();
        List<String> allFieldNames = rowType.getFieldNames();

        // Resolve value column indices
        int[] valueColIndices = new int[valueColumnNames.size()];
        DataType[] valueColTypes = new DataType[valueColumnNames.size()];
        String[] valueColTypeNames = new String[valueColumnNames.size()];
        for (int i = 0; i < valueColumnNames.size(); i++) {
            String colName = valueColumnNames.get(i);
            int idx = allFieldNames.indexOf(colName);
            if (idx < 0) {
                throw new RuntimeException("Column not found: " + colName);
            }
            valueColIndices[i] = idx;
            valueColTypes[i] = allFields.get(idx).type();
            valueColTypeNames[i] = mapPaimonTypeToSR(valueColTypes[i]);
        }

        // 4. Read all data using Paimon ReadBuilder
        ReadBuilder readBuilder = paimonTable.newReadBuilder();
        // Project to only read value columns
        readBuilder.withProjection(valueColIndices);

        List<KVEntry> entries = new ArrayList<>();
        long rowIdCounter = 0;

        RecordReader<InternalRow> reader = readBuilder.newRead().createReader(readBuilder.newScan().plan());
        RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader);
        try {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                Object[] values = new Object[valueColumnNames.size()];
                for (int i = 0; i < valueColumnNames.size(); i++) {
                    values[i] = extractValue(row, i, valueColTypes[i]);
                }
                entries.add(new KVEntry(rowIdCounter, values));
                rowIdCounter++;
            }
        } finally {
            iterator.close();
        }

        LOG.info("KV index build: read {} rows from {}.{}.{}", entries.size(), catalogName, dbName, tableName);

        // 5. Sort by rowId (already sequential, but ensure)
        entries.sort(Comparator.comparingLong(e -> e.rowId));

        // 6. Determine output directory
        String tableLocation;
        if (paimonTable instanceof DataTable) {
            tableLocation = ((DataTable) paimonTable).location().toString();
        } else {
            throw new RuntimeException("Cannot determine table location for non-DataTable");
        }
        // Strip file:// prefix for local filesystem
        String localPath = tableLocation;
        if (localPath.startsWith("file:")) {
            localPath = localPath.substring(5);
            while (localPath.startsWith("//")) {
                localPath = localPath.substring(1);
            }
        }

        String indexDir = localPath + "/.starrocks_kv_index/" + indexName;
        String dataDir = indexDir + "/data";
        new File(dataDir).mkdirs();

        String sstFilePath = dataDir + "/kv_00001.sst";
        String manifestPath = indexDir + "/manifest.json";

        // 7. Write SSTable file
        long sstFileSize;
        try (FileOutputStream fos = new FileOutputStream(sstFilePath);
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             KVIndexSSTWriter writer = new KVIndexSSTWriter(bos)) {
            for (KVEntry entry : entries) {
                byte[] key = KVIndexSSTWriter.encodeInt64Key(entry.rowId);
                byte[] value = KVIndexSSTWriter.encodeRowValue(entry.values, valueColTypeNames);
                writer.add(key, value);
            }
            writer.finish();
            sstFileSize = writer.getFileSize();
        }

        LOG.info("KV index build: wrote SSTable {} ({} bytes, {} entries)",
                sstFilePath, sstFileSize, entries.size());

        // 8. Write manifest.json
        writeManifest(manifestPath, indexName, snapshotId, entries.size(), sstFileSize,
                sstFilePath, valueColumnNames, valueColTypeNames);

        // 9. Update metadata to READY
        meta.setBuildState(KVIndexMetadataManager.BuildState.READY);
        meta.setBaseSnapshotId(snapshotId);
        meta.setSstFilePath(sstFilePath);
        meta.setSstFileSize(sstFileSize);
        meta.setIndexedRowCount(entries.size());
        meta.setManifestPath(manifestPath);
        meta.setBuildEndTimeMs(System.currentTimeMillis());

        LOG.info("KV index build completed: {}.{}.{}/{}, snapshot={}, rows={}, size={}",
                catalogName, dbName, tableName, indexName, snapshotId, entries.size(), sstFileSize);
    }

    private void updateStateFailed(String catalogName, String dbName, String tableName,
                                   String indexName, String errorMessage) {
        KVIndexMetadataManager kvMgr = GlobalStateMgr.getCurrentState().getKVIndexMetadataManager();
        KVIndexMetadataManager.KVIndexMeta meta = kvMgr.getIndexMeta(catalogName, dbName, tableName, indexName);
        if (meta != null) {
            meta.setBuildState(KVIndexMetadataManager.BuildState.FAILED);
            meta.setErrorMessage(errorMessage);
            meta.setBuildEndTimeMs(System.currentTimeMillis());
        }
    }

    private Object extractValue(InternalRow row, int fieldIndex, DataType dataType) {
        if (row.isNullAt(fieldIndex)) {
            return null;
        }
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case INTEGER:
                return row.getInt(fieldIndex);
            case BIGINT:
                return row.getLong(fieldIndex);
            case FLOAT:
                return row.getFloat(fieldIndex);
            case DOUBLE:
                return row.getDouble(fieldIndex);
            case VARCHAR:
            case CHAR: {
                BinaryString bs = row.getString(fieldIndex);
                return bs != null ? bs.toString() : null;
            }
            case SMALLINT:
                return (int) row.getShort(fieldIndex);
            case TINYINT:
                return (int) row.getByte(fieldIndex);
            case BOOLEAN:
                return row.getBoolean(fieldIndex) ? 1 : 0;
            default:
                // Fallback: try getString
                BinaryString bs = row.getString(fieldIndex);
                return bs != null ? bs.toString() : null;
        }
    }

    private String mapPaimonTypeToSR(DataType paimonType) {
        switch (paimonType.getTypeRoot()) {
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case VARCHAR:
            case CHAR:
                return "VARCHAR";
            case SMALLINT:
                return "INT";
            case TINYINT:
                return "INT";
            case BOOLEAN:
                return "INT";
            default:
                return "VARCHAR";
        }
    }

    private void writeManifest(String manifestPath, String indexName, long snapshotId,
                               long rowCount, long sstFileSize, String sstFilePath,
                               List<String> columnNames, String[] columnTypes) throws IOException {
        JsonObject manifest = new JsonObject();
        manifest.addProperty("indexName", indexName);
        manifest.addProperty("indexType", "KV");
        manifest.addProperty("baseSnapshotId", snapshotId);
        manifest.addProperty("rowCount", rowCount);
        manifest.addProperty("buildTimeMs", System.currentTimeMillis());

        JsonArray columns = new JsonArray();
        for (int i = 0; i < columnNames.size(); i++) {
            JsonObject col = new JsonObject();
            col.addProperty("name", columnNames.get(i));
            col.addProperty("type", columnTypes[i]);
            columns.add(col);
        }
        manifest.add("columns", columns);

        JsonArray sstFiles = new JsonArray();
        JsonObject sstFile = new JsonObject();
        sstFile.addProperty("path", sstFilePath);
        sstFile.addProperty("size", sstFileSize);
        sstFile.addProperty("rowCount", rowCount);
        sstFiles.add(sstFile);
        manifest.add("sstFiles", sstFiles);

        try (FileWriter fw = new FileWriter(manifestPath)) {
            fw.write(manifest.toString());
        }
    }

    public void shutdown() {
        executor.shutdown();
    }

    private static class KVEntry {
        final long rowId;
        final Object[] values;

        KVEntry(long rowId, Object[] values) {
            this.rowId = rowId;
            this.values = values;
        }
    }
}
