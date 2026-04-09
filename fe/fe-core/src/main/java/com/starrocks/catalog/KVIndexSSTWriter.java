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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Pure Java LevelDB SSTable writer, binary-compatible with BE's sstable::Table::Open.
 *
 * Format:
 *   [Data Block 0] [Trailer] [Data Block 1] [Trailer] ...
 *   [Filter Block] [Trailer]
 *   [MetaIndex Block] [Trailer]
 *   [Index Block] [Trailer]
 *   [Footer: 48 bytes]
 *
 * Block Trailer = type(1 byte) + crc32c_masked(4 bytes little-endian)
 * Footer = metaindex_handle(varint64 pair) + index_handle(varint64 pair) + padding to 40 bytes + magic(8 bytes LE)
 */
public class KVIndexSSTWriter implements Closeable {

    private static final long TABLE_MAGIC_NUMBER = 0xdb4775248b80fb57L;
    private static final int BLOCK_TRAILER_SIZE = 5;
    private static final int FOOTER_ENCODED_LENGTH = 48; // 2 * MaxEncodedLength(10+10) + 8
    private static final int BLOCK_SIZE = 4096;
    private static final int BLOCK_RESTART_INTERVAL = 16;
    private static final int BITS_PER_KEY = 10;
    private static final int FILTER_BASE_LG = 11;
    private static final int FILTER_BASE = 1 << FILTER_BASE_LG;
    private static final byte COMPRESSION_NONE = 0;
    private static final byte COMPRESSION_SNAPPY = 1;
    private static final int CRC_MASK_DELTA = 0xa282ead8;
    private static final HashFunction CRC32C = Hashing.crc32c();

    private final OutputStream output;
    private long fileOffset = 0;
    private long numEntries = 0;
    private boolean closed = false;

    // Current data block
    private final BlockBuilder dataBlock = new BlockBuilder(BLOCK_RESTART_INTERVAL);
    // Index block (restart_interval = 1)
    private final BlockBuilder indexBlock = new BlockBuilder(1);
    // Filter block
    private final FilterBlockBuilder filterBlock = new FilterBlockBuilder();

    // Pending index entry state
    private boolean pendingIndexEntry = false;
    private long pendingHandleOffset;
    private long pendingHandleSize;
    private byte[] lastKey = new byte[0];

    public KVIndexSSTWriter(OutputStream output) {
        this.output = output;
        filterBlock.startBlock(0);
    }

    /**
     * Add a key-value pair. Keys must be added in strictly increasing order.
     */
    public void add(byte[] key, byte[] value) throws IOException {
        if (closed) {
            throw new IOException("Writer is closed");
        }

        if (pendingIndexEntry) {
            // Use lastKey as-is (no FindShortestSeparator optimization needed for fixed-size keys)
            byte[] handleEncoding = encodeBlockHandle(pendingHandleOffset, pendingHandleSize);
            indexBlock.add(lastKey, handleEncoding);
            pendingIndexEntry = false;
        }

        filterBlock.addKey(key);
        lastKey = key.clone();
        numEntries++;
        dataBlock.add(key, value);

        if (dataBlock.currentSizeEstimate() >= BLOCK_SIZE) {
            flush();
        }
    }

    private void flush() throws IOException {
        if (dataBlock.isEmpty()) {
            return;
        }
        writeBlock(dataBlock);
        pendingIndexEntry = true;
        filterBlock.startBlock(fileOffset);
    }

    /**
     * Finish writing the SSTable. Must be called before close.
     */
    public void finish() throws IOException {
        flush();
        closed = true;

        // Save pending data block handle before writing non-data blocks,
        // because writeRawBlock overwrites pendingHandleOffset/Size.
        long savedHandleOffset = pendingHandleOffset;
        long savedHandleSize = pendingHandleSize;

        // Write filter block (no compression)
        byte[] filterData = filterBlock.finish();
        long filterOffset = fileOffset;
        long filterSize = filterData.length;
        writeRawBlock(filterData, COMPRESSION_NONE);

        // Write metaindex block
        BlockBuilder metaIndexBlock = new BlockBuilder(BLOCK_RESTART_INTERVAL);
        byte[] filterHandleEncoding = encodeBlockHandle(filterOffset, filterSize);
        metaIndexBlock.add("filter.leveldb.BuiltinBloomFilter2".getBytes(), filterHandleEncoding);
        long metaIndexOffset = fileOffset;
        byte[] metaIndexData = metaIndexBlock.finish();
        long metaIndexSize = metaIndexData.length;
        writeRawBlock(metaIndexData, COMPRESSION_NONE);

        // Write index block (use saved handle, not the overwritten one)
        if (pendingIndexEntry) {
            // FindShortSuccessor: for binary keys, just use lastKey as-is
            byte[] handleEncoding = encodeBlockHandle(savedHandleOffset, savedHandleSize);
            indexBlock.add(lastKey, handleEncoding);
            pendingIndexEntry = false;
        }
        long indexOffset = fileOffset;
        byte[] indexData = indexBlock.finish();
        long indexSize = indexData.length;
        writeRawBlock(indexData, COMPRESSION_NONE);

        // Write footer
        byte[] footer = encodeFooter(metaIndexOffset, metaIndexSize, indexOffset, indexSize);
        output.write(footer);
        fileOffset += footer.length;

        output.flush();
    }

    @Override
    public void close() throws IOException {
        output.close();
    }

    public long getFileSize() {
        return fileOffset;
    }

    public long getNumEntries() {
        return numEntries;
    }

    // --- Key encoding (order-preserving, compatible with BE encode_integral<int64_t>) ---

    /**
     * Encode int64 key as big-endian with sign-bit flip for order preservation.
     * Matches BE's encoding_utils::encode_integral<int64_t>.
     */
    public static byte[] encodeInt64Key(long rowId) {
        // XOR with MIN_VALUE to flip sign bit (signed → unsigned preserving order)
        long unsigned = rowId ^ Long.MIN_VALUE;
        byte[] key = new byte[8];
        for (int i = 0; i < 8; i++) {
            key[i] = (byte) (unsigned >>> (56 - i * 8));
        }
        return key;
    }

    // --- Value encoding (RowStoreEncoderSimple format, compatible with BE decoder) ---

    /**
     * Encode a row of values in RowStoreEncoderSimple format.
     * Supports: INT (4 bytes LE), BIGINT (8 bytes LE), DOUBLE (8 bytes IEEE754 LE),
     *           VARCHAR/STRING (4 bytes length LE + content).
     * Null values are tracked via an empty Roaring bitmap placeholder.
     *
     * @param values array of column values (Integer, Long, Double, String, or null)
     * @param types  array of column type names ("INT", "BIGINT", "DOUBLE", "VARCHAR")
     */
    public static byte[] encodeRowValue(Object[] values, String[] types) {
        int numCols = values.length;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();

        // Header: version(4B big-endian int32) + num_cols(4B big-endian int32)
        // ROW_STORE_VERSION = 0
        writeInt32BE(buf, 0);
        writeInt32BE(buf, numCols);

        // Null bitmap: for v1 simplicity, all non-null → empty bitmap (size=0)
        // encode_integral<size_t> writes 8 bytes big-endian on 64-bit
        writeInt64BE(buf, 0); // bitmap byte size = 0

        // Encode each column's data and track offsets
        int[] offsets = new int[numCols];
        byte[][] colData = new byte[numCols][];
        for (int i = 0; i < numCols; i++) {
            if (values[i] == null) {
                offsets[i] = 0;
                colData[i] = new byte[0];
            } else {
                colData[i] = serializeColumn(values[i], types[i]);
                offsets[i] = colData[i].length;
            }
        }

        // Offsets: per-column data_size(4B big-endian int32)
        for (int offset : offsets) {
            writeInt32BE(buf, offset);
        }

        // Column data
        for (int i = 0; i < numCols; i++) {
            if (colData[i].length > 0) {
                buf.write(colData[i], 0, colData[i].length);
            }
        }

        return buf.toByteArray();
    }

    private static byte[] serializeColumn(Object value, String type) {
        switch (type.toUpperCase()) {
            case "INT":
            case "INT32":
            case "INTEGER": {
                int v = ((Number) value).intValue();
                // Column::serialize for FixedLengthColumn: memcpy (native byte order = little-endian on x86)
                ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                bb.putInt(v);
                return bb.array();
            }
            case "BIGINT":
            case "INT64":
            case "LONG": {
                long v = ((Number) value).longValue();
                ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                bb.putLong(v);
                return bb.array();
            }
            case "FLOAT": {
                float v = ((Number) value).floatValue();
                ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                bb.putFloat(v);
                return bb.array();
            }
            case "DOUBLE": {
                double v = ((Number) value).doubleValue();
                ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                bb.putDouble(v);
                return bb.array();
            }
            case "VARCHAR":
            case "STRING":
            case "CHAR": {
                byte[] strBytes = value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                // BinaryColumn::serialize: uint32_t length (native/LE) + content
                // The serialize format is: column_separator(1B 0x00) + length(4B LE) + data
                // Actually looking at BinaryColumn::serialize more carefully:
                // binary_size as uint32_t LE + data bytes
                ByteBuffer bb = ByteBuffer.allocate(4 + strBytes.length).order(ByteOrder.LITTLE_ENDIAN);
                bb.putInt(strBytes.length);
                bb.put(strBytes);
                return bb.array();
            }
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type);
        }
    }

    private static void writeInt32BE(ByteArrayOutputStream buf, int value) {
        // encode_integral<int32_t>: XOR sign bit + big-endian
        int unsigned = value ^ Integer.MIN_VALUE;
        buf.write((unsigned >>> 24) & 0xFF);
        buf.write((unsigned >>> 16) & 0xFF);
        buf.write((unsigned >>> 8) & 0xFF);
        buf.write(unsigned & 0xFF);
    }

    private static void writeInt64BE(ByteArrayOutputStream buf, long value) {
        // encode_integral<size_t>: unsigned, so just big-endian (no XOR for unsigned types)
        // size_t is unsigned, so to_bigendian without XOR
        buf.write((int) ((value >>> 56) & 0xFF));
        buf.write((int) ((value >>> 48) & 0xFF));
        buf.write((int) ((value >>> 40) & 0xFF));
        buf.write((int) ((value >>> 32) & 0xFF));
        buf.write((int) ((value >>> 24) & 0xFF));
        buf.write((int) ((value >>> 16) & 0xFF));
        buf.write((int) ((value >>> 8) & 0xFF));
        buf.write((int) (value & 0xFF));
    }

    // --- Block writing ---

    private void writeBlock(BlockBuilder block) throws IOException {
        byte[] raw = block.finish();
        byte[] blockContents;
        byte compressionType;

        // Try snappy compression
        try {
            byte[] compressed = Snappy.compress(raw);
            if (compressed.length < raw.length - (raw.length / 8)) {
                blockContents = compressed;
                compressionType = COMPRESSION_SNAPPY;
            } else {
                blockContents = raw;
                compressionType = COMPRESSION_NONE;
            }
        } catch (Exception e) {
            blockContents = raw;
            compressionType = COMPRESSION_NONE;
        }

        writeRawBlock(blockContents, compressionType);
        block.reset();
    }

    private void writeRawBlock(byte[] blockContents, byte compressionType) throws IOException {
        pendingHandleOffset = fileOffset;
        pendingHandleSize = blockContents.length;

        output.write(blockContents);

        // Trailer: type(1B) + crc32c_masked(4B LE)
        byte[] trailer = new byte[BLOCK_TRAILER_SIZE];
        trailer[0] = compressionType;

        // CRC32C over block_contents + type byte
        int crc = computeCrc32c(blockContents, compressionType);
        int masked = maskCrc(crc);
        putFixed32(trailer, 1, masked);

        output.write(trailer);
        fileOffset += blockContents.length + BLOCK_TRAILER_SIZE;
    }

    private static int computeCrc32c(byte[] data, byte type) {
        com.google.common.hash.Hasher hasher = CRC32C.newHasher();
        hasher.putBytes(data);
        hasher.putByte(type);
        return hasher.hash().asInt();
    }

    private static int maskCrc(int crc) {
        return ((crc >>> 15) | (crc << 17)) + CRC_MASK_DELTA;
    }

    // --- Encoding utilities ---

    private static byte[] encodeBlockHandle(long offset, long size) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(20);
        putVarint64(buf, offset);
        putVarint64(buf, size);
        return buf.toByteArray();
    }

    private static byte[] encodeFooter(long metaIndexOffset, long metaIndexSize,
                                        long indexOffset, long indexSize) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(FOOTER_ENCODED_LENGTH);
        // metaindex handle
        putVarint64(buf, metaIndexOffset);
        putVarint64(buf, metaIndexSize);
        // index handle
        putVarint64(buf, indexOffset);
        putVarint64(buf, indexSize);
        // Padding to 40 bytes (2 * MaxEncodedLength = 40)
        while (buf.size() < 40) {
            buf.write(0);
        }
        // Magic number: 8 bytes little-endian, stored as two 4-byte LE ints
        putFixed32LE(buf, (int) (TABLE_MAGIC_NUMBER & 0xFFFFFFFFL));
        putFixed32LE(buf, (int) (TABLE_MAGIC_NUMBER >>> 32));
        return buf.toByteArray();
    }

    private static void putVarint64(ByteArrayOutputStream buf, long value) {
        // LevelDB varint64 encoding
        long v = value;
        while (v >= 0x80) {
            buf.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        buf.write((int) v);
    }

    private static void putFixed32(byte[] dst, int offset, int value) {
        // Little-endian
        dst[offset] = (byte) value;
        dst[offset + 1] = (byte) (value >>> 8);
        dst[offset + 2] = (byte) (value >>> 16);
        dst[offset + 3] = (byte) (value >>> 24);
    }

    private static void putFixed32LE(ByteArrayOutputStream buf, int value) {
        buf.write(value & 0xFF);
        buf.write((value >>> 8) & 0xFF);
        buf.write((value >>> 16) & 0xFF);
        buf.write((value >>> 24) & 0xFF);
    }

    // --- BlockBuilder (prefix-compressed block) ---

    static class BlockBuilder {
        private final int restartInterval;
        private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final List<Integer> restarts = new ArrayList<>();
        private int counter = 0;
        private byte[] lastKey = new byte[0];

        BlockBuilder(int restartInterval) {
            this.restartInterval = restartInterval;
            restarts.add(0); // First restart point at offset 0
        }

        void add(byte[] key, byte[] value) {
            int shared = 0;
            if (counter < restartInterval) {
                int minLength = Math.min(lastKey.length, key.length);
                while (shared < minLength && lastKey[shared] == key[shared]) {
                    shared++;
                }
            } else {
                restarts.add(buffer.size());
                counter = 0;
            }
            int nonShared = key.length - shared;

            // shared_bytes(varint32) + unshared_bytes(varint32) + value_length(varint32)
            putVarint32(buffer, shared);
            putVarint32(buffer, nonShared);
            putVarint32(buffer, value.length);

            // key delta + value
            buffer.write(key, shared, nonShared);
            buffer.write(value, 0, value.length);

            lastKey = key.clone();
            counter++;
        }

        byte[] finish() {
            // Append restart array (each as fixed32 LE)
            for (int restart : restarts) {
                putFixed32LE(buffer, restart);
            }
            // Append num_restarts (fixed32 LE)
            putFixed32LE(buffer, restarts.size());
            return buffer.toByteArray();
        }

        int currentSizeEstimate() {
            return buffer.size() + restarts.size() * 4 + 4;
        }

        boolean isEmpty() {
            return buffer.size() == 0;
        }

        void reset() {
            buffer = new ByteArrayOutputStream();
            restarts.clear();
            restarts.add(0);
            counter = 0;
            lastKey = new byte[0];
        }

        private static void putVarint32(ByteArrayOutputStream buf, int value) {
            int v = value;
            while (v >= 0x80) {
                buf.write((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            buf.write(v);
        }

        private static void putFixed32LE(ByteArrayOutputStream buf, int value) {
            buf.write(value & 0xFF);
            buf.write((value >>> 8) & 0xFF);
            buf.write((value >>> 16) & 0xFF);
            buf.write((value >>> 24) & 0xFF);
        }
    }

    // --- FilterBlockBuilder (LevelDB bloom filter) ---

    static class FilterBlockBuilder {
        private final ByteArrayOutputStream result = new ByteArrayOutputStream();
        private final List<Integer> filterOffsets = new ArrayList<>();
        private final ByteArrayOutputStream keys = new ByteArrayOutputStream();
        private final List<Integer> starts = new ArrayList<>();
        // Bloom filter params: k = floor(bits_per_key * 0.69)
        private final int bitsPerKey = BITS_PER_KEY;
        private final int k;

        FilterBlockBuilder() {
            int numProbes = (int) (bitsPerKey * 0.69);
            if (numProbes < 1) {
                numProbes = 1;
            }
            if (numProbes > 30) {
                numProbes = 30;
            }
            this.k = numProbes;
        }

        void startBlock(long blockOffset) {
            long filterIndex = blockOffset / FILTER_BASE;
            while (filterIndex > filterOffsets.size()) {
                generateFilter();
            }
        }

        void addKey(byte[] key) {
            starts.add(keys.size());
            keys.write(key, 0, key.length);
        }

        byte[] finish() {
            if (!starts.isEmpty()) {
                generateFilter();
            }
            // Append array of per-filter offsets
            int arrayOffset = result.size();
            for (int offset : filterOffsets) {
                putFixed32LE(result, offset);
            }
            putFixed32LE(result, arrayOffset);
            result.write(FILTER_BASE_LG);
            return result.toByteArray();
        }

        private void generateFilter() {
            int numKeys = starts.size();
            if (numKeys == 0) {
                filterOffsets.add(result.size());
                return;
            }

            byte[] keysData = keys.toByteArray();
            starts.add(keysData.length); // Simplify length computation

            // Compute bloom filter
            int bits = numKeys * bitsPerKey;
            if (bits < 64) {
                bits = 64;
            }
            int bytes = (bits + 7) / 8;
            bits = bytes * 8;

            filterOffsets.add(result.size());
            byte[] filter = new byte[bytes + 1]; // +1 for k
            for (int i = 0; i < numKeys; i++) {
                int start = starts.get(i);
                int end = starts.get(i + 1);
                byte[] keyBytes = new byte[end - start];
                System.arraycopy(keysData, start, keyBytes, 0, keyBytes.length);

                // MurmurHash3 x86_32 with seed 0xbc9f1d34
                int h = murmurHash3x8632(keyBytes, 0xbc9f1d34);
                int delta = (h >>> 17) | (h << 15); // Rotate right 17 bits
                for (int j = 0; j < k; j++) {
                    int bitpos = Integer.remainderUnsigned(h, bits);
                    filter[bitpos / 8] |= (byte) (1 << (bitpos % 8));
                    h += delta;
                }
            }
            filter[bytes] = (byte) k; // Remember # of probes
            result.write(filter, 0, filter.length);

            // Clear
            keys.reset();
            starts.clear();
        }

        private static void putFixed32LE(ByteArrayOutputStream buf, int value) {
            buf.write(value & 0xFF);
            buf.write((value >>> 8) & 0xFF);
            buf.write((value >>> 16) & 0xFF);
            buf.write((value >>> 24) & 0xFF);
        }

        /**
         * MurmurHash3 x86 32-bit, matching BE's murmur_hash3_x86_32.
         */
        @SuppressWarnings("fallthrough")
        static int murmurHash3x8632(byte[] data, int seed) {
            int len = data.length;
            int h1 = seed;
            int c1 = 0xcc9e2d51;
            int c2 = 0x1b873593;

            int nblocks = len / 4;
            for (int i = 0; i < nblocks; i++) {
                int k1 = getBlock32(data, i * 4);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
                h1 = Integer.rotateLeft(h1, 13);
                h1 = h1 * 5 + 0xe6546b64;
            }

            // tail
            int tail = nblocks * 4;
            int k1 = 0;
            switch (len & 3) {
                case 3:
                    k1 ^= (data[tail + 2] & 0xFF) << 16;
                case 2:
                    k1 ^= (data[tail + 1] & 0xFF) << 8;
                case 1:
                    k1 ^= (data[tail] & 0xFF);
                    k1 *= c1;
                    k1 = Integer.rotateLeft(k1, 15);
                    k1 *= c2;
                    h1 ^= k1;
            }

            // finalization
            h1 ^= len;
            h1 = fmix32(h1);
            return h1;
        }

        private static int getBlock32(byte[] data, int offset) {
            return (data[offset] & 0xFF) |
                    ((data[offset + 1] & 0xFF) << 8) |
                    ((data[offset + 2] & 0xFF) << 16) |
                    ((data[offset + 3] & 0xFF) << 24);
        }

        private static int fmix32(int h) {
            h ^= h >>> 16;
            h *= 0x85ebca6b;
            h ^= h >>> 13;
            h *= 0xc2b2ae35;
            h ^= h >>> 16;
            return h;
        }
    }
}
