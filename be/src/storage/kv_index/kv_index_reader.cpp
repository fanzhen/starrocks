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

#include "storage/kv_index/kv_index_reader.h"

#include <algorithm>
#include <numeric>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/nullable_column.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/row_store_encoder_simple.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table.h"

namespace starrocks {

KVIndexReader::~KVIndexReader() {
    delete _table;
}

StatusOr<std::unique_ptr<KVIndexReader>> KVIndexReader::open(const Schema& value_schema, RandomAccessFile* file,
                                                              uint64_t file_size) {
    auto reader = std::unique_ptr<KVIndexReader>(new KVIndexReader());
    reader->_value_schema = value_schema;
    reader->_filter_policy.reset(sstable::NewBloomFilterPolicy(10));

    sstable::Options options;
    options.filter_policy = reader->_filter_policy.get();

    sstable::Table* table = nullptr;
    RETURN_IF_ERROR(sstable::Table::Open(options, file, file_size, &table));
    reader->_table = table;

    return reader;
}

StatusOr<ChunkUniquePtr> KVIndexReader::multi_get(const std::vector<int64_t>& keys, std::vector<bool>* found_mask) {
    size_t num_keys = keys.size();
    found_mask->assign(num_keys, false);

    // Create output chunk with nullable columns
    size_t num_value_cols = _value_schema.num_fields();
    ChunkUniquePtr result = ChunkHelper::new_chunk(_value_schema, num_keys);

    // Make all columns nullable and fill with NULLs initially
    auto mcols = result->mutable_columns();
    for (size_t col_idx = 0; col_idx < num_value_cols; col_idx++) {
        mcols[col_idx]->resize(num_keys);
        if (mcols[col_idx]->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(mcols[col_idx].get());
            nullable->null_column_data().assign(num_keys, 1);
            nullable->set_has_null(true);
        }
    }

    if (num_keys == 0) {
        return result;
    }

    // Sort keys by value while tracking original positions
    std::vector<uint32_t> sorted_indices(num_keys);
    std::iota(sorted_indices.begin(), sorted_indices.end(), 0);
    std::sort(sorted_indices.begin(), sorted_indices.end(),
              [&keys](uint32_t a, uint32_t b) { return keys[a] < keys[b]; });

    // Use SSTable iterator for sequential seek
    sstable::ReadOptions read_options;
    std::unique_ptr<sstable::Iterator> iter(_table->NewIterator(read_options));

    // Prepare read_column_ids for decode: all value columns
    std::vector<uint32_t> read_column_ids;
    for (size_t i = 0; i < num_value_cols; i++) {
        read_column_ids.push_back(static_cast<uint32_t>(i));
    }

    // Batch: collect found entries, then decode in bulk
    auto encoded_batch = BinaryColumn::create();
    std::vector<uint32_t> found_positions; // original positions of found keys

    for (uint32_t sorted_pos : sorted_indices) {
        int64_t key = keys[sorted_pos];

        std::string encoded_key;
        encoding_utils::encode_integral<int64_t>(key, &encoded_key);

        iter->Seek(Slice(encoded_key));
        if (!iter->Valid() || !iter->status().ok()) {
            continue;
        }
        if (iter->key() != Slice(encoded_key)) {
            continue;
        }

        encoded_batch->append(iter->value());
        found_positions.push_back(sorted_pos);
        (*found_mask)[sorted_pos] = true;
    }

    if (found_positions.empty()) {
        return result;
    }

    // Decode all found values at once
    MutableColumns decoded_columns;
    for (size_t i = 0; i < num_value_cols; i++) {
        decoded_columns.push_back(ChunkHelper::column_from_field(*_value_schema.field(i)));
    }

    RowStoreEncoderSimple decoder;
    RETURN_IF_ERROR(decoder.decode_columns_from_full_row_column(
            _value_schema, *encoded_batch, read_column_ids, &decoded_columns));

    // Scatter decoded values into result at original positions
    // update_rows copies src[i] → dest[indexes[i]], so we need indexes = found_positions
    // and src must have found_positions.size() rows (which decoded_columns does)
    for (size_t col_idx = 0; col_idx < num_value_cols; col_idx++) {
        mcols[col_idx]->update_rows(*decoded_columns[col_idx], found_positions.data());
    }

    return result;
}

StatusOr<ChunkUniquePtr> KVIndexReader::scan_all() {
    static constexpr size_t kDecodeBatchSize = 4096;

    sstable::ReadOptions read_options;
    std::unique_ptr<sstable::Iterator> iter(_table->NewIterator(read_options));

    size_t num_value_cols = _value_schema.num_fields();
    std::vector<uint32_t> read_column_ids;
    for (size_t i = 0; i < num_value_cols; i++) {
        read_column_ids.push_back(static_cast<uint32_t>(i));
    }

    // Create output columns once
    MutableColumns output_columns;
    for (size_t i = 0; i < num_value_cols; i++) {
        output_columns.push_back(ChunkHelper::column_from_field(*_value_schema.field(i)));
    }

    // Batch decode: collect encoded values, then decode in bulk
    RowStoreEncoderSimple decoder;
    auto encoded_batch = BinaryColumn::create();
    encoded_batch->reserve(kDecodeBatchSize);

    iter->SeekToFirst();
    while (iter->Valid()) {
        // Collect a batch of encoded values
        encoded_batch->reset_column();
        size_t batch_count = 0;
        while (iter->Valid() && batch_count < kDecodeBatchSize) {
            encoded_batch->append(iter->value());
            iter->Next();
            batch_count++;
        }

        if (batch_count == 0) break;

        // Decode entire batch at once into output columns directly
        RETURN_IF_ERROR(decoder.decode_columns_from_full_row_column(
                _value_schema, *encoded_batch, read_column_ids, &output_columns));
    }
    RETURN_IF_ERROR(iter->status());

    // Build chunk from columns
    auto chunk = std::make_unique<Chunk>();
    for (size_t i = 0; i < num_value_cols; i++) {
        chunk->append_column(std::move(output_columns[i]), static_cast<SlotId>(i));
    }

    return chunk;
}

} // namespace starrocks
