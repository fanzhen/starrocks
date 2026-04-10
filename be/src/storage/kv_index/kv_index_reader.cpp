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

StatusOr<std::unique_ptr<KVIndexReader>> KVIndexReader::open(const Schema& value_schema,
                                                              std::unique_ptr<RandomAccessFile> file,
                                                              uint64_t file_size) {
    ASSIGN_OR_RETURN(auto reader, open(value_schema, file.get(), file_size));
    reader->_owned_file = std::move(file);
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

    // Collect found entries, decode directly via Column::deserialize_and_append
    MutableColumns decoded_columns;
    for (size_t i = 0; i < num_value_cols; i++) {
        decoded_columns.push_back(ChunkHelper::column_from_field(*_value_schema.field(i)));
    }
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

        // Deserialize value directly into decoded columns
        Slice value = iter->value();
        const uint8_t* pos = reinterpret_cast<const uint8_t*>(value.data);
        for (size_t col = 0; col < num_value_cols; col++) {
            pos = decoded_columns[col]->deserialize_and_append(pos);
        }

        found_positions.push_back(sorted_pos);
        (*found_mask)[sorted_pos] = true;
    }

    if (found_positions.empty()) {
        return result;
    }

    // Scatter decoded values into result at original positions
    for (size_t col_idx = 0; col_idx < num_value_cols; col_idx++) {
        mcols[col_idx]->update_rows(*decoded_columns[col_idx], found_positions.data());
    }

    return result;
}

StatusOr<ChunkUniquePtr> KVIndexReader::scan_all() {
    sstable::ReadOptions read_options;
    std::unique_ptr<sstable::Iterator> iter(_table->NewIterator(read_options));

    size_t num_value_cols = _value_schema.num_fields();

    // Create output columns once
    MutableColumns output_columns;
    for (size_t i = 0; i < num_value_cols; i++) {
        output_columns.push_back(ChunkHelper::column_from_field(*_value_schema.field(i)));
    }

    // Deserialize directly via Column::deserialize_and_append
    iter->SeekToFirst();
    while (iter->Valid()) {
        Slice value = iter->value();
        const uint8_t* pos = reinterpret_cast<const uint8_t*>(value.data);
        for (size_t col = 0; col < num_value_cols; col++) {
            pos = output_columns[col]->deserialize_and_append(pos);
        }
        iter->Next();
    }
    RETURN_IF_ERROR(iter->status());

    // Build chunk from columns
    auto chunk = std::make_unique<Chunk>();
    for (size_t i = 0; i < num_value_cols; i++) {
        chunk->append_column(std::move(output_columns[i]), static_cast<SlotId>(i));
    }

    return chunk;
}

Status KVIndexReader::init_scan() {
    sstable::ReadOptions read_options;
    _scan_iter.reset(_table->NewIterator(read_options));
    _scan_iter->SeekToFirst();
    return _scan_iter->status();
}

StatusOr<ChunkUniquePtr> KVIndexReader::scan_batch(size_t batch_size) {
    size_t num_value_cols = _value_schema.num_fields();

    MutableColumns output_columns;
    for (size_t i = 0; i < num_value_cols; i++) {
        output_columns.push_back(ChunkHelper::column_from_field(*_value_schema.field(i)));
    }

    size_t count = 0;
    while (_scan_iter->Valid() && count < batch_size) {
        Slice value = _scan_iter->value();
        const uint8_t* pos = reinterpret_cast<const uint8_t*>(value.data);
        for (size_t col = 0; col < num_value_cols; col++) {
            pos = output_columns[col]->deserialize_and_append(pos);
        }
        _scan_iter->Next();
        count++;
    }
    RETURN_IF_ERROR(_scan_iter->status());

    auto chunk = std::make_unique<Chunk>();
    for (size_t i = 0; i < num_value_cols; i++) {
        chunk->append_column(std::move(output_columns[i]), static_cast<SlotId>(i));
    }

    return chunk;
}

} // namespace starrocks
