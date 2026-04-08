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

#include "storage/kv_index/kv_index_writer.h"

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/status.h"
#include "storage/primary_key_encoder.h"
#include "storage/row_store_encoder_simple.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table_builder.h"

namespace starrocks {

KVIndexWriter::KVIndexWriter(const Schema& value_schema, WritableFile* file)
        : _value_schema(value_schema),
          _filter_policy(sstable::NewBloomFilterPolicy(10)) {
    sstable::Options options;
    options.filter_policy = _filter_policy.get();
    options.compression = sstable::kSnappyCompression;
    options.block_size = 4 * 1024;
    _builder = std::make_unique<sstable::TableBuilder>(options, file);
}

KVIndexWriter::~KVIndexWriter() {
    if (_builder && !_finished) {
        _builder->Abandon();
    }
}

Status KVIndexWriter::add_chunk(const Column& keys, const Chunk& value_chunk) {
    size_t num_rows = keys.size();
    if (num_rows == 0) {
        return Status::OK();
    }
    if (num_rows != value_chunk.num_rows()) {
        return Status::InvalidArgument("keys and value_chunk must have the same number of rows");
    }

    // Get raw int64 key data
    auto* key_col = down_cast<const Int64Column*>(&keys);
    const auto& key_data = key_col->get_data();

    // Encode all values using RowStoreEncoderSimple
    Columns value_columns;
    for (size_t i = 0; i < value_chunk.num_columns(); i++) {
        value_columns.push_back(value_chunk.get_column_by_index(i));
    }
    auto encoded_values = BinaryColumn::create();
    RowStoreEncoderSimple encoder;
    RETURN_IF_ERROR(encoder.encode_columns_to_full_row_column(_value_schema, value_columns, *encoded_values));

    // Add each KV pair to SSTable
    for (size_t i = 0; i < num_rows; i++) {
        int64_t key = key_data[i];

        // Check strictly increasing order
        if (_has_data && key <= _last_key) {
            return Status::InvalidArgument(
                    fmt::format("keys must be strictly increasing: got {} after {}", key, _last_key));
        }

        // Encode key as big-endian (order-preserving)
        std::string encoded_key;
        encoding_utils::encode_integral<int64_t>(key, &encoded_key);

        // Get encoded value
        Slice value_slice = encoded_values->get_slice(i);

        RETURN_IF_ERROR(_builder->Add(Slice(encoded_key), value_slice));

        _last_key = key;
        _has_data = true;
    }

    return _builder->status();
}

Status KVIndexWriter::finish() {
    RETURN_IF_ERROR(_builder->Finish());
    _finished = true;
    return Status::OK();
}

uint64_t KVIndexWriter::file_size() const {
    return _builder->FileSize();
}

std::pair<Slice, Slice> KVIndexWriter::key_range() const {
    return _builder->KeyRange();
}

} // namespace starrocks
