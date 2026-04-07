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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {

class WritableFile;
class Slice;
class Column;
class Chunk;

namespace sstable {
class TableBuilder;
class FilterPolicy;
struct Options;
} // namespace sstable

// KVIndexWriter writes _ROW_ID → {value_cols} mappings into an SSTable file.
// Keys must be added in strictly increasing order.
class KVIndexWriter {
public:
    // value_schema: Schema of value columns only (num_key_fields = 0).
    KVIndexWriter(const Schema& value_schema, WritableFile* file);
    ~KVIndexWriter();

    // Add a batch of KV pairs.
    // keys: Int64Column containing _ROW_ID values, must be strictly increasing
    //        across all add_chunk calls.
    // value_chunk: Chunk containing value columns matching value_schema.
    Status add_chunk(const Column& keys, const Chunk& value_chunk);

    // Finalize the SSTable. Must be called after all add_chunk calls.
    Status finish();

    uint64_t file_size() const;

    // Returns [min_key, max_key] as encoded Slices.
    // Only valid after finish() is called.
    std::pair<Slice, Slice> key_range() const;

private:
    Schema _value_schema;
    std::unique_ptr<const sstable::FilterPolicy> _filter_policy;
    std::unique_ptr<sstable::TableBuilder> _builder;
    int64_t _last_key = std::numeric_limits<int64_t>::min();
    bool _has_data = false;
};

} // namespace starrocks
