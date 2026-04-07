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
#include <vector>

#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {

class RandomAccessFile;

namespace sstable {
class Table;
class FilterPolicy;
} // namespace sstable

// KVIndexReader reads _ROW_ID → {value_cols} mappings from an SSTable file.
// Supports batch point lookups via multi_get.
class KVIndexReader {
public:
    ~KVIndexReader();

    // Open an SSTable file for reading.
    // value_schema: Schema of value columns (num_key_fields = 0).
    static StatusOr<std::unique_ptr<KVIndexReader>> open(const Schema& value_schema, RandomAccessFile* file,
                                                          uint64_t file_size);

    // Batch point lookup.
    // keys: list of _ROW_ID values to look up (any order).
    // found_mask: output, found_mask[i] = true if keys[i] was found.
    // Returns a Chunk with value columns. Found rows contain actual values,
    // not-found rows contain NULLs.
    StatusOr<ChunkUniquePtr> multi_get(const std::vector<int64_t>& keys, std::vector<bool>* found_mask);

private:
    KVIndexReader() = default;

    Schema _value_schema;
    std::unique_ptr<const sstable::FilterPolicy> _filter_policy;
    sstable::Table* _table = nullptr;
};

} // namespace starrocks
