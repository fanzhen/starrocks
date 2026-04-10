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
#include "fs/fs.h"

namespace sstable {
class Table;
class Iterator;
class FilterPolicy;
} // namespace sstable

// KVIndexReader reads _ROW_ID → {value_cols} mappings from an SSTable file.
// Supports batch point lookups via multi_get, full scan via scan_all,
// and streaming scan via init_scan / scan_batch.
class KVIndexReader {
public:
    ~KVIndexReader();

    // Open an SSTable file for reading (non-owning file pointer, caller must keep file alive).
    static StatusOr<std::unique_ptr<KVIndexReader>> open(const Schema& value_schema, RandomAccessFile* file,
                                                          uint64_t file_size);

    // Open an SSTable file for reading (takes ownership of file).
    static StatusOr<std::unique_ptr<KVIndexReader>> open(const Schema& value_schema,
                                                          std::unique_ptr<RandomAccessFile> file,
                                                          uint64_t file_size);

    // Batch point lookup.
    StatusOr<ChunkUniquePtr> multi_get(const std::vector<int64_t>& keys, std::vector<bool>* found_mask);

    // Full scan: iterate through all entries and return a single chunk.
    StatusOr<ChunkUniquePtr> scan_all();

    // Streaming scan: initialize iterator, then call scan_batch repeatedly.
    Status init_scan();

    // Read up to batch_size rows into a new chunk. Returns empty chunk when exhausted.
    StatusOr<ChunkUniquePtr> scan_batch(size_t batch_size);

private:
    KVIndexReader() = default;

    Schema _value_schema;
    std::unique_ptr<const sstable::FilterPolicy> _filter_policy;
    std::unique_ptr<RandomAccessFile> _owned_file; // optional: keeps file alive when reader owns it
    sstable::Table* _table = nullptr;

    // Streaming scan state
    std::unique_ptr<sstable::Iterator> _scan_iter;
};

} // namespace starrocks
