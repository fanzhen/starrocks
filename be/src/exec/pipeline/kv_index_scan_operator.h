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

#include <memory>

#include "column/chunk.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks {
class TupleDescriptor;
class KVIndexReader;
} // namespace starrocks

namespace starrocks::pipeline {

class KVIndexScanOperator final : public SourceOperator {
public:
    KVIndexScanOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                        std::string sst_file_path, std::vector<std::string> value_column_names,
                        std::vector<std::string> value_column_types, const TupleDescriptor* tuple_desc);

    ~KVIndexScanOperator() override = default;

    bool has_output() const override { return !_is_finished; }
    bool is_finished() const override { return _is_finished; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    Status _init_reader();

    std::string _sst_file_path;
    std::vector<std::string> _value_column_names;
    std::vector<std::string> _value_column_types;
    const TupleDescriptor* _tuple_desc;
    bool _is_finished = false;

    // Streaming scan state
    std::unique_ptr<KVIndexReader> _reader;
    bool _reader_initialized = false;
    // Column name to KV index column index mapping
    std::vector<std::pair<SlotId, size_t>> _slot_to_kv_col;
};

class KVIndexScanOpFactory final : public SourceOperatorFactory {
public:
    KVIndexScanOpFactory(int32_t id, int32_t plan_node_id, std::string sst_file_path,
                         std::vector<std::string> value_column_names, std::vector<std::string> value_column_types,
                         const TupleDescriptor* tuple_desc);

    ~KVIndexScanOpFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }

private:
    std::string _sst_file_path;
    std::vector<std::string> _value_column_names;
    std::vector<std::string> _value_column_types;
    const TupleDescriptor* _tuple_desc;
};

} // namespace starrocks::pipeline
