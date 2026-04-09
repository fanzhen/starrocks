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

#include "exec/pipeline/kv_index_scan_operator.h"

#include "column/chunk.h"
#include "column/field.h"
#include "column/schema.h"
#include "fs/fs_util.h"
#include "runtime/descriptors.h"
#include "storage/kv_index/kv_index_reader.h"
#include "types/logical_type.h"

namespace starrocks::pipeline {

// === KVIndexScanOperator ===

KVIndexScanOperator::KVIndexScanOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                         int32_t driver_sequence, std::string sst_file_path,
                                         std::vector<std::string> value_column_names,
                                         std::vector<std::string> value_column_types,
                                         const TupleDescriptor* tuple_desc)
        : SourceOperator(factory, id, "kv_index_scan", plan_node_id, false, driver_sequence),
          _sst_file_path(std::move(sst_file_path)),
          _value_column_names(std::move(value_column_names)),
          _value_column_types(std::move(value_column_types)),
          _tuple_desc(tuple_desc) {}

StatusOr<ChunkPtr> KVIndexScanOperator::pull_chunk(RuntimeState* state) {
    _is_finished = true;

    // Build value schema from column names/types
    Fields fields;
    for (size_t i = 0; i < _value_column_names.size(); i++) {
        LogicalType ltype = string_to_logical_type(_value_column_types[i]);
        if (ltype == TYPE_UNKNOWN) {
            return Status::InvalidArgument(fmt::format("Unknown column type: {}", _value_column_types[i]));
        }
        auto field = std::make_shared<Field>(static_cast<ColumnId>(i), _value_column_names[i], ltype, true);
        fields.push_back(field);
    }
    Schema value_schema(std::move(fields), KeysType::DUP_KEYS, std::vector<ColumnId>{});

    // Open SSTable file
    ASSIGN_OR_RETURN(auto file, fs::new_random_access_file(_sst_file_path));
    ASSIGN_OR_RETURN(auto file_size_signed, file->get_size());
    uint64_t file_size = static_cast<uint64_t>(file_size_signed);
    ASSIGN_OR_RETURN(auto reader, KVIndexReader::open(value_schema, file.get(), file_size));

    // Full scan
    ASSIGN_OR_RETURN(auto full_chunk, reader->scan_all());

    if (full_chunk == nullptr || full_chunk->num_rows() == 0) {
        return std::make_shared<Chunk>();
    }

    // Map KV index columns to output slots based on column name matching.
    // The output chunk must have columns in the order defined by _tuple_desc slots.
    auto output_chunk = std::make_shared<Chunk>();
    const auto& slots = _tuple_desc->slots();
    for (auto* slot : slots) {
        bool found = false;
        for (size_t kv_idx = 0; kv_idx < _value_column_names.size(); kv_idx++) {
            if (slot->col_name() == _value_column_names[kv_idx]) {
                output_chunk->append_column(full_chunk->get_column_by_index(kv_idx), slot->id());
                found = true;
                break;
            }
        }
        if (!found) {
            return Status::InternalError(
                    fmt::format("Column '{}' not found in KV index", slot->col_name()));
        }
    }

    return output_chunk;
}

// === KVIndexScanOpFactory ===

KVIndexScanOpFactory::KVIndexScanOpFactory(int32_t id, int32_t plan_node_id, std::string sst_file_path,
                                           std::vector<std::string> value_column_names,
                                           std::vector<std::string> value_column_types,
                                           const TupleDescriptor* tuple_desc)
        : SourceOperatorFactory(id, "kv_index_scan", plan_node_id),
          _sst_file_path(std::move(sst_file_path)),
          _value_column_names(std::move(value_column_names)),
          _value_column_types(std::move(value_column_types)),
          _tuple_desc(tuple_desc) {}

OperatorPtr KVIndexScanOpFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<KVIndexScanOperator>(this, _id, _plan_node_id, driver_sequence, _sst_file_path,
                                                 _value_column_names, _value_column_types, _tuple_desc);
}

} // namespace starrocks::pipeline
