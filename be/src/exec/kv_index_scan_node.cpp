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

#include "exec/kv_index_scan_node.h"

#include "exec/pipeline/kv_index_scan_operator.h"
#include "exec/pipeline/pipeline_builder.h"

namespace starrocks {

KVIndexScanNode::KVIndexScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {}

Status KVIndexScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    if (tnode.__isset.kv_index_scan_node) {
        const auto& kv_node = tnode.kv_index_scan_node;
        if (kv_node.__isset.sst_file_path) {
            _sst_file_path = kv_node.sst_file_path;
        }
        if (kv_node.__isset.value_column_names) {
            _value_column_names = kv_node.value_column_names;
        }
        if (kv_node.__isset.value_column_types) {
            _value_column_types = kv_node.value_column_types;
        }
        if (kv_node.__isset.tuple_id) {
            _tuple_id = kv_node.tuple_id;
        }
    }
    return Status::OK();
}

StatusOr<pipeline::OpFactories> KVIndexScanNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    auto* desc = context->runtime_state()->desc_tbl().get_tuple_descriptor(_tuple_id);
    auto op_factory = std::make_shared<pipeline::KVIndexScanOpFactory>(
            context->next_operator_id(), id(), _sst_file_path, _value_column_names, _value_column_types, desc);
    return pipeline::OpFactories{std::move(op_factory)};
}

} // namespace starrocks
