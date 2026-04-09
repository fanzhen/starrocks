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

#include "exec/exec_node.h"

namespace starrocks {

class KVIndexScanNode final : public ExecNode {
public:
    KVIndexScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~KVIndexScanNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    StatusOr<pipeline::OpFactories> decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

    const std::string& sst_file_path() const { return _sst_file_path; }
    const std::vector<std::string>& value_column_names() const { return _value_column_names; }
    const std::vector<std::string>& value_column_types() const { return _value_column_types; }
    TupleId tuple_id() const { return _tuple_id; }

private:
    std::string _sst_file_path;
    std::vector<std::string> _value_column_names;
    std::vector<std::string> _value_column_types;
    TupleId _tuple_id = 0;
};

} // namespace starrocks
