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

package com.starrocks.planner;

import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TKVIndexScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.Collections;
import java.util.List;

public class KVIndexScanNode extends ScanNode {
    private final String sstFilePath;
    private final List<String> valueColumnNames;
    private final List<String> valueColumnTypes;

    public KVIndexScanNode(PlanNodeId id, TupleDescriptor desc,
                           String sstFilePath,
                           List<String> valueColumnNames,
                           List<String> valueColumnTypes) {
        super(id, desc, "KVIndexScan");
        this.sstFilePath = sstFilePath;
        this.valueColumnNames = valueColumnNames;
        this.valueColumnTypes = valueColumnTypes;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        // KV index scan reads a single local SST file, no distributed scan ranges needed.
        return Collections.emptyList();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.KV_INDEX_SCAN_NODE;
        TKVIndexScanNode kvNode = new TKVIndexScanNode();
        kvNode.setTuple_id(desc.getId().asInt());
        kvNode.setSst_file_path(sstFilePath);
        kvNode.setValue_column_names(valueColumnNames);
        kvNode.setValue_column_types(valueColumnTypes);
        msg.setKv_index_scan_node(kvNode);
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("SST: ").append(sstFilePath).append("\n");
        sb.append(prefix).append("Columns: ").append(String.join(", ", valueColumnNames)).append("\n");
        return sb.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return false;
    }
}
