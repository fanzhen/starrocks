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

package com.starrocks.common.proc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.DaftCoordinatorException;
import com.starrocks.coordinator.proto.StatusResponse;
import com.starrocks.service.DaftCoordinatorClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class DaftCoordinatorProcNode implements ProcNodeInterface {
    private static final Logger LOG = LogManager.getLogger(DaftCoordinatorProcNode.class);

    public static final ImmutableList<String> TITLE_NAMES = ImmutableList.of(
            "Key", "Value");

    @Override
    public ProcResult fetchResult() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        if (!Config.enable_daft_coordinator) {
            result.addRow(Lists.newArrayList("Status", "DISABLED"));
            result.addRow(Lists.newArrayList("Host", Config.daft_coordinator_host));
            result.addRow(Lists.newArrayList("Port", String.valueOf(Config.daft_coordinator_port)));
            return result;
        }

        DaftCoordinatorClient client = new DaftCoordinatorClient(
                Config.daft_coordinator_host, Config.daft_coordinator_port);
        try {
            StatusResponse response = client.getStatus();
            result.addRow(Lists.newArrayList("Status", response.getStatus()));
            result.addRow(Lists.newArrayList("RegisteredFunctions",
                    String.valueOf(response.getRegisteredFunctions())));

            Map<String, String> resources = response.getRayResourcesMap();
            result.addRow(Lists.newArrayList("RayResources",
                    resources.isEmpty() ? "{}" : resources.toString()));
            result.addRow(Lists.newArrayList("Host", Config.daft_coordinator_host));
            result.addRow(Lists.newArrayList("Port", String.valueOf(Config.daft_coordinator_port)));
        } catch (DaftCoordinatorException e) {
            LOG.warn("Failed to query Daft Coordinator", e);
            result.addRow(Lists.newArrayList("Status", "DOWN"));
            result.addRow(Lists.newArrayList("Error", e.getMessage()));
            result.addRow(Lists.newArrayList("Host", Config.daft_coordinator_host));
            result.addRow(Lists.newArrayList("Port", String.valueOf(Config.daft_coordinator_port)));
        } finally {
            client.close();
        }

        return result;
    }
}
