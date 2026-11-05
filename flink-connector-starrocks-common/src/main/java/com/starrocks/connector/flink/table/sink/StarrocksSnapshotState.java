/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.table.sink;

import com.starrocks.connector.flink.tools.EnvUtils;
import com.starrocks.data.load.stream.StreamLoadSnapshot;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public class StarrocksSnapshotState implements Serializable {
    private String version;
    private Map<Long, List<StreamLoadSnapshot>> data;
    @Nullable
    private List<ExactlyOnceLabelGeneratorSnapshot> labelSnapshots;

    public static StarrocksSnapshotState of(
            Map<Long, List<StreamLoadSnapshot>> data, List<ExactlyOnceLabelGeneratorSnapshot> labelSnapshots) {
        StarrocksSnapshotState starrocksSnapshotState = new StarrocksSnapshotState();
        starrocksSnapshotState.version = EnvUtils.getSRFCVersion();
        starrocksSnapshotState.data = data;
        starrocksSnapshotState.labelSnapshots = labelSnapshots;
        return starrocksSnapshotState;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Map<Long, List<StreamLoadSnapshot>> getData() {
        return data;
    }

    public void setData(Map<Long, List<StreamLoadSnapshot>> data) {
        this.data = data;
    }

    @Nullable
    public List<ExactlyOnceLabelGeneratorSnapshot> getLabelSnapshots() {
        return labelSnapshots;
    }

    public void setLabelSnapshots(@Nullable List<ExactlyOnceLabelGeneratorSnapshot> labelSnapshots) {
        this.labelSnapshots = labelSnapshots;
    }
}
