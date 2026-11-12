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

package com.starrocks.connector.flink.table.sink.v2;

import com.starrocks.connector.flink.table.sink.StarrocksSnapshotState;
import com.starrocks.connector.flink.tools.JsonWrapper;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class StarRocksWriterStateSerializer implements SimpleVersionedSerializer<StarRocksWriterState> {

    private final JsonWrapper jsonWrapper;

    public StarRocksWriterStateSerializer() {
        this.jsonWrapper = new JsonWrapper();
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(StarRocksWriterState state) throws IOException {
        return jsonWrapper.toJSONBytes(state);
    }

    @Override
    public StarRocksWriterState deserialize(int version, byte[] serialized) throws IOException {
        // Try to deserialize as StarRocksWriterState first (new format).
        // Fall back to StarrocksSnapshotState for backward compatibility with
        // checkpoints created by older connector versions, and convert it.
        try {
            return jsonWrapper.parseObject(serialized, StarRocksWriterState.class);
        } catch (Exception e) {
            StarrocksSnapshotState snapshotState = jsonWrapper.parseObject(serialized, StarrocksSnapshotState.class);
            return new StarRocksWriterState(snapshotState.getLabelSnapshots());
        }
    }
}
