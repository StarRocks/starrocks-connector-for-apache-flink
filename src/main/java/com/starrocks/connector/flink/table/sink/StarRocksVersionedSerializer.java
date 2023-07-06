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

import com.alibaba.fastjson.JSON;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class StarRocksVersionedSerializer implements SimpleVersionedSerializer<StarrocksSnapshotState> {

    enum SerializerVersion {
        V1(1);
        private final int version;

        SerializerVersion(int version) {
            this.version = version;
        }

        public int getVersion() {
            return version;
        }
    }

    @Override
    public int getVersion() {
        return SerializerVersion.V1.getVersion();
    }

    @Override
    public byte[] serialize(StarrocksSnapshotState state) throws IOException {
        return JSON.toJSONBytes(state);
    }

    @Override
    public StarrocksSnapshotState deserialize(int version, byte[] serialized) throws IOException {
        return JSON.parseObject(serialized, StarrocksSnapshotState.class);
    }
}
