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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.starrocks.connector.flink.table.sink.v2;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;

import com.starrocks.connector.flink.table.sink.ExactlyOnceLabelGeneratorSnapshot;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class StarRocksSinkTest {

    @Mocked
    private Sink.InitContext initContext;

    @Test
    public void testRestoreWriterPassesRecoveredStateToWriter() throws Exception {
        // Replace only the writer constructor, which would otherwise connect to StarRocks,
        // and record the state the sink hands to it.
        AtomicReference<Collection<StarRocksWriterState>> stateGivenToWriter = new AtomicReference<>();
        new MockUp<StarRocksWriter<?>>() {
            @Mock
            public void $init(StarRocksSinkOptions sinkOptions, Sink.InitContext context,
                              SerializationSchema.InitializationContext schemaContext,
                              RecordSerializationSchema<?> serializationSchema,
                              StreamLoadProperties streamLoadProperties,
                              Collection<StarRocksWriterState> recoveredState) {
                stateGivenToWriter.set(recoveredState);
            }
        };

        List<StarRocksWriterState> recoveredState = Collections.singletonList(new StarRocksWriterState(
                Collections.singletonList(new ExactlyOnceLabelGeneratorSnapshot(
                        1L, "db", "table", "label", 2, 0, 3L))));
        StarRocksSink<Object> sink = new StarRocksSink<>(null, null, null);
        sink.restoreWriter(initContext, recoveredState);

        assertEquals(recoveredState, stateGivenToWriter.get());
    }
}
