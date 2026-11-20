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

import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.connector.flink.table.sink.LingeringTransactionAborterTest.MockStreamLoader;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.mergecommit.MetricListener;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.v2.StreamLoadListener;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import mockit.Mock;
import mockit.MockUp;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class StarRocksDynamicSinkFunctionV2Test {

    @Test
    public void testFirstWriteChecksTablesNotInSinkOptions() throws Exception {
        MockStreamLoader streamLoader = new MockStreamLoader();
        AtomicReference<LabelGeneratorFactory> factoryGivenToManager = mockStarRocks(streamLoader);
        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", "db")
                .withProperty("table-name", "table")
                .withProperty("sink.semantic", "exactly-once")
                .withProperty("sink.label-prefix", "prefix")
                .build();
        // The job also writes to db.other, which is not in the sink options. It failed before its
        // first checkpoint completed, and had begun a transaction on db.other with label number 1,
        // where a job that does not restore from a checkpoint begins the labels of a table.
        String lingeringLabel = "prefix-other-0-1";
        streamLoader.addLabelStatus("db", "other", lingeringLabel, TransactionStatus.PREPARED);

        StarRocksDynamicSinkFunctionV2<Object> sink = new StarRocksDynamicSinkFunctionV2<>(sinkOptions);
        try (OneInputStreamOperatorTestHarness<Object, Object> harness =
                     new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink))) {
            harness.open();

            factoryGivenToManager.get().create("db", "other");
            assertEquals(TransactionStatus.ABORTED, streamLoader.getLoadStatus("db", "other", lingeringLabel));
        }
    }

    // Replace the methods that would connect to StarRocks, and record the label
    // generator factory that the sink gives to the sink manager.
    private static AtomicReference<LabelGeneratorFactory> mockStarRocks(StreamLoader streamLoader) {
        new MockUp<StarRocksSinkTable>() {
            @Mock
            public String getVersion() {
                return "3.3.0";
            }
        };
        AtomicReference<LabelGeneratorFactory> factoryGivenToManager = new AtomicReference<>();
        new MockUp<StreamLoadManagerV2>() {
            @Mock
            public void $init(StreamLoadProperties properties, boolean enableAutoCommit) {
            }

            @Mock
            public void setStreamLoadListener(StreamLoadListener streamLoadListener) {
            }

            @Mock
            public void setMetricListener(MetricListener metricListener) {
            }

            @Mock
            public void setLabelGeneratorFactory(LabelGeneratorFactory labelGeneratorFactory) {
                factoryGivenToManager.set(labelGeneratorFactory);
            }

            @Mock
            public void init() {
            }

            @Mock
            public StreamLoader getStreamLoader() {
                return streamLoader;
            }

            @Mock
            public void flush() {
            }

            @Mock
            public StreamLoadSnapshot snapshot() {
                return null;
            }

            @Mock
            public boolean abort(StreamLoadSnapshot snapshot) {
                return true;
            }

            @Mock
            public void close() {
            }
        };
        return factoryGivenToManager;
    }
}
