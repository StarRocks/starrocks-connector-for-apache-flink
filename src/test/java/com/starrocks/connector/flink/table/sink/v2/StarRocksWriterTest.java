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

import org.apache.flink.api.connector.sink2.Sink;

import com.starrocks.connector.flink.table.sink.ExactlyOnceLabelGeneratorSnapshot;
import com.starrocks.connector.flink.table.sink.LingeringTransactionAborterTest.MockStreamLoader;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.v2.StreamLoadListener;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class StarRocksWriterTest {

    @Mocked
    private Sink.InitContext initContext;

    @Mocked
    private RecordSerializationSchema<Object> serializationSchema;

    @Test
    public void testRestoredLabelsContinueFromSnapshot() throws Exception {
        AtomicReference<LabelGeneratorFactory> factoryGivenToManager = mockSinkManager(null);
        // Restore from checkpoint 5. The mocked context reports subtask 0.
        new Expectations() {{
            initContext.getRestoredCheckpointId();
            result = OptionalLong.of(5L);
        }};
        StarRocksSinkOptions sinkOptions = buildExactlyOnceSinkOptions()
                // Skip the abort, which would ask StarRocks about the transactions.
                .withProperty("sink.exactly-once.enable-abort-lingering-txn", "false")
                .build();
        // At checkpoint 5, the next label number of subtask 0 for db.table was 4,
        // because no label was used for the table in one checkpoint interval.
        StarRocksWriterState recoveredState = new StarRocksWriterState(Collections.singletonList(
                new ExactlyOnceLabelGeneratorSnapshot(5L, "db", "table", "prefix", 1, 0, 4L)));

        new StarRocksWriter<>(sinkOptions, initContext, null, serializationSchema, null,
                Collections.singletonList(recoveredState));

        // The writer goes on from label 4. That is where LingeringTransactionAborter starts
        // to look for lingering transactions if the job restores from checkpoint 5 again.
        assertEquals("prefix-table-0-4", factoryGivenToManager.get().create("db", "table").next());
    }

    @Test
    public void testFirstWriteChecksTablesNotInSinkOptions() throws Exception {
        MockStreamLoader streamLoader = new MockStreamLoader();
        AtomicReference<LabelGeneratorFactory> factoryGivenToManager = mockSinkManager(streamLoader);
        // Restore from checkpoint 5. The mocked context reports subtask 0.
        new Expectations() {{
            initContext.getRestoredCheckpointId();
            result = OptionalLong.of(5L);
        }};
        StarRocksSinkOptions sinkOptions = buildExactlyOnceSinkOptions().build();
        // The job also writes to db.other, which is not in the sink options. An earlier restore
        // from checkpoint 5 began a transaction on it with label number 6, and failed.
        String lingeringLabel = "prefix-other-0-6";
        streamLoader.addLabelStatus("db", "other", lingeringLabel, TransactionStatus.PREPARED);

        new StarRocksWriter<>(sinkOptions, initContext, null, serializationSchema, null,
                Collections.emptyList());

        factoryGivenToManager.get().create("db", "other");
        assertEquals(TransactionStatus.ABORTED, streamLoader.getLoadStatus("db", "other", lingeringLabel));
    }

    @Test
    public void testRestoreAbortsTransactionOfFirstWriteAfterTheCheckpoint() throws Exception {
        MockStreamLoader streamLoader = new MockStreamLoader();
        AtomicReference<LabelGeneratorFactory> factoryGivenToManager = mockSinkManager(streamLoader);
        // The first run starts without a checkpoint, the second restores from checkpoint 1.
        // The mocked context reports subtask 0.
        new Expectations() {{
            initContext.getRestoredCheckpointId();
            result = OptionalLong.empty();
            result = OptionalLong.of(1L);
        }};
        StarRocksSinkOptions sinkOptions = buildExactlyOnceSinkOptions().build();

        // The first run takes checkpoint 1 before its first write to db.table. Then it begins a
        // transaction on db.table with label number 1, where a run that does not restore from a
        // checkpoint begins the labels of a table, and fails before checkpoint 2 completes.
        StarRocksWriter<Object> firstRun = new StarRocksWriter<>(sinkOptions, initContext, null,
                serializationSchema, null, Collections.emptyList());
        List<StarRocksWriterState> checkpoint1 = firstRun.snapshotState(1L);
        String lingeringLabel = factoryGivenToManager.get().create("db", "table").next();
        assertEquals("prefix-table-0-1", lingeringLabel);
        streamLoader.addLabelStatus("db", "table", lingeringLabel, TransactionStatus.PREPARED);

        // The restore from checkpoint 1 must abort it.
        new StarRocksWriter<>(sinkOptions, initContext, null, serializationSchema, null, checkpoint1);

        assertEquals(TransactionStatus.ABORTED, streamLoader.getLoadStatus("db", "table", lingeringLabel));
    }

    // Replace the sink manager methods that would connect to StarRocks, and record
    // the label generator factory that the writer gives it.
    private static AtomicReference<LabelGeneratorFactory> mockSinkManager(StreamLoader streamLoader) {
        AtomicReference<LabelGeneratorFactory> factoryGivenToManager = new AtomicReference<>();
        new MockUp<StreamLoadManagerV2>() {
            @Mock
            public void $init(StreamLoadProperties properties, boolean enableAutoCommit) {
            }

            @Mock
            public void setStreamLoadListener(StreamLoadListener streamLoadListener) {
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
        };
        return factoryGivenToManager;
    }

    private static StarRocksSinkOptions.Builder buildExactlyOnceSinkOptions() {
        return StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", "db")
                .withProperty("table-name", "table")
                .withProperty("sink.semantic", "exactly-once")
                .withProperty("sink.label-prefix", "prefix");
    }
}
