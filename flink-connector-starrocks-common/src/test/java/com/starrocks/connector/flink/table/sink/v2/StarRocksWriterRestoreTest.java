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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;

import com.starrocks.connector.flink.manager.StarRocksStreamLoadListener;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.ExactlyOnceLabelGenerator;
import com.starrocks.connector.flink.table.sink.ExactlyOnceLabelGeneratorFactory;
import com.starrocks.connector.flink.table.sink.ExactlyOnceLabelGeneratorSnapshot;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StarRocksWriterRestoreTest {

    private static final String LABEL_PREFIX = "restoretest";

    @Test
    public void testRestoredLabelSnapshotsSeedLabelGenerator(
            @Mocked StreamLoadManagerV2 anyManager,
            @Mocked StarRocksStreamLoadListener anyListener,
            @Injectable MetricGroup metricGroup,
            @Injectable SerializationSchema.InitializationContext schemaContext) throws Exception {
        ExactlyOnceLabelGeneratorSnapshot snapshot = new ExactlyOnceLabelGeneratorSnapshot(
                5L, "test_db", "test_table", LABEL_PREFIX, 1, 0, 42L);
        StarRocksWriter<Object> writer = new StarRocksWriter<>(
                sinkOptions(),
                5L,
                1,
                0,
                metricGroup,
                schemaContext,
                new NoopSerializationSchema(),
                null,
                Collections.singletonList(new StarRocksWriterState(Collections.singletonList(snapshot))));

        Field field = StarRocksWriter.class.getDeclaredField("labelGeneratorFactory");
        field.setAccessible(true);
        Object factory = field.get(writer);
        assertTrue(factory instanceof ExactlyOnceLabelGeneratorFactory);
        ExactlyOnceLabelGenerator generator =
                ((ExactlyOnceLabelGeneratorFactory) factory).create("test_db", "test_table");
        assertEquals(42L, generator.snapshot(6L).getNextId());
    }

    private static StarRocksSinkOptions sinkOptions() {
        Configuration conf = new Configuration();
        conf.set(StarRocksSinkOptions.TABLE_NAME, "test_table");
        conf.set(StarRocksSinkOptions.DATABASE_NAME, "test_db");
        conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:8030");
        conf.set(StarRocksSinkOptions.JDBC_URL, "jdbc:mysql://127.0.0.1:9030");
        conf.set(StarRocksSinkOptions.USERNAME, "root");
        conf.set(StarRocksSinkOptions.PASSWORD, "");
        conf.set(StarRocksSinkOptions.SINK_SEMANTIC, StarRocksSinkSemantic.EXACTLY_ONCE.getName());
        conf.set(StarRocksSinkOptions.SINK_LABEL_PREFIX, LABEL_PREFIX);
        conf.set(StarRocksSinkOptions.SINK_ABORT_LINGERING_TXNS, false);
        return new StarRocksSinkOptions(conf, conf.toMap());
    }

    private static class NoopSerializationSchema implements RecordSerializationSchema<Object> {
        @Override
        public void open(SerializationSchema.InitializationContext context, StarRocksSinkContext sinkContext) {
        }

        @Override
        public StarRocksRowData serialize(Object record) {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
