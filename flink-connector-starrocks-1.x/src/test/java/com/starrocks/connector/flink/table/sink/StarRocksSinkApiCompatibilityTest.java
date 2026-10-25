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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import com.starrocks.connector.flink.row.sink.StarRocksGenericRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.table.sink.v2.DefaultStarRocksSinkContext;
import com.starrocks.connector.flink.table.sink.v2.StarRocksSinkContext;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** The public sink APIs keep the signatures user code compiled against before 1.2.17. */
public class StarRocksSinkApiCompatibilityTest {

    private static final TableSchema SCHEMA = TableSchema.builder()
            .field("id", DataTypes.INT().notNull())
            .field("name", DataTypes.STRING())
            .primaryKey("id")
            .build();

    @Test
    public void testTransformerWrittenAgainstTableSchemaStillReceivesIt() {
        LegacyTransformer transformer = new LegacyTransformer();
        transformer.setTableSchema(SCHEMA);
        assertSame(SCHEMA, transformer.schema);
    }

    @Test
    public void testBuiltInTransformerAcceptsTableSchema() {
        StarRocksGenericRowTransformer<String> transformer =
                new StarRocksGenericRowTransformer<>((row, record) -> row[0] = record);
        transformer.setTableSchema(SCHEMA);
        // one slot per schema column, plus __op
        assertEquals(3, transformer.transform("a", true).length);
    }

    @Test
    public void testSinkContextExposesTheInitContext(@Injectable Sink.InitContext initContext) {
        new Expectations() {{
            initContext.getNumberOfParallelSubtasks();
            result = 4;
            initContext.getSubtaskId();
            result = 2;
        }};
        StarRocksSinkContext context = new DefaultStarRocksSinkContext(initContext, sinkOptions());

        assertSame(initContext, context.getInitContext());
        assertEquals(4, context.getNumberOfParallelSubtasks());
        assertEquals(2, context.getSubtaskIndex());
    }

    // implements only what the interface declared before 1.2.17
    private static class LegacyTransformer implements StarRocksIRowTransformer<String> {

        private TableSchema schema;

        @Override
        public void setStarRocksColumns(Map<String, StarRocksDataType> columns) {
        }

        @Override
        public void setTableSchema(TableSchema tableSchema) {
            this.schema = tableSchema;
        }

        @Override
        public void setRuntimeContext(RuntimeContext ctx) {
        }

        @Override
        public Object[] transform(String record, boolean supportUpsertDelete) {
            return new Object[] {record};
        }
    }

    private static StarRocksSinkOptions sinkOptions() {
        Configuration conf = new Configuration();
        conf.set(StarRocksSinkOptions.TABLE_NAME, "test_table");
        conf.set(StarRocksSinkOptions.DATABASE_NAME, "test_db");
        conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:8030");
        conf.set(StarRocksSinkOptions.JDBC_URL, "jdbc:mysql://127.0.0.1:9030");
        conf.set(StarRocksSinkOptions.USERNAME, "root");
        conf.set(StarRocksSinkOptions.PASSWORD, "");
        return new StarRocksSinkOptions(conf, conf.toMap());
    }
}
