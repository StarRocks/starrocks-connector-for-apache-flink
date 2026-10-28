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

package com.starrocks.connector.flink.it.source;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.starrocks.connector.flink.it.StarRocksITTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Reads a table in SQL with a source parallelism other than the parallelism of the job. */
public class StarRocksSourceParallelismITTest extends StarRocksITTestBase {

    private static final int ROWS = 100;

    @Test
    public void testScanParallelismSetsTheParallelismOfTheSource() throws Exception {
        String tableName = "scan_parallelism_" + genRandomUuid();
        executeSrSQL(String.format("CREATE TABLE `%s`.`%s` (c0 INT) ENGINE = OLAP DUPLICATE KEY(c0) " +
                "DISTRIBUTED BY HASH (c0) BUCKETS 8 PROPERTIES (\"replication_num\" = \"1\")", DB_NAME, tableName));
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < ROWS; i++) {
            values.append(i == 0 ? "" : ",").append('(').append(i).append(')');
        }
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES %s", DB_NAME, tableName, values));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(String.format("CREATE TABLE src (c0 INT) WITH ('connector' = 'starrocks', " +
                        "'scan-url' = '%s', 'jdbc-url' = '%s', 'username' = '%s', 'password' = '%s', " +
                        "'database-name' = '%s', 'table-name' = '%s', 'scan.parallelism' = '3')",
                getHttpUrls(), getJdbcUrl(), USERNAME, PASSWORD, DB_NAME, tableName));
        DataStream<Row> stream = tEnv.toDataStream(tEnv.sqlQuery("SELECT c0 FROM src"));

        Transformation<?> source = stream.getTransformation().getTransitivePredecessors().stream()
                .filter(t -> t instanceof SourceTransformation)
                .findFirst()
                .orElseThrow(AssertionError::new);
        assertEquals(3, source.getParallelism());

        List<Integer> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = stream.executeAndCollect()) {
            iterator.forEachRemaining(row -> rows.add((Integer) row.getField(0)));
        }
        assertEquals(ROWS, rows.size());
        assertEquals(ROWS, new HashSet<>(rows).size());
    }
}
