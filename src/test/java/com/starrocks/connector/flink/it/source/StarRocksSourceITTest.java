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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import com.starrocks.connector.flink.it.StarRocksITTestBase;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class StarRocksSourceITTest extends StarRocksITTestBase {

    @Test
    public void testMapSubsetColumns() throws Exception {
        String tableName = createPartialTables("testMapSubsetColumns");
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES (%s)", DB_NAME, tableName,
                        "0, 1.1, '1', [1], map{1:'1'}"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String createSrcSQL = "CREATE TABLE sr_src(" +
                "c3 ARRAY<BIGINT>," +
                "c4 MAP<INT, STRING>," +
                "c1 FLOAT," +
                "c2 STRING" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + getJdbcUrl() + "'," +
                "'scan-url'='" + String.join(";", getHttpUrls()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + tableName + "'," +
                "'username' = 'root'," +
                "'password' = ''" +
                ")";
        tEnv.executeSql(createSrcSQL);
        List<Row> result1 =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("SELECT * FROM sr_src").collect());
        Map<Integer, String> c4 = new HashMap<>();
        c4.put(1, "1");
        Row row1 = Row.of(
                new Long[]{1L},
                c4,
                1.1f,
                "1"
            );
        assertThat(result1).containsExactlyInAnyOrderElementsOf(Collections.singleton(row1));

        List<Row> result2 =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("SELECT c4 FROM sr_src").collect());
        Row row2 = Row.of(c4);
        assertThat(result2).containsExactlyInAnyOrderElementsOf(Collections.singleton(row2));

        List<Row> result3 =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("SELECT c1, c2, c3, c4 FROM sr_src WHERE c2 = '1'").collect());
        Row row3 = Row.of(
                1.1f,
                "1",
                new Long[]{1L},
                c4
            );
        assertThat(result3).containsExactlyInAnyOrderElementsOf(Collections.singleton(row3));
    }

    private String createPartialTables(String tablePrefix) throws Exception {
        String tableName = tablePrefix + "_" + genRandomUuid();
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 FLOAT," +
                                "c2 STRING," +
                                "c3 ARRAY<BIGINT>," +
                                "c4 MAP<INT, STRING>" +
                                ") ENGINE = OLAP " +
                                "DUPLICATE KEY(c0) " +
                                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
        return tableName;
    }

    @Test
    public void testArrayType() throws Exception {
        String tableName = createArrayTypeTable("testArrayType");
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES (%s)", DB_NAME, tableName,
                "0, [true], [2], [3], [4], [5], [6.6], [7.7], [8.8], ['2024-03-22'], ['2024-03-22 12:00:00'], ['11'], ['12'], " +
                "[[true], [true]], [[2], [2]], [[3], [3]], [[4], [4]], [[5], [5]], [[6.6], [6.6]], " +
                "[[7.7], [7.7]], [[8.8], [8.8]], [['2024-03-22'], ['2024-03-22']], " +
                "[['2024-03-22 12:00:00'], ['2024-03-22 12:00:00']], [['11'], ['11']], [['12'], ['12']]"
                )
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String createSrcSQL = "CREATE TABLE sr_src(" +
                    "c0 INT," +
                    "c1 ARRAY<BOOLEAN>," +
                    "c2 ARRAY<TINYINT>," +
                    "c3 ARRAY<SMALLINT>," +
                    "c4 ARRAY<INT>," +
                    "c5 ARRAY<BIGINT>," +
                    "c6 ARRAY<FLOAT>," +
                    "c7 ARRAY<DOUBLE>," +
                    "c8 ARRAY<DECIMAL(38,10)>," +
                    "c9 ARRAY<DATE>," +
                    "c10 ARRAY<TIMESTAMP>," +
                    "c11 ARRAY<CHAR>," +
                    "c12 ARRAY<VARCHAR>," +
                    "c13 ARRAY<ARRAY<BOOLEAN>>," +
                    "c14 ARRAY<ARRAY<TINYINT>>," +
                    "c15 ARRAY<ARRAY<SMALLINT>>," +
                    "c16 ARRAY<ARRAY<INT>>," +
                    "c17 ARRAY<ARRAY<BIGINT>>," +
                    "c18 ARRAY<ARRAY<FLOAT>>," +
                    "c19 ARRAY<ARRAY<DOUBLE>>," +
                    "c20 ARRAY<ARRAY<DECIMAL(38,10)>>," +
                    "c21 ARRAY<ARRAY<DATE>>," +
                    "c22 ARRAY<ARRAY<TIMESTAMP>>," +
                    "c23 ARRAY<ARRAY<CHAR>>," +
                    "c24 ARRAY<ARRAY<VARCHAR>>" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + getJdbcUrl() + "'," +
                "'scan-url'='" + String.join(";", getHttpUrls()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + tableName + "'," +
                "'username' = 'root'," +
                "'password' = ''" +
                ")";
        tEnv.executeSql(createSrcSQL);
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("SELECT * FROM sr_src WHERE c0 = 0").collect());
        Row row = Row.of(
                0,
                new Boolean[] {true},
                new Byte[] {(byte) 2},
                new Short[] {(short) 3},
                new Integer[] {4},
                new Long[] {5L},
                new Float[] {6.6f},
                new Double[] {7.7},
                new BigDecimal[] {new BigDecimal("8.8000000000")},
                new LocalDate[] {LocalDate.of(2024, 3, 22)},
                new LocalDateTime[] {LocalDateTime.of(2024, 3, 22, 12, 0, 0)},
                new String[] {"11"},
                new String[] {"12"},
                new Boolean[][] {{true}, {true}},
                new Byte[][] {{(byte) 2}, {(byte) 2}},
                new Short[][] {{(short) 3}, {(short) 3}},
                new Integer[][] {{4}, {4}},
                new Long[][] {{5L}, {5L}},
                new Float[][] {{6.6f}, {6.6f}},
                new Double[][] {{7.7}, {7.7}},
                new BigDecimal[][] {{new BigDecimal("8.8000000000")}, {new BigDecimal("8.8000000000")}},
                new LocalDate[][] {
                        {LocalDate.of(2024, 3, 22)},
                        {LocalDate.of(2024, 3, 22)}
                },
                new LocalDateTime[][] {
                        {LocalDateTime.of(2024, 3, 22, 12, 0, 0)},
                        {LocalDateTime.of(2024, 3, 22, 12, 0, 0)},
                },
                new String[][] {{"11"}, {"11"}},
                new String[][] {{"12"}, {"12"}}
        );
        assertThat(results).containsExactlyInAnyOrderElementsOf(Collections.singleton(row));
    }

    private String createArrayTypeTable(String tablePrefix) throws Exception {
        String tableName = tablePrefix + "_" + genRandomUuid();
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 ARRAY<BOOLEAN>," +
                                "c2 ARRAY<TINYINT>," +
                                "c3 ARRAY<SMALLINT>," +
                                "c4 ARRAY<INT>," +
                                "c5 ARRAY<BIGINT>," +
                                "c6 ARRAY<FLOAT>," +
                                "c7 ARRAY<DOUBLE>," +
                                "c8 ARRAY<DECIMAL(38,10)>," +
                                "c9 ARRAY<DATE>," +
                                "c10 ARRAY<DATETIME>," +
                                "c11 ARRAY<CHAR(200)>," +
                                "c12 ARRAY<VARCHAR(200)>," +
                                "c13 ARRAY<ARRAY<BOOLEAN>>," +
                                "c14 ARRAY<ARRAY<TINYINT>>," +
                                "c15 ARRAY<ARRAY<SMALLINT>>," +
                                "c16 ARRAY<ARRAY<INT>>," +
                                "c17 ARRAY<ARRAY<BIGINT>>," +
                                "c18 ARRAY<ARRAY<FLOAT>>," +
                                "c19 ARRAY<ARRAY<DOUBLE>>," +
                                "c20 ARRAY<ARRAY<DECIMAL(38,10)>>," +
                                "c21 ARRAY<ARRAY<DATE>>," +
                                "c22 ARRAY<ARRAY<DATETIME>>," +
                                "c23 ARRAY<ARRAY<CHAR(200)>>," +
                                "c24 ARRAY<ARRAY<VARCHAR(200)>>" +
                                ") ENGINE = OLAP " +
                                "DUPLICATE KEY(c0) " +
                                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
        return tableName;
    }

    @Test
    public void testStructType() throws Exception {
        String tableName = createStructTypeTable("testStructType");
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES (%s)", DB_NAME, tableName,
                        "1, row(1, '1'), row('2024-03-27', 8.9)"
                )
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String createSrcSQL = "CREATE TABLE sr_src(" +
                "c0 INT," +
                "c1 ROW<id INT, name STRING>," +
                "c2 ROW<ts DATE, score DECIMAL(38, 10)>" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + getJdbcUrl() + "'," +
                "'scan-url'='" + String.join(";", getHttpUrls()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + tableName + "'," +
                "'username' = 'root'," +
                "'password' = ''" +
                ")";
        tEnv.executeSql(createSrcSQL);
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("SELECT * FROM sr_src").collect());
        Row row = Row.of(
                1,
                Row.of(1, "1"),
                Row.of(LocalDate.of(2024, 3, 27), new BigDecimal("8.9000000000"))
            );
        assertThat(results).containsExactlyInAnyOrderElementsOf(Collections.singleton(row));

    }

    private String createStructTypeTable(String tablePrefix) throws Exception {
        String tableName = tablePrefix + "_" + genRandomUuid();
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 STRUCT<id INT, name STRING>," +
                                "c2 STRUCT<ts DATE, score DECIMAL(38, 10)>" +
                                ") ENGINE = OLAP " +
                                "DUPLICATE KEY(c0) " +
                                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
        return tableName;
    }

    @Test
    public void testMapType() throws Exception {
        String tableName = createMapTypeTable("testMapType");
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES (%s)", DB_NAME, tableName,
                        "1, map{1:'1'}, map{'2024-03-27':8.9}"
                )
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String createSrcSQL = "CREATE TABLE sr_src(" +
                "c0 INT," +
                "c1 MAP<INT, STRING>," +
                "c2 MAP<DATE, DECIMAL(38, 10)>" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + getJdbcUrl() + "'," +
                "'scan-url'='" + String.join(";", getHttpUrls()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + tableName + "'," +
                "'username' = 'root'," +
                "'password' = ''" +
                ")";
        tEnv.executeSql(createSrcSQL);
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("SELECT * FROM sr_src").collect());
        Map<Integer, String> c1 = new HashMap<>();
        c1.put(1, "1");
        Map<LocalDate, BigDecimal> c2 = new HashMap<>();
        c2.put(LocalDate.of(2024, 3, 27), new BigDecimal("8.9000000000"));
        Row row = Row.of(1, c1, c2);
        assertThat(results).containsExactlyInAnyOrderElementsOf(Collections.singleton(row));
    }

    private String createMapTypeTable(String tablePrefix) throws Exception {
        String tableName = tablePrefix + "_" + genRandomUuid();
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 MAP<INT, STRING>," +
                                "c2 MAP<DATE, DECIMAL(38, 10)>" +
                                ") ENGINE = OLAP " +
                                "DUPLICATE KEY(c0) " +
                                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
        return tableName;
    }

    @Test
    public void testNestedType() throws Exception {
        String tableName = createNestedTypeTable("testNestedType");
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES (%s)", DB_NAME, tableName,
                        "1, row(1, [1, 11, 111], map{1:'1'})"
                )
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String createSrcSQL = "CREATE TABLE sr_src(" +
                "c0 INT," +
                "c1 ROW<a INT, b ARRAY<INT>, c MAP<INT, STRING>>" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + getJdbcUrl() + "'," +
                "'scan-url'='" + String.join(";", getHttpUrls()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + tableName + "'," +
                "'username' = 'root'," +
                "'password' = ''" +
                ")";
        tEnv.executeSql(createSrcSQL);
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("SELECT * FROM sr_src").collect());
        Map<Integer, String> c = new HashMap<>();
        c.put(1, "1");
        Row row = Row.of(1, Row.of(1, new Integer[] {1, 11, 111}, c));
        assertThat(results).containsExactlyInAnyOrderElementsOf(Collections.singleton(row));
    }

    private String createNestedTypeTable(String tablePrefix) throws Exception {
        String tableName = tablePrefix + "_" + genRandomUuid();
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 STRUCT<a INT, b ARRAY<INT>, c MAP<INT, STRING>>" +
                                ") ENGINE = OLAP " +
                                "DUPLICATE KEY(c0) " +
                                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
        return tableName;
    }

    @Test
    public void testDim() throws Exception {
        String tableName = createDimTable("testDim");
        executeSrSQL(
                String.format(
                        "INSERT INTO `%s`.`%s` VALUES (%s), (%s)", DB_NAME, tableName, "1", "2"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String srcSQL = "CREATE TABLE sr_src(" +
                "c0 INT," +
                "proc_time AS PROCTIME()" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + getJdbcUrl() + "'," +
                "'scan-url'='" + String.join(";", getHttpUrls()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + tableName + "'," +
                "'username' = 'root'," +
                "'password' = ''" +
                ")";
        tEnv.executeSql(srcSQL);
        List<Row> results =
                CollectionUtil.iteratorToList(tEnv.executeSql(
                    "SELECT t0.c0 FROM sr_src AS t0 JOIN sr_src " +
                            "FOR SYSTEM_TIME AS OF t0.proc_time AS t1 ON t0.c0 = t1.c0;")
                    .collect());
        assertThat(results).containsExactlyInAnyOrderElementsOf(Arrays.asList(Row.of(1), Row.of(2)));
    }

    private String createDimTable(String tablePrefix) throws Exception {
        String tableName = tablePrefix + "_" + genRandomUuid();
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT" +
                                ") ENGINE = OLAP " +
                                "DUPLICATE KEY(c0) " +
                                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
        return tableName;
    }

}
