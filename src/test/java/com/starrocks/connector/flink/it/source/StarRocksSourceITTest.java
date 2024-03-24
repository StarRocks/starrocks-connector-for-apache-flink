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
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StarRocksSourceITTest extends StarRocksITTestBase {

    @Test
    public void testArrayType() throws Exception {
        String tableName = createComplexTypeTable("testComplexType");
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
                        tEnv.executeSql("SELECT * FROM sr_src").collect());
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

    private String createComplexTypeTable(String tablePrefix) throws Exception {
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
}
