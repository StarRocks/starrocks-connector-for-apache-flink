/*
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

package com.starrocks.connector.flink.it.sink;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
public class StarRocksSinkTest {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkTest.class);

    private static String DB_NAME;

    private static String getHttpUrls() {
        return "127.0.0.1:11901";
    }

    private static String getJdbcUrl() {
        return "jdbc:mysql://127.0.0.1:11903";
    }

    private static Connection DB_CONNECTION;

    @BeforeClass
    public static void setUp() throws Exception {
        DB_NAME = "sr_sink_test_" + genRandomUuid();
        try {
            DB_CONNECTION = DriverManager.getConnection(getJdbcUrl(), "root", "");
            LOG.info("Success to create db connection via jdbc {}", getJdbcUrl());
        } catch (Exception e) {
            LOG.error("Failed to create db connection via jdbc {}", getJdbcUrl(), e);
            throw e;
        }

        try {
            String createDb = "CREATE DATABASE " + DB_NAME;
            executeSRDDLSQL(createDb);
            LOG.info("Successful to create database {}", DB_NAME);
        } catch (Exception e) {
            LOG.error("Failed to create database {}", DB_NAME, e);
            throw e;
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (DB_CONNECTION != null) {
            try {
                String dropDb = String.format("DROP DATABASE IF EXISTS %s FORCE", DB_NAME);
                executeSRDDLSQL(dropDb);
                LOG.info("Successful to drop database {}", DB_NAME);
            } catch (Exception e) {
                LOG.error("Failed to drop database {}", DB_NAME, e);
            }
            DB_CONNECTION.close();
        }
    }

    private static String genRandomUuid() {
        return UUID.randomUUID().toString().replace("-", "_");
    }

    private static void executeSRDDLSQL(String sql) throws Exception {
        try (PreparedStatement statement = DB_CONNECTION.prepareStatement(sql)) {
            statement.execute();
        }
    }

    @Test
    public void testDupKeyWriteFullColumnsInOrder() throws Exception {
        String ddl = "c0 INT, c1 FLOAT, c2 STRING";
        List<Row> testData = new ArrayList<>();
        testData.add(Row.of(1, 10.1f, "abc"));
        testData.add(Row.of(2, 20.2f, "def"));
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.INT, Types.FLOAT, Types.STRING},
                new String[]{"c0", "c1", "c2"});
        String tableName = "testDupKeyWriteFullColumnsInOrder_" + genRandomUuid();
        testDupKeyWriteBase(tableName, ddl, rowTypeInfo, testData);
    }

    @Test
    public void testDupKeyWriteFullColumnsOutOfOrder() throws Exception {
        String ddl = "c2 STRING, c1 FLOAT, c0 INT";
        List<Row> testData = new ArrayList<>();
        testData.add(Row.of("abc", 10.1f, 1));
        testData.add(Row.of("def", 20.2f, 2));
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.FLOAT, Types.INT},
                new String[]{"c2", "c1", "c0"});
        String tableName = "testDupKeyWriteFullColumnsOutOfOrder_" + genRandomUuid();
        testDupKeyWriteBase(tableName, ddl, rowTypeInfo, testData);
    }

    @Test
    public void testDupKeyWritePartialColumnsInOrder() throws Exception {
        String ddl = "c0 INT, c2 STRING";
        List<Row> testData = new ArrayList<>();
        testData.add(Row.of(1, "abc"));
        testData.add(Row.of(2, "def"));
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.INT, Types.STRING},
                new String[]{"c0", "c2"});
        String tableName = "testDupKeyWritePartialColumnsInOrder_" + genRandomUuid();
        testDupKeyWriteBase(tableName, ddl, rowTypeInfo, testData);
    }

    @Test
    public void testDupKeyWritePartialColumnsOutOfOrder() throws Exception {
        String ddl = "c2 STRING, c0 INT";
        List<Row> testData = new ArrayList<>();
        testData.add(Row.of("abc", 1));
        testData.add(Row.of("def", 2));
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.INT},
                new String[]{"c2", "c0"});
        String tableName = "testDupKeyWritePartialColumnsOutOfOrder_" + genRandomUuid();
        testDupKeyWriteBase(tableName, ddl, rowTypeInfo, testData);
    }

    private void testDupKeyWriteBase(String tableName, String flinkDDL,  RowTypeInfo rowTypeInfo, List<Row> testData) throws Exception {
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                        "c0 INT," +
                        "c1 FLOAT," +
                        "c2 STRING" +
                        ") ENGINE = OLAP " +
                        "DUPLICATE KEY(c0) " +
                        "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                        "PROPERTIES (" +
                        "\"replication_num\" = \"1\"" +
                        ")",
                        DB_NAME, tableName);
        executeSRDDLSQL(createStarRocksTable);

        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", getHttpUrls())
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", tableName)
                .withProperty("username", "root")
                .withProperty("password", "")
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        String createSQL = "CREATE TABLE sink(" + flinkDDL +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + sinkOptions.getJdbcUrl() + "'," +
                "'load-url'='" + String.join(";", sinkOptions.getLoadUrlList()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + sinkOptions.getTableName() + "'," +
                "'username' = '" + sinkOptions.getUsername() + "'," +
                "'password' = '" + sinkOptions.getPassword() + "'" +
                ")";
        tEnv.executeSql(createSQL);
        DataStream<Row> srcDs = env.fromCollection(testData).returns(rowTypeInfo);
        Table in = tEnv.fromDataStream(srcDs);
        tEnv.createTemporaryView("src", in);
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM src");
        result.await();
    }

    @Test
    public void testPkWriteFullColumnsInOrder() throws Exception {
        String ddl = "c0 INT, c1 FLOAT, c2 STRING";
        List<Row> testData = new ArrayList<>();
        testData.add(Row.of(1, 10.1f, "abc"));
        testData.add(Row.of(2, 20.2f, "def"));
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.INT, Types.FLOAT, Types.STRING},
                new String[]{"c0", "c1", "c2"});
        String tableName = "testPkWriteFullColumnsInOrder_" + genRandomUuid();
        testPkWriteForBase(tableName, ddl, rowTypeInfo, testData);
    }

    @Test
    public void testPkWriteFullColumnsOutOfOrder() throws Exception {
        String ddl = "c2 STRING, c1 FLOAT, c0 INT";
        List<Row> testData = new ArrayList<>();
        testData.add(Row.of("abc", 10.1f, 1));
        testData.add(Row.of("def", 20.2f, 2));
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.FLOAT, Types.INT},
                new String[]{"c2", "c1", "c0"});
        String tableName = "testPkWriteFullColumnsOutOfOrder_" + genRandomUuid();
        testPkWriteForBase(tableName, ddl, rowTypeInfo, testData);
    }

    @Test
    public void testPkWritePartialColumnsInOrder() throws Exception {
        String ddl = "c0 INT, c2 STRING";
        List<Row> testData = new ArrayList<>();
        testData.add(Row.of(1, "abc"));
        testData.add(Row.of(2, "def"));
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.INT, Types.STRING},
                new String[]{"c0", "c2"});
        String tableName = "testPkWritePartialColumnsInOrder_" + genRandomUuid();
        testPkWriteForBase(tableName, ddl, rowTypeInfo, testData);
    }

    @Test
    public void testPkWritePartialColumnsOutOfOrder() throws Exception {
        String ddl = "c2 STRING, c0 INT";
        List<Row> testData = new ArrayList<>();
        testData.add(Row.of("abc", 1));
        testData.add(Row.of("def", 2));
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.INT},
                new String[]{"c2", "c0"});
        String tableName = "testPkWritePartialColumnsOutOfOrder_" + genRandomUuid();
        testPkWriteForBase(tableName, ddl, rowTypeInfo, testData);
    }

    private void testPkWriteForBase(String tableName, String flinkDDL,  RowTypeInfo rowTypeInfo, List<Row> testData) throws Exception {
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 FLOAT," +
                                "c2 STRING" +
                                ") ENGINE = OLAP " +
                                "PRIMARY KEY(c0) " +
                                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSRDDLSQL(createStarRocksTable);

        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", getHttpUrls())
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", tableName)
                .withProperty("username", "root")
                .withProperty("password", "")
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        String createSQL = "CREATE TABLE sink(" + flinkDDL +
                ", PRIMARY KEY (`c0`) NOT ENFORCED" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + sinkOptions.getJdbcUrl() + "'," +
                "'load-url'='" + String.join(";", sinkOptions.getLoadUrlList()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + sinkOptions.getTableName() + "'," +
                "'username' = '" + sinkOptions.getUsername() + "'," +
                "'password' = '" + sinkOptions.getPassword() + "'" +
                ")";
        tEnv.executeSql(createSQL);
        DataStream<Row> srcDs = env.fromCollection(testData).returns(rowTypeInfo);
        Table in = tEnv.fromDataStream(srcDs);
        tEnv.createTemporaryView("src", in);
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM src");
        result.await();
    }
}
