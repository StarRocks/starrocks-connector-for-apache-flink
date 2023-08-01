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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;

import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.scanTable;
import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.verifyResult;

public class StarRocksSinkITTest extends StarRocksSinkITTestBase {

    @Test
    public void testDupKeyWriteFullColumnsInOrder() throws Exception {
        String ddl = "c0 INT, c1 FLOAT, c2 STRING";
        List<Row> testData = new ArrayList<>();
        testData.add(Row.of(1, 10.1f, "abc"));
        testData.add(Row.of(2, 20.2f, "def"));
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.INT, Types.FLOAT, Types.STRING},
                new String[]{"c0", "c1", "c2"});
        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, 10.1f, "abc"),
                Arrays.asList(2, 20.2f, "def")
        );
        testDupKeyWriteBase(ddl, rowTypeInfo, testData, expectedData);
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
        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, 10.1f, "abc"),
                Arrays.asList(2, 20.2f, "def")
        );
        testDupKeyWriteBase(ddl, rowTypeInfo, testData, expectedData);
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
        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, null, "abc"),
                Arrays.asList(2, null, "def")
        );
        testDupKeyWriteBase(ddl, rowTypeInfo, testData, expectedData);
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
        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, null, "abc"),
                Arrays.asList(2, null, "def")
        );
        testDupKeyWriteBase(ddl, rowTypeInfo, testData, expectedData);
    }

    private void testDupKeyWriteBase(String flinkDDL,  RowTypeInfo rowTypeInfo,
                                     List<Row> testData, List<List<Object>> expectedData) throws Exception {
        String tableName = createDupTable("testDupKeyWriteBase");
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
        tEnv.executeSql("INSERT INTO sink SELECT * FROM src").await();
        List<List<Object>> actualData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualData);
    }

    private String createDupTable(String tablePrefix) throws Exception {
        String tableName = tablePrefix + "_" + genRandomUuid();
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
        executeSrSQL(createStarRocksTable);
        return tableName;
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
        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, 10.1f, "abc"),
                Arrays.asList(2, 20.2f, "def")
        );
        testPkWriteForBase(ddl, rowTypeInfo, testData, expectedData);
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
        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, 10.1f, "abc"),
                Arrays.asList(2, 20.2f, "def")
        );
        testPkWriteForBase(ddl, rowTypeInfo, testData, expectedData);
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
        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, null, "abc"),
                Arrays.asList(2, null, "def")
        );
        testPkWriteForBase(ddl, rowTypeInfo, testData, expectedData);
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
        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, null, "abc"),
                Arrays.asList(2, null, "def")
        );
        testPkWriteForBase(ddl, rowTypeInfo, testData, expectedData);
    }

    private void testPkWriteForBase(String flinkDDL,  RowTypeInfo rowTypeInfo,
                                    List<Row> testData, List<List<Object>> expectedData) throws Exception {
        String tableName = createPkTable("testPkWriteForBase");
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
        tEnv.executeSql("INSERT INTO sink SELECT * FROM src").await();
        List<List<Object>> actualData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualData);
    }

    @Test
    public void testPKUpsertAndDelete() throws Exception {
        String tableName = createPkTable("testPKUpsertAndDelete");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        DataStream<Row> dataStream =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, 1, 1.0f, "row1"),
                        Row.ofKind(RowKind.INSERT, 2, 2.0f, "row2"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 3.0f, "row3"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 3, 3.0f, "row3"),
                        Row.ofKind(RowKind.DELETE, 2, 2.0f, "row2"));
        Table table = tEnv.fromChangelogStream(dataStream, Schema.newBuilder().primaryKey("f0").build(), ChangelogMode.all());
        tEnv.createTemporaryView("src", table);

        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", getHttpUrls())
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", tableName)
                .withProperty("username", "root")
                .withProperty("password", "")
                .build();

        String createSQL = "CREATE TABLE sink(" +
                "c0 INT," +
                "c1 FLOAT," +
                "c2 STRING," +
                "PRIMARY KEY (`c0`) NOT ENFORCED" +
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
        tEnv.executeSql("INSERT INTO sink SELECT * FROM src").await();

        List<List<Object>> expectedData = Arrays.asList(
            Arrays.asList(1, 3.0f, "row3"),
            Arrays.asList(3, 3.0f, "row3")
        );
        List<List<Object>> actualData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualData);
    }

    @Test
    public void testPKPartialUpdateDelete() throws Exception {
        String tableName = createPkTable("testPKPartialUpdateDelete");
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES (1, 1.0, '1')", DB_NAME, tableName));
        verifyResult(Collections.singletonList(Arrays.asList(1, 1.0f, "1")), scanTable(DB_CONNECTION, DB_NAME, tableName));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        DataStream<Row> dataStream = env.fromElements(
                Row.ofKind(RowKind.DELETE, 1, "1"),
                Row.ofKind(RowKind.INSERT, 2, "2"));
        Table table = tEnv.fromChangelogStream(dataStream, Schema.newBuilder().primaryKey("f0").build(), ChangelogMode.all());
        tEnv.createTemporaryView("src", table);

        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", getHttpUrls())
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", tableName)
                .withProperty("username", "root")
                .withProperty("password", "")
                .build();

        String createSQL = "CREATE TABLE sink(" +
                "c0 INT," +
                "c2 STRING," +
                "PRIMARY KEY (`c0`) NOT ENFORCED" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + sinkOptions.getJdbcUrl() + "'," +
                "'load-url'='" + String.join(";", sinkOptions.getLoadUrlList()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + sinkOptions.getTableName() + "'," +
                "'username' = '" + sinkOptions.getUsername() + "'," +
                "'password' = '" + sinkOptions.getPassword() + "'," +
                "'sink.properties.partial_update' = 'true'" +
                ")";
        tEnv.executeSql(createSQL);
        tEnv.executeSql("INSERT INTO sink SELECT * FROM src").await();

        List<List<Object>> expectedData = Collections.singletonList(
                Arrays.asList(2, null, "2"));
        List<List<Object>> actualData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(expectedData, actualData);
    }

    @Test
    public void testAtLeastOnceWithTransaction() throws Exception {
        testConfigurationBase(Collections.emptyMap(), env -> null);
    }

    @Test
    public void testAtLeastOnceWithoutTransaction() throws Exception {
        testConfigurationBase(
                Collections.singletonMap("sink.at-least-once.use-transaction-stream-load", "false"), env -> null);
    }

    private void testConfigurationBase(Map<String, String> options, Function<StreamExecutionEnvironment, Void> setFlinkEnv) throws Exception {
        String tableName = createPkTable("testAtLeastOnceBase");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        setFlinkEnv.apply(env);

        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);

        StringJoiner joiner = new StringJoiner(",\n");
        for (Map.Entry<String, String> entry : options.entrySet()) {
            joiner.add(String.format("'%s' = '%s'", entry.getKey(), entry.getValue()));
        }
        String optionStr = joiner.toString();
        String createSQL = "CREATE TABLE sink(" +
                "c0 INT," +
                "c1 FLOAT," +
                "c2 STRING," +
                "PRIMARY KEY (`c0`) NOT ENFORCED" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + getJdbcUrl() + "'," +
                "'load-url'='" + String.join(";", getHttpUrls()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + tableName + "'," +
                "'username' = 'root'," +
                "'password' = ''" +
                (optionStr.isEmpty() ? "" : "," + optionStr) +
                ")";
        tEnv.executeSql(createSQL);
        tEnv.executeSql("INSERT INTO sink VALUES (1, 1.0, '1')").await();
        List<List<Object>> actualData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(Collections.singletonList(Arrays.asList(1, 1.0, '1')), actualData);
    }

    private String createPkTable(String tablePrefix) throws Exception {
        String tableName = tablePrefix + "_" + genRandomUuid();
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
        executeSrSQL(createStarRocksTable);
        return tableName;
    }
}
