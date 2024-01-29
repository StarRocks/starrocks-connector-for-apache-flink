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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import com.starrocks.connector.flink.it.StarRocksITTestBase;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;

import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.scanTable;
import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.verifyResult;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class StarRocksSinkITTest extends StarRocksITTestBase {

    @Parameterized.Parameters(name = "sinkV2={0}, newSinkApi={1}")
    public static List<Object[]> parameters() {
        return Arrays.asList(
                new Object[] {false, false},
                new Object[] {true, false},
                new Object[] {true, true}
            );
    }

    @Parameterized.Parameter
    public boolean isSinkV2;

    @Parameterized.Parameter(1)
    public boolean newSinkApi;

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
        assumeTrue(isSinkV2);
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
        assumeTrue(isSinkV2);
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
                "'sink.version' = '" + (isSinkV2 ? "V2" : "V1") + "'," +
                "'sink.use.new-sink-api' = '" + (newSinkApi ? "true" : "false") + "'," +
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
        assumeTrue(isSinkV2);
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
        assumeTrue(isSinkV2);
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
                "'sink.version' = '" + (isSinkV2 ? "V2" : "V1") + "'," +
                "'sink.use.new-sink-api' = '" + (newSinkApi ? "true" : "false") + "'," +
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
                "'sink.version' = '" + (isSinkV2 ? "V2" : "V1") + "'," +
                "'sink.use.new-sink-api' = '" + (newSinkApi ? "true" : "false") + "'," +
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
        assumeTrue(isSinkV2);
        String tableName = createPkTable("testPKPartialUpdateDelete");
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES (1, 1.0, '1'), (2, 2.0, '2')", DB_NAME, tableName));
        verifyResult(Arrays.asList(Arrays.asList(1, 1.0f, "1"), Arrays.asList(2, 2.0f, "2")),
                scanTable(DB_CONNECTION, DB_NAME, tableName));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        DataStream<Row> dataStream = env.fromElements(
                Row.ofKind(RowKind.DELETE, 1, "1"),
                Row.ofKind(RowKind.UPDATE_AFTER, 2, "22"),
                Row.ofKind(RowKind.INSERT, 3, "3"));
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
                "'sink.version' = '" + (isSinkV2 ? "V2" : "V1") + "'," +
                "'sink.use.new-sink-api' = '" + (newSinkApi ? "true" : "false") + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + sinkOptions.getTableName() + "'," +
                "'username' = '" + sinkOptions.getUsername() + "'," +
                "'password' = '" + sinkOptions.getPassword() + "'," +
                "'sink.properties.partial_update' = 'true'" +
                ")";
        tEnv.executeSql(createSQL);
        tEnv.executeSql("INSERT INTO sink SELECT * FROM src").await();

        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(2, 2.0, "22"),
                Arrays.asList(3, null, "3")
        );
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

    @Test
    public void testExactlyOnce() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.at-least-once.use-transaction-stream-load", "false");
        options.put("sink.semantic", "exactly-once");
        String checkpointDir = temporaryFolder.newFolder().toURI().toString();
        testConfigurationBase(options,
            env -> {
                env.enableCheckpointing(1000);
                env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
                Configuration config = new Configuration();
                config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
                env.configure(config);
                return null;
            }
        );
    }

    @Test
    public void testEnableExactlyOnceLabelGen() throws Exception {
        assumeTrue(isSinkV2);
        Map<String, String> options = new HashMap<>();
        options.put("sink.semantic", "exactly-once");
        options.put("sink.label-prefix", "test_label");
        options.put("sink.exactly-once.enable-label-gen", "true");
        String checkpointDir = temporaryFolder.newFolder().toURI().toString();
        testConfigurationBase(options,
                env -> {
                    env.enableCheckpointing(1000);
                    env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
                    Configuration config = new Configuration();
                    config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
                    env.configure(config);
                    return null;
                }
        );
    }

    @Test
    public void testAbortLingeringTransactions() throws Exception {
        assumeTrue(isSinkV2);
        Map<String, String> options = new HashMap<>();
        options.put("sink.semantic", "exactly-once");
        options.put("sink.label-prefix", "test_label");
        String checkpointDir = temporaryFolder.newFolder().toURI().toString();
        testConfigurationBase(options,
                env -> {
                    env.enableCheckpointing(1000);
                    env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
                    Configuration config = new Configuration();
                    config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
                    env.configure(config);
                    return null;
                }
        );
    }

    @Test
    public void testAbortLingeringTransactionsWithCheckNum() throws Exception {
        assumeTrue(isSinkV2);
        Map<String, String> options = new HashMap<>();
        options.put("sink.semantic", "exactly-once");
        options.put("sink.label-prefix", "test_label");
        options.put("sink.exactly-once.check-num-lingering-txn", "10");
        String checkpointDir = temporaryFolder.newFolder().toURI().toString();
        testConfigurationBase(options,
                env -> {
                    env.enableCheckpointing(1000);
                    env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
                    Configuration config = new Configuration();
                    config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
                    env.configure(config);
                    return null;
                }
        );
    }

    @Test
    public void testCsvFormatWithColumnSeparatorAndRowDelimiter() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("sink.properties.column_separator", "\\x01");
        map.put("sink.properties.row_delimiter", "\\0x2");
        testConfigurationBase(map, env -> null);
    }

    @Test
    public void testJsonFormat() throws Exception {
        if (isSinkV2) {
            testConfigurationBase(
                    Collections.singletonMap("sink.properties.format", "json"), env -> null);
        } else {
            Map<String, String> map = new HashMap<>();
            map.put("sink.properties.format", "json");
            map.put("sink.properties.strip_outer_array", "true");
            testConfigurationBase(map, env -> null);
        }
    }

    private void testConfigurationBase(Map<String, String> options, Function<StreamExecutionEnvironment, Void> setFlinkEnv) throws Exception {
        String tableName = createPkTable("testConfigurationBase");
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
                "'sink.version' = '" + (isSinkV2 ? "V2" : "V1") + "'," +
                "'sink.use.new-sink-api' = '" + (newSinkApi ? "true" : "false") + "'," +
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

    @Test
    public void testUnalignedTypes() throws Exception {
        String tableName = "testUnalignedTypes_" + genRandomUuid();
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 LARGEINT," +
                                "c2 JSON" +
                                ") ENGINE = OLAP " +
                                "PRIMARY KEY(c0) " +
                                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

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
                    "c1 STRING," +
                    "c2 STRING," +
                    "PRIMARY KEY (`c0`) NOT ENFORCED" +
                ") WITH ( " +
                    "'connector' = 'starrocks'," +
                    "'jdbc-url'='" + sinkOptions.getJdbcUrl() + "'," +
                    "'load-url'='" + String.join(";", sinkOptions.getLoadUrlList()) + "'," +
                    "'sink.version' = '" + (isSinkV2 ? "V2" : "V1") + "'," +
                    "'sink.use.new-sink-api' = '" + (newSinkApi ? "true" : "false") + "'," +
                    "'database-name' = '" + DB_NAME + "'," +
                    "'table-name' = '" + sinkOptions.getTableName() + "'," +
                    "'username' = '" + sinkOptions.getUsername() + "'," +
                    "'password' = '" + sinkOptions.getPassword() + "'" +
                ")";

        tEnv.executeSql(createSQL);
        tEnv.executeSql("INSERT INTO sink VALUES (1, '123', '{\"key\": 1, \"value\": 2}')").await();
        List<List<Object>> actualData = scanTable(DB_CONNECTION, DB_NAME, tableName);
        verifyResult(Collections.singletonList(Arrays.asList(1, "123", "{\"key\": 1, \"value\": 2}")), actualData);
    }

    @Test
    public void testJsonType() throws Exception {
        String tableName = createJsonTable("testJsonType");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        DataStream<Row> dataStream =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, 1, 1.0, "{\"a\": 1, \"b\": true}"),
                        Row.ofKind(RowKind.INSERT, 2, 2.0, "{\"a\": 2, \"b\": false}"));
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

        String createSinkSQL = "CREATE TABLE sink(" +
                "c0 INT," +
                "c1 DOUBLE," +
                "c2 STRING," +
                "PRIMARY KEY (`c0`) NOT ENFORCED" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + sinkOptions.getJdbcUrl() + "'," +
                "'load-url'='" + String.join(";", sinkOptions.getLoadUrlList()) + "'," +
                "'sink.version' = '" + (isSinkV2 ? "V2" : "V1") + "'," +
                "'sink.use.new-sink-api' = '" + (newSinkApi ? "true" : "false") + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + sinkOptions.getTableName() + "'," +
                "'username' = '" + sinkOptions.getUsername() + "'," +
                "'password' = '" + sinkOptions.getPassword() + "'" +
                ")";
        tEnv.executeSql(createSinkSQL);
        tEnv.executeSql("INSERT INTO sink SELECT * FROM src").await();

        String createSrcSQL = "CREATE TABLE sr_src(" +
                "c0 INT," +
                "c1 DOUBLE," +
                "c2 STRING," +
                "PRIMARY KEY (`c0`) NOT ENFORCED" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + sinkOptions.getJdbcUrl() + "'," +
                "'scan-url'='" + String.join(";", sinkOptions.getLoadUrlList()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + sinkOptions.getTableName() + "'," +
                "'username' = '" + sinkOptions.getUsername() + "'," +
                "'password' = '" + sinkOptions.getPassword() + "'" +
                ")";
        tEnv.executeSql(createSrcSQL);
        List<List<Object>> actualData;
        try (CloseableIterator<Row> result = tEnv.executeSql("SELECT * FROM sr_src").collect()) {
            actualData = collectResult(result);
        }

        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, 1.0, "{\"a\": 1, \"b\": true}"),
                Arrays.asList(2, 2.0, "{\"a\": 2, \"b\": false}")
        );

        verifyResult(expectedData, actualData);
    }

    private List<List<Object>> collectResult(CloseableIterator<Row> result) {
        List<List<Object>> data = new ArrayList<>();
        while (result.hasNext()) {
            Row row = result.next();
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < row.getArity(); i++) {
                list.add(row.getField(i).toString());
            }
            data.add(list);
        }
        return data;
    }

    private String createJsonTable(String tablePrefix) throws Exception {
        String tableName = tablePrefix + "_" + genRandomUuid();
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 DOUBLE," +
                                "c2 JSON" +
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
