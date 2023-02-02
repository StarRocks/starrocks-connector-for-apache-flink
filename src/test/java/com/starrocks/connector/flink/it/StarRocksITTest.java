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

package com.starrocks.connector.flink.it;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.StarRocksSource;
import com.starrocks.connector.flink.container.StarRocksCluster;
import com.starrocks.connector.flink.row.sink.StarRocksSinkRowBuilder;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertNull;

/** IT tests for StarRocks sink and source. */
public class StarRocksITTest extends StarRocksITTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksITTest.class);

    private final static String DB_NAME = "starrocks_connector_it";

    @BeforeClass
    public static void prepare() {
        String createDbCmd = "CREATE DATABASE " + DB_NAME;
        STARROCKS_CLUSTER.executeMysqlCommand(createDbCmd);
    }

    @Test
    public void testSink() throws Exception {
        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", STARROCKS_CLUSTER.getBeLoadUrl())
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", "sink_table" )
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("sink.label-prefix", "sink-prefix")
                .withProperty("sink.semantic", StarRocksSinkSemantic.AT_LEAST_ONCE.getName())
                .withProperty("sink.buffer-flush.interval-ms", "2000")
                .withProperty("sink.buffer-flush.max-bytes", "74002019")
                .withProperty("sink.buffer-flush.max-rows", "1002000")
                .withProperty("sink.max-retries", "2")
                .withProperty("sink.connect.timeout-ms", "2000")
                .withProperty("sink.version","v1")
                .build();

        String createTable =
                "CREATE TABLE " + String.join(".", DB_NAME, sinkOptions.getTableName()) + " (" +
                    "name STRING," +
                    "score BIGINT," +
                    "a DATETIME," +
                    "b STRING," +
                    "c DECIMAL(2,1)," +
                    "d DATE" +
                 ") ENGINE = OLAP " +
                 "DUPLICATE KEY(name)" +
                 "DISTRIBUTED BY HASH (name) BUCKETS 8";
        STARROCKS_CLUSTER.executeMysqlCommand(createTable);

        Configuration conf = new Configuration();
        conf.setBoolean("classloader.check-leaked-classloader", false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        String createSQL = "CREATE TABLE USER_RESULT(" +
                "name STRING," +
                "score BIGINT," +
                "a TIMESTAMP(3)," +
                "b STRING," +
                "c DECIMAL(2,1)," +
                "d DATE" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + sinkOptions.getJdbcUrl() + "'," +
                "'load-url'='" + String.join(";", sinkOptions.getLoadUrlList()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + sinkOptions.getTableName() + "'," +
                "'username' = '" + sinkOptions.getUsername() + "'," +
                "'password' = '" + sinkOptions.getPassword() + "'," +
                "'sink.buffer-flush.max-rows' = '" + sinkOptions.getSinkMaxRows() + "'," +
                "'sink.buffer-flush.max-bytes' = '" + sinkOptions.getSinkMaxBytes() + "'," +
                "'sink.buffer-flush.interval-ms' = '" + sinkOptions.getSinkMaxFlushInterval() + "'," +
                "'sink.buffer-flush.enqueue-timeout-ms' = '" + sinkOptions.getSinkOfferTimeout() + "'," +
                "'sink.properties.column_separator' = '\\x01'," +
                "'sink.properties.row_delimiter' = '\\x02'" +
                ")";
        tEnv.executeSql(createSQL);
        TableResult result = tEnv.executeSql("INSERT INTO USER_RESULT\n" +
                    "VALUES ('lebron', 99, TO_TIMESTAMP('2020-01-01 01:00:01'), 'b', 2.3, TO_DATE('2020-01-01'))");
        result.await(1, TimeUnit.MINUTES);
        ArrayList<Row> queryResult = getSQLResult(String.format("select * from %s.%s limit 1", sinkOptions.getDatabaseName(), sinkOptions.getTableName()));
        long score = (Long) (queryResult.get(0).getField(1));
        Assert.assertEquals(score, 99);
    }

    @Test
    public void testSource() {
        StarRocksSourceOptions options = StarRocksSourceOptions.builder()
                .withProperty("scan-url", STARROCKS_CLUSTER.getHttpUrls())
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", "source_table")
                .withProperty("scan.be-host-mapping-list", STARROCKS_CLUSTER.getBeUrlMapping())
                .build();

        String createTable =
                "CREATE TABLE " + String.join(".", DB_NAME, options.getTableName()) + " (" +
                            "date_1 DATE," +
                            "datetime_1 DATETIME,"+
                            "char_1 CHAR(3),"+
                            "varchar_1 VARCHAR(10),"+
                            "boolean_1 BOOLEAN,"+
                            "tinyint_1 TINYINT,"+
                            "smallint_1 SMALLINT,"+
                            "int_1 INT,"+
                            "bigint_1 BIGINT,"+
                            "float_1 FLOAT,"+
                            "double_1 DOUBLE"+
                        ") ENGINE = OLAP " +
                        "DUPLICATE KEY(date_1)" +
                        "DISTRIBUTED BY HASH (date_1) BUCKETS 8";
        STARROCKS_CLUSTER.executeMysqlCommand(createTable);

        String inserIntoData =
                "INSERT INTO " + String.join(".", DB_NAME, options.getTableName()) + " " +
                        "WITH LABEL starrocks_source_" + UUID.randomUUID().toString().substring(0, 5) + " VALUES" +
                        "('2022-09-01', '2022-09-01 21:28:30', 'c11', 'vc11', 0, 125, -32000, 600000, 9232322, 1.2, 2.23)," +
                        "('2022-09-02', '2022-09-02 05:12:32', 'c12', 'vc12', 1, 3, 948, 93, -12, 43.334, -9494.2);";
        STARROCKS_CLUSTER.executeMysqlCommand(inserIntoData);

        Configuration conf = new Configuration();
        conf.setBoolean("classloader.check-leaked-classloader", false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE flink_type_test (" +
                            "date_1 DATE," +
                            "datetime_1 TIMESTAMP(6),"+
                            "char_1 CHAR(3),"+
                            "varchar_1 VARCHAR(10),"+
                            "boolean_1 BOOLEAN,"+
                            "tinyint_1 TINYINT,"+
                            "smallint_1 SMALLINT,"+
                            "int_1 INT,"+
                            "bigint_1 BIGINT,"+
                            "float_1 FLOAT,"+
                            "double_1 DOUBLE"+
                        ") WITH (\n" +
                        "  'connector' = 'starrocks',\n" +
                        "  'scan.be-host-mapping-list' = '" + options.getBeHostMappingList() + "',\n" +
                        "  'scan-url' = '" + options.getScanUrl() + "',\n" +
                        "  'scan.connect.timeout-ms' = '5000', " +
                        "  'jdbc-url' = '" + options.getJdbcUrl() + "',\n" +
                        "  'username' = '" + options.getUsername() + "',\n" +
                        "  'password' = '" + options.getPassword() + "',\n" +
                        "  'database-name' = '" + options.getDatabaseName() + "',\n" +
                        "  'table-name' = '" + options.getTableName() + "'\n" +
                        ")"
        );
        Exception e = null;
        try {
            tEnv.executeSql("select * from flink_type_test").print();
            Thread.sleep(2000);
        } catch (Exception ex) {
            ex.printStackTrace();
            e = ex;
        }
        assertNull(e);
    }

    @Test
    public void testLookup() {
        StarRocksSourceOptions options = StarRocksSourceOptions.builder()
                .withProperty("scan-url", STARROCKS_CLUSTER.getHttpUrls())
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", "lookup_table")
                .withProperty("scan.be-host-mapping-list", STARROCKS_CLUSTER.getBeUrlMapping())
                .build();

        String createTable =
                "CREATE TABLE " + String.join(".", DB_NAME, options.getTableName()) + " (" +
                        "date_1 DATE," +
                        "datetime_1 DATETIME,"+
                        "char_1 CHAR(3),"+
                        "varchar_1 VARCHAR(10),"+
                        "boolean_1 BOOLEAN,"+
                        "tinyint_1 TINYINT,"+
                        "smallint_1 SMALLINT,"+
                        "int_1 INT,"+
                        "bigint_1 BIGINT,"+
                        "float_1 FLOAT,"+
                        "double_1 DOUBLE"+
                        ") ENGINE = OLAP " +
                        "DUPLICATE KEY(date_1)" +
                        "DISTRIBUTED BY HASH (date_1) BUCKETS 8";
        STARROCKS_CLUSTER.executeMysqlCommand(createTable);

        String inserIntoData =
                "INSERT INTO " + String.join(".", DB_NAME, options.getTableName()) + " " +
                        "WITH LABEL starrocks_lookup_" + UUID.randomUUID().toString().substring(0, 5) + " VALUES" +
                        "('2022-09-01', '2022-09-01 21:28:30', 'c11', 'vc11', 0, 125, -32000, 600000, 9232322, 1.2, 2.23)," +
                        "('2022-09-02', '2022-09-02 05:12:32', 'c12', 'vc12', 1, 3, 948, 93, -12, 43.334, -9494.2);";
        STARROCKS_CLUSTER.executeMysqlCommand(inserIntoData);

        Configuration conf = new Configuration();
        conf.setBoolean("classloader.check-leaked-classloader", false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        Exception e = null;
        try {
            RowTypeInfo testTypeInfo = new RowTypeInfo(
                            new TypeInformation[] {Types.BOOLEAN, Types.STRING},
                            new String[] {"k1", "k2"});
            List<Row> testData = new ArrayList<>();
            testData.add(Row.of(false, "row1"));
            testData.add(Row.of(true, "row2"));

            DataStream<Row> srcDs = env.fromCollection(testData).returns(testTypeInfo);
            Table in = tEnv.fromDataStream(srcDs, $("k1"), $("k2"), $("proctime").proctime());
            tEnv.registerTable("datagen", in);

            tEnv.executeSql(
                    "CREATE TABLE flink_type_test (" +
                                "date_1 DATE," +
                                "datetime_1 TIMESTAMP(6),"+
                                "char_1 CHAR(3),"+
                                "varchar_1 VARCHAR(10),"+
                                "boolean_1 BOOLEAN,"+
                                "tinyint_1 TINYINT,"+
                                "smallint_1 SMALLINT,"+
                                "int_1 INT,"+
                                "bigint_1 BIGINT,"+
                                "float_1 FLOAT,"+
                                "double_1 DOUBLE"+
                            ") WITH (\n" +
                            "'connector' = 'starrocks',\n" +
                            "'scan.be-host-mapping-list' = '" + options.getBeHostMappingList() + "',\n" +
                            "'scan-url' = '" + options.getScanUrl() + "',\n" +
                            "'scan.connect.timeout-ms' = '5000', " +
                            "'jdbc-url' = '" + options.getJdbcUrl() + "',\n" +
                            "'username' = '" + options.getUsername() + "',\n" +
                            "'password' = '" + options.getPassword() + "',\n" +
                            "'database-name' = '" + options.getDatabaseName() + "',\n" +
                            "'table-name' = '" + options.getTableName() + "')"
            );

            tEnv.executeSql("SELECT * FROM datagen LEFT JOIN flink_type_test FOR SYSTEM_TIME AS OF datagen.proctime ON " +
                "datagen.k1 = flink_type_test.boolean_1").print();

            Thread.sleep(2000);
        } catch (Exception ex) {
            ex.printStackTrace();
            e = ex;
        }
        assertNull(e);
    }

    @Test
    public void testTableAPIWithRestartCluster() throws Exception {
        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", STARROCKS_CLUSTER.getBeLoadUrl())
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", "sink_table_restart")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("sink.label-prefix", "sink-prefix")
                .withProperty("sink.semantic", StarRocksSinkSemantic.AT_LEAST_ONCE.getName())
                .withProperty("sink.buffer-flush.interval-ms", "2000")
                .withProperty("sink.buffer-flush.max-bytes", "74002019")
                .withProperty("sink.buffer-flush.max-rows", "1002000")
                .withProperty("sink.max-retries", "2")
                .withProperty("sink.connect.timeout-ms", "2000")
                .withProperty("sink.version","v1")
                .build();

        String createTable =
                "CREATE TABLE " + String.join(".", DB_NAME, sinkOptions.getTableName()) + " (" +
                        "name STRING," +
                        "score BIGINT," +
                        "a DATETIME," +
                        "b STRING," +
                        "c DECIMAL(2,1)," +
                        "d DATE" +
                        ") ENGINE = OLAP " +
                        "DUPLICATE KEY(name)" +
                        "DISTRIBUTED BY HASH (name) BUCKETS 8";
        STARROCKS_CLUSTER.executeMysqlCommand(createTable);

        Configuration conf = new Configuration();
        conf.setBoolean("classloader.check-leaked-classloader", false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
        tEnv = StreamTableEnvironment.create(env);
        String createSQL = "CREATE TABLE SR_SINK (" +
                "name STRING," +
                "score BIGINT," +
                "a TIMESTAMP(3)," +
                "b STRING," +
                "c DECIMAL(2,1)," +
                "d DATE" +
                ") WITH ( " +
                "'connector' = 'starrocks'," +
                "'jdbc-url'='" + sinkOptions.getJdbcUrl() + "'," +
                "'load-url'='" + String.join(";", sinkOptions.getLoadUrlList()) + "'," +
                "'database-name' = '" + DB_NAME + "'," +
                "'table-name' = '" + sinkOptions.getTableName() + "'," +
                "'username' = '" + sinkOptions.getUsername() + "'," +
                "'password' = '" + sinkOptions.getPassword() + "'," +
                "'sink.buffer-flush.max-rows' = '" + sinkOptions.getSinkMaxRows() + "'," +
                "'sink.buffer-flush.max-bytes' = '" + sinkOptions.getSinkMaxBytes() + "'," +
                "'sink.buffer-flush.interval-ms' = '" + sinkOptions.getSinkMaxFlushInterval() + "'," +
                "'sink.buffer-flush.enqueue-timeout-ms' = '" + sinkOptions.getSinkOfferTimeout() + "'," +
                "'sink.properties.column_separator' = '\\x01'," +
                "'sink.properties.row_delimiter' = '\\x02'" +
                ")";
        tEnv.executeSql(createSQL);
        StarRocksSourceOptions sourceOptions = StarRocksSourceOptions.builder()
                .withProperty("scan-url", STARROCKS_CLUSTER.getHttpUrls())
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", "source_table_restart")
                .withProperty("scan.be-host-mapping-list", STARROCKS_CLUSTER.getBeUrlMapping())
                .build();

        createTable =
                "CREATE TABLE " + String.join(".", DB_NAME, sourceOptions.getTableName()) + " (" +
                        "name STRING," +
                        "score BIGINT," +
                        "a DATETIME," +
                        "b STRING," +
                        "c DECIMAL(2,1)," +
                        "d DATE" +
                        ") ENGINE = OLAP " +
                        "DUPLICATE KEY(name)" +
                        "DISTRIBUTED BY HASH (name) BUCKETS 8";
        STARROCKS_CLUSTER.executeMysqlCommand(createTable);

        tEnv.executeSql(
                "CREATE TABLE SR_SOURCE (" +
                        "name STRING," +
                        "score BIGINT," +
                        "a TIMESTAMP(3)," +
                        "b STRING," +
                        "c DECIMAL(2,1)," +
                        "d DATE" +
                        ") WITH (\n" +
                        "  'connector' = 'starrocks',\n" +
                        "  'scan.be-host-mapping-list' = '" + sourceOptions.getBeHostMappingList() + "',\n" +
                        "  'scan-url' = '" + sourceOptions.getScanUrl() + "',\n" +
                        "  'scan.connect.timeout-ms' = '5000', " +
                        "  'jdbc-url' = '" + sourceOptions.getJdbcUrl() + "',\n" +
                        "  'username' = '" + sourceOptions.getUsername() + "',\n" +
                        "  'password' = '" + sourceOptions.getPassword() + "',\n" +
                        "  'database-name' = '" + sourceOptions.getDatabaseName() + "',\n" +
                        "  'table-name' = '" + sourceOptions.getTableName() + "'\n" +
                        ")"
        );
        String insertIntoData =
                "INSERT INTO " + String.join(".", DB_NAME, sourceOptions.getTableName()) + " " +
                        "WITH LABEL starrocks_source_" + UUID.randomUUID().toString().substring(0, 5) + " VALUES" +
                        "('lebron', 99, '2020-01-01 01:00:01', 'ss', 2.3, '2020-01-01')," +
                        "('kobe', 98, '2020-01-03 01:00:01', 's', 2.1, '2020-01-03')," +
                        "('jordon', 100, '2020-01-04 01:00:01', 'sss', 2.1, '2020-01-03')," +
                        "('curry', 98, '2020-01-02 01:00:01', 's', 2.1, '2020-01-02');";

        STARROCKS_CLUSTER.executeMysqlCommand(insertIntoData);
        Thread.sleep(10000);

        CompletableFuture<Boolean> restart = STARROCKS_CLUSTER.restartAllFe();
        if (!restart.get()) {
            Assert.fail();
        }

        TableResult result = tEnv.executeSql("INSERT INTO SR_SINK SELECT * FROM SR_SOURCE;");
        result.await(1, TimeUnit.MINUTES);
        Thread.sleep(10000);

        verifyResult(new Row[]{Row.of("jordon", 100, Timestamp.valueOf("2020-01-04 01:00:01"), "sss", 2.1, Date.valueOf("2020-01-03"))},
                String.format("select * from %s.%s where name = 'jordon' limit 1", sinkOptions.getDatabaseName(), sinkOptions.getTableName()));
        verifyResult(rows -> rows.length >= 4,
                String.format("select distinct name from %s.%s ;", sinkOptions.getDatabaseName(), sinkOptions.getTableName()));
    }

    private static class ClusterKillingMapper<T> extends RichMapFunction<T, T>
            implements ListCheckpointed<Integer>, CheckpointListener {

        private static final long serialVersionUID = 6334389850158707313L;

        public static volatile boolean killedLeaderBefore = false;
        public static volatile boolean hasBeenCheckpointedBeforeFailure;

        private static StarRocksCluster starRocksServerToKill;
        private int numElementsTotal;

        private boolean failer;
        private boolean hasBeenCheckpointed;
        private int failCount;

        public ClusterKillingMapper(
                StarRocksCluster starRocksServerToKill, int failCount) {
            this.starRocksServerToKill = starRocksServerToKill;
            this.failCount = failCount;
        }

        @Override
        public void open(Configuration parameters) {
            failer = getRuntimeContext().getIndexOfThisSubtask() == 0;
        }

        @Override
        public T map(T value) throws Exception {
            numElementsTotal++;
            if (!killedLeaderBefore) {
                Thread.sleep(10);
                if (failer && numElementsTotal >= failCount) {
                    System.out.println("ClusterKillingMapper restart!");
                    starRocksServerToKill.restartAllFe().get();
                    hasBeenCheckpointedBeforeFailure = hasBeenCheckpointed;
                    killedLeaderBefore = true;
                }
            }
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            hasBeenCheckpointed = true;
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(this.numElementsTotal);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            if (state.size() != 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            this.numElementsTotal = state.get(0);
        }
    }


    @Test
    public void testDataStreamAPIWithRestartCluster() throws Exception {
        StarRocksSourceOptions sourceOptions = StarRocksSourceOptions.builder()
                .withProperty("scan-url", STARROCKS_CLUSTER.getHttpUrls())
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", "ds_source_restart_" )
                .withProperty("scan.be-host-mapping-list", STARROCKS_CLUSTER.getBeUrlMapping())
                .build();

        TableSchema tableSchema = TableSchema.builder()
                .field("runoob_id", DataTypes.BIGINT().notNull())
                .field("runoob_title", DataTypes.STRING())
                .field("runoob_author", DataTypes.STRING())
                .primaryKey("runoob_id")
                .build();

        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", STARROCKS_CLUSTER.getBeLoadUrl())
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", "ds_sink_restart_" )
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("sink.label-prefix", "sink-prefix")
                .withProperty("sink.semantic", StarRocksSinkSemantic.AT_LEAST_ONCE.getName())
                .withProperty("sink.buffer-flush.interval-ms", "2000")
                .withProperty("sink.buffer-flush.max-bytes", "74002019")
                .withProperty("sink.buffer-flush.max-rows", "1002000")
                .withProperty("sink.max-retries", "2")
                .withProperty("sink.connect.timeout-ms", "2000")
                .build();


        String createTable =
                "CREATE TABLE " + String.join(".", DB_NAME, sinkOptions.getTableName()) + " (" +
                        "runoob_id BIGINT," +
                        "runoob_title STRING," +
                        "runoob_author STRING" +
                        ") ENGINE = OLAP " +
                        "DUPLICATE KEY(runoob_id)" +
                        "DISTRIBUTED BY HASH (runoob_id) BUCKETS 8";
        STARROCKS_CLUSTER.executeMysqlCommand(createTable);
        createTable =
                "CREATE TABLE " + String.join(".", DB_NAME, sourceOptions.getTableName()) + " (" +
                        "runoob_id BIGINT," +
                        "runoob_title STRING," +
                        "runoob_author STRING" +
                        ") ENGINE = OLAP " +
                        "DUPLICATE KEY(runoob_id)" +
                        "DISTRIBUTED BY HASH (runoob_id) BUCKETS 8";
        STARROCKS_CLUSTER.executeMysqlCommand(createTable);

        String insertIntoData =
                "INSERT INTO " + String.join(".", DB_NAME, sourceOptions.getTableName()) + " " +
                        "WITH LABEL starrocks_source_" + UUID.randomUUID().toString().substring(0, 5) + " VALUES" +
                        "(1,'tom','blue')," +
                        "(2,'tony','pink')," +
                        "(3,'tim','black')," +
                        "(4,'bruce','green')," +
                        "(5,'sam','blue')," +
                        "(6,'husky','brown')," +
                        "(7,'tomson','white')," +
                        "(8,'jack','green');";

        STARROCKS_CLUSTER.executeMysqlCommand(insertIntoData);
        Thread.sleep(10000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(30, Time.seconds(40)));
        env.addSource(StarRocksSource.source(tableSchema, sourceOptions)).map(new ClusterKillingMapper(STARROCKS_CLUSTER,2)).addSink(StarRocksSink.sink(tableSchema, sinkOptions, new StarRocksSinkRowBuilder<RowData>() {
            @Override
            public void accept(Object[] objects, RowData rowData) {
                objects[0] = rowData.getLong(0);
                objects[1] = rowData.getString(1);
                objects[2] = rowData.getString(2);
            }
        })).setParallelism(1);
        env.registerJobListener(new JobListener() {
            @Override
            public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
            }

            @Override
            public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
                if (throwable != null) {
                    log.info("Job executed with exception.",throwable);
                    return;
                }
                log.info("Job succeed.");
                try {
                    verifyResult(rows -> rows.length >= 8,
                            String.format("select distinct runoob_id from %s.%s ;", sinkOptions.getDatabaseName(), sinkOptions.getTableName()));

                    verifyResult(new Row[]{Row.of(1, "tom", "blue")},
                            String.format("select * from %s.%s where runoob_id = '1' limit 1", sinkOptions.getDatabaseName(), sinkOptions.getTableName()));
                } catch (Exception e) {
                    log.error("testDataStreamAPIWithRestartCluster verify failed!", e);
                    Assert.fail();
                }
            }
        });
        env.execute();
    }
}
