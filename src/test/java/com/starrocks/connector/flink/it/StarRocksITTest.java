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

import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.StarRocksSinkSemantic;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        result.wait(60000);
        ArrayList<Row> queryResult = getSQLResult(String.format("select * from %s.%s limit 1", sinkOptions.getDatabaseName(), sinkOptions.getTableName()));
        long score = (Long) (queryResult.get(0).getField(1));
        Assert.assertEquals(score, 99);
    }
}
