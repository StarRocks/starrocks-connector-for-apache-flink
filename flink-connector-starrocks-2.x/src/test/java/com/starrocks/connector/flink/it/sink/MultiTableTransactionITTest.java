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

package com.starrocks.connector.flink.it.sink;

import com.starrocks.connector.flink.it.StarRocksITTestBase;
import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.v2.RecordSerializationSchema;
import com.starrocks.connector.flink.table.sink.v2.StarRocksSink;
import com.starrocks.connector.flink.table.sink.v2.StarRocksSinkContext;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.scanTable;
import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.verifyResult;
import static org.junit.Assume.assumeTrue;

/**
 * Integration tests for multi-table atomic transaction stream load through the
 * sink v2 ({@code sinkTo}) API.
 *
 * <p>This is the Flink 2.x subset of the multi-table transaction test suite: the
 * {@code SinkFunction}-based tests live in the 1.x module because the legacy
 * sink API was removed in Flink 2.0.
 *
 * <p>Requires StarRocks &gt;= 4.0 for multi-table transaction support, or a main-branch build
 * whose {@code current_version()} string contains {@code main}.
 *
 * <p>Run against an external cluster:
 * <pre>
 *   mvn test -Dtest=MultiTableTransactionITTest \
 *     -Dit.starrocks.fe.http=172.26.95.228:8030 \
 *     -Dit.starrocks.fe.jdbc=jdbc:mysql://172.26.95.228:9030
 *   (or export SR_HTTP_URLS / SR_JDBC_URLS with the same host:port values)
 * </pre>
 */
public class MultiTableTransactionITTest extends StarRocksITTestBase {

    @Before
    public void checkVersion() {
        assumeTrue(
                "Multi-table transaction requires StarRocks >= 4.0 or a main-branch build (current_version contains 'main')",
                isMultiTableTransactionClusterSupported());
    }

    /**
     * Flush interval used in timing-sensitive tests (ms).
     * Must be larger than the SDK's minimum scanningFrequency (50 ms).
     */
    private static final int FLUSH_INTERVAL_MS = 500;

    /**
     * Max time to wait for {@link StreamExecutionEnvironment#execute(String)} when it runs
     * on the test thread's behalf in a worker thread. Prevents an indefinite hang (no Surefire
     * "[INFO] Results:") if the Flink job or sink never completes.
     */
    private static final long FLINK_EXECUTE_TIMEOUT_MS = 30_000L;

    // -------------------------------------------------------------------------
    // DDL helpers
    // -------------------------------------------------------------------------

    private String createOrdersTable() throws Exception {
        String tableName = "orders_" + genRandomUuid();
        executeSrSQL(String.format(
                "CREATE TABLE `%s`.`%s` (" +
                        "order_id BIGINT NOT NULL," +
                        "customer_id BIGINT NOT NULL," +
                        "total_amount DECIMAL(10,2) DEFAULT '0'," +
                        "order_status VARCHAR(32) DEFAULT ''" +
                        ") ENGINE=OLAP PRIMARY KEY(order_id) " +
                        "DISTRIBUTED BY HASH(order_id) BUCKETS 4 " +
                        "PROPERTIES (\"replication_num\" = \"1\")",
                DB_NAME, tableName));
        return tableName;
    }

    private String createOrderItemsTable() throws Exception {
        String tableName = "order_items_" + genRandomUuid();
        executeSrSQL(String.format(
                "CREATE TABLE `%s`.`%s` (" +
                        "item_id BIGINT NOT NULL," +
                        "order_id BIGINT NOT NULL," +
                        "product_name VARCHAR(128) DEFAULT ''," +
                        "quantity INT DEFAULT '0'," +
                        "price DECIMAL(10,2) DEFAULT '0'" +
                        ") ENGINE=OLAP PRIMARY KEY(item_id) " +
                        "DISTRIBUTED BY HASH(item_id) BUCKETS 4 " +
                        "PROPERTIES (\"replication_num\" = \"1\")",
                DB_NAME, tableName));
        return tableName;
    }

    // -------------------------------------------------------------------------
    // Test cases
    // -------------------------------------------------------------------------

    /**
     * End-to-end multi-table transaction through the sink v2 API.
     *
     * <p>This verifies that multi-table transactions work correctly through the
     * two-phase commit path: {@code StarRocksWriter.write()} routes data by
     * partition → {@code flush(endOfInput=true)} triggers savepoint → data committed.
     */
    @Test
    public void testEndToEndSinkV2Api() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        StreamExecutionEnvironment env = buildEnv(1);

        env.fromElements(
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":88.88,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, ordersTable,
                        "{\"order_id\":2,\"customer_id\":101,\"total_amount\":55.55,\"order_status\":\"pending\"}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":1,\"order_id\":1,\"product_name\":\"alpha\",\"quantity\":2,\"price\":44.44}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":2,\"order_id\":2,\"product_name\":\"beta\",\"quantity\":1,\"price\":55.55}",
                        0, true)
        ).returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .sinkTo(buildSinkV2(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
                .setParallelism(1);

        runFlinkJobWithTimeout(env, "testEndToEndSinkV2Api");

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 100L, new BigDecimal("88.88"), "created"),
                        Arrays.asList(2L, 101L, new BigDecimal("55.55"), "pending")),
                scanTable(DB_CONNECTION, DB_NAME, ordersTable));

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 1L, "alpha", 2, new BigDecimal("44.44")),
                        Arrays.asList(2L, 2L, "beta",  1, new BigDecimal("55.55"))),
                scanTable(DB_CONNECTION, DB_NAME, orderItemsTable));
    }

    /**
     * SinkV2 API test with 2 partitions, verifying that keyBy + sinkTo
     * correctly routes data from different partitions.
     */
    @Test
    public void testSinkV2ApiMultiPartition() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        StreamExecutionEnvironment env = buildEnv(2);

        DataStream<DefaultStarRocksRowData> partition0 = env.fromElements(
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":10.00,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, ordersTable,
                        "{\"order_id\":2,\"customer_id\":101,\"total_amount\":20.00,\"order_status\":\"created\"}",
                        0, true)
        ).returns(TypeInformation.of(DefaultStarRocksRowData.class));

        DataStream<DefaultStarRocksRowData> partition1 = env.fromElements(
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":5.00}",
                        1, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":2,\"order_id\":2,\"product_name\":\"gadget\",\"quantity\":3,\"price\":6.67}",
                        1, true)
        ).returns(TypeInformation.of(DefaultStarRocksRowData.class));

        partition0.union(partition1)
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .sinkTo(buildSinkV2(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
                .setParallelism(2);

        runFlinkJobWithTimeout(env, "testSinkV2ApiMultiPartition");

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 100L, new BigDecimal("10.00"), "created"),
                        Arrays.asList(2L, 101L, new BigDecimal("20.00"), "created")),
                scanTable(DB_CONNECTION, DB_NAME, ordersTable));

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 1L, "widget", 2, new BigDecimal("5.00")),
                        Arrays.asList(2L, 2L, "gadget", 3, new BigDecimal("6.67"))),
                scanTable(DB_CONNECTION, DB_NAME, orderItemsTable));
    }

    // -------------------------------------------------------------------------
    // Shared builder helpers
    // -------------------------------------------------------------------------

    private StreamExecutionEnvironment buildEnv(int parallelism) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelism);
        // Checkpointing is intentionally disabled. The tests that verify
        // "no data visible before txnEnd" rely on precise timing and would
        // break if a Flink checkpoint triggered an early commit.  The
        // savepoint code path is still exercised through finish() / close()
        // which call sinkManager.flush().
        return env;
    }

    /**
     * Runs {@code env.execute(jobName)} on a separate thread and fails if it does not return
     * within {@link #FLINK_EXECUTE_TIMEOUT_MS}. Avoids blocking the Surefire thread forever
     * when the job deadlocks.
     */
    private void runFlinkJobWithTimeout(StreamExecutionEnvironment env, String jobName) throws Exception {
        AtomicReference<Throwable> executionError = new AtomicReference<>();
        Thread jobThread = new Thread(() -> {
            try {
                env.execute(jobName);
            } catch (Throwable t) {
                executionError.set(t);
            }
        }, "flink-job-" + jobName);
        jobThread.start();
        jobThread.join(FLINK_EXECUTE_TIMEOUT_MS);
        if (jobThread.isAlive()) {
            jobThread.interrupt();
            throw new AssertionError(String.format(
                    "Flink job '%s' did not finish within %d ms (stuck in execute?); thread interrupted",
                    jobName, FLINK_EXECUTE_TIMEOUT_MS));
        }
        Throwable t = executionError.get();
        if (t != null) {
            if (t instanceof Exception) {
                throw (Exception) t;
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            throw new Exception(t);
        }
    }

    private static DefaultStarRocksRowData row(String db, String table, String json,
                                               int partition, boolean txnEnd) {
        DefaultStarRocksRowData r = new DefaultStarRocksRowData(null, db, table, json);
        r.setSourcePartition(partition);
        r.setTransactionEnd(txnEnd);
        return r;
    }

    private StarRocksSink<DefaultStarRocksRowData> buildSinkV2(
            String ordersTable, String orderItemsTable, int flushIntervalMs) {
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", getHttpUrls())
                .withProperty("database-name", "*")
                .withProperty("table-name", "*")
                .withProperty("username", USERNAME)
                .withProperty("password", PASSWORD)
                .withProperty("sink.version", "V2")
                .withProperty("sink.semantic", "at-least-once")
                .withProperty("sink.transaction.multi-table.enabled", "true")
                .withProperty("sink.buffer-flush.interval-ms", String.valueOf(flushIntervalMs))
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();

        options.addTableProperties(StreamLoadTableProperties.builder()
                .database(DB_NAME)
                .table(ordersTable)
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build());

        options.addTableProperties(StreamLoadTableProperties.builder()
                .database(DB_NAME)
                .table(orderItemsTable)
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build());

        return SinkFunctionFactory.createSink(options, new PassThroughSerializationSchema());
    }

    /**
     * A pass-through {@link RecordSerializationSchema} that returns the input
     * {@link DefaultStarRocksRowData} directly as the output {@link StarRocksRowData}.
     * Used in SinkV2 API tests where the input already contains database, table,
     * partition, and transaction boundary information.
     */
    private static class PassThroughSerializationSchema
            implements RecordSerializationSchema<DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public void open(SerializationSchema.InitializationContext context,
                         StarRocksSinkContext sinkContext) {
            // no-op
        }

        @Override
        public StarRocksRowData serialize(DefaultStarRocksRowData record) {
            return record;
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
