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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.scanTable;
import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.verifyResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Integration tests for multi-table atomic transaction stream load through the
 * sink v2 ({@code sinkTo}) API.
 *
 * <p>The Flink 2.x port of the 1.x suite: the same scenarios, with the
 * {@code SourceFunction}-based controlled sources replaced by gated elements and
 * a rate-limited generator source.
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

    private static final Logger LOG = LoggerFactory.getLogger(MultiTableTransactionITTest.class);

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
     * How long to wait after a txnEnd row is emitted before asserting that data
     * is visible in StarRocks (ms).
     *
     * <p>With the event-driven commit implementation, the task thread executes
     * {@code switchChunk} immediately on txnEnd, then the manager thread drives:
     * HTTP load (~100 ms) → prepare (~50 ms) → commit (~50 ms).
     * We add a 2 s buffer for CI/network variance.
     */
    private static final long COMMIT_PROPAGATION_MS = 2_000L;

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
     * End-to-end test: two source partitions, sink parallelism = 2.
     *
     * <p>Verifies the core routing contract: {@code keyBy(sourcePartition)} ensures
     * each sink subtask handles exactly one partition, so one partition's data is
     * always processed by the same sink instance.
     *
     * <ul>
     *   <li>Partition 0 → {@code orders} table (2 rows + txnEnd)</li>
     *   <li>Partition 1 → {@code order_items} table (3 rows + txnEnd)</li>
     * </ul>
     *
     * <p>After the job finishes ({@code close()} flushes remaining data), both
     * tables must contain the expected rows.
     */
    @Test
    public void testEndToEndMultiPartition() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        StreamExecutionEnvironment env = buildEnv(2);

        DataStream<DefaultStarRocksRowData> partition0 = env.fromElements(
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":99.99,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, ordersTable,
                        "{\"order_id\":2,\"customer_id\":101,\"total_amount\":25.00,\"order_status\":\"created\"}",
                        0, true)
        ).returns(TypeInformation.of(DefaultStarRocksRowData.class));

        DataStream<DefaultStarRocksRowData> partition1 = env.fromElements(
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":49.99}",
                        1, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":2,\"order_id\":1,\"product_name\":\"gadget\",\"quantity\":1,\"price\":50.00}",
                        1, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":3,\"order_id\":2,\"product_name\":\"doohickey\",\"quantity\":3,\"price\":25.00}",
                        1, true)
        ).returns(TypeInformation.of(DefaultStarRocksRowData.class));

        partition0.union(partition1)
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .sinkTo(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS, null))
                .setParallelism(2);

        runFlinkJobWithTimeout(env, "testEndToEndMultiPartition");

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 100L, new BigDecimal("99.99"), "created"),
                        Arrays.asList(2L, 101L, new BigDecimal("25.00"), "created")),
                scanTable(DB_CONNECTION, DB_NAME, ordersTable));

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 1L, "widget",    2, new BigDecimal("49.99")),
                        Arrays.asList(2L, 1L, "gadget",    1, new BigDecimal("50.00")),
                        Arrays.asList(3L, 2L, "doohickey", 3, new BigDecimal("25.00"))),
                scanTable(DB_CONNECTION, DB_NAME, orderItemsTable));
    }

    /**
     * Transaction consistency timing test — the core correctness guarantee.
     *
     * <p>Verifies that data is NOT visible in StarRocks before the txnEnd marker
     * arrives, even after the flush interval has elapsed multiple times.
     * Only after txnEnd is received should the data become visible.
     *
     * <p>Timeline:
     * <pre>
     *   t=0              source emits data rows (no txnEnd)
     *   t=2×interval     assert tables EMPTY  ← flush timer fired but no commit
     *   t=2×interval     emit txnEnd          ← event-driven commit triggered
     *   t=2×interval+Δ   assert tables have data (Δ ≈ network round-trip)
     * </pre>
     *
     * <p>This test validates the {@code setCommitAllowed} path in
     * {@code DefaultStreamLoadManager}: on txnEnd the task thread marks the
     * partition's regions at a clean boundary ({@code markCleanBoundary}) and
     * the partition-level {@code lockstepSwitchPartition} freezes activeChunks
     * into inactiveChunks, and the manager thread subsequently runs autonomous
     * flush → prepare → commit when the commit interval elapses.
     */
    @Test
    public void testNoFlushBeforeTxnEnd() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        // data rows, then the txnEnd row once the test releases it
        Gate txnEnd = new Gate();
        StreamExecutionEnvironment env = buildEnv(1);
        gated(env, Collections.singletonMap(3, txnEnd),
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":99.99,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":49.99}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":2,\"order_id\":1,\"product_name\":\"gadget\",\"quantity\":1,\"price\":50.00}",
                        0, false),
                row(DB_NAME, ordersTable, null, 0, true))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .sinkTo(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS, null))
                .setParallelism(1);

        Thread jobThread = new Thread(() -> {
            try {
                env.execute("testNoFlushBeforeTxnEnd");
            } catch (Exception e) {
                LOG.warn("Job thread finished (may be expected on cancel)", e);
            }
        }, "flink-job-thread");
        jobThread.start();

        try {
            // Wait 2× flush interval — the age-based timer fires but commitAllowed=false
            Thread.sleep(2L * FLUSH_INTERVAL_MS);

            assertEquals("orders must be empty before txnEnd",
                    0, scanTable(DB_CONNECTION, DB_NAME, ordersTable).size());
            assertEquals("order_items must be empty before txnEnd",
                    0, scanTable(DB_CONNECTION, DB_NAME, orderItemsTable).size());

            LOG.info("Confirmed: no data visible before txnEnd. Sending txnEnd signal.");
            txnEnd.release();

            assertTrue("txnEnd row should be emitted within 10 s", txnEnd.awaitEmitted());

            // Event-driven commit: switchChunk already done on task thread;
            // manager thread only needs one HTTP round-trip to flush + prepare + commit.
            Thread.sleep(COMMIT_PROPAGATION_MS);

            List<List<Object>> ordersAfter = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            List<List<Object>> itemsAfter  = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);

            assertEquals("orders must have 1 row after txnEnd", 1, ordersAfter.size());
            assertEquals("order_items must have 2 rows after txnEnd", 2, itemsAfter.size());

            verifyResult(
                    Arrays.asList(Arrays.asList(1L, 100L, new BigDecimal("99.99"), "created")),
                    ordersAfter);
            verifyResult(
                    Arrays.asList(
                            Arrays.asList(1L, 1L, "widget", 2, new BigDecimal("49.99")),
                            Arrays.asList(2L, 1L, "gadget", 1, new BigDecimal("50.00"))),
                    itemsAfter);

            LOG.info("Confirmed: data visible after txnEnd. Transaction consistency verified.");
        } finally {
            jobThread.interrupt();
            jobThread.join(5_000);
        }
    }

    /**
     * Consecutive transaction isolation test.
     *
     * <p>Verifies that after txn-1 commits, txn-2's data is NOT visible until
     * txn-2's own txnEnd arrives. This exercises the "re-arm" logic: after a
     * commit cycle completes, {@code commitInFlight} is reset to {@code false}
     * so the next source transaction is blocked until its own txnEnd marker.
     *
     * <p>Timeline:
     * <pre>
     *   t=0              txn-1 data emitted
     *   t=2×interval     assert EMPTY  (no txnEnd yet)
     *   t=2×interval     emit txn-1 txnEnd → commit
     *   t=2×interval+Δ   assert 1 row  (txn-1 visible, txn-2 not started)
     *   t=...            txn-2 data emitted (commitInFlight re-armed to false)
     *   t=...+2×interval assert still 1 row (txn-2 data buffered, no txnEnd)
     *   t=...            emit txn-2 txnEnd → commit
     *   t=...+Δ          assert 2 rows (both txns visible)
     * </pre>
     */
    @Test
    public void testMultipleConsecutiveTransactions() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        // two transactions, each txnEnd held until the test releases it
        Gate txn1End = new Gate();
        Gate txn2End = new Gate();
        Map<Integer, Gate> gates = new HashMap<>();
        gates.put(1, txn1End);
        gates.put(3, txn2End);
        StreamExecutionEnvironment env = buildEnv(1);
        gated(env, gates,
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":10.00,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, ordersTable, null, 0, true),
                row(DB_NAME, ordersTable,
                        "{\"order_id\":2,\"customer_id\":101,\"total_amount\":20.00,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, ordersTable, null, 0, true))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .sinkTo(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS, null))
                .setParallelism(1);

        Thread jobThread = new Thread(() -> {
            try {
                env.execute("testMultipleConsecutiveTransactions");
            } catch (Exception e) {
                LOG.warn("Job thread finished (may be expected on cancel)", e);
            }
        }, "flink-job-thread");
        jobThread.start();

        try {
            // ---- Phase 1: txn-1 data emitted, no txnEnd yet ----
            Thread.sleep(2L * FLUSH_INTERVAL_MS);
            assertEquals("orders empty before txn-1 txnEnd",
                    0, scanTable(DB_CONNECTION, DB_NAME, ordersTable).size());

            // Allow txn-1 to commit
            txn1End.release();
            assertTrue("txn-1 end emitted within 10 s", txn1End.awaitEmitted());
            Thread.sleep(COMMIT_PROPAGATION_MS);

            // txn-1 data must now be visible
            List<List<Object>> afterTxn1 = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            assertEquals("orders must have 1 row after txn-1", 1, afterTxn1.size());
            verifyResult(
                    Arrays.asList(Arrays.asList(1L, 100L, new BigDecimal("10.00"), "created")),
                    afterTxn1);

            // ---- Phase 2: txn-2 data emitted, no txnEnd yet ----
            // commitInFlight was reset to false after txn-1 commit
            Thread.sleep(2L * FLUSH_INTERVAL_MS);
            List<List<Object>> midTxn2 = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            assertEquals("orders must still have only 1 row (txn-2 not committed yet)",
                    1, midTxn2.size());

            // Allow txn-2 to commit
            txn2End.release();
            assertTrue("txn-2 end emitted within 10 s", txn2End.awaitEmitted());
            Thread.sleep(COMMIT_PROPAGATION_MS);

            // Both txn-1 and txn-2 data must be visible
            List<List<Object>> afterTxn2 = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            assertEquals("orders must have 2 rows after txn-2", 2, afterTxn2.size());
            verifyResult(
                    Arrays.asList(
                            Arrays.asList(1L, 100L, new BigDecimal("10.00"), "created"),
                            Arrays.asList(2L, 101L, new BigDecimal("20.00"), "created")),
                    afterTxn2);

            LOG.info("Confirmed: consecutive transaction isolation verified.");
        } finally {
            jobThread.interrupt();
            jobThread.join(5_000);
        }
    }

    /**
     * Boundary test: a txnEnd row with no preceding data rows must not cause
     * an error and must leave the tables empty.
     */
    @Test
    public void testEmptyTransaction() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        StreamExecutionEnvironment env = buildEnv(1);
        // Only a txnEnd row, no actual data
        env.fromElements(
                row(DB_NAME, ordersTable, null, 0, true)
        ).returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .sinkTo(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS, null));

        runFlinkJobWithTimeout(env, "testEmptyTransaction");

        assertEquals("orders must be empty after empty transaction",
                0, scanTable(DB_CONNECTION, DB_NAME, ordersTable).size());
        assertEquals("order_items must be empty after empty transaction",
                0, scanTable(DB_CONNECTION, DB_NAME, orderItemsTable).size());
    }

    /**
     * Atomicity test: verifies that both tables become visible via a single
     * shared StarRocks transaction label.
     *
     * <p>This is the strongest atomicity guarantee: after txnEnd triggers a commit,
     * we verify via {@code show proc '/transactions/{db}/finished'} that there is
     * exactly one COMMITTED transaction for both tables' data, and both tables
     * become non-empty at the same query point.
     *
     * <p>Timeline:
     * <pre>
     *   t=0              source emits rows to BOTH orders and order_items (no txnEnd)
     *   t=2×interval     assert both tables EMPTY
     *   t=2×interval     emit txnEnd
     *   t=2×interval+Δ   assert both tables non-empty simultaneously
     *   t=2×interval+Δ   verify exactly 1 finished transaction with the label prefix
     * </pre>
     */
    @Test
    public void testAtomicVisibilityAcrossTables() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        String labelPrefix = "test-atomic-" + genRandomUuid().substring(0, 8) + "-";

        // rows for both tables in partition 0, then the txnEnd row once the test releases it
        Gate txnEnd = new Gate();
        StreamExecutionEnvironment env = buildEnv(1);
        gated(env, Collections.singletonMap(3, txnEnd),
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":88.88,\"order_status\":\"pending\"}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":1,\"order_id\":1,\"product_name\":\"alpha\",\"quantity\":1,\"price\":44.44}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":2,\"order_id\":1,\"product_name\":\"beta\",\"quantity\":2,\"price\":22.22}",
                        0, false),
                row(DB_NAME, ordersTable, null, 0, true))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .sinkTo(buildSink(ordersTable, orderItemsTable,
                        FLUSH_INTERVAL_MS, labelPrefix))
                .setParallelism(1);

        Thread jobThread = new Thread(() -> {
            try {
                env.execute("testAtomicVisibilityAcrossTables");
            } catch (Exception e) {
                LOG.warn("Job thread finished (may be expected on cancel)", e);
            }
        }, "flink-job-thread");
        jobThread.start();

        try {
            // Wait for data to be buffered but not committed
            Thread.sleep(2L * FLUSH_INTERVAL_MS);

            assertEquals("orders must be empty before txnEnd",
                    0, scanTable(DB_CONNECTION, DB_NAME, ordersTable).size());
            assertEquals("order_items must be empty before txnEnd",
                    0, scanTable(DB_CONNECTION, DB_NAME, orderItemsTable).size());

            // Signal txnEnd
            txnEnd.release();
            assertTrue("txnEnd row should be emitted within 10 s", txnEnd.awaitEmitted());

            Thread.sleep(COMMIT_PROPAGATION_MS);

            // Both tables must be non-empty at the same query point
            List<List<Object>> ordersAfter = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            List<List<Object>> itemsAfter = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);

            assertEquals("orders must have 1 row after txnEnd", 1, ordersAfter.size());
            assertEquals("order_items must have 2 rows after txnEnd", 2, itemsAfter.size());

            // Verify atomicity: exactly 1 finished transaction with our label prefix
            List<TransactionInfo> txns = getFinishedTransactionInfo(labelPrefix);
            LOG.info("Finished transactions with prefix '{}': {}", labelPrefix, txns.size());
            for (TransactionInfo txn : txns) {
                LOG.info("  txn: label={}, status={}", txn.label, txn.transactionStatus);
            }

            // Filter for COMMITTED (not ABORTED) transactions
            long committedCount = txns.stream()
                    .filter(t -> "COMMITTED".equalsIgnoreCase(t.transactionStatus)
                            || "VISIBLE".equalsIgnoreCase(t.transactionStatus))
                    .count();
            assertEquals("Exactly 1 committed transaction expected (shared label for both tables)",
                    1, committedCount);

            LOG.info("Confirmed: cross-table atomic visibility with single shared transaction.");
        } finally {
            jobThread.interrupt();
            jobThread.join(5_000);
        }
    }

    /**
     * Per-partition independence test: completed source transactions on one
     * partition are committed independently of in-progress transactions on
     * another partition.
     *
     * <p>In the current design, each {@code sourcePartition} represents an
     * independent source transaction stream. When P0's transaction completes
     * (txnEnd) but P1's is still in progress, P0's data is committed on the
     * next commit interval — it is NOT held hostage by P1's incomplete
     * transaction. P1's data stays in its activeChunk (not yet frozen into
     * inactiveChunks) and becomes committable only after P1's own txnEnd
     * arrives.
     *
     * <p>This per-partition independence preserves cross-TABLE atomicity
     * within a single source transaction (all tables written by one partition
     * within one txnEnd commit together) while avoiding unnecessary commit
     * latency caused by unrelated laggard partitions.
     *
     * <p>Timeline:
     * <pre>
     *   t=0              P0 emits data + txnEnd; P1 emits data only (no txnEnd)
     *   t=2×interval+Δ   assert orders has P0's row, order_items is EMPTY
     *   t=2×interval+Δ   P1 sends txnEnd
     *   t=3×interval+Δ   assert order_items has P1's row
     * </pre>
     */
    @Test
    public void testPerPartitionIndependentCommit() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        // P0 writes orders + txnEnd, P1 writes order_items; P1's txnEnd waits for the test
        Gate p1TxnEnd = new Gate();
        StreamExecutionEnvironment env = buildEnv(1);
        gated(env, Collections.singletonMap(3, p1TxnEnd),
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":50.00,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, ordersTable, null, 0, true),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":25.00}",
                        1, false),
                row(DB_NAME, orderItemsTable, null, 1, true))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .sinkTo(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS, null))
                .setParallelism(1);

        Thread jobThread = new Thread(() -> {
            try {
                env.execute("testPerPartitionIndependentCommit");
            } catch (Exception e) {
                LOG.warn("Job thread finished (may be expected on cancel)", e);
            }
        }, "flink-job-thread");
        jobThread.start();

        try {
            // Wait for P0 data + txnEnd and P1 data (no txnEnd) to be processed
            assertTrue("Phase 1 should complete within 10 s", p1TxnEnd.awaitReached());

            // Wait enough flush intervals for the commit interval to elapse and
            // the manager thread to trigger a time-driven commit of P0's data.
            Thread.sleep(2L * FLUSH_INTERVAL_MS + COMMIT_PROPAGATION_MS);

            // P0's orders row must be committed (P0 sent txnEnd, its source
            // transaction is complete). P1's order_items row must NOT yet be
            // committed because P1's activeChunk is still dirty (no txnEnd).
            assertEquals("orders must have P0's row committed independently",
                    1, scanTable(DB_CONNECTION, DB_NAME, ordersTable).size());
            assertEquals("order_items must be empty (P1 still in-progress)",
                    0, scanTable(DB_CONNECTION, DB_NAME, orderItemsTable).size());

            LOG.info("Confirmed: P0's completed transaction committed without waiting for P1.");

            // Now let P1 send txnEnd; its data should become committable.
            p1TxnEnd.release();
            assertTrue("P1 txnEnd should be emitted within 10 s", p1TxnEnd.awaitEmitted());

            Thread.sleep(FLUSH_INTERVAL_MS + COMMIT_PROPAGATION_MS);

            // P1's order_items row should now be visible
            List<List<Object>> ordersAfter = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            List<List<Object>> itemsAfter = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);

            assertEquals("orders must still have 1 row", 1, ordersAfter.size());
            assertEquals("order_items must have 1 row after P1 txnEnd",
                    1, itemsAfter.size());

            verifyResult(
                    Arrays.asList(Arrays.asList(1L, 100L, new BigDecimal("50.00"), "created")),
                    ordersAfter);
            verifyResult(
                    Arrays.asList(Arrays.asList(1L, 1L, "widget", 2, new BigDecimal("25.00"))),
                    itemsAfter);

            LOG.info("Confirmed: P1's data visible after P1 sent txnEnd.");
        } finally {
            jobThread.interrupt();
            jobThread.join(5_000);
        }
    }

    /**
     * Flush data integrity test with a large flush interval.
     *
     * <p>Verifies that when the timer-driven flush never fires (because the flush
     * interval is very large), the {@code finish()} → {@code flush()} savepoint
     * path still commits all buffered multi-table data correctly.
     *
     * <p>The source emits data to two tables with a txnEnd marker on the last row,
     * then the bounded source finishes. The sink's {@code finish()} calls
     * {@code flush()}, which acts as a savepoint and must commit everything.
     * After the job completes, both tables must have all expected rows.
     */
    @Test
    public void testCheckpointTriggeredFlushDataIntegrity() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        StreamExecutionEnvironment env = buildEnv(1);
        // Use a very large flush interval so the timer never fires —
        // only the flush() from finish()/close() should commit the data.
        int largeFlushInterval = 60_000;

        // Emit data to two tables with txnEnd on the last row, then source finishes.
        // The sink's finish() will call flush(), which must commit everything.
        env.fromElements(
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":10.00,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, ordersTable,
                        "{\"order_id\":2,\"customer_id\":101,\"total_amount\":20.00,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":3,\"price\":10.00}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":2,\"order_id\":2,\"product_name\":\"gadget\",\"quantity\":1,\"price\":20.00}",
                        0, true)
        ).returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .sinkTo(buildSink(ordersTable, orderItemsTable, largeFlushInterval, null))
                .setParallelism(1);

        // finish()/close() triggers flush(); bounded wait so a hang still yields a test failure + Results
        runFlinkJobWithTimeout(env, "testCheckpointTriggeredFlushDataIntegrity");

        // All data must be committed by the savepoint path
        List<List<Object>> ordersAfter = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
        List<List<Object>> itemsAfter = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 100L, new BigDecimal("10.00"), "created"),
                        Arrays.asList(2L, 101L, new BigDecimal("20.00"), "created")),
                ordersAfter);

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 1L, "widget", 3, new BigDecimal("10.00")),
                        Arrays.asList(2L, 2L, "gadget", 1, new BigDecimal("20.00"))),
                itemsAfter);

        LOG.info("Confirmed: flush committed all multi-table data via savepoint path.");
    }

    /**
     * Checkpoint test: transaction fits entirely within one checkpoint.
     *
     * <p>Verifies that when the source emits data rows followed by a txnEnd marker
     * <em>before</em> the Flink checkpoint barrier arrives, the checkpoint succeeds
     * and all data is committed correctly to StarRocks.
     *
     * <p>This exercises the happy path of the checkpoint + multi-table transaction
     * interaction: by the time {@code flush()} is called during {@code snapshotState()},
     * all partitions have already received txnEnd, so
     * {@code getPartitionsWithoutTxnEnd()} returns an empty list and the shared
     * transaction commits successfully.
     *
     * <p>Timeline:
     * <pre>
     *   t=0              source emits data + txnEnd for both tables
     *   t=Δ              txnEnd triggers event-driven commit → data committed
     *   t=checkpoint     checkpoint barrier arrives → flush() finds no active partitions → OK
     *   t=finish         job finishes → verify data in StarRocks
     * </pre>
     */
    @Test
    public void testTransactionWithinSingleCheckpoint() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        File checkpointDir = temporaryFolder.newFolder();
        StreamExecutionEnvironment env = buildEnvWithCheckpointing(1, checkpointDir.toURI().toString());

        // the transaction is complete before the first checkpoint; the source then idles for ~5 s
        scripted(env, 500,
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":10.00,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, ordersTable,
                        "{\"order_id\":2,\"customer_id\":101,\"total_amount\":20.00,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":10.00}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":2,\"order_id\":2,\"product_name\":\"gadget\",\"quantity\":1,\"price\":20.00}",
                        0, false),
                row(DB_NAME, ordersTable, null, 0, true))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .sinkTo(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS, null))
                .setParallelism(1);

        runFlinkJobWithTimeout(env, "testTransactionWithinSingleCheckpoint");

        // Verify all data committed successfully
        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 100L, new BigDecimal("10.00"), "created"),
                        Arrays.asList(2L, 101L, new BigDecimal("20.00"), "created")),
                scanTable(DB_CONNECTION, DB_NAME, ordersTable));

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 1L, "widget", 2, new BigDecimal("10.00")),
                        Arrays.asList(2L, 2L, "gadget", 1, new BigDecimal("20.00"))),
                scanTable(DB_CONNECTION, DB_NAME, orderItemsTable));

        LOG.info("Confirmed: transaction within single checkpoint committed successfully.");
    }

    /**
     * Checkpoint test: transaction spans two checkpoints — must fail.
     *
     * <p>Verifies that when a source transaction is still open (no txnEnd marker
     * received) when the Flink checkpoint barrier arrives, the job fails with an
     * {@link IllegalStateException}. This enforces the contract that upstream must
     * complete all transactions before the checkpoint barrier.
     *
     * <p>The source emits data rows without txnEnd, then waits long enough for a
     * checkpoint to fire. The checkpoint's {@code snapshotState()} calls
     * {@code flush()}, which detects active partitions and throws:
     * <pre>
     *   [MultiTxn] Partitions [...] have written data but never received
     *   txnEnd at checkpoint. Upstream must ensure all transactions are
     *   complete before checkpoint barrier.
     * </pre>
     *
     * <p>Timeline:
     * <pre>
     *   t=0              source emits data rows (no txnEnd)
     *   t=checkpoint     checkpoint barrier arrives → flush() finds active partitions → FAIL
     * </pre>
     */
    @Test
    public void testTransactionSpansTwoCheckpointsFails() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        File checkpointDir = temporaryFolder.newFolder();
        // Short checkpoint interval so the barrier fires quickly
        StreamExecutionEnvironment env = buildEnvWithCheckpointing(1, checkpointDir.toURI().toString());

        // deliberately NO txnEnd; the source then idles past the first checkpoints
        scripted(env, 3000,
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":50.00,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":3,\"price\":16.67}",
                        0, false))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .sinkTo(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS, null))
                .setParallelism(1);

        AtomicReference<Throwable> executionError = new AtomicReference<>();
        Thread jobThread = new Thread(() -> {
            try {
                env.execute("testTransactionSpansTwoCheckpointsFails");
            } catch (Throwable t) {
                executionError.set(t);
            }
        }, "flink-job-testTransactionSpansTwoCheckpointsFails");
        jobThread.start();
        jobThread.join(FLINK_EXECUTE_TIMEOUT_MS);

        if (jobThread.isAlive()) {
            jobThread.interrupt();
            jobThread.join(5_000);
        }

        // The job must have failed
        Throwable error = executionError.get();
        assertTrue("Job should fail when transaction spans checkpoint, but completed successfully",
                error != null);

        // Unwrap to find the IllegalStateException about active partitions.
        // The manager thread reports the violation via one of two messages
        // (partition-level "never received txnEnd" or region-level "in-progress
        // transaction data"); either satisfies the contract.
        Throwable cause = error;
        boolean foundExpectedError = false;
        while (cause != null) {
            if (cause instanceof IllegalStateException && cause.getMessage() != null) {
                String msg = cause.getMessage();
                if (msg.contains("[MultiTxn]") && msg.contains("at checkpoint")
                        && (msg.contains("never received txnEnd")
                                || msg.contains("in-progress transaction data"))) {
                    foundExpectedError = true;
                    break;
                }
            }
            cause = cause.getCause();
        }

        assertTrue("Expected IllegalStateException about [MultiTxn] transaction state at checkpoint, "
                        + "but got: " + error.getMessage(),
                foundExpectedError);

        // Tables should be empty — the transaction was never committed
        assertEquals("orders must be empty after failed checkpoint",
                0, scanTable(DB_CONNECTION, DB_NAME, ordersTable).size());
        assertEquals("order_items must be empty after failed checkpoint",
                0, scanTable(DB_CONNECTION, DB_NAME, orderItemsTable).size());

        LOG.info("Confirmed: transaction spanning checkpoint correctly rejected with IllegalStateException.");
    }

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
                .sinkTo(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS, null))
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
                .sinkTo(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS, null))
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
    // Sources: SourceFunction is gone in Flink 2.x
    // -------------------------------------------------------------------------

    /** Lets the test hold a row back: the row is emitted only after {@link #release()}. */
    private static class Gate implements Serializable {

        private static final long serialVersionUID = 1L;
        // the job runs in this JVM; static maps keep the latches out of the serialized function
        private static final Map<String, CountDownLatch[]> LATCHES = new ConcurrentHashMap<>();

        private final String id = UUID.randomUUID().toString();

        Gate() {
            LATCHES.put(id, new CountDownLatch[] {new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1)});
        }

        void pass() throws InterruptedException {
            CountDownLatch[] latches = LATCHES.get(id);
            latches[0].countDown();
            latches[1].await();
            latches[2].countDown();
        }

        /** Whether every row before the gated one was emitted. */
        boolean awaitReached() throws InterruptedException {
            return LATCHES.get(id)[0].await(10, TimeUnit.SECONDS);
        }

        void release() {
            LATCHES.get(id)[1].countDown();
        }

        boolean awaitEmitted() throws InterruptedException {
            return LATCHES.get(id)[2].await(10, TimeUnit.SECONDS);
        }
    }

    /** Emits the rows in order, holding the row at each gated index until its gate is released. */
    private static DataStream<DefaultStarRocksRowData> gated(
            StreamExecutionEnvironment env, Map<Integer, Gate> gates, DefaultStarRocksRowData... rows) {
        return env.fromElements(rows)
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .setParallelism(1)
                .map(new GateFunction(new HashMap<>(gates)))
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .setParallelism(1);
    }

    private static class GateFunction implements MapFunction<DefaultStarRocksRowData, DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;
        private final Map<Integer, Gate> gates;
        private int index;

        GateFunction(Map<Integer, Gate> gates) {
            this.gates = gates;
        }

        @Override
        public DefaultStarRocksRowData map(DefaultStarRocksRowData row) throws Exception {
            Gate gate = gates.get(index++);
            if (gate != null) {
                gate.pass();
            }
            return row;
        }
    }

    /**
     * Emits the rows, then idles with no-op rows (no data, no txnEnd) so checkpoints run while the
     * source is still alive. It never blocks the task thread, so checkpoint barriers get through.
     */
    private static DataStream<DefaultStarRocksRowData> scripted(
            StreamExecutionEnvironment env, int idleRows, DefaultStarRocksRowData... rows) {
        DefaultStarRocksRowData idle = row(rows[0].getDatabase(), rows[0].getTable(), null, 0, false);
        return env.fromSource(
                        new DataGeneratorSource<>(new ScriptedRows(rows, idle), rows.length + idleRows,
                                RateLimiterStrategy.perSecond(100), TypeInformation.of(DefaultStarRocksRowData.class)),
                        WatermarkStrategy.noWatermarks(), "scripted rows")
                .setParallelism(1);
    }

    private static class ScriptedRows implements GeneratorFunction<Long, DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;
        // DefaultStarRocksRowData is not serializable; the job runs in this JVM
        private static final Map<String, DefaultStarRocksRowData[]> ROWS = new ConcurrentHashMap<>();

        private final String id = UUID.randomUUID().toString();

        ScriptedRows(DefaultStarRocksRowData[] rows, DefaultStarRocksRowData idle) {
            DefaultStarRocksRowData[] script = Arrays.copyOf(rows, rows.length + 1);
            script[rows.length] = idle;
            ROWS.put(id, script);
        }

        @Override
        public DefaultStarRocksRowData map(Long index) {
            DefaultStarRocksRowData[] script = ROWS.get(id);
            return script[(int) Math.min(index, script.length - 1)];
        }
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

    private StreamExecutionEnvironment buildEnvWithCheckpointing(int parallelism, String checkpointDir) {
        Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        // the first checkpoint fires at a random point of the interval, but never before the min pause,
        // so the scripted rows of a transaction are all written before it
        conf.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofMillis(900));
        conf.setString("restart-strategy.type", "none");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelism);
        env.enableCheckpointing(1000);
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

    private StarRocksSink<DefaultStarRocksRowData> buildSink(
            String ordersTable, String orderItemsTable, int flushIntervalMs, @Nullable String labelPrefix) {
        StarRocksSinkOptions.Builder builder = StarRocksSinkOptions.builder()
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
                .withProperty("sink.properties.strip_outer_array", "true");
        if (labelPrefix != null) {
            builder.withProperty("sink.label-prefix", labelPrefix);
        }
        StarRocksSinkOptions options = builder.build();

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
