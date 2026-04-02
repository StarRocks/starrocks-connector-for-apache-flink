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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assume.assumeTrue;

import java.io.File;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.scanTable;
import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.verifyResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for multi-table atomic transaction stream load.
 *
 * <p>Requires StarRocks >= 4.0 for multi-table transaction support.
 *
 * <p>Run against an external cluster:
 * <pre>
 *   mvn test -Dtest=MultiTableTransactionITTest \
 *     -Dit.starrocks.fe.http=172.26.95.228:8030 \
 *     -Dit.starrocks.fe.jdbc=jdbc:mysql://172.26.95.228:9030
 * </pre>
 *
 * <h2>Test coverage</h2>
 * <ol>
 *   <li>{@link #testEndToEndMultiPartition} — end-to-end write with parallelism=2 and
 *       per-partition routing; verifies data correctness across two tables.</li>
 *   <li>{@link #testNoFlushBeforeTxnEnd} — transaction consistency: data must NOT be
 *       visible before txnEnd, and MUST be visible shortly after.</li>
 *   <li>{@link #testMultipleConsecutiveTransactions} — consecutive transaction isolation:
 *       txn-2 data must not be committed until txn-2's own txnEnd arrives (re-arm logic).</li>
 *   <li>{@link #testEmptyTransaction} — boundary: a txnEnd with no preceding data must
 *       not cause errors.</li>
 *   <li>{@link #testAtomicVisibilityAcrossTables} — atomicity: both tables become visible
 *       simultaneously via a single shared StarRocks transaction label.</li>
 *   <li>{@link #testPartialPartitionTxnEndBlocking} — blocking: when only one of two
 *       partitions has sent txnEnd, data must NOT be committed.</li>
 *   <li>{@link #testCheckpointTriggeredFlushDataIntegrity} — checkpoint: Flink checkpoint
 *       triggers flush(), all buffered data must be committed completely.</li>
 *   <li>{@link #testTransactionWithinSingleCheckpoint} — checkpoint: when the entire
 *       transaction (data + txnEnd) completes before the checkpoint barrier, the
 *       checkpoint succeeds and data is committed correctly.</li>
 *   <li>{@link #testTransactionSpansTwoCheckpointsFails} — checkpoint: when a transaction
 *       is still open (no txnEnd) when the checkpoint barrier arrives, the job must
 *       fail with an {@link IllegalStateException} indicating active partitions.</li>
 * </ol>
 */
public class MultiTableTransactionITTest extends StarRocksITTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MultiTableTransactionITTest.class);

    @Before
    public void checkVersion() {
        assumeTrue("Multi-table transaction requires StarRocks >= 4.0",
                isStarRocksVersionAtLeast(4, 0));
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
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
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
     * <p>This test validates the event-driven {@code setCommitAllowed} path in
     * {@code DefaultStreamLoadManager}: on txnEnd the task thread immediately calls
     * {@code switchChunkForCommit()} on all regions and signals the manager thread,
     * which then drives flush → prepare → commit without waiting for the age timer.
     */
    @Test
    public void testNoFlushBeforeTxnEnd() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        TxnEndControlledSource.reset(DB_NAME, ordersTable, orderItemsTable);

        StreamExecutionEnvironment env = buildEnv(1);
        env.addSource(new TxnEndControlledSource())
                .setParallelism(1)
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
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
            TxnEndControlledSource.SEND_TXN_END_LATCH.countDown();

            assertTrue("txnEnd row should be emitted within 10 s",
                    TxnEndControlledSource.TXN_END_EMITTED_LATCH.await(10, TimeUnit.SECONDS));

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

        MultiTxnControlledSource.reset(DB_NAME, ordersTable, orderItemsTable);

        StreamExecutionEnvironment env = buildEnv(1);
        env.addSource(new MultiTxnControlledSource())
                .setParallelism(1)
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
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
            MultiTxnControlledSource.TXN1_END_LATCH.countDown();
            assertTrue("txn-1 end emitted within 10 s",
                    MultiTxnControlledSource.TXN1_END_EMITTED_LATCH.await(10, TimeUnit.SECONDS));
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
            MultiTxnControlledSource.TXN2_END_LATCH.countDown();
            assertTrue("txn-2 end emitted within 10 s",
                    MultiTxnControlledSource.TXN2_END_EMITTED_LATCH.await(10, TimeUnit.SECONDS));
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
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS));

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

        AtomicVisibilitySource.reset(DB_NAME, ordersTable, orderItemsTable);

        StreamExecutionEnvironment env = buildEnv(1);
        env.addSource(new AtomicVisibilitySource())
                .setParallelism(1)
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSinkWithLabelPrefix(ordersTable, orderItemsTable,
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
            AtomicVisibilitySource.SEND_TXN_END_LATCH.countDown();
            assertTrue("txnEnd row should be emitted within 10 s",
                    AtomicVisibilitySource.TXN_END_EMITTED_LATCH.await(10, TimeUnit.SECONDS));

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
     * Partial partition blocking test: when two partitions are active but only
     * one has sent txnEnd, data must NOT be committed.
     *
     * <p>This validates that {@code PartitionCommitTracker.allSwitched()} correctly
     * blocks the commit until ALL active partitions have reached txnEnd.
     *
     * <p>Timeline:
     * <pre>
     *   t=0              P0 emits data + txnEnd; P1 emits data only (no txnEnd)
     *   t=3×interval     assert BOTH tables EMPTY (P1 blocks the commit)
     *   t=3×interval     P1 sends txnEnd
     *   t=3×interval+Δ   assert both tables have data
     * </pre>
     */
    @Test
    public void testPartialPartitionTxnEndBlocking() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        PartialPartitionSource.reset(DB_NAME, ordersTable, orderItemsTable);

        StreamExecutionEnvironment env = buildEnv(1);
        env.addSource(new PartialPartitionSource())
                .setParallelism(1)
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
                .setParallelism(1);

        Thread jobThread = new Thread(() -> {
            try {
                env.execute("testPartialPartitionTxnEndBlocking");
            } catch (Exception e) {
                LOG.warn("Job thread finished (may be expected on cancel)", e);
            }
        }, "flink-job-thread");
        jobThread.start();

        try {
            // Wait for P0 data + txnEnd and P1 data (no txnEnd) to be processed
            assertTrue("Phase 1 should complete within 10 s",
                    PartialPartitionSource.PHASE1_DONE_LATCH.await(10, TimeUnit.SECONDS));

            // Wait extra flush intervals to ensure the timer fires multiple times
            Thread.sleep(3L * FLUSH_INTERVAL_MS);

            // P1 has NOT sent txnEnd → allSwitched() is false → no commit
            assertEquals("orders must be empty (P1 blocks commit)",
                    0, scanTable(DB_CONNECTION, DB_NAME, ordersTable).size());
            assertEquals("order_items must be empty (P1 blocks commit)",
                    0, scanTable(DB_CONNECTION, DB_NAME, orderItemsTable).size());

            LOG.info("Confirmed: partial partition txnEnd correctly blocks commit.");

            // Now let P1 send txnEnd
            PartialPartitionSource.P1_TXN_END_LATCH.countDown();
            assertTrue("P1 txnEnd should be emitted within 10 s",
                    PartialPartitionSource.P1_TXN_END_EMITTED_LATCH.await(10, TimeUnit.SECONDS));

            Thread.sleep(COMMIT_PROPAGATION_MS);

            // Now both partitions have txnEnd → data should be committed
            List<List<Object>> ordersAfter = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            List<List<Object>> itemsAfter = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);

            assertEquals("orders must have 1 row after both partitions txnEnd",
                    1, ordersAfter.size());
            assertEquals("order_items must have 1 row after both partitions txnEnd",
                    1, itemsAfter.size());

            verifyResult(
                    Arrays.asList(Arrays.asList(1L, 100L, new BigDecimal("50.00"), "created")),
                    ordersAfter);
            verifyResult(
                    Arrays.asList(Arrays.asList(1L, 1L, "widget", 2, new BigDecimal("25.00"))),
                    itemsAfter);

            LOG.info("Confirmed: data visible after all partitions sent txnEnd.");
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
                .addSink(buildSink(ordersTable, orderItemsTable, largeFlushInterval))
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
     * all partitions have already received txnEnd, so {@code getActivePartitions()}
     * returns an empty list and the shared transaction commits successfully.
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

        CheckpointTxnWithinSource.reset(DB_NAME, ordersTable, orderItemsTable);

        File checkpointDir = temporaryFolder.newFolder();
        StreamExecutionEnvironment env = buildEnvWithCheckpointing(1, checkpointDir.toURI().toString());

        env.addSource(new CheckpointTxnWithinSource())
                .setParallelism(1)
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
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
     *   [MultiTxn] Partitions [...] still have uncommitted transaction data
     *   at checkpoint.
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

        CheckpointTxnSpansSource.reset(DB_NAME, ordersTable, orderItemsTable);

        File checkpointDir = temporaryFolder.newFolder();
        // Short checkpoint interval so the barrier fires quickly
        StreamExecutionEnvironment env = buildEnvWithCheckpointing(1, checkpointDir.toURI().toString());

        env.addSource(new CheckpointTxnSpansSource())
                .setParallelism(1)
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
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

        // Unwrap to find the IllegalStateException about active partitions
        Throwable cause = error;
        boolean foundExpectedError = false;
        while (cause != null) {
            if (cause instanceof IllegalStateException
                    && cause.getMessage() != null
                    && cause.getMessage().contains("uncommitted transaction data at checkpoint")) {
                foundExpectedError = true;
                break;
            }
            cause = cause.getCause();
        }

        assertTrue("Expected IllegalStateException about uncommitted transaction data at checkpoint, "
                        + "but got: " + error.getMessage(),
                foundExpectedError);

        // Tables should be empty — the transaction was never committed
        assertEquals("orders must be empty after failed checkpoint",
                0, scanTable(DB_CONNECTION, DB_NAME, ordersTable).size());
        assertEquals("order_items must be empty after failed checkpoint",
                0, scanTable(DB_CONNECTION, DB_NAME, orderItemsTable).size());

        LOG.info("Confirmed: transaction spanning checkpoint correctly rejected with IllegalStateException.");
    }

    // -------------------------------------------------------------------------
    // Item 13: SinkV2 API (new unified Flink Sink API) multi-table test
    // -------------------------------------------------------------------------

    /**
     * End-to-end test using the new Flink Sink V2 API ({@link StarRocksSink} +
     * {@link com.starrocks.connector.flink.table.sink.v2.StarRocksWriter} +
     * {@link com.starrocks.connector.flink.table.sink.v2.StarRocksCommitter})
     * instead of the legacy {@code SinkFunction} API.
     *
     * <p>This verifies that multi-table transactions work correctly through the
     * two-phase commit path: {@code StarRocksWriter.write()} routes data by
     * partition → {@code flush(endOfInput=true)} triggers savepoint → data committed.
     *
     * <p>Uses {@code sinkTo()} instead of {@code addSink()}.
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
    // Controlled source functions (use static fields to avoid ClosureCleaner)
    // -------------------------------------------------------------------------

    /**
     * Source for {@link #testNoFlushBeforeTxnEnd}.
     *
     * <p>Phase 1: emits data rows without txnEnd (sink must not commit).
     * Phase 2: waits for {@link #SEND_TXN_END_LATCH}, then emits the txnEnd marker.
     */
    private static class TxnEndControlledSource
            extends RichParallelSourceFunction<DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;

        static volatile CountDownLatch SEND_TXN_END_LATCH;
        static volatile CountDownLatch TXN_END_EMITTED_LATCH;
        static volatile String DB;
        static volatile String ORDERS_TABLE;
        static volatile String ORDER_ITEMS_TABLE;

        static void reset(String db, String ordersTable, String orderItemsTable) {
            DB = db;
            ORDERS_TABLE = ordersTable;
            ORDER_ITEMS_TABLE = orderItemsTable;
            SEND_TXN_END_LATCH    = new CountDownLatch(1);
            TXN_END_EMITTED_LATCH = new CountDownLatch(1);
        }

        @Override
        public void run(SourceContext<DefaultStarRocksRowData> ctx) throws Exception {
            // Phase 1: data rows, no txnEnd
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":1,\"customer_id\":100,\"total_amount\":99.99,\"order_status\":\"created\"}",
                    0, false));
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":49.99}",
                    0, false));
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":2,\"order_id\":1,\"product_name\":\"gadget\",\"quantity\":1,\"price\":50.00}",
                    0, false));

            SEND_TXN_END_LATCH.await();

            // Phase 2: txnEnd marker (null row = no data, just signal end-of-transaction)
            ctx.collect(row(DB, ORDERS_TABLE, null, 0, true));
            TXN_END_EMITTED_LATCH.countDown();
        }

        @Override
        public void cancel() {
            CountDownLatch l = SEND_TXN_END_LATCH;
            if (l != null) l.countDown();
        }
    }

    /**
     * Source for {@link #testMultipleConsecutiveTransactions}.
     *
     * <p>Emits two back-to-back transactions, each gated by its own latch pair so
     * the test thread can control exactly when each txnEnd is delivered.
     */
    private static class MultiTxnControlledSource
            extends RichParallelSourceFunction<DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;

        static volatile CountDownLatch TXN1_END_LATCH;
        static volatile CountDownLatch TXN1_END_EMITTED_LATCH;
        static volatile CountDownLatch TXN2_END_LATCH;
        static volatile CountDownLatch TXN2_END_EMITTED_LATCH;
        static volatile String DB;
        static volatile String ORDERS_TABLE;

        static void reset(String db, String ordersTable, String orderItemsTable) {
            DB = db;
            ORDERS_TABLE = ordersTable;
            TXN1_END_LATCH          = new CountDownLatch(1);
            TXN1_END_EMITTED_LATCH  = new CountDownLatch(1);
            TXN2_END_LATCH          = new CountDownLatch(1);
            TXN2_END_EMITTED_LATCH  = new CountDownLatch(1);
        }

        @Override
        public void run(SourceContext<DefaultStarRocksRowData> ctx) throws Exception {
            // txn-1 data
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":1,\"customer_id\":100,\"total_amount\":10.00,\"order_status\":\"created\"}",
                    0, false));

            TXN1_END_LATCH.await();
            ctx.collect(row(DB, ORDERS_TABLE, null, 0, true));  // txn-1 end
            TXN1_END_EMITTED_LATCH.countDown();

            // txn-2 data (commitInFlight re-armed to false after txn-1 commit)
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":2,\"customer_id\":101,\"total_amount\":20.00,\"order_status\":\"created\"}",
                    0, false));

            TXN2_END_LATCH.await();
            ctx.collect(row(DB, ORDERS_TABLE, null, 0, true));  // txn-2 end
            TXN2_END_EMITTED_LATCH.countDown();
        }

        @Override
        public void cancel() {
            CountDownLatch l1 = TXN1_END_LATCH;
            CountDownLatch l2 = TXN2_END_LATCH;
            if (l1 != null) l1.countDown();
            if (l2 != null) l2.countDown();
        }
    }

    /**
     * Source for {@link #testAtomicVisibilityAcrossTables}.
     *
     * <p>Emits data rows to both orders and order_items in a single partition,
     * then waits for the test thread to signal txnEnd.
     */
    private static class AtomicVisibilitySource
            extends RichParallelSourceFunction<DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;

        static volatile CountDownLatch SEND_TXN_END_LATCH;
        static volatile CountDownLatch TXN_END_EMITTED_LATCH;
        static volatile String DB;
        static volatile String ORDERS_TABLE;
        static volatile String ORDER_ITEMS_TABLE;

        static void reset(String db, String ordersTable, String orderItemsTable) {
            DB = db;
            ORDERS_TABLE = ordersTable;
            ORDER_ITEMS_TABLE = orderItemsTable;
            SEND_TXN_END_LATCH    = new CountDownLatch(1);
            TXN_END_EMITTED_LATCH = new CountDownLatch(1);
        }

        @Override
        public void run(SourceContext<DefaultStarRocksRowData> ctx) throws Exception {
            // Emit data to both tables in partition 0
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":1,\"customer_id\":100,\"total_amount\":88.88,\"order_status\":\"pending\"}",
                    0, false));
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":1,\"order_id\":1,\"product_name\":\"alpha\",\"quantity\":1,\"price\":44.44}",
                    0, false));
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":2,\"order_id\":1,\"product_name\":\"beta\",\"quantity\":2,\"price\":22.22}",
                    0, false));

            SEND_TXN_END_LATCH.await();
            ctx.collect(row(DB, ORDERS_TABLE, null, 0, true));
            TXN_END_EMITTED_LATCH.countDown();
        }

        @Override
        public void cancel() {
            CountDownLatch l = SEND_TXN_END_LATCH;
            if (l != null) l.countDown();
        }
    }

    /**
     * Source for {@link #testPartialPartitionTxnEndBlocking}.
     *
     * <p>Phase 1: P0 writes to orders + txnEnd; P1 writes to order_items (no txnEnd).
     * Phase 2: waits for latch, then P1 sends txnEnd.
     */
    private static class PartialPartitionSource
            extends RichParallelSourceFunction<DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;

        static volatile CountDownLatch PHASE1_DONE_LATCH;
        static volatile CountDownLatch P1_TXN_END_LATCH;
        static volatile CountDownLatch P1_TXN_END_EMITTED_LATCH;
        static volatile String DB;
        static volatile String ORDERS_TABLE;
        static volatile String ORDER_ITEMS_TABLE;

        static void reset(String db, String ordersTable, String orderItemsTable) {
            DB = db;
            ORDERS_TABLE = ordersTable;
            ORDER_ITEMS_TABLE = orderItemsTable;
            PHASE1_DONE_LATCH       = new CountDownLatch(1);
            P1_TXN_END_LATCH        = new CountDownLatch(1);
            P1_TXN_END_EMITTED_LATCH = new CountDownLatch(1);
        }

        @Override
        public void run(SourceContext<DefaultStarRocksRowData> ctx) throws Exception {
            // P0: write to orders + txnEnd
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":1,\"customer_id\":100,\"total_amount\":50.00,\"order_status\":\"created\"}",
                    0, false));
            ctx.collect(row(DB, ORDERS_TABLE, null, 0, true));  // P0 txnEnd

            // P1: write to order_items, NO txnEnd yet
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":25.00}",
                    1, false));

            // Signal test thread that phase 1 is done
            PHASE1_DONE_LATCH.countDown();

            // Wait for test thread to verify blocking, then send P1 txnEnd
            P1_TXN_END_LATCH.await();
            ctx.collect(row(DB, ORDER_ITEMS_TABLE, null, 1, true));  // P1 txnEnd
            P1_TXN_END_EMITTED_LATCH.countDown();
        }

        @Override
        public void cancel() {
            CountDownLatch l1 = PHASE1_DONE_LATCH;
            CountDownLatch l2 = P1_TXN_END_LATCH;
            if (l1 != null) l1.countDown();
            if (l2 != null) l2.countDown();
        }
    }

    /**
     * Source for {@link #testTransactionWithinSingleCheckpoint}.
     *
     * <p>Emits data to both tables followed by a txnEnd marker, then stays alive
     * long enough for at least one Flink checkpoint to succeed before finishing.
     */
    private static class CheckpointTxnWithinSource
            extends RichParallelSourceFunction<DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;

        static volatile String DB;
        static volatile String ORDERS_TABLE;
        static volatile String ORDER_ITEMS_TABLE;

        static void reset(String db, String ordersTable, String orderItemsTable) {
            DB = db;
            ORDERS_TABLE = ordersTable;
            ORDER_ITEMS_TABLE = orderItemsTable;
        }

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<DefaultStarRocksRowData> ctx) throws Exception {
            // Emit data to both tables
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":1,\"customer_id\":100,\"total_amount\":10.00,\"order_status\":\"created\"}",
                    0, false));
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":2,\"customer_id\":101,\"total_amount\":20.00,\"order_status\":\"created\"}",
                    0, false));
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":10.00}",
                    0, false));
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":2,\"order_id\":2,\"product_name\":\"gadget\",\"quantity\":1,\"price\":20.00}",
                    0, false));

            // txnEnd — transaction is complete before any checkpoint fires
            ctx.collect(row(DB, ORDERS_TABLE, null, 0, true));

            // Stay alive long enough for at least one checkpoint to complete,
            // then finish so the job terminates.
            long waited = 0;
            while (running && waited < 5_000) {
                Thread.sleep(100);
                waited += 100;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * Source for {@link #testTransactionSpansTwoCheckpointsFails}.
     *
     * <p>Emits data rows without txnEnd, then blocks until cancelled.
     * This guarantees that a Flink checkpoint will fire while the transaction
     * is still open, which must trigger an {@link IllegalStateException}.
     */
    private static class CheckpointTxnSpansSource
            extends RichParallelSourceFunction<DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;

        static volatile String DB;
        static volatile String ORDERS_TABLE;
        static volatile String ORDER_ITEMS_TABLE;

        static void reset(String db, String ordersTable, String orderItemsTable) {
            DB = db;
            ORDERS_TABLE = ordersTable;
            ORDER_ITEMS_TABLE = orderItemsTable;
        }

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<DefaultStarRocksRowData> ctx) throws Exception {
            // Emit data to both tables — deliberately NO txnEnd
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":1,\"customer_id\":100,\"total_amount\":50.00,\"order_status\":\"created\"}",
                    0, false));
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":3,\"price\":16.67}",
                    0, false));

            // Block until cancelled — the checkpoint barrier WILL arrive
            // while the transaction is still open (no txnEnd sent)
            while (running) {
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            running = false;
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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelism);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
        env.setRestartStrategy(RestartStrategies.noRestart());
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

    private SinkFunction<DefaultStarRocksRowData> buildSink(
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

        return SinkFunctionFactory.createSinkFunction(options);
    }

    private SinkFunction<DefaultStarRocksRowData> buildSinkWithLabelPrefix(
            String ordersTable, String orderItemsTable, int flushIntervalMs, String labelPrefix) {
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
                .withProperty("sink.label-prefix", labelPrefix)
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

        return SinkFunctionFactory.createSinkFunction(options);
    }

    /** Builds a {@link DefaultStarRocksRowData}. {@code json} may be {@code null} for txnEnd-only rows. */
    private static DefaultStarRocksRowData row(String db, String table, String json,
                                               int partition, boolean txnEnd) {
        DefaultStarRocksRowData r = new DefaultStarRocksRowData(null, db, table, json);
        r.setSourcePartition(partition);
        r.setTransactionEnd(txnEnd);
        return r;
    }

    // -------------------------------------------------------------------------
    // SinkV2 API helpers
    // -------------------------------------------------------------------------

    /**
     * Builds a {@link StarRocksSink} (new Flink Sink V2 API) for multi-table
     * transaction mode, using a pass-through {@link RecordSerializationSchema}.
     */
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
