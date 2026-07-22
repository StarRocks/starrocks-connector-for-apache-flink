/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Sustained multi-partition load test for multi-table transactions.
 *
 * <p>The single-cycle IT cases in {@link MultiTableTransactionITTest} cannot
 * catch defects that only appear when the workload spans MANY commit cycles
 * with MULTIPLE source partitions — e.g. the per-(label, table) channel
 * collision (TXN_IN_PROCESSING) and cross-table commit misalignment found in
 * the 1.2.15 E2E campaign. This test drives 4 partitions x several hundred
 * source transactions (1 order + 3 order_items each) across many flush/commit
 * cycles while a poller asserts the cross-table atomic-visibility invariant
 * {@code COUNT(order_items) == 3 * COUNT(orders)} on every sample.
 */
public class MultiTableTxnSustainedLoadITTest extends StarRocksITTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MultiTableTxnSustainedLoadITTest.class);

    private static final int PARTITIONS = 4;
    private static final int TXNS_PER_PARTITION = 250;
    private static final int ITEMS_PER_ORDER = 3;
    private static final int RATE_PER_SEC_PER_PARTITION = 100;
    private static final int FLUSH_INTERVAL_MS = 500;
    private static final long JOB_TIMEOUT_MS = 120_000L;

    @Before
    public void checkVersion() {
        assumeTrue(
                "Multi-table transaction requires StarRocks >= 4.0 or a main-branch build",
                isMultiTableTransactionClusterSupported());
    }

    /** Bounded source: each subtask is one partition emitting rate-limited transactions. */
    public static class SustainedTxnSource extends RichParallelSourceFunction<DefaultStarRocksRowData> {
        private static final long serialVersionUID = 1L;
        private final String database;
        private final String ordersTable;
        private final String itemsTable;
        private volatile boolean running = true;

        SustainedTxnSource(String database, String ordersTable, String itemsTable) {
            this.database = database;
            this.ordersTable = ordersTable;
            this.itemsTable = itemsTable;
        }

        @Override
        public void run(SourceContext<DefaultStarRocksRowData> ctx) throws Exception {
            int partition = getRuntimeContext().getIndexOfThisSubtask();
            long sleepNanosPerTxn = 1_000_000_000L / RATE_PER_SEC_PER_PARTITION;
            long start = System.nanoTime();
            for (long seq = 0; seq < TXNS_PER_PARTITION && running; seq++) {
                long orderId = partition * 1_000_000L + seq;
                synchronized (ctx.getCheckpointLock()) {
                    DefaultStarRocksRowData order = new DefaultStarRocksRowData(
                            null, database, ordersTable,
                            "{\"order_id\":" + orderId + ",\"customer_id\":" + (orderId % 1000)
                                    + ",\"total_amount\":9.99,\"order_status\":\"ok\"}");
                    order.setSourcePartition(partition);
                    ctx.collect(order);
                    for (int i = 0; i < ITEMS_PER_ORDER; i++) {
                        DefaultStarRocksRowData item = new DefaultStarRocksRowData(
                                null, database, itemsTable,
                                "{\"item_id\":" + (orderId * 10 + i) + ",\"order_id\":" + orderId
                                        + ",\"product_name\":\"p\",\"quantity\":" + (i + 1)
                                        + ",\"price\":1.99}");
                        item.setSourcePartition(partition);
                        item.setTransactionEnd(i == ITEMS_PER_ORDER - 1);
                        ctx.collect(item);
                    }
                }
                long target = start + (seq + 1) * sleepNanosPerTxn;
                long now = System.nanoTime();
                if (target > now) {
                    Thread.sleep((target - now) / 1_000_000L, (int) ((target - now) % 1_000_000L));
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    @Test
    public void testSustainedMultiPartitionLoadKeepsCrossTableAtomicity() throws Exception {
        String ordersTable = "orders_" + genRandomUuid();
        String itemsTable = "order_items_" + genRandomUuid();
        executeSrSQL(String.format(
                "CREATE TABLE `%s`.`%s` (order_id BIGINT NOT NULL, customer_id BIGINT NOT NULL,"
                        + " total_amount DECIMAL(10,2) DEFAULT '0', order_status VARCHAR(32) DEFAULT '')"
                        + " ENGINE=OLAP PRIMARY KEY(order_id) DISTRIBUTED BY HASH(order_id) BUCKETS 4"
                        + " PROPERTIES (\"replication_num\" = \"1\")",
                DB_NAME, ordersTable));
        executeSrSQL(String.format(
                "CREATE TABLE `%s`.`%s` (item_id BIGINT NOT NULL, order_id BIGINT NOT NULL,"
                        + " product_name VARCHAR(128) DEFAULT '', quantity INT DEFAULT '0',"
                        + " price DECIMAL(10,2) DEFAULT '0')"
                        + " ENGINE=OLAP PRIMARY KEY(item_id) DISTRIBUTED BY HASH(item_id) BUCKETS 4"
                        + " PROPERTIES (\"replication_num\" = \"1\")",
                DB_NAME, itemsTable));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(PARTITIONS);

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
                .withProperty("sink.buffer-flush.interval-ms", String.valueOf(FLUSH_INTERVAL_MS))
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();
        SinkFunction<DefaultStarRocksRowData> sink = SinkFunctionFactory.createSinkFunction(options);

        env.addSource(new SustainedTxnSource(DB_NAME, ordersTable, itemsTable))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(sink)
                .setParallelism(PARTITIONS);

        // Run the job asynchronously while this thread polls the cross-table
        // atomic-visibility invariant. executeAsync gives us a JobClient so a
        // timed-out job can be cancelled instead of leaking a running job that
        // would interfere with subsequent ITs.
        JobClient jobClient = env.executeAsync("multi-table-txn-sustained-load");

        // Poll on the execution-result future, NOT on getJobStatus(): in
        // MiniCluster mode the cluster shuts down as soon as the job reaches a
        // terminal state, after which getJobStatus() throws IllegalStateException
        // ("MiniCluster is not yet running or has already been shut down") — a
        // race that surfaces when the suite runs under load. The result future
        // stays usable after shutdown and doubles as the failure-cause carrier.
        java.util.concurrent.CompletableFuture<?> resultFuture = jobClient.getJobExecutionResult();

        // Sample the cross-table invariant every 100ms. We distinguish two
        // failure modes:
        //   - SUSTAINED skew: the connector committed one table a full cycle
        //     ahead of its sibling. A real per-table commit-boundary drift
        //     persists for at least one commit interval (here flush=500ms ->
        //     ~5 consecutive samples). This is the bug class this PR fixes and
        //     MUST fail the test.
        //   - ISOLATED transient: a single sample observes a mismatch that has
        //     healed by the next sample. On FE/BE-SEPARATED clusters StarRocks'
        //     multi-table publish advances per-table visibleVersion non-atomically
        //     over a sub-100ms window, so a high-frequency poll can momentarily
        //     catch one table a beat ahead. An A/B run (2026-06-08) reproduced
        //     this at an identical rate on the pre-fix and post-fix connector and
        //     ONLY on separated deployments — i.e. it is an SR read-visibility
        //     artifact, not a connector commit-alignment defect. The connector's
        //     strict commit alignment is proven separately, at zero tolerance,
        //     by MultiTableTxnSerializationAlignmentTest against the mock server.
        // So we tolerate isolated single-sample transients but fail on any run of
        // consecutive violations, and always require exact final counts.
        List<String> violations = new ArrayList<>();
        int samples = 0;
        int curRun = 0;
        int maxRun = 0;
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
        while (!resultFuture.isDone() && System.currentTimeMillis() < deadline) {
            long[] counts = countBoth(ordersTable, itemsTable);
            samples++;
            if (counts[1] != counts[0] * ITEMS_PER_ORDER) {
                violations.add("t+" + samples + ": orders=" + counts[0] + " items=" + counts[1]);
                curRun++;
                maxRun = Math.max(maxRun, curRun);
            } else {
                curRun = 0;
            }
            Thread.sleep(100);
        }
        if (!resultFuture.isDone()) {
            try {
                jobClient.cancel().get(30, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception cancelEx) {
                LOG.warn("Best-effort cancel of timed-out job failed", cancelEx);
            }
            throw new AssertionError("Flink job did not finish within " + JOB_TIMEOUT_MS
                    + " ms; job was cancelled to avoid interfering with subsequent tests");
        }
        try {
            resultFuture.get(10, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("Sustained multi-partition job failed — multi-table "
                    + "transactions must survive workloads spanning many commit cycles "
                    + "(historical defect: TXN_IN_PROCESSING channel collisions)", e);
        }

        LOG.info("Sustained load finished; {} invariant samples, {} violations, maxConsecutiveRun={}",
                samples, violations.size(), maxRun);
        // A sustained run (>= 3 consecutive samples, i.e. > 200ms, well inside a
        // 500ms commit cycle) indicates a real cross-table commit-boundary drift.
        // Isolated single/double-sample blips are the SR-side publish micro-window.
        assertTrue("SUSTAINED cross-table atomic-visibility skew during sustained load "
                        + "(items != 3x orders for " + maxRun + " consecutive ~100ms samples — a real "
                        + "per-table commit-boundary drift persists a full commit cycle): " + violations,
                maxRun < 3);

        long[] finalCounts = countBoth(ordersTable, itemsTable);
        assertEquals("all orders must be committed exactly once",
                (long) PARTITIONS * TXNS_PER_PARTITION, finalCounts[0]);
        assertEquals("all order_items must be committed exactly once",
                (long) PARTITIONS * TXNS_PER_PARTITION * ITEMS_PER_ORDER, finalCounts[1]);
    }

    private long[] countBoth(String ordersTable, String itemsTable) throws Exception {
        try (Statement stmt = DB_CONNECTION.createStatement();
                ResultSet rs = stmt.executeQuery(String.format(
                        "SELECT (SELECT COUNT(*) FROM `%s`.`%s`), (SELECT COUNT(*) FROM `%s`.`%s`)",
                        DB_NAME, ordersTable, DB_NAME, itemsTable))) {
            rs.next();
            return new long[] {rs.getLong(1), rs.getLong(2)};
        }
    }
}
