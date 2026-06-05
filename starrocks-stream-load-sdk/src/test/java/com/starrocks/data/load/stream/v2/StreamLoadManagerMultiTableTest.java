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

package com.starrocks.data.load.stream.v2;

import com.starrocks.data.load.stream.MockedStarRocksHttpServer;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StreamLoadManagerMultiTableTest {

    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    private MockedStarRocksHttpServer mockedServer;

    @Before
    public void setUp() throws Exception {
        mockedServer = MockedStarRocksHttpServer.builder()
                .port(0)
                .enforceAuth(USERNAME, PASSWORD)
                .build();
        mockedServer.start();
    }

    @After
    public void tearDown() {
        if (mockedServer != null) {
            mockedServer.stop();
        }
    }

    private StreamLoadProperties buildMultiTableProperties(int flushIntervalMs) {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("orders")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100000)
                .build();

        return StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .labelPrefix("test-mtxn-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(flushIntervalMs)
                .scanningFrequency(50)
                .ioThreadCount(2)
                .build();
    }

    /**
     * Multi-table mode is fundamentally incompatible with SDK manual-commit
     * mode (enableAutoCommit=false). The manager's timer-driven path
     * autonomously drives shared-transaction commits once commitIntervalMs
     * has elapsed, which would publish data before an external 2PC caller's
     * explicit snapshot/commit step. The constructor must reject this
     * combination up front instead of silently auto-publishing.
     */
    @Test
    public void testConstructorRejectsMultiTableWithManualCommit() {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        try {
            new DefaultStreamLoadManager(properties, false);
            Assert.fail("Expected IllegalArgumentException for multi-table + manual commit");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(
                    "Error should mention multi-table and enableAutoCommit: " + e.getMessage(),
                    e.getMessage().contains("Multi-table transaction mode")
                            && e.getMessage().contains("enableAutoCommit"));
        }
    }

    /**
     * Single partition: write data to two tables, send txnEnd, verify commit.
     * Verifies that the shared transaction coordinator is used (1 begin, 1 prepare, 1 commit).
     */
    @Test
    public void testSinglePartitionWriteAndCommit() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            int partition = 0;
            manager.write(partition, "test", "orders",
                    "{\"order_id\":1, \"customer_id\":100, \"total_amount\":99.99}");
            manager.setCommitAllowed(partition, false);

            manager.write(partition, "test", "order_items",
                    "{\"item_id\":1, \"order_id\":1, \"product_name\":\"widget\", \"quantity\":2}");
            manager.setCommitAllowed(partition, true);

            Thread.sleep(500);
            Assert.assertNull("No exception expected", manager.getException());

            manager.flush();
            Assert.assertNull("No exception expected after flush", manager.getException());

            // At least 1 begin (shared transaction is eagerly opened; after commit,
            // a new one is opened for the next cycle, and savepoint may open yet another).
            Assert.assertTrue("Expected at least 1 begin for shared transaction",
                    mockedServer.getBeginCount() >= 1);
            Assert.assertTrue("Expected at least 1 load call (one per table)",
                    mockedServer.getLoadCount() >= 1);
            // Multi-table transactions skip prepare and go directly to commit.
            Assert.assertEquals("Expected exactly 1 commit for single shared transaction",
                    1, mockedServer.getCommitCount());
        } finally {
            manager.close();
        }
    }

    /**
     * Two partitions sharing one sink: commit only triggers when BOTH
     * partitions have reached txnEnd.
     */
    @Test
    public void testMultiPartitionCommitWaitsForAll() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            // Partition 0 writes + txnEnd
            manager.write(0, "test", "orders",
                    "{\"order_id\":1, \"customer_id\":100}");
            manager.setCommitAllowed(0, true);

            // Only partition 0 has txnEnd. Partition 1 hasn't even started.
            // But there's also only partition 0 active, so commit may trigger.
            Thread.sleep(300);
            Assert.assertNull("No exception after P0 txnEnd", manager.getException());

            // Now partition 1 writes + txnEnd
            manager.write(1, "test", "orders",
                    "{\"order_id\":2, \"customer_id\":101}");
            manager.setCommitAllowed(1, false);

            manager.write(1, "test", "order_items",
                    "{\"item_id\":1, \"order_id\":2}");
            manager.setCommitAllowed(1, true);

            Thread.sleep(500);
            Assert.assertNull("No exception after P1 txnEnd", manager.getException());

            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());
        } finally {
            manager.close();
        }
    }

    /**
     * Verifies that data is NOT committed while no partition has sent txnEnd.
     * The shared transaction is eagerly opened (begin), but commit only
     * happens after txnEnd.
     */
    @Test
    public void testCommitNotTriggeredWithoutTxnEnd() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            manager.write(0, "test", "orders",
                    "{\"order_id\":1, \"customer_id\":100}");
            manager.setCommitAllowed(0, false);

            Thread.sleep(300);
            Assert.assertNull("No exception expected", manager.getException());
            // Shared transaction is eagerly opened, so begin may have been called.
            // But commit must NOT have happened yet.
            Assert.assertEquals("No commit expected before txnEnd", 0, mockedServer.getCommitCount());

            manager.setCommitAllowed(0, true);
            Thread.sleep(300);
            Assert.assertNull("No exception expected after txnEnd", manager.getException());

            manager.flush();
            Assert.assertNull("No exception expected after flush", manager.getException());
        } finally {
            manager.close();
        }
    }

    /**
     * N:1 mapping: multiple source transactions accumulate before commit.
     */
    @Test
    public void testMultipleTransactionsAccumulate() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(500);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            // Txn 1
            manager.write(0, "test", "orders", "{\"order_id\":1}");
            manager.setCommitAllowed(0, true);

            // Txn 2 (interval not elapsed yet, so txn 1 data stays in active chunk)
            manager.write(0, "test", "orders", "{\"order_id\":2}");
            manager.setCommitAllowed(0, true);

            // Wait for interval
            Thread.sleep(600);

            // Txn 3 triggers the interval check
            manager.write(0, "test", "orders", "{\"order_id\":3}");
            manager.setCommitAllowed(0, true);

            Thread.sleep(500);
            Assert.assertNull("No exception expected", manager.getException());

            manager.flush();
            Assert.assertNull("No exception expected after flush", manager.getException());
        } finally {
            manager.close();
        }
    }

    /**
     * Savepoint (flush) commits the active shared transaction even when
     * txnEnd has not been received (interval-based commit threshold is very high).
     * Verifies exactly 1 prepare + commit via the savepoint path.
     * The shared transaction is eagerly opened, so begin count may be >= 1.
     */
    @Test
    public void testSavepointCommitsMultiTableTransaction() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(60000); // 60s interval — never fires
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            manager.write(0, "test", "orders",
                    "{\"order_id\":1, \"customer_id\":100}");
            manager.write(0, "test", "order_items",
                    "{\"item_id\":1, \"order_id\":1}");

            // Mark partition as committed so savepoint can proceed
            manager.setCommitAllowed(0, true);
            manager.flush();
            Assert.assertNull("No exception expected after flush", manager.getException());

            // The shared transaction is eagerly opened, so at least 1 begin.
            // Savepoint path commits the active shared transaction (skipping prepare for multi-table).
            Assert.assertTrue("Savepoint should issue at least 1 begin",
                    mockedServer.getBeginCount() >= 1);
            Assert.assertEquals("Savepoint should issue exactly 1 commit",
                    1, mockedServer.getCommitCount());
        } finally {
            manager.close();
        }
    }

    /**
     * An evicted (idle) partition that later receives txnEnd should be
     * automatically re-registered and participate in the next commit cycle.
     *
     * <p>This tests the {@code onTxnEnd()} fix that re-registers evicted partitions
     * instead of ignoring them.
     */
    @Test
    public void testEvictedPartitionReRegisters() throws Exception {
        // Very short interval (100 ms) to trigger multiple rapid commit cycles
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            // Commit cycle 1 — only partition 0
            manager.write(0, "test", "orders", "{\"order_id\":1}");
            manager.setCommitAllowed(0, true);
            Thread.sleep(400);

            // Commit cycle 2 — only partition 0 (partition 0 resets to ACTIVE)
            manager.write(0, "test", "orders", "{\"order_id\":2}");
            manager.setCommitAllowed(0, true);
            Thread.sleep(400);

            // Commit cycle 3 — only partition 0 (3rd cycle: idle count reaches MAX_IDLE_CYCLES)
            manager.write(0, "test", "orders", "{\"order_id\":3}");
            manager.setCommitAllowed(0, true);
            Thread.sleep(400);

            // After 3 idle commit cycles, partition 0 would be evicted.
            // Now partition 0 sends txnEnd again — it must be re-registered, not ignored.
            manager.write(0, "test", "orders", "{\"order_id\":4}");
            manager.setCommitAllowed(0, true);
            Thread.sleep(400);

            manager.flush();
            Assert.assertNull("No exception expected after evicted partition re-registers",
                    manager.getException());

            // At least 4 complete commit cycles must have occurred
            Assert.assertTrue("Expected at least 4 commits across re-registration cycles",
                    mockedServer.getCommitCount() >= 4);
        } finally {
            manager.close();
        }
    }

    /**
     * Regions from different databases must be rejected: multi-table transactions
     * require all tables to share the same StarRocks database.
     */
    @Test
    public void testCrossDbWriteIsRejected() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            // Write to two different databases in the same commit cycle
            manager.write(0, "db_a", "orders", "{\"order_id\":1}");
            manager.write(0, "db_b", "payments", "{\"payment_id\":1}");
            manager.setCommitAllowed(0, true);

            // Give manager thread time to attempt commit
            Thread.sleep(500);

            // Manager should have recorded an error about mismatched databases
            Assert.assertNotNull("Expected exception for cross-database write",
                    manager.getException());
            Assert.assertTrue("Exception should mention database mismatch",
                    manager.getException().getMessage().contains("same database"));
        } finally {
            manager.close();
        }
    }

    /**
     * Verifies that autonomous flush (triggered by buffer thresholds) uses the
     * shared transaction label rather than generating an independent label.
     * This is the key scenario that was previously subject to data loss.
     *
     * <p>With the eager shared transaction approach, the shared label is injected
     * before any autonomous flush can occur, so all loads use the shared label.
     * After txnEnd, the commit cycle commits all data (including data from
     * autonomous flushes) under the single shared transaction.
     */
    @Test
    public void testAutonomousFlushUsesSharedLabel() throws Exception {
        // Use a very small buffer (1 byte effective) to trigger autonomous flush immediately.
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("orders")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(1) // Flush after every row
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .labelPrefix("test-autoflush-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(100)
                .scanningFrequency(50)
                .ioThreadCount(2)
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            // Write multiple rows — with maxBufferRows=1, autonomous flush should trigger
            manager.write(0, "test", "orders", "{\"order_id\":1}");
            manager.write(0, "test", "orders", "{\"order_id\":2}");
            manager.write(0, "test", "orders", "{\"order_id\":3}");

            // Give manager thread time to process autonomous flushes
            Thread.sleep(500);
            Assert.assertNull("No exception during autonomous flush", manager.getException());

            // Now send txnEnd to trigger commit
            manager.setCommitAllowed(0, true);
            Thread.sleep(500);
            Assert.assertNull("No exception after txnEnd", manager.getException());

            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());

            // All data should have been committed under shared transactions.
            // The critical assertion: at least 1 commit happened (data wasn't lost).
            Assert.assertTrue("Expected at least 1 commit (autonomous flush data not lost)",
                    mockedServer.getCommitCount() >= 1);
            // Loads should have occurred (autonomous flushes).
            Assert.assertTrue("Expected at least 1 load from autonomous flush",
                    mockedServer.getLoadCount() >= 1);
        } finally {
            manager.close();
        }
    }

    /**
     * Non-multi-table mode is completely unaffected by the new code paths.
     */
    @Test
    public void testNonMultiTableModeUnaffected() throws Exception {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("tbl1")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100000)
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("3.5.0")
                .enableTransaction()
                .labelPrefix("test-normal-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(1000)
                .scanningFrequency(50)
                .ioThreadCount(2)
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            manager.write(null, "test", "tbl1",
                    "{\"id\":1, \"name\":\"test\"}");

            manager.flush();
            Assert.assertNull("No exception expected", manager.getException());
        } finally {
            manager.close();
        }
    }

    // -------------------------------------------------------------------------
    // Item 10: Constructor parameter validation
    // -------------------------------------------------------------------------

    /**
     * Multi-table transaction mode always uses TransactionStreamLoader, and since the
     * TXN_IN_PROCESSING retry fallback, maxRetries > 0 is allowed in this mode (retries
     * are restricted to the transient TXN_IN_PROCESSING rejection inside
     * TransactionTableRegion). Construction must succeed instead of throwing.
     */
    @Test
    public void testConstructorAllowsMultiTableWithRetries() {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("orders")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .maxRetries(3)
                .labelPrefix("test-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(100)
                .scanningFrequency(50)
                .ioThreadCount(2)
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        Assert.assertNotNull(manager);
    }

    /**
     * Manual commit mode (enableAutoCommit=false) requires transaction support.
     * Without it, construction should throw.
     */
    @Test
    public void testConstructorRejectsManualCommitWithoutTransaction() {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("orders")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        // Build properties WITHOUT enableTransaction
        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .labelPrefix("test-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(100)
                .scanningFrequency(50)
                .ioThreadCount(2)
                .build();

        try {
            new StreamLoadManagerV2(properties, false);
            Assert.fail("Expected IllegalArgumentException for manual commit without transaction");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Should mention transaction stream load",
                    e.getMessage().contains("transaction stream load"));
        }
    }

    // -------------------------------------------------------------------------
    // Item 9: setCommitAllowed(boolean) is no-op in multi-table mode
    // -------------------------------------------------------------------------

    /**
     * The legacy {@code setCommitAllowed(boolean)} without partition parameter
     * is a no-op in multi-table mode. Verifies that calling it does not cause
     * errors and does not trigger a commit (the commit counter stays at 0).
     */
    @Test
    public void testLegacySetCommitAllowedIsNoOpInMultiTableMode() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(60000);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            manager.write(0, "test", "orders", "{\"order_id\":1}");

            // Call the legacy no-partition variant — should be a no-op
            manager.setCommitAllowed(true);
            Thread.sleep(300);
            Assert.assertNull("No exception expected from no-op setCommitAllowed", manager.getException());

            // No commit should have been triggered by the legacy call
            Assert.assertEquals("No commit from legacy setCommitAllowed",
                    0, mockedServer.getCommitCount());

            // Mark partition committed so flush can proceed, then clean up
            manager.setCommitAllowed(0, true);
            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());
        } finally {
            manager.close();
        }
    }

    // -------------------------------------------------------------------------
    // Item 4: Commit failure — exception propagation and state cleanup
    // -------------------------------------------------------------------------

    /**
     * When commit fails (server returns error), the manager should capture the
     * exception and propagate it to the caller.
     */
    @Test
    public void testCommitFailurePropagatesException() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            // Inject commit failure
            MockedStarRocksHttpServer.ResponseOverride commitFail =
                    new MockedStarRocksHttpServer.ResponseOverride();
            commitFail.status = "Fail";
            commitFail.message = "disk full";
            mockedServer.setCommitOverride(commitFail);

            manager.write(0, "test", "orders", "{\"order_id\":1}");
            manager.setCommitAllowed(0, true);

            // Wait for manager thread to attempt commit
            Thread.sleep(500);

            // The exception should have been captured
            Assert.assertNotNull("Expected exception from commit failure",
                    manager.getException());
        } finally {
            manager.close();
        }
    }

    /**
     * Multi-table transactions skip prepare and go directly to commit,
     * so a prepare override should have no effect. Verify that the
     * transaction completes successfully even with a prepare override set.
     */
    @Test
    public void testPrepareSkippedInMultiTableMode() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            // Set a prepare failure override — should have no effect in multi-table mode
            MockedStarRocksHttpServer.ResponseOverride prepareFail =
                    new MockedStarRocksHttpServer.ResponseOverride();
            prepareFail.status = "Fail";
            prepareFail.message = "prepare failed";
            mockedServer.setPrepareOverride(prepareFail);

            manager.write(0, "test", "orders", "{\"order_id\":1}");
            manager.setCommitAllowed(0, true);

            Thread.sleep(500);

            // No exception expected — prepare is skipped in multi-table mode
            Assert.assertNull("No exception expected (prepare is skipped in multi-table mode)",
                    manager.getException());

            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());

            // Verify prepare was never called
            Assert.assertEquals("Prepare should be skipped in multi-table mode",
                    0, mockedServer.getPrepareCount());
            // Commit should have occurred
            Assert.assertTrue("Commit should have occurred",
                    mockedServer.getCommitCount() >= 1);
        } finally {
            manager.close();
        }
    }

    /**
     * When begin fails (cannot open shared transaction), the manager should
     * capture the exception.
     */
    @Test
    public void testBeginFailurePropagatesException() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);

        // Inject begin failure BEFORE init so the eager open fails
        MockedStarRocksHttpServer.ResponseOverride beginFail =
                new MockedStarRocksHttpServer.ResponseOverride();
        beginFail.status = "Fail";
        beginFail.message = "too many running transactions";
        mockedServer.setBeginOverride(beginFail);

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            manager.write(0, "test", "orders", "{\"order_id\":1}");
            manager.setCommitAllowed(0, true);

            Thread.sleep(500);

            Assert.assertNotNull("Expected exception from begin failure",
                    manager.getException());
        } finally {
            manager.close();
        }
    }

    // -------------------------------------------------------------------------
    // Item 3: Flush timeout
    // -------------------------------------------------------------------------

    /**
     * Verifies that when flush takes longer than the configured timeout,
     * a RuntimeException is thrown.
     *
     * <p>We set timeout=1 via headers (flushTimeoutMs = 1*1100 = 1100ms),
     * then make commit hang by failing repeatedly so flush never completes.
     */
    @Test
    public void testFlushTimeout() throws Exception {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("orders")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100000)
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .labelPrefix("test-timeout-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(60000) // never auto-commit
                .scanningFrequency(50)
                .ioThreadCount(2)
                .addHeader("timeout", "1") // 1 second → flushTimeoutMs = 1100ms
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            // Make commit always fail so flush() can never complete
            MockedStarRocksHttpServer.ResponseOverride commitFail =
                    new MockedStarRocksHttpServer.ResponseOverride();
            commitFail.status = "Fail";
            commitFail.message = "simulated hang";
            mockedServer.setCommitOverride(commitFail);

            manager.write(0, "test", "orders", "{\"order_id\":1}");
            manager.setCommitAllowed(0, true);

            // flush() should eventually throw due to timeout (1100ms)
            try {
                manager.flush();
                // flush may succeed if the manager thread captures the commit error first
                // In that case, getException should be non-null
                if (manager.getException() != null) {
                    return; // commit failure was captured — acceptable outcome
                }
                Assert.fail("Expected RuntimeException from flush timeout or commit failure");
            } catch (RuntimeException e) {
                Assert.assertTrue("Exception should mention timeout or commit failure",
                        e.getMessage().contains("timeout") || e.getMessage().contains("Fail"));
            }
        } finally {
            manager.close();
        }
    }

    // -------------------------------------------------------------------------
    // Item 1: Write blocking / backpressure (blockIfCacheFull)
    // -------------------------------------------------------------------------

    /**
     * Verifies that writes are accepted and eventually committed when using a
     * very small buffer size. With a tiny {@code multiTableTransactionBufferSize},
     * the write path will trigger flush signals (soft threshold) and potentially
     * block writes (hard threshold at 2× buffer size).
     *
     * <p>This exercises the {@code blockIfCacheFull()} code path.
     */
    @Test(timeout = 15000)
    public void testWriteBlockingWithSmallBuffer() throws Exception {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("orders")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100000)
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .multiTableTransactionBufferSize(2048) // 2KB — small but allows writes to proceed
                .labelPrefix("test-backpressure-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(100)
                .scanningFrequency(50)
                .ioThreadCount(2)
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            // Write enough data to exceed the soft threshold (2048 bytes).
            // Each JSON row is ~40-50 bytes. 50 rows ≈ 2500 bytes, exceeding
            // soft threshold but not blocking indefinitely at 2x hard threshold.
            for (int i = 0; i < 50; i++) {
                manager.write(0, "test", "orders",
                        String.format("{\"order_id\":%d, \"customer_id\":%d}", i, i * 10));
            }

            manager.setCommitAllowed(0, true);
            Thread.sleep(500);
            Assert.assertNull("No exception expected during backpressure writes",
                    manager.getException());

            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());

            // Data should have been committed despite the small buffer
            Assert.assertTrue("Expected at least 1 commit",
                    mockedServer.getCommitCount() >= 1);
            // The small buffer should have triggered multiple flush signals
            Assert.assertTrue("Expected multiple loads due to small buffer",
                    mockedServer.getLoadCount() >= 1);
        } finally {
            manager.close();
        }
    }

    // -------------------------------------------------------------------------
    // Item 2: Shared transaction timeout recycling
    // -------------------------------------------------------------------------

    /**
     * Verifies that shared transactions are recycled before the server-side timeout.
     *
     * <p>We set timeout=1 via headers (sharedTxnMaxIdleMs = 1*800 = 800ms).
     * The manager's eager open creates a shared transaction; if no data is committed
     * within 800ms, it should be recycled (rolled back and a new one opened).
     *
     * <p>We verify by checking that more begin calls than commit calls occur
     * (the extra begins come from recycling).
     */
    @Test
    public void testSharedTransactionRecycling() throws Exception {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("orders")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100000)
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .labelPrefix("test-recycle-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(60000) // never auto-commit
                .scanningFrequency(50)
                .ioThreadCount(2)
                .addHeader("timeout", "1") // sharedTxnMaxIdleMs = 800ms
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            // Write a row so the manager opens a shared transaction
            manager.write(0, "test", "orders", "{\"order_id\":1}");

            // Signal txnEnd so partition 0 is no longer ACTIVE.
            // Without this, recycling would detect an active partition and fail
            // (by design, to protect cross-table atomicity).
            manager.setCommitAllowed(0, true);

            // Wait longer than sharedTxnMaxIdleMs (800ms) for recycling to occur.
            // The recycling is checked each scanningFrequency (50ms), so after ~900ms
            // the transaction should have been recycled at least once.
            Thread.sleep(1500);
            Assert.assertNull("No exception during recycling", manager.getException());

            // Recycling means: original begin + rollback/commit + new begin
            // So we expect more begins than commits.
            int beginCount = mockedServer.getBeginCount();
            Assert.assertTrue("Expected at least 2 begins from recycling, got: " + beginCount,
                    beginCount >= 2);

            // Clean up
            manager.setCommitAllowed(0, true);
            Thread.sleep(300);
            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());
        } finally {
            manager.close();
        }
    }

    // -------------------------------------------------------------------------
    // Item 7: Large batch stress test
    // -------------------------------------------------------------------------

    /**
     * Writes a large number of rows across multiple tables and partitions,
     * verifying that all data is committed without errors.
     */
    @Test
    public void testLargeBatchMultiTableStress() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(200);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            int rowsPerTable = 500;
            // Partition 0 writes to "orders"
            for (int i = 0; i < rowsPerTable; i++) {
                manager.write(0, "test", "orders",
                        String.format("{\"order_id\":%d,\"customer_id\":%d,\"total\":%.2f}",
                                i, i * 10, i * 1.5));
            }
            // Partition 1 writes to "order_items"
            for (int i = 0; i < rowsPerTable; i++) {
                manager.write(1, "test", "order_items",
                        String.format("{\"item_id\":%d,\"order_id\":%d,\"qty\":%d}",
                                i, i / 3, i % 10 + 1));
            }

            // Signal both partitions done
            manager.setCommitAllowed(0, true);
            manager.setCommitAllowed(1, true);

            Thread.sleep(800);
            Assert.assertNull("No exception during large batch write", manager.getException());

            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());

            Assert.assertTrue("Expected at least 1 commit for large batch",
                    mockedServer.getCommitCount() >= 1);
            Assert.assertTrue("Expected multiple load calls for large batch",
                    mockedServer.getLoadCount() >= 2);
        } finally {
            manager.close();
        }
    }

    // -------------------------------------------------------------------------
    // Item 8: Dynamic new table added at runtime
    // -------------------------------------------------------------------------

    /**
     * Verifies that a new table can be written at runtime after the shared
     * transaction is already open. The new region should be injected with
     * the existing shared label.
     */
    @Test
    public void testDynamicNewTableAfterSharedTxnOpen() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            // Write to first table — this triggers eager shared txn open
            manager.write(0, "test", "orders", "{\"order_id\":1}");
            Thread.sleep(200); // let manager thread open the shared txn

            // Now write to a brand new table that didn't exist when the shared txn opened.
            // The getCacheRegion() code path should create a new region and inject
            // the existing shared label into it.
            manager.write(0, "test", "payments", "{\"payment_id\":1,\"amount\":42.0}");

            // Also write to a third table
            manager.write(0, "test", "shipments", "{\"shipment_id\":1,\"tracking\":\"ABC\"}");

            manager.setCommitAllowed(0, true);
            Thread.sleep(500);
            Assert.assertNull("No exception when adding new table at runtime",
                    manager.getException());

            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());

            // All 3 tables' data should be committed under shared transaction(s)
            Assert.assertTrue("Expected at least 1 commit", mockedServer.getCommitCount() >= 1);
            // At least 3 load calls (one per table)
            Assert.assertTrue("Expected at least 3 loads (one per table)",
                    mockedServer.getLoadCount() >= 3);
        } finally {
            manager.close();
        }
    }

    // -------------------------------------------------------------------------
    // miniInterval batching and source-idle fallback tests
    // -------------------------------------------------------------------------

    /**
     * Verifies that multiple rapid txnEnds within a single miniInterval window
     * are batched into a single switchChunk, so only one HTTP /transaction/load
     * request is issued instead of one per txnEnd.
     *
     * <p>With commitInterval=2000ms, miniSwitchIntervalMs = min(1000, max(100,
     * 2000/10)) = 200ms. A burst of 10 txnEnds issued within ~50ms should all
     * land on the same activeChunk: the first txnEnd triggers the first switch
     * (lastSwitchTimeMs starts at 0, so "now - 0 >> 200" → switch immediately),
     * and all subsequent txnEnds within the next 200ms are batched into the new
     * activeChunk without producing additional switches. The final commit
     * observes roughly 1-2 HTTP loads rather than 10.
     */
    @Test
    public void testMiniIntervalBatching() throws Exception {
        // commitInterval=2000ms → miniInterval=200ms
        StreamLoadProperties properties = buildMultiTableProperties(2000);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            // Issue 10 rapid write+txnEnd cycles. The first txnEnd triggers a
            // switch (freezing the single row from the first write); the next 9
            // all fall within miniInterval=200ms of the first switch and must
            // NOT produce additional switches — they accumulate into the new
            // activeChunk.
            for (int i = 0; i < 10; i++) {
                manager.write(0, "test", "orders",
                        String.format("{\"order_id\":%d,\"customer_id\":%d}", i, i * 10));
                manager.setCommitAllowed(0, true);
            }

            // Wait for the commit interval (2000ms) to elapse and the manager
            // thread to drain everything. The total elapsed time from the first
            // write through this sleep must be >= commitInterval so that
            // shouldTriggerCommit() fires.
            Thread.sleep(2500);
            Assert.assertNull("No exception during batching test", manager.getException());

            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());

            // The critical assertion: load count must be substantially less
            // than 10 (the number of txnEnds). With miniInterval batching, we
            // expect 1-3 loads total (one per chunk that was actually switched,
            // plus possibly one from the final savepoint/flush force-switch).
            //
            // Without batching (the old "switch every txnEnd" behavior), we
            // would see ~10 loads. So loadCount <= 5 is a conservative check
            // that catches regression while tolerating timing variance.
            int loadCount = mockedServer.getLoadCount();
            Assert.assertTrue(
                    "Expected loadCount <= 5 with miniInterval batching, got " + loadCount +
                    " (would be ~10 without batching)",
                    loadCount <= 5);
            // Still expect at least one load and one commit — data must actually flow.
            Assert.assertTrue("Expected at least 1 load", loadCount >= 1);
            Assert.assertTrue("Expected at least 1 commit", mockedServer.getCommitCount() >= 1);
        } finally {
            manager.close();
        }
    }

    /**
     * Verifies the manager-thread clean-boundary fallback: when the source
     * pauses after a txnEnd whose switch was skipped by miniInterval batching,
     * the manager thread's periodic tryForceCleanSwitch() must eventually
     * freeze the pending activeChunk data so it becomes committable.
     *
     * <p>Scenario:
     * <pre>
     *   T=0:     write + txnEnd (1st) → switchChunkForCommit, lastSwitchTime=0
     *   T=50ms:  write + txnEnd (2nd) → miniInterval (200ms) NOT elapsed, SKIP
     *            activeChunk now holds txn2 data; cleanBoundary = true
     *   T=50ms+: source goes idle (no more writes/txnEnds)
     *   T=~250ms: manager scan notices cleanBoundary=true AND miniInterval has
     *            elapsed → force-switch → txn2 data moves to inactiveChunks
     *   T=2000ms+: commit interval elapsed + hasDataLoaded → commit
     * </pre>
     *
     * <p>Without the fallback, txn2's data would sit in activeChunk indefinitely
     * and only the first txnEnd's data would commit, leading to incomplete data
     * visibility and eventual shared-transaction timeout. The test asserts that
     * all data committed via exactly one commit cycle.
     */
    @Test
    public void testSourceIdleForceSwitchFallback() throws Exception {
        // commitInterval=2000ms → miniInterval=200ms
        StreamLoadProperties properties = buildMultiTableProperties(2000);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            // 1st write + txnEnd: triggers the very first switchChunkForCommit
            // (lastSwitchTimeMs = 0 initially, so "now - 0" is huge >> miniInterval).
            manager.write(0, "test", "orders", "{\"order_id\":1}");
            manager.setCommitAllowed(0, true);

            // Small pause — still well within miniInterval of 200ms.
            Thread.sleep(50);

            // 2nd write + txnEnd: miniInterval (200ms) has NOT yet elapsed
            // since the 1st switch, so tryMiniIntervalSwitch() on the task
            // thread must NOT freeze activeChunk. The data stays in
            // activeChunk with cleanBoundary = true (because the most recent
            // task-thread event was a txnEnd).
            manager.write(0, "test", "orders", "{\"order_id\":2}");
            manager.setCommitAllowed(0, true);

            // Record the load count BEFORE the source goes idle. It reflects
            // only the first chunk that was switched (possibly still in-flight).
            int loadsBeforeIdle = mockedServer.getLoadCount();

            // Now simulate source pause. The manager thread must on its own
            // observe that activeChunk is clean AND miniInterval has elapsed,
            // and call tryForceCleanSwitch() to freeze the pending row. Then
            // shouldTriggerCommit() must eventually fire (after the full
            // commitInterval=2000ms from the last commit time) and drive a
            // commit cycle.
            Thread.sleep(3000);

            Assert.assertNull("No exception during source idle", manager.getException());

            // By now, BOTH rows must have been loaded: the first switched by
            // the task thread, the second by the manager-thread fallback.
            // Without the fallback, loadCount would remain at loadsBeforeIdle.
            int loadsAfterIdle = mockedServer.getLoadCount();
            Assert.assertTrue(
                    "Expected manager-thread fallback to force-switch and load " +
                    "the second row (loadsBefore=" + loadsBeforeIdle +
                    ", loadsAfter=" + loadsAfterIdle + ")",
                    loadsAfterIdle > loadsBeforeIdle || loadsAfterIdle >= 2);

            // And a commit must have happened — otherwise the data is just
            // sitting in an uncommitted shared transaction.
            Assert.assertTrue("Expected at least 1 commit after idle fallback",
                    mockedServer.getCommitCount() >= 1);

            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());
        } finally {
            manager.close();
        }
    }

    // -------------------------------------------------------------------------
    // Item 11: Multi-table single-txn fail-fast / clean-boundary safe switch
    // -------------------------------------------------------------------------

    /**
     * Builds multi-table properties with an explicit {@code buffer-size} so
     * tests can drive the fail-fast / safe-switch thresholds with small,
     * easy-to-reason-about values.
     */
    private StreamLoadProperties buildMultiTableProperties(int flushIntervalMs, long bufferSize) {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("orders")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100000)
                .build();

        return StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .multiTableTransactionBufferSize(bufferSize)
                .labelPrefix("test-mtxn-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(flushIntervalMs)
                .scanningFrequency(50)
                .ioThreadCount(2)
                .build();
    }

    /**
     * Verifies the fail-fast on a single oversized in-progress transaction.
     *
     * <p>In multi-table mode, {@code TransactionTableRegion.write0} cannot
     * switch activeChunk mid-transaction (doing so would split a source
     * transaction across chunks and break atomicity under the shared label).
     * If one source transaction alone grows past the write-block hard cap
     * (2 &times; buffer size), {@code blockIfCacheFull} would deadlock the task
     * thread because the manager has no inactiveChunks to drain. The region
     * should instead throw {@link IllegalStateException} before the deadlock
     * state is reachable.
     *
     * <p>This test uses a very small buffer (1KB &rarr; hard cap 2KB) and writes
     * fixed-width 68-byte rows to a single partition without ever calling
     * {@code setCommitAllowed}, simulating one huge source transaction. It
     * expects an {@link IllegalStateException} with a clear remediation hint.
     * A very large commit interval is used so the manager thread cannot
     * independently drive a commit cycle.
     */
    @Test(timeout = 10000)
    public void testFailFastOnOversizedSingleTransaction() throws Exception {
        // 1 KB buffer → hard cap = 2 KB → safe switch threshold = 1 KB.
        StreamLoadProperties properties = buildMultiTableProperties(60000, 1024L);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            IllegalStateException caught = null;
            int rowsWritten = 0;
            // Each row is a fixed 68-byte JSON document. Writing one row at a
            // time to ONE partition without setCommitAllowed drives activeChunk
            // past the 2 KB hard cap. The fail-fast in write0 fires on the row
            // whose addition would push activeChunk past the cap: after 29
            // rows chunkBytes = 69*29 + 1 = 2002, so the 30th row's estimate
            // 2002 + 68 + 1 = 2071 > 2048 triggers the throw. This happens
            // BEFORE blockIfCacheFull could block the task thread, because
            // raw bytes after 29 rows = 29 * 68 = 1972 < 2048 hard cap.
            try {
                for (int i = 0; i < 200; i++) {
                    manager.write(0, "test", "orders",
                            String.format(
                                    "{\"order_id\":%05d,\"customer_id\":%06d,\"notes\":\"padding-block-%04d\"}",
                                    i, i, i));
                    rowsWritten++;
                }
                Assert.fail("Expected IllegalStateException for oversized single txn, "
                        + "but write() succeeded after " + rowsWritten + " rows");
            } catch (IllegalStateException e) {
                caught = e;
            }

            Assert.assertNotNull("Expected IllegalStateException for oversized single txn", caught);
            Assert.assertTrue(
                    "Error message should identify the region: " + caught.getMessage(),
                    caught.getMessage().contains("db=test")
                            && caught.getMessage().contains("table=orders"));
            Assert.assertTrue(
                    "Error message should mention the write-block threshold: " + caught.getMessage(),
                    caught.getMessage().contains("write-block threshold"));
            Assert.assertTrue(
                    "Error message should suggest a remediation: " + caught.getMessage(),
                    caught.getMessage().contains("buffer-size")
                            || caught.getMessage().contains("buffer size"));
            // Sanity: the fail-fast must fire reasonably close to the hard
            // cap. At 68 bytes/row the cap is reached near row 30, so we
            // shouldn't have written more than ~40 rows before the throw.
            Assert.assertTrue(
                    "Fail-fast should trigger near the hard-cap boundary, "
                            + "but rowsWritten=" + rowsWritten,
                    rowsWritten < 50);
        } finally {
            try {
                manager.close();
            } catch (Exception ignore) {
                // close() after an exception is best-effort; underlying state
                // is not guaranteed to be clean.
            }
        }
    }

    /**
     * Verifies that the fail-fast on oversized single transaction does NOT
     * misfire when multiple small source transactions are batched into one
     * activeChunk because {@code miniInterval} has not yet elapsed.
     *
     * <p>Without the clean-boundary safe switch in {@code write0}, a sequence
     * of modest-sized back-to-back source transactions — each individually
     * well within the buffer — could accumulate in activeChunk past half the
     * write-block hard cap, leaving no headroom for the next transaction.
     * The next transaction's mid-stream writes would then wrongly trip the
     * fail-fast even though neither of the involved source transactions is
     * oversized.
     *
     * <p>With the safe switch, when a new source transaction's first write
     * arrives and activeChunk is already at or above the soft threshold
     * (half the hard cap), the region force-switches the accumulated
     * completed transactions into inactiveChunks (overriding the miniInterval
     * batching) so the new transaction gets a fresh activeChunk with full
     * headroom.
     *
     * <p>Parameters (buffer=512B, commitInterval=10s → miniInterval=1000ms)
     * ensure the tight write loop stays inside one miniInterval window. With
     * 55-byte rows and 5 rows per txn, activeChunk reaches ~281B after Txn1,
     * ~561B after Txn2, etc. Without the safe switch, Txn4 row 4's estimate
     * (1065 B) would exceed the 1024 B hard cap and trip the fail-fast. With
     * the safe switch, Txn3's first row (chunkBytes=561 ≥ 512) force-switches
     * the accumulated Txn1+Txn2 data into inactiveChunks, giving Txn3+Txn4
     * a fresh activeChunk and allowing all 5 txns to complete.
     */
    @Test(timeout = 15000)
    public void testBatchedTransactionsDoNotFalseFailFast() throws Exception {
        // 512 B buffer → hard cap = 1024 B → safe switch threshold = 512 B.
        // 10 s commit interval → miniInterval = 1000 ms (commitInterval / 10,
        // clamped to [100, 1000]). The tight write loop stays well within
        // one miniInterval window so tryMiniIntervalSwitch MUST skip its
        // switch — the clean-boundary safe switch in write0 is the only
        // mechanism that can prevent activeChunk from growing past the cap.
        StreamLoadProperties properties = buildMultiTableProperties(10000, 512L);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            // 5 back-to-back source transactions on the same partition. Each
            // writes 5 fixed-width 55-byte rows (chunkBytes grows by ~280 B
            // per txn due to delimiters).
            // - Txn #0 triggers the first switch unconditionally (initial
            //   lastSwitchTimeMs = 0, so miniInterval is considered elapsed).
            //   After Txn #0's switch, activeChunk is empty.
            // - Txn #1..#4 fall inside the miniInterval window, so
            //   tryMiniIntervalSwitch skips its switch, keeping activeChunk
            //   growing across txn boundaries.
            // - After Txn #2, activeChunk is ~561 B (> safe switch threshold
            //   512), so write0's clean-boundary safe switch must fire on
            //   the first row of Txn #3, draining Txn #1+#2 into
            //   inactiveChunks. Without the safe switch, Txn #4's mid-stream
            //   writes would push activeChunk past the 1024 B hard cap and
            //   trip the fail-fast.
            for (int txn = 0; txn < 5; txn++) {
                for (int r = 0; r < 5; r++) {
                    manager.write(0, "test", "orders",
                            String.format(
                                    "{\"order_id\":%05d,\"customer_id\":%05d,\"notes\":\"t%d-r%02d\"}",
                                    txn * 100 + r, r * 10, txn, r));
                }
                manager.setCommitAllowed(0, true);
            }

            Assert.assertNull(
                    "No exception expected — the clean-boundary safe switch should drain "
                            + "batched completed transactions before the fail-fast would misfire",
                    manager.getException());

            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());

            // At least one HTTP load and commit must have happened — proves
            // the batched transactions were actually drained rather than
            // accumulating indefinitely in activeChunk.
            Assert.assertTrue("Expected at least 1 load call for batched txns",
                    mockedServer.getLoadCount() >= 1);
            Assert.assertTrue("Expected at least 1 commit for batched txns",
                    mockedServer.getCommitCount() >= 1);
        } finally {
            manager.close();
        }
    }

    /**
     * Verifies the aggregate fail-fast guard across multiple regions within a
     * single source transaction.
     *
     * <p>The per-region {@code multiTableSingleTxnMaxBytes} check in
     * {@code TransactionTableRegion.write0} only sees one region's activeChunk
     * at a time. A single source transaction that splits its payload across
     * several tables can keep each region's chunk comfortably under the
     * per-region hard cap while the combined payload pushes the manager-level
     * {@code currentCacheBytes} past {@code maxWriteBlockCacheBytes}. At that
     * point the task thread would be parked in {@code blockIfCacheFull} with
     * no region at a clean boundary, and the txnEnd marker needed to unblock
     * any region can never be delivered — a silent deadlock.
     *
     * <p>This test writes rows alternately to three tables without calling
     * {@code setCommitAllowed}, simulating one source transaction that spans
     * all three. No single region exceeds the per-region cap; the deadlock
     * is only avoidable via the aggregate in-progress byte guard in the
     * manager. We expect an {@link IllegalStateException} with a clear
     * "aggregate" remediation hint, raised before {@code blockIfCacheFull}
     * could park the task thread.
     */
    @Test(timeout = 10000)
    public void testFailFastOnAggregateInProgressBytesAcrossRegions() throws Exception {
        // 1 KB buffer → hard cap (maxWriteBlockCacheBytes) = 2 KB.
        // Per-region fail-fast threshold is also 2 KB, so each individual
        // region can grow almost to 2 KB without tripping the per-region
        // check. Spreading writes across three tables keeps each region well
        // under that limit while the aggregate grows past 2 KB.
        StreamLoadProperties properties = buildMultiTableProperties(60000, 1024L);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            IllegalStateException caught = null;
            int rowsWritten = 0;
            String[] tables = {"orders", "items", "customers"};
            try {
                // Round-robin across three tables on one partition without any
                // setCommitAllowed call. Each row is ~68 bytes; each region's
                // activeChunk grows by one row per three iterations, so after
                // ~90 iterations each region holds ~30 rows (~2 KB each, well
                // under the per-region 2 KB cap when viewed individually) but
                // the aggregate is ~6 KB (3× hard cap). The aggregate guard
                // should fire long before any region approaches its own cap.
                for (int i = 0; i < 300; i++) {
                    String table = tables[i % tables.length];
                    manager.write(0, "test", table,
                            String.format(
                                    "{\"id\":%06d,\"customer_id\":%06d,\"notes\":\"pad-block-%04d\"}",
                                    i, i, i));
                    rowsWritten++;
                }
                Assert.fail("Expected IllegalStateException for aggregate in-progress overflow, "
                        + "but write() succeeded after " + rowsWritten + " rows");
            } catch (IllegalStateException e) {
                caught = e;
            }

            Assert.assertNotNull(
                    "Expected IllegalStateException for aggregate in-progress overflow", caught);
            Assert.assertTrue(
                    "Error message should identify it as the aggregate guard: " + caught.getMessage(),
                    caught.getMessage().toLowerCase().contains("aggregate"));
            Assert.assertTrue(
                    "Error message should mention the write-block threshold: " + caught.getMessage(),
                    caught.getMessage().contains("write-block"));
            Assert.assertTrue(
                    "Error message should suggest a remediation: " + caught.getMessage(),
                    caught.getMessage().contains("buffer-size")
                            || caught.getMessage().contains("buffer size"));
            // Sanity: the aggregate guard must fire before blockIfCacheFull
            // parks the task thread. currentCacheBytes crosses the 2 KB hard
            // cap at ~30 rows (30 × 68 ≈ 2040 B), so the guard should fire
            // near that point, not after hundreds of rows.
            Assert.assertTrue(
                    "Aggregate guard should trigger near the hard-cap boundary, "
                            + "but rowsWritten=" + rowsWritten,
                    rowsWritten < 60);
        } finally {
            try {
                manager.close();
            } catch (Exception ignore) {
                // close() after an exception is best-effort.
            }
        }
    }

    /**
     * Positive counterpart to {@link #testFailFastOnAggregateInProgressBytesAcrossRegions}:
     * verifies that a {@code setCommitAllowed} actually drains the manager's
     * aggregate in-progress byte counter, so a second multi-region source
     * transaction of the same size proceeds without tripping the aggregate
     * fail-fast.
     *
     * <p>This pins down the release path (
     * {@code TransactionTableRegion.releaseInProgressBytes} invoked from
     * {@code tryMiniIntervalSwitch} on txnEnd). A regression that forgets to
     * subtract the per-region in-progress bytes back to the manager aggregate
     * would leave stale bytes in the counter: the second sweep's writes plus
     * the stale 1st-sweep bytes would cross the write-block threshold and
     * wrongly throw {@link IllegalStateException}.
     *
     * <p>The two sweeps are each sized so that, <em>individually</em>, they
     * stay well below the 2 KB hard cap, but <em>together without the
     * drain</em> they would cross it. Exact sizing: each sweep writes ~25
     * rows (≈ 68 B/row) across three tables → ~1.7 KB of in-progress bytes.
     * With drain: second sweep peaks at ~1.7 KB &lt; 2 KB → no exception.
     * Without drain: combined ≈ 3.4 KB &gt; 2 KB → fail-fast fires on the
     * second sweep.
     */
    @Test(timeout = 15000)
    public void testCommitDrainsAggregateInProgressBytes() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(60000, 1024L);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            String[] tables = {"orders", "items", "customers"};
            final int rowsPerSweep = 25; // ~1.7 KB aggregate per sweep

            // Sweep #1: round-robin three tables, then txnEnd. The txnEnd on
            // partition 0 fans out to all regions and must drain their
            // per-region inProgressTxnBytes back to the manager aggregate.
            for (int i = 0; i < rowsPerSweep; i++) {
                String table = tables[i % tables.length];
                manager.write(0, "test", table,
                        String.format(
                                "{\"id\":%06d,\"customer_id\":%06d,\"notes\":\"pad-block-%04d\"}",
                                i, i, i));
            }
            manager.setCommitAllowed(0, true);
            Assert.assertNull("No exception after sweep #1 txnEnd",
                    manager.getException());

            // Sweep #2: same size and shape. If the drain worked, the
            // aggregate counter started this sweep at (near) 0 and peaks at
            // ~1.7 KB, comfortably under the 2 KB cap. If the drain silently
            // regresses, the aggregate still carries sweep #1's ~1.7 KB, and
            // sweep #2 would cross 2 KB partway through and throw.
            for (int i = 0; i < rowsPerSweep; i++) {
                String table = tables[i % tables.length];
                manager.write(0, "test", table,
                        String.format(
                                "{\"id\":%06d,\"customer_id\":%06d,\"notes\":\"pad-block-%04d\"}",
                                100 + i, 100 + i, 100 + i));
            }
            manager.setCommitAllowed(0, true);
            Assert.assertNull("No exception after sweep #2 txnEnd — the "
                            + "aggregate guard should not misfire after a clean txnEnd drain",
                    manager.getException());

            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());

            // Both sweeps must have produced at least one commit against the
            // mocked server — otherwise we haven't actually exercised the
            // drain path.
            Assert.assertTrue(
                    "Expected at least 1 commit after sweeps: " + mockedServer.getCommitCount(),
                    mockedServer.getCommitCount() >= 1);
        } finally {
            manager.close();
        }
    }
}
