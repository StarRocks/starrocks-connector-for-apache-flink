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
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * Verifies the structural multi-table transaction fixes under sustained
 * multi-partition load against the mocked server:
 *
 * <ol>
 *   <li><b>Per-(db,table) load serialization</b> — the server allows a single
 *       in-flight /api/transaction/load per (label, table) channel; sibling
 *       partition-regions must take turns instead of colliding into
 *       TXN_IN_PROCESSING.</li>
 *   <li><b>Cross-table commit alignment</b> — for every shared transaction
 *       label, the committed orders/order_items row sets must belong to the
 *       same source transactions (items == 3 * orders per label), which is
 *       guaranteed by partition-lockstep switching plus the commit-cut
 *       watermark.</li>
 *   <li><b>No data loss</b> — every produced row is delivered exactly once
 *       across all labels.</li>
 * </ol>
 */
public class MultiTableTxnSerializationAlignmentTest {

    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String DB = "test";
    private static final String ORDERS = "orders";
    private static final String ITEMS = "order_items";
    private static final String ORDER_MARKER = "\"omark\"";
    private static final String ITEM_MARKER = "\"imark\"";
    private static final int PARTITIONS = 4;
    private static final int TXNS_PER_PARTITION = 60;
    private static final int ITEMS_PER_ORDER = 3;

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

    private StreamLoadProperties buildProperties(int commitIntervalMs) {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(DB)
                .table(ORDERS)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100000)
                .build();
        return StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .maxRetries(3)
                .retryIntervalInMs(2000)
                .labelPrefix("test-align-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(commitIntervalMs)
                .scanningFrequency(50)
                .ioThreadCount(4)
                .build();
    }

    /**
     * Drives PARTITIONS x TXNS_PER_PARTITION source transactions (1 order +
     * ITEMS_PER_ORDER items each) through the manager, interleaving partitions
     * like a keyed Flink sink subtask would, then drains via the savepoint
     * flush.
     */
    private void driveWorkload(StreamLoadManagerV2 manager) throws Exception {
        for (long seq = 0; seq < TXNS_PER_PARTITION; seq++) {
            for (int p = 0; p < PARTITIONS; p++) {
                long orderId = p * 1_000_000L + seq;
                manager.write(p, DB, ORDERS,
                        "{\"order_id\":" + orderId + ", " + ORDER_MARKER + ":1}");
                for (int i = 0; i < ITEMS_PER_ORDER; i++) {
                    manager.write(p, DB, ITEMS,
                            "{\"item_id\":" + (orderId * 10 + i) + ", \"order_id\":" + orderId
                                    + ", " + ITEM_MARKER + ":1}");
                }
                manager.setCommitAllowed(p, true);
            }
            // Pace the source so the workload spans multiple commit cycles and
            // the commit cut races against ongoing writes.
            Thread.sleep(5);
        }
        manager.flush();
    }

    private static int countOccurrences(String haystack, String needle) {
        if (haystack == null || haystack.isEmpty()) {
            return 0;
        }
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    /** Aggregates delivered marker counts per label: label -> [orders, items]. */
    private Map<String, int[]> collectPerLabelCounts() {
        Map<String, int[]> perLabel = new HashMap<>();
        for (Map.Entry<String, String> entry : mockedServer.getTxnLoadBodies().entrySet()) {
            String[] key = entry.getKey().split("\\|", 3);
            String table = key[1];
            String label = key[2];
            int[] counts = perLabel.computeIfAbsent(label, k -> new int[2]);
            if (ORDERS.equals(table)) {
                counts[0] += countOccurrences(entry.getValue(), ORDER_MARKER);
            } else if (ITEMS.equals(table)) {
                counts[1] += countOccurrences(entry.getValue(), ITEM_MARKER);
            }
        }
        return perLabel;
    }

    /** Polls {@code cond} every 20ms until true or the timeout elapses; fails otherwise. */
    private static void awaitUntil(BooleanSupplier cond, long timeoutMs, String message) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        Assert.fail("Timed out after " + timeoutMs + "ms: " + message);
    }

    /**
     * Regression for the stale/null shared-label race (2026-07): a region can land
     * in {@code flushQ} holding a label that is NOT the coordinator's current shared
     * label while a shared transaction is active, and nothing re-validates it before
     * the autonomous flush selects it — so its first flush mints an independent
     * (orphan) transaction, splitting a source transaction across labels and breaking
     * cross-table atomicity.
     *
     * <p><b>How the state arises in the wild</b> (verified by whole-system trace):
     * region creation ({@code getCacheRegion}) injects the shared label and enqueues
     * to {@code flushQ} under {@code synchronized(regions)}, but the commit-tail
     * re-open ({@code ensureSharedTransaction}) publishes the new label via
     * {@code SharedTransactionCoordinator.begin()} BEFORE its weakly-consistent
     * {@code ConcurrentLinkedQueue} inject-loop iterates — and takes no {@code regions}
     * monitor. A first write to a NEW (db,table) whose {@code isActive()} read lands in
     * the {@code commitInFlight==false && !isActive()} gap (so it captures neither the
     * defensive watermark nor a label) and whose {@code flushQ.offer} lands after the
     * inject-loop's cursor passes the tail is MISSED by injection. The per-scan
     * re-injection guard ({@code if (!isActive())}) is then dormant because the txn is
     * active, so the region is never re-injected.
     *
     * <p><b>Why the state is constructed directly here:</b> that interleaving is a
     * microsecond window between {@code getCacheRegion}'s {@code isActive()} read and
     * its {@code flushQ.offer}; it cannot be forced deterministically through the
     * public API (the autonomous manager thread will not stall on demand). We therefore
     * reproduce the PROVEN-REACHABLE resulting state — a {@code flushQ} region with a
     * null label while the shared transaction stays active — and assert the manager
     * repairs it before any flush. This test is RED on the pre-fix code (nothing
     * reconciles the label while active) and GREEN once the manager reconciles every
     * {@code flushQ} region to the live shared label before flush selection.
     */
    @Test
    public void testActiveTxnReconcilesRegionMissingSharedLabel() throws Exception {
        // Large commit interval + no txnEnd + default (large) recycle idle => the shared
        // transaction opened below stays active and stable for the whole test.
        DefaultStreamLoadManager manager = new DefaultStreamLoadManager(buildProperties(60000), true);
        manager.init();
        try {
            // 1. Open a shared transaction by writing to the first table (partition 0).
            manager.write(0, DB, ORDERS, "{\"order_id\":1, " + ORDER_MARKER + ":1}");

            // 2. Wait until the manager thread eagerly opened the shared txn and injected
            //    its label into the orders region.
            String ordersKey = "P0-" + StreamLoadUtils.getTableUniqueKey(DB, ORDERS);
            TableRegion orders = manager.getCacheRegion(ordersKey, DB, ORDERS, 0);
            awaitUntil(() -> orders.getLabel() != null, 5000,
                    "shared transaction should open and inject a label into the orders region");
            final String sharedLabel = orders.getLabel();

            // 3. A sibling region created while the txn is active DOES get the shared label
            //    (getCacheRegion's isActive()-gated injection path).
            String itemsKey = "P0-" + StreamLoadUtils.getTableUniqueKey(DB, ITEMS);
            TableRegion items = manager.getCacheRegion(itemsKey, DB, ITEMS, 0);
            Assert.assertEquals("precondition: sibling created while active receives the shared label",
                    sharedLabel, items.getLabel());

            // 4. Reproduce the missed-injection state: the region sits in flushQ with a null
            //    label while the shared transaction remains active.
            items.setLabel(null);

            // 5. Let manager scans run. BUG: no re-validation while a txn is active -> the
            //    region keeps its null label (its first flush would mint an orphan txn).
            //    FIX: the manager reconciles flushQ regions to the live shared label before
            //    flush selection, restoring it.
            awaitUntil(() -> sharedLabel.equals(items.getLabel()), 3000,
                    "manager must reconcile the flushQ region back to the live shared label "
                            + "while the shared transaction is active (else its flush mints an orphan txn)");

            Assert.assertEquals("region must carry the live shared label, never null/stale",
                    sharedLabel, items.getLabel());
            Assert.assertNull("no failure expected: " + manager.getException(), manager.getException());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testPerTableLoadSerialization() throws Exception {
        // Make the channel-busy window wide and the rejection real: any
        // overlapping same-channel load would be rejected and (without the
        // serialization gate) deterministically observed as concurrency > 1.
        mockedServer.setSimulateChannelBusy(true);
        mockedServer.setTxnLoadDelayMs(20);
        mockedServer.resetCounters();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(buildProperties(200), true);
        manager.init();
        try {
            driveWorkload(manager);
            Assert.assertNull("manager must not fail under sustained multi-partition load: "
                    + manager.getException(), manager.getException());
            Assert.assertEquals("per-(label, table) channel concurrency must never exceed 1 "
                            + "(loads of sibling partition-regions must be serialized)",
                    1, mockedServer.getMaxChannelConcurrency());
        } finally {
            manager.close();
        }
    }

    /**
     * Regression for the mid-cut region race (PR #491 review, Codex P2): a region
     * created while a commit cycle is in flight (first write to a new table) must
     * not contribute chunks to the already-cut shared transaction — its
     * transactions wait for the next label. With slow loads stretching every
     * commit cycle, the first write to the late table lands inside a commit
     * window with high probability. The invariant asserted here is stronger and
     * fully deterministic: for every committed label, the per-table sets of
     * source-transaction sequence numbers must be identical for all tables
     * participating in those transactions — a source transaction never splits
     * across labels.
     */
    @Test
    public void testLateTableTransactionsNeverSplitAcrossLabels() throws Exception {
        mockedServer.setSimulateChannelBusy(true);
        mockedServer.setTxnLoadDelayMs(250); // stretch commit cycles past several txnEnds
        mockedServer.resetCounters();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(buildProperties(200), true);
        manager.init();
        try {
            int lateTableFromSeq = 30;
            for (long seq = 0; seq < 90; seq++) {
                manager.write(0, DB, ORDERS,
                        "{\"order_id\":" + seq + ", \"seqo\":" + seq + "}");
                manager.write(0, DB, ITEMS,
                        "{\"item_id\":" + (seq * 10) + ", \"seqi\":" + seq + "}");
                if (seq >= lateTableFromSeq) {
                    // first write at seq==30 creates the region mid-stream,
                    // very likely inside an in-flight commit cycle
                    manager.write(0, DB, "audit_log",
                            "{\"order_id\":" + seq + ", \"seqa\":" + seq + "}");
                }
                manager.setCommitAllowed(0, true);
                Thread.sleep(20);
            }
            manager.flush();
            Assert.assertNull("manager must not fail: " + manager.getException(),
                    manager.getException());

            // Per-label transaction-sequence sets per table
            Map<String, Map<String, java.util.Set<Long>>> labelToTableSeqs = new HashMap<>();
            java.util.regex.Pattern p = java.util.regex.Pattern.compile("\"seq[oia]\":(\\d+)");
            for (Map.Entry<String, String> entry : mockedServer.getTxnLoadBodies().entrySet()) {
                // mock channel key format: db|table|label
                String[] parts = entry.getKey().split("\\|", 3);
                String table = parts[1];
                String label = parts[2];
                java.util.Set<Long> seqs = labelToTableSeqs
                        .computeIfAbsent(label, k -> new HashMap<>())
                        .computeIfAbsent(table, k -> new java.util.HashSet<>());
                java.util.regex.Matcher m = p.matcher(entry.getValue());
                while (m.find()) {
                    seqs.add(Long.parseLong(m.group(1)));
                }
            }
            Assert.assertFalse("expected committed labels", labelToTableSeqs.isEmpty());

            java.util.Set<Long> allOrders = new java.util.HashSet<>();
            java.util.Set<Long> allAudit = new java.util.HashSet<>();
            for (Map.Entry<String, Map<String, java.util.Set<Long>>> e : labelToTableSeqs.entrySet()) {
                java.util.Set<Long> o = e.getValue().getOrDefault(ORDERS, java.util.Collections.emptySet());
                java.util.Set<Long> i = e.getValue().getOrDefault(ITEMS, java.util.Collections.emptySet());
                java.util.Set<Long> a = e.getValue().getOrDefault("audit_log", java.util.Collections.emptySet());
                Assert.assertEquals("label " + e.getKey() + ": orders/items of the same source "
                        + "transactions must commit under the same label", o, i);
                for (Long seq : a) {
                    Assert.assertTrue("label " + e.getKey() + ": audit_log row of txn " + seq
                                    + " committed under a label that does not contain the txn's "
                                    + "orders row — source transaction split across labels "
                                    + "(mid-cut region race)",
                            o.contains(seq));
                }
                allOrders.addAll(o);
                allAudit.addAll(a);
            }
            Assert.assertEquals("every transaction must be delivered", 90, allOrders.size());
            Assert.assertEquals("every late-table transaction must be delivered",
                    60, allAudit.size());
        } finally {
            manager.close();
        }
    }

    /**
     * Regression for the recycle-path cross-table skew (PR #491 review, banmoy):
     * {@code recycleSharedTransaction()} fires from the normal scan branch on the
     * {@code sharedTxnMaxIdleMs} timer WITHOUT writer quiescence. Before the fix
     * it froze each region independently (per-region {@code tryForceCleanSwitch})
     * — so if the freeze loop interleaved with a task-thread write mid source
     * transaction (order row written -> orders dirty; item row not yet -> items
     * still clean), one table would freeze while its sibling was skipped, and the
     * recycled commit would publish one table's rows of a transaction without the
     * other's.
     *
     * <p>This test forces recycle to fire repeatedly mid-stream by setting a tiny
     * server timeout ({@code timeout=1} header -> sharedTxnMaxIdleMs = 800ms) and
     * driving paced two-table transactions for several seconds. The invariant is
     * the same deterministic one as the mid-cut test: for every committed label,
     * the set of source-transaction seqs in {@code orders} must EQUAL the set in
     * {@code order_items} — a source transaction never splits across labels. On
     * the buggy per-region recycle this fails; on the lockstep recycle it holds.
     */
    @Test(timeout = 60000)
    public void testRecycleKeepsCrossTableAlignment() throws Exception {
        // No channel-busy / load delay here: recycle's internal wait is bounded
        // by flushTimeoutMs (= timeout*1100 = 1100ms), so loads must settle fast.
        mockedServer.resetCounters();

        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(DB)
                .table(ORDERS)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100000)
                .build();
        StreamLoadProperties props = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .maxRetries(3)
                .retryIntervalInMs(500)
                .labelPrefix("test-recycle-")
                .defaultTableProperties(tableProps)
                // Commit interval (5s) LARGER than sharedTxnMaxIdleMs (800ms) so the
                // timer-driven lockstep-cut commit never fires during the run —
                // recycle becomes the SOLE commit driver, exercising exactly the
                // path under test.
                .expectDelayTime(5000)
                .scanningFrequency(50)
                .ioThreadCount(4)
                .addHeader("timeout", "1")       // -> sharedTxnMaxIdleMs = 800ms, flushTimeoutMs = 1100ms
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(props, true);
        manager.init();
        try {
            // ~4s of traffic at ~15ms/txn spans ~5 recycle cycles (800ms each),
            // so recycle is virtually guaranteed to interleave with writes.
            int total = 200;
            for (long seq = 0; seq < total; seq++) {
                manager.write(0, DB, ORDERS,
                        "{\"order_id\":" + seq + ", \"seqo\":" + seq + "}");
                // Widen the intermediate "orders dirty, order_items still clean"
                // window so a non-lockstep recycle freeze landing here would
                // freeze order_items (holding completed prior txns) while
                // skipping the dirty orders region — exactly banmoy's TOCTOU.
                // A per-partition lockstep freeze takes both write locks and
                // freezes neither (orders dirty) or both, so alignment holds.
                Thread.sleep(20);
                manager.write(0, DB, ITEMS,
                        "{\"item_id\":" + (seq * 10) + ", \"seqi\":" + seq + "}");
                manager.setCommitAllowed(0, true);
                Thread.sleep(10);
                Assert.assertNull("manager must not fail mid-run: " + manager.getException(),
                        manager.getException());
            }
            manager.flush();
            Assert.assertNull("manager must not fail: " + manager.getException(),
                    manager.getException());

            Map<String, Map<String, java.util.Set<Long>>> labelToTableSeqs = new HashMap<>();
            java.util.regex.Pattern p = java.util.regex.Pattern.compile("\"seq[oi]\":(\\d+)");
            for (Map.Entry<String, String> entry : mockedServer.getTxnLoadBodies().entrySet()) {
                String[] parts = entry.getKey().split("\\|", 3);
                String table = parts[1];
                String label = parts[2];
                java.util.Set<Long> seqs = labelToTableSeqs
                        .computeIfAbsent(label, k -> new HashMap<>())
                        .computeIfAbsent(table, k -> new java.util.HashSet<>());
                java.util.regex.Matcher m = p.matcher(entry.getValue());
                while (m.find()) {
                    seqs.add(Long.parseLong(m.group(1)));
                }
            }
            Assert.assertFalse("expected committed labels", labelToTableSeqs.isEmpty());
            // Recycle must actually have fired (else the test proves nothing):
            // begins > commits would mean only timer-driven commits ran. With an
            // 800ms idle cap over ~4s, several recycle-driven begins are expected.
            Assert.assertTrue("expected multiple shared-transaction labels (recycle + "
                            + "timer commits) over the run, got " + labelToTableSeqs.size(),
                    labelToTableSeqs.size() >= 3);

            java.util.Set<Long> allOrders = new java.util.HashSet<>();
            for (Map.Entry<String, Map<String, java.util.Set<Long>>> e : labelToTableSeqs.entrySet()) {
                java.util.Set<Long> o = e.getValue().getOrDefault(ORDERS, java.util.Collections.emptySet());
                java.util.Set<Long> i = e.getValue().getOrDefault(ITEMS, java.util.Collections.emptySet());
                Assert.assertEquals("label " + e.getKey() + ": a source transaction's orders "
                        + "and order_items rows must commit under the SAME label — recycle froze "
                        + "one table without its sibling (cross-table skew)", o, i);
                allOrders.addAll(o);
            }
            Assert.assertEquals("every transaction must be delivered exactly once",
                    total, allOrders.size());
        } finally {
            manager.close();
        }
    }

    /**
     * Recycle alignment under ASYMMETRIC per-table load latency.
     *
     * <p>This is the recycle counterpart of
     * {@link #testRecycleKeepsCrossTableAlignment}, hardened with very different
     * sibling load times (orders 50ms vs order_items 600ms) so the recycle
     * commit path is exercised while one table's region has long returned to
     * ACTIVE and the other is still loading. It asserts the outcome that the
     * watermark + drain-retry protocol guarantees: for every recycle-committed
     * label, the orders and order_items source-transaction seq sets are EQUAL —
     * no table's rows of a transaction are published without the sibling's.
     *
     * <p><b>Scope note (honest):</b> the specific drain micro-window banmoy
     * described — a fresh aligned pair switched <i>during</i> the recycle drain
     * and chain-loaded asymmetrically — could NOT be isolated as a RED/GREEN
     * unit test in this mock harness: the manager's continuous autonomous flush
     * keeps the inactive-chunk backlog drained between recycles, so recycle
     * never drains a backlog long enough to overlap a fresh switch, and forcing
     * a large unloaded backlog (loads slower than production) instead trips the
     * drain timeout rather than producing a clean skew. That sub-window is
     * covered structurally (recycle now uses the exact same
     * cutPartitionWithWatermark + drain-on-hasEligiblePendingChunks +
     * finishCommitCut protocol as the proven timer-driven commit) and by the
     * real-cluster E2E recycle run. This test guards the recycle commit's
     * cross-table alignment outcome under adverse asymmetric timing.
     */
    @Test(timeout = 120000)
    public void testRecycleAlignmentUnderAsymmetricLoadLatency() throws Exception {
        mockedServer.resetCounters();
        mockedServer.setTxnLoadDelayMsForTable(ORDERS, 50);
        mockedServer.setTxnLoadDelayMsForTable(ITEMS, 600);

        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(DB)
                .table(ORDERS)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100000)
                .build();
        StreamLoadProperties props = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .maxRetries(3)
                .retryIntervalInMs(500)
                .labelPrefix("test-drain-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(30000)          // commit interval >> idle cap: recycle drives all commits
                .scanningFrequency(50)
                .ioThreadCount(4)
                .addHeader("timeout", "5")       // sharedTxnMaxIdleMs=4000ms, flushTimeoutMs=5500ms
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(props, true);
        manager.init();
        try {
            // ~20s of two-table transactions at 100ms pace (slow enough that the
            // 1.2s items loads keep up overall) spanning ~5 recycle cycles
            // (recycle fires every 4s). Each cycle's ~1.2s items drain overlaps a
            // fresh pair switch (mini gate 1s), repeatedly exposing the window —
            // while staying well under flushTimeoutMs (5.5s) so neither version
            // times out; the difference is purely whether the watermark holds the
            // post-cut pair back.
            int total = 150;
            for (long seq = 0; seq < total; seq++) {
                manager.write(0, DB, ORDERS,
                        "{\"order_id\":" + seq + ", \"seqo\":" + seq + "}");
                manager.write(0, DB, ITEMS,
                        "{\"item_id\":" + (seq * 10) + ", \"seqi\":" + seq + "}");
                manager.setCommitAllowed(0, true);
                Thread.sleep(100);
                Assert.assertNull("manager must not fail mid-run: " + manager.getException(),
                        manager.getException());
            }
            manager.flush();
            Assert.assertNull("manager must not fail: " + manager.getException(),
                    manager.getException());

            Map<String, Map<String, java.util.Set<Long>>> labelToTableSeqs = new HashMap<>();
            java.util.regex.Pattern p = java.util.regex.Pattern.compile("\"seq[oi]\":(\\d+)");
            for (Map.Entry<String, String> entry : mockedServer.getTxnLoadBodies().entrySet()) {
                String[] parts = entry.getKey().split("\\|", 3);
                String table = parts[1];
                String label = parts[2];
                java.util.Set<Long> seqs = labelToTableSeqs
                        .computeIfAbsent(label, k -> new HashMap<>())
                        .computeIfAbsent(table, k -> new java.util.HashSet<>());
                java.util.regex.Matcher m = p.matcher(entry.getValue());
                while (m.find()) {
                    seqs.add(Long.parseLong(m.group(1)));
                }
            }
            // At least one recycle-driven commit must have happened (the slow
            // items table makes recycle drain a large backlog over several
            // cycles; the exact label count is timing-dependent, but the
            // per-label skew assertion below is what matters).
            Assert.assertFalse("expected at least one recycle-driven label",
                    labelToTableSeqs.isEmpty());

            java.util.Set<Long> allOrders = new java.util.HashSet<>();
            for (Map.Entry<String, Map<String, java.util.Set<Long>>> e : labelToTableSeqs.entrySet()) {
                java.util.Set<Long> o = e.getValue().getOrDefault(ORDERS, java.util.Collections.emptySet());
                java.util.Set<Long> i = e.getValue().getOrDefault(ITEMS, java.util.Collections.emptySet());
                Assert.assertEquals("label " + e.getKey() + ": post-cut chunks leaked "
                        + "asymmetrically into the recycled commit (drain race — one table's "
                        + "[N] published without its sibling's)", o, i);
                allOrders.addAll(o);
            }
            Assert.assertEquals("every transaction must be delivered exactly once",
                    total, allOrders.size());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testCrossTableCommitAlignmentAndCompleteness() throws Exception {
        mockedServer.setSimulateChannelBusy(true);
        // Slow loads stretch the commit cycle so the commit cut must hold back
        // chunks switched while the commit is in flight.
        mockedServer.setTxnLoadDelayMs(30);
        mockedServer.resetCounters();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(buildProperties(200), true);
        manager.init();
        try {
            driveWorkload(manager);
            Assert.assertNull("manager must not fail: " + manager.getException(),
                    manager.getException());

            Map<String, int[]> perLabel = collectPerLabelCounts();
            Assert.assertFalse("expected at least one committed label", perLabel.isEmpty());

            int totalOrders = 0;
            int totalItems = 0;
            for (Map.Entry<String, int[]> entry : perLabel.entrySet()) {
                int orders = entry.getValue()[0];
                int items = entry.getValue()[1];
                totalOrders += orders;
                totalItems += items;
                Assert.assertEquals("label " + entry.getKey() + " must contain aligned "
                                + "source transactions across tables (orders=" + orders
                                + ", items=" + items + ")",
                        orders * ITEMS_PER_ORDER, items);
            }
            Assert.assertEquals("all orders must be delivered exactly once",
                    PARTITIONS * TXNS_PER_PARTITION, totalOrders);
            Assert.assertEquals("all items must be delivered exactly once",
                    PARTITIONS * TXNS_PER_PARTITION * ITEMS_PER_ORDER, totalItems);
        } finally {
            manager.close();
        }
    }
}
