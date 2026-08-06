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

package com.starrocks.data.load.stream;

import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Regression for the "commit response lost" failure:
 *
 * <ul>
 *   <li><b>A1 — bounded RPC socket timeout</b>: the blocking transaction RPCs (begin/prepare/
 *       commit) run on the sink's manager thread; with the default {@code socketTimeout == -1}
 *       (infinite) a lost response would hang the thread indefinitely. {@code
 *       DefaultStreamLoader.boundedRpcSocketTimeoutMs} must never return an unbounded value.</li>
 *   <li><b>A2 — label-state reconciliation on a lost commit response</b>: when the commit HTTP
 *       fails before a status can be read (socket timeout, connection reset, non-200 from an
 *       LB/proxy, unparseable body) but the transaction actually committed server-side, {@code
 *       commit()} must reconcile against the real label state and return success instead of
 *       failing the job. Previously the {@code catch} block threw without checking label state,
 *       so a committed-but-response-lost transaction failed the job (then restart-stormed).</li>
 * </ul>
 */
public class TransactionCommitRecoveryTest {

    private static final String USER = "root";
    private static final String PASS = "";
    private static final String DB = "test";
    private static final String TABLE = "orders";
    /** Port 1 is reserved and never listening, so connections are refused immediately. */
    private static final String DEAD_HOST = "http://127.0.0.1:1";

    private MockedStarRocksHttpServer server;
    private final List<TransactionStreamLoader> loaders = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        server = MockedStarRocksHttpServer.builder().port(0).enforceAuth(USER, PASS).build();
        server.start();
    }

    @After
    public void tearDown() {
        // Close every loader started by the test so its ScheduledExecutorService threads do not
        // leak across the suite.
        for (TransactionStreamLoader loader : loaders) {
            try {
                loader.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
        loaders.clear();
        if (server != null) {
            server.stop();
        }
    }

    private StreamLoadProperties props(int socketTimeoutMs) {
        StreamLoadTableProperties table = StreamLoadTableProperties.builder()
                .database(DB)
                .table(TABLE)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();
        StreamLoadProperties.Builder b = StreamLoadProperties.builder()
                .loadUrls(server.getBaseUrl())
                .username(USER)
                .password(PASS)
                .version("4.0.0")
                .labelPrefix("test-commit-")
                .ioThreadCount(2)
                .defaultTableProperties(table)
                .socketTimeout(socketTimeoutMs); // -1 = default(unset), 0 = explicit infinite, >0 = explicit
        return b.build();
    }

    /** Same as {@link #props(int)} but with the stream-load {@code timeout} header set. */
    private StreamLoadProperties propsWithTimeoutHeader(int socketTimeoutMs, String timeoutSec) {
        StreamLoadTableProperties table = StreamLoadTableProperties.builder()
                .database(DB)
                .table(TABLE)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();
        return StreamLoadProperties.builder()
                .loadUrls(server.getBaseUrl())
                .username(USER)
                .password(PASS)
                .version("4.0.0")
                .labelPrefix("test-commit-")
                .ioThreadCount(2)
                .defaultTableProperties(table)
                .socketTimeout(socketTimeoutMs)
                .addHeader("timeout", timeoutSec)
                .build();
    }

    private TransactionStreamLoader startedLoader(StreamLoadProperties p) {
        TransactionStreamLoader loader = new TransactionStreamLoader(true);
        loader.start(p, new NoopManager());
        loaders.add(loader);
        return loader;
    }

    private static MockedStarRocksHttpServer.ResponseOverride lostCommitResponse() {
        // A non-200 from an intermediary makes parseHttpResponse throw *inside* commit()'s try,
        // landing in the exact catch block A2 fixes — the same block a socket timeout / reset hits.
        MockedStarRocksHttpServer.ResponseOverride ov = new MockedStarRocksHttpServer.ResponseOverride();
        ov.httpCode = 500;
        ov.status = StreamLoadConstants.RESULT_STATUS_FAILED;
        return ov;
    }

    // ---------- A1 ----------

    @Test
    public void testBoundedRpcSocketTimeoutIsNeverInfinite() {
        // The -1 default (unset) is infinite in Apache HttpClient -> must be bounded.
        int bounded = DefaultStreamLoader.boundedRpcSocketTimeoutMs(props(-1));
        Assert.assertTrue("bounded socket timeout must be > 0 (never -1/infinite), was " + bounded, bounded > 0);
        Assert.assertTrue("bounded socket timeout must stay well under the 660s flush timeout, was " + bounded,
                bounded <= 300_000);
        // An explicitly configured positive socketTimeout is honored as-is.
        Assert.assertEquals(7_000, DefaultStreamLoader.boundedRpcSocketTimeoutMs(props(7_000)));
        // An explicit 0 (opt-in infinite per the sink.socket.timeout-ms contract) is honored, NOT bounded.
        Assert.assertEquals(0, DefaultStreamLoader.boundedRpcSocketTimeoutMs(props(0)));
    }

    @Test
    public void testBoundedRpcSocketTimeoutStaysWithinManagerFlushBudget() {
        // DefaultStreamLoadManager derives flushTimeoutMs = timeoutSec * 1100 from the stream-load
        // `timeout` header. Blocking the manager thread past that budget is never useful: the
        // server-side transaction is already gone. A short timeout must therefore shrink the bound.
        Assert.assertEquals("timeout=1 -> 1.1s flush budget must cap the 90s default bound",
                1_100, DefaultStreamLoader.boundedRpcSocketTimeoutMs(propsWithTimeoutHeader(-1, "1")));
        Assert.assertEquals("timeout=30 -> 33s flush budget must cap the 90s default bound",
                33_000, DefaultStreamLoader.boundedRpcSocketTimeoutMs(propsWithTimeoutHeader(-1, "30")));

        // Above the crossover (~82s) the publish-derived bound is already the smaller of the two.
        Assert.assertEquals("timeout=600 -> 660s flush budget leaves the 90s bound untouched",
                90_000, DefaultStreamLoader.boundedRpcSocketTimeoutMs(propsWithTimeoutHeader(-1, "600")));
        // No timeout header at all: the manager keeps its own default, so no extra cap applies.
        Assert.assertEquals(90_000, DefaultStreamLoader.boundedRpcSocketTimeoutMs(props(-1)));

        // An unparseable or non-positive header must not shrink (or zero out) the bound — the
        // manager falls back to its default in exactly these cases.
        Assert.assertEquals(90_000, DefaultStreamLoader.boundedRpcSocketTimeoutMs(propsWithTimeoutHeader(-1, "abc")));
        Assert.assertEquals(90_000, DefaultStreamLoader.boundedRpcSocketTimeoutMs(propsWithTimeoutHeader(-1, "0")));

        // Whitespace must be treated exactly as the manager treats it. DefaultStreamLoadManager
        // calls Long.parseLong on the raw header, so " 1 " throws there and it keeps its 660s
        // default. Parsing more leniently here would cap the RPC at 1.1s against a budget the
        // manager never adopted, failing begin/prepare/commit prematurely.
        Assert.assertEquals("a padded header must not shrink the bound: the manager rejects it too",
                90_000, DefaultStreamLoader.boundedRpcSocketTimeoutMs(propsWithTimeoutHeader(-1, " 1 ")));

        // An explicit socketTimeout still wins over the flush-budget cap: the user opted in.
        Assert.assertEquals(7_000, DefaultStreamLoader.boundedRpcSocketTimeoutMs(propsWithTimeoutHeader(7_000, "1")));
    }

    // ---------- A2 ----------

    @Test
    public void testCommitResponseLostButCommittedTreatedAsSuccess() {
        TransactionStreamLoader loader = startedLoader(props(-1));
        String label = "lbl-committed-but-response-lost";
        server.setCommitOverride(lostCommitResponse());
        // The transaction actually committed server-side (FE committed; the reply was dropped).
        server.setLabelState(DB, TABLE, label, TransactionStatus.VISIBLE);

        boolean ok = loader.commit(new StreamLoadSnapshot.Transaction(DB, TABLE, label, true));
        Assert.assertTrue("a committed-but-response-lost transaction must be reconciled to success", ok);
        Assert.assertTrue("the lost commit response must actually have been exercised", server.getCommitCount() >= 1);
    }

    @Test
    public void testCommitResponseWithoutStatusIsReconciledNotFailed() {
        // An intermediary can substitute a syntactically valid body that carries no FE status —
        // `{}` (parses to a body whose status is null) or a bare `null` (parses to null) — AFTER
        // the FE already committed. Neither is a decision from the FE, so both belong on the
        // reconciliation path, not the fail-fast one. Previously `{}` threw outside the recovery
        // catch and `null` NPE'd there, so a committed txn still failed the job.
        for (String rawBody : new String[]{"{}", "null"}) {
            TransactionStreamLoader loader = startedLoader(props(-1));
            String label = "lbl-no-status-" + rawBody.hashCode();
            MockedStarRocksHttpServer.ResponseOverride ov = new MockedStarRocksHttpServer.ResponseOverride();
            ov.httpCode = 200;
            ov.rawBody = rawBody;
            server.setCommitOverride(ov);
            server.setLabelState(DB, TABLE, label, TransactionStatus.VISIBLE);

            Assert.assertTrue("a status-less commit body must reconcile to success, body=" + rawBody,
                    loader.commit(new StreamLoadSnapshot.Transaction(DB, TABLE, label, true)));
        }
    }

    @Test
    public void testCommitResponseWithoutStatusStillFailsWhenNotCommitted() {
        // The reconciliation must not turn a status-less body into blanket success: if the label
        // is genuinely not committed, the commit still has to fail.
        TransactionStreamLoader loader = startedLoader(props(-1));
        String label = "lbl-no-status-uncommitted";
        MockedStarRocksHttpServer.ResponseOverride ov = new MockedStarRocksHttpServer.ResponseOverride();
        ov.httpCode = 200;
        ov.rawBody = "{}";
        server.setCommitOverride(ov);
        server.setLabelState(DB, TABLE, label, TransactionStatus.ABORTED);

        try {
            loader.commit(new StreamLoadSnapshot.Transaction(DB, TABLE, label, true));
            Assert.fail("an uncommitted label must still fail even when the body carried no status");
        } catch (RuntimeException expected) {
            // expected: reconciliation found a non-committed terminal state and rethrew
        }
    }

    @Test
    public void testCommitResponseLostWhileStillCommittingWaitsInsteadOfFailing() throws Exception {
        // The bounded socket timeout can fire WHILE the FE is still committing: the txn is
        // transiently PREPARE, not lost. The reconciliation must poll until it settles, not fail
        // on the first (in-progress) state read. Model that: label starts PREPARE, then flips to
        // VISIBLE shortly after (as the FE finishes committing).
        TransactionStreamLoader loader = startedLoader(props(-1));
        String label = "lbl-still-committing";
        server.setCommitOverride(lostCommitResponse());
        server.setLabelState(DB, TABLE, label, TransactionStatus.PREPARE);
        Thread flip = new Thread(() -> {
            try {
                Thread.sleep(400);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            server.setLabelState(DB, TABLE, label, TransactionStatus.VISIBLE);
        });
        flip.setDaemon(true);
        flip.start();

        boolean ok = loader.commit(new StreamLoadSnapshot.Transaction(DB, TABLE, label, true));
        Assert.assertTrue("a commit that is still in progress (transient PREPARE) must be waited "
                + "out and reconciled to success once it settles, not prematurely failed", ok);
    }

    @Test
    public void testCommitResponseLostAndNotCommittedStillFails() {
        TransactionStreamLoader loader = startedLoader(props(-1));
        String label = "lbl-genuinely-uncommitted";
        server.setCommitOverride(lostCommitResponse());
        // The transaction did NOT commit (still UNKNOWN/absent server-side) -> must fail, not mask.
        server.setLabelState(DB, TABLE, label, TransactionStatus.UNKNOWN);

        try {
            loader.commit(new StreamLoadSnapshot.Transaction(DB, TABLE, label, true));
            Assert.fail("a genuinely-uncommitted lost commit must still fail (no false success)");
        } catch (RuntimeException expected) {
            // expected
        }
    }

    @Test
    public void testCommitResponseLostIsReconciledViaAnotherConfiguredFe() {
        // The FE that took the commit can be exactly the thing that died: it commits, then becomes
        // unreachable before the reply is read. getLabelState does no host selection of its own, so
        // pinning reconciliation to that FE turned a durably committed txn into a job failure even
        // though another configured FE could answer. The query must fail over to the other hosts.
        String deadFe = DEAD_HOST;
        StreamLoadProperties p = StreamLoadProperties.builder()
                .loadUrls(deadFe, server.getBaseUrl())
                .username(USER)
                .password(PASS)
                .version("4.0.0")
                .labelPrefix("test-commit-")
                .ioThreadCount(2)
                .defaultTableProperties(StreamLoadTableProperties.builder()
                        .database(DB)
                        .table(TABLE)
                        .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                        .build())
                .socketTimeout(-1)
                .build();
        // Force the commit onto the dead FE, so both the commit and the first label-state query hit
        // it — the exact "committed, then that FE went away" shape.
        TransactionStreamLoader loader = new TransactionStreamLoader(true) {
            @Override
            protected String getAvailableHost() {
                return deadFe;
            }
        };
        loader.start(p, new NoopManager());
        loaders.add(loader);

        String label = "lbl-committed-fe-gone";
        // The surviving FE reports the truth: the transaction did commit.
        server.setLabelState(DB, TABLE, label, TransactionStatus.VISIBLE);

        Assert.assertTrue("a commit whose FE died after committing must be reconciled through another "
                        + "configured FE, not failed",
                loader.commit(new StreamLoadSnapshot.Transaction(DB, TABLE, label, true)));
    }

    @Test
    public void testCommitReconciliationStillFailsWhenNoConfiguredFeAnswers() {
        // Failover must not become blanket success: if no configured FE can answer, the commit
        // still fails rather than being assumed committed.
        StreamLoadProperties p = StreamLoadProperties.builder()
                .loadUrls(DEAD_HOST)
                .username(USER)
                .password(PASS)
                .version("4.0.0")
                .labelPrefix("test-commit-")
                .ioThreadCount(2)
                .defaultTableProperties(StreamLoadTableProperties.builder()
                        .database(DB)
                        .table(TABLE)
                        .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                        .build())
                .socketTimeout(-1)
                .build();
        TransactionStreamLoader loader = startedLoader(p);

        try {
            loader.commit(new StreamLoadSnapshot.Transaction(DB, TABLE, "lbl-no-fe-answers", true));
            Assert.fail("with no FE able to report the label state the commit must still fail");
        } catch (RuntimeException expected) {
            // expected: every candidate host failed, so the original cause is rethrown
        }
    }

    private static class NoopManager implements StreamLoadManager {
        @Override public void init() {}
        @Override public void write(String uniqueKey, String database, String table, String... rows) {}
        @Override public void callback(StreamLoadResponse response) {}
        @Override public void callback(Throwable e) {}
        @Override public void flush() {}
        @Override public StreamLoadSnapshot snapshot() { return null; }
        @Override public boolean prepare(StreamLoadSnapshot snapshot) { return true; }
        @Override public boolean commit(StreamLoadSnapshot snapshot) { return true; }
        @Override public boolean abort(StreamLoadSnapshot snapshot) { return true; }
        @Override public void close() {}
    }
}
