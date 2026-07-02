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
