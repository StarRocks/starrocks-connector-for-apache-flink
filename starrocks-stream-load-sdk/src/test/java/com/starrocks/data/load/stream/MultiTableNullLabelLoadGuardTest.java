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

import com.starrocks.data.load.stream.http.StreamLoadEntityMeta;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.http.HttpEntity;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Regression for the multi-table "Empty label" / orphan-transaction hazard (2026-07).
 *
 * <p>A multi-table region's shared label can be transiently {@code null}: a first write to
 * a new (db,table) that missed {@code ensureSharedTransaction()}'s weakly-consistent label
 * injection, or a label just cleared by a concurrent commit/recycle/savepoint on the manager
 * thread. If such a region reaches the loader, two distinct failures are possible, both fatal
 * to cross-table atomicity:
 *
 * <ol>
 *   <li><b>Orphan mint</b>: {@link TransactionStreamLoader#begin} historically minted a fresh
 *       independent label for a null-label region and opened a single-table transaction,
 *       splitting a source transaction across labels.</li>
 *   <li><b>Empty label</b>: {@link DefaultStreamLoader#sendToSR} reads the label a second time
 *       and would send a literal {@code null} label to the FE, which rejects it fatally with
 *       {@code {Status:FAILED, Message: Empty label}} and fails the whole job.</li>
 * </ol>
 *
 * <p>The fix makes the load path treat a null shared label as a transient, recoverable state
 * in multi-table mode: {@code begin()} refuses (never mints), and {@code sendToSR()} aborts
 * WITHOUT sending, releasing FLUSHING so the manager reconciles the shared label and retries.
 *
 * <p>These tests are deterministic (no timing/race dependence): they drive the loader entry
 * points directly with a null-label region. RED on the pre-fix code (begin mints; sendToSR
 * builds and sends an entity), GREEN with the fix.
 */
public class MultiTableNullLabelLoadGuardTest {

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

    private StreamLoadProperties multiTableProperties() {
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
                .maxRetries(3)
                .retryIntervalInMs(2000)
                .labelPrefix("test-nl-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(60000)
                .scanningFrequency(50)
                .ioThreadCount(2)
                .build();
    }

    /** No-op manager: the loader only needs it for callbacks, which these tests do not exercise. */
    private static StreamLoadManager noopManager() {
        return new StreamLoadManager() {
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
        };
    }

    /**
     * Minimal {@link TableRegion} whose label is externally controllable and that records
     * whether the loader tried to build an HTTP entity ({@code getHttpEntity}) or release
     * FLUSHING ({@code exitFlushing}). {@code getLabelGenerator()} throws so an accidental
     * orphan-mint attempt is caught loudly.
     */
    private static final class StubRegion implements TableRegion {
        private volatile String label;
        final AtomicBoolean httpEntityRequested = new AtomicBoolean(false);
        final AtomicBoolean exitFlushingCalled = new AtomicBoolean(false);
        final AtomicBoolean failCalled = new AtomicBoolean(false);

        StubRegion(String label) {
            this.label = label;
        }

        @Override public StreamLoadTableProperties getProperties() { return null; }
        @Override public String getUniqueKey() { return "P0-test.orders"; }
        @Override public String getDatabase() { return "test"; }
        @Override public String getTable() { return "orders"; }
        @Override public LabelGenerator getLabelGenerator() {
            throw new AssertionError("getLabelGenerator() must not be called: a multi-table region "
                    + "with a null label must never mint an independent (orphan) label");
        }
        @Override public void setLabel(String label) { this.label = label; }
        @Override public String getLabel() { return label; }
        @Override public long getCacheBytes() { return 0; }
        @Override public long getFlushBytes() { return 0; }
        @Override public StreamLoadEntityMeta getEntityMeta() { return null; }
        @Override public long getLastWriteTimeMillis() { return 0; }
        @Override public void resetAge() {}
        @Override public long getAndIncrementAge() { return 0; }
        @Override public long getAge() { return 0; }
        @Override public int write(byte[] row) { return 0; }
        @Override public byte[] read() { return null; }
        @Override public boolean testPrepare() { return false; }
        @Override public boolean prepare() { return false; }
        @Override public boolean flush() { return false; }
        @Override public boolean cancel() { return false; }
        @Override public void callback(StreamLoadResponse response) {}
        @Override public void fail(Throwable e) { failCalled.set(true); }
        @Override public void complete(StreamLoadResponse response) {}
        @Override public void setResult(Future<?> result) {}
        @Override public Future<?> getResult() { return null; }
        @Override public boolean isReadable() { return false; }
        @Override public boolean isFlushing() { return true; }
        @Override public void exitFlushing() { exitFlushingCalled.set(true); }
        @Override public HttpEntity getHttpEntity() {
            httpEntityRequested.set(true);
            // Reached only if the null-label short-circuit is absent (i.e. the bug):
            // signal it as a load failure so sendToSR returns without contacting a server.
            throw new RuntimeException("sendToSR built an HTTP entity for a null-label region; "
                    + "a null label must never be sent to the FE");
        }
    }

    /**
     * {@code begin()} must NOT mint an independent label for a null-label region in
     * multi-table mode (that opens an orphan transaction and breaks cross-table atomicity);
     * it refuses so the caller defers and the manager reconciles the shared label.
     * A region already carrying the shared label passes through unchanged.
     */
    @Test
    public void testBeginRefusesNullLabelInMultiTableMode() {
        TransactionStreamLoader loader = new TransactionStreamLoader(true);
        loader.start(multiTableProperties(), noopManager());
        try {
            StubRegion nullLabel = new StubRegion(null);
            boolean began = loader.begin(nullLabel);
            Assert.assertFalse("multi-table begin() must refuse a null shared label (no orphan mint)", began);
            Assert.assertNull("begin() must NOT mint an independent label in multi-table mode",
                    nullLabel.getLabel());

            // A region that already carries the shared label passes through with no re-begin.
            StubRegion labeled = new StubRegion("shared-label-1");
            Assert.assertTrue("begin() must accept a region already carrying the shared label",
                    loader.begin(labeled));
            Assert.assertEquals("shared-label-1", labeled.getLabel());
        } finally {
            loader.close();
        }
    }

    /**
     * {@code sendToSR()} must never send a null label to the FE in multi-table mode. It must
     * abort BEFORE building/sending an HTTP entity and release FLUSHING so the manager
     * reconciles the label and retries.
     */
    @Test
    public void testSendToSRNeverSendsNullLabelInMultiTableMode() {
        TransactionStreamLoader loader = new TransactionStreamLoader(true);
        loader.start(multiTableProperties(), noopManager());
        try {
            StubRegion nullLabel = new StubRegion(null);
            StreamLoadResponse response = loader.sendToSR(nullLabel);

            Assert.assertNull("sendToSR must not perform a load when the shared label is null", response);
            Assert.assertFalse("sendToSR must NOT build/send an HTTP entity with a null label "
                    + "(would fail fatally with \"Empty label\")", nullLabel.httpEntityRequested.get());
            Assert.assertTrue("sendToSR must release FLUSHING so the manager reconciles and retries",
                    nullLabel.exitFlushingCalled.get());
        } finally {
            loader.close();
        }
    }

    /**
     * The null-label refusal must be a <em>deferral</em>, not a failure. {@code send()} treats a
     * {@code begin()} refusal as a hard transaction-start failure and calls
     * {@code region.fail(StreamLoadFailException)}; in multi-table mode
     * {@code TransactionTableRegion.fail()} only retries {@code TXN_IN_PROCESSING}, so that
     * synthetic error would reach {@code manager.callback()} and abort the job — turning the
     * race this guard exists to survive into a terminal failure. {@code send()} must instead
     * leave the region untouched and return {@code null} so the caller releases FLUSHING and
     * the manager's reconcile pass re-triggers the load.
     */
    @Test
    public void testSendDefersInsteadOfFailingRegionOnNullLabel() {
        TransactionStreamLoader loader = new TransactionStreamLoader(true);
        loader.start(multiTableProperties(), noopManager());
        try {
            StubRegion immediate = new StubRegion(null);
            Assert.assertNull("send() must not schedule a load for a null-label region",
                    loader.send(immediate));
            Assert.assertFalse("send() must NOT fail the region on a null-label deferral: in multi-table "
                    + "mode fail() is terminal for a non-TXN_IN_PROCESSING error and aborts the job",
                    immediate.failCalled.get());
            Assert.assertNull("the deferred region must keep its null label (no orphan mint)",
                    immediate.getLabel());

            // The delayed overload takes the same begin()-refusal branch and must defer identically.
            StubRegion delayed = new StubRegion(null);
            Assert.assertNull("send(region, delayMs) must not schedule a load for a null-label region",
                    loader.send(delayed, 10));
            Assert.assertFalse("send(region, delayMs) must defer, not fail, on a null shared label",
                    delayed.failCalled.get());
            Assert.assertNull(delayed.getLabel());
        } finally {
            loader.close();
        }
    }
}
