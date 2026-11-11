/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * HTTP keep-alive of the stream loaders ({@link StreamLoadProperties#isEnableHttpKeepAlive()}): loads
 * and transaction requests share pooled connections, and a pooled connection is not reused when that
 * would break a later request.
 */
public class HttpKeepAliveTest {

    private static final String USER = "root";
    private static final String PASS = "";
    private static final String DB = "test";
    private static final String TABLE = "orders";
    private static final int IO_THREADS = 2;

    private MockedStarRocksHttpServer server;
    private final List<StreamLoader> loaders = new ArrayList<>();
    private final List<Closeable> closeables = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        server = MockedStarRocksHttpServer.builder()
                .port(0)
                .enforceAuth(USER, PASS)
                .handlerThreads(8)
                .build();
        server.start();
    }

    @After
    public void tearDown() {
        for (StreamLoader loader : loaders) {
            try {
                loader.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
        if (server != null) {
            server.stop();
        }
    }

    private StreamLoadProperties.Builder properties(String loadUrl, boolean keepAlive) {
        return StreamLoadProperties.builder()
                .loadUrls(loadUrl)
                .username(USER)
                .password(PASS)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .labelPrefix("test-keep-alive-")
                .ioThreadCount(IO_THREADS)
                .defaultTableProperties(StreamLoadTableProperties.builder()
                        .database(DB)
                        .table(TABLE)
                        .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                        .build())
                .enableHttpKeepAlive(keepAlive);
    }

    private TransactionStreamLoader startedLoader(StreamLoadProperties properties) {
        TransactionStreamLoader loader = new TransactionStreamLoader(true);
        loader.start(properties, new NoopManager());
        loaders.add(loader);
        return loader;
    }

    /** Runs one shared transaction on the calling thread: begin, {@code loads} loads one after another, commit. */
    private static void runTransaction(TransactionStreamLoader loader, String label, int loads) {
        Assert.assertTrue("begin failed", loader.beginTransaction(label, DB));
        for (int i = 0; i < loads; i++) {
            LoadRegion region = new LoadRegion(label);
            loader.sendToSR(region);
            region.assertCompleted();
        }
        Assert.assertTrue("commit failed", loader.commit(new StreamLoadSnapshot.Transaction(DB, null, label, true)));
    }

    @Test(timeout = 60_000)
    public void testRequestsShareOneConnection() {
        TransactionStreamLoader loader = startedLoader(properties(server.getBaseUrl(), true).build());
        server.resetCounters();

        runTransaction(loader, "label-reuse", 3);

        // begin, 3 loads and commit, one after another
        Assert.assertEquals(5, server.getClientPorts().size());
        Assert.assertEquals("the requests should all use one connection: " + server.getClientPorts(),
                1, server.getConnectionCount());
        PoolStats stats = loader.getKeepAliveClient().getTotalStats();
        Assert.assertEquals("every response should hand its connection back to the pool", 0, stats.getLeased());
        Assert.assertEquals(1, stats.getAvailable());
    }

    @Test(timeout = 60_000)
    public void testEveryRequestOpensAConnectionWhenKeepAliveIsOff() {
        TransactionStreamLoader loader = startedLoader(properties(server.getBaseUrl(), false).build());
        Assert.assertNull(loader.getKeepAliveClient());
        server.resetCounters();

        runTransaction(loader, "label-no-reuse", 3);

        Assert.assertEquals("each request should open its own connection: " + server.getClientPorts(),
                5, server.getConnectionCount());
    }

    @Test(timeout = 60_000)
    public void testLoadDoesNotInheritTheSocketTimeoutOfAnEarlierRequest() {
        // With the stream load timeout header at 1 s, begin reads with a 1.1 s socket timeout (see
        // DefaultStreamLoader#boundedRpcSocketTimeoutMs), while a load has none (-1, the default).
        StreamLoadProperties properties = properties(server.getBaseUrl(), true).addHeader("timeout", "1").build();
        Assert.assertEquals(1100, DefaultStreamLoader.boundedRpcSocketTimeoutMs(properties));
        TransactionStreamLoader loader = startedLoader(properties);
        server.setTxnLoadDelayMs(2000);
        server.resetCounters();

        String label = "label-socket-timeout";
        Assert.assertTrue(loader.beginTransaction(label, DB));
        LoadRegion region = new LoadRegion(label);
        loader.sendToSR(region);

        Assert.assertEquals("the load must reuse the connection of begin, or this test proves nothing",
                1, server.getConnectionCount());
        // A load that kept the 1.1 s timeout of begin would fail while the server takes 2 s
        region.assertCompleted();
    }

    @Test(timeout = 60_000)
    public void testConnectionAnsweredBeforeTheBodyIsNotReused() throws Exception {
        // The FE answers a transaction load with a 307 before the body is sent, and then reads the
        // connection for that body. The stand-in FE redirects every request to the mocked server.
        RawHttpServer fe = RawHttpServer.redirectingTo(server.getBaseUrl());
        closeables.add(fe);
        TransactionStreamLoader loader = startedLoader(properties(fe.getBaseUrl(), true).build());
        server.resetCounters();

        runTransaction(loader, "label-redirect", 3);

        Assert.assertEquals(3, server.getLoadCount());
        List<List<String>> requestsByConnection = fe.getRequestsByConnection();
        int feRequests = 0;
        for (List<String> requests : requestsByConnection) {
            feRequests += requests.size();
            for (int i = 0; i < requests.size() - 1; i++) {
                Assert.assertFalse("a request was sent on a connection after the FE answered a load on it "
                        + "before its body: " + requestsByConnection, requests.get(i).startsWith("PUT "));
            }
        }
        Assert.assertEquals(5, feRequests);
        // Behind the redirect, all requests share one connection
        Assert.assertEquals(1, server.getConnectionCount());
    }

    @Test(timeout = 60_000)
    public void testStockPooledClientReusesConnectionAnsweredBeforeTheBody() throws Exception {
        // What KeepAliveHttpClient guards against: with the default reuse strategy, a pooled HttpClient
        // puts the connection back after a 307 that arrived before the body, and sends the next request on it.
        RawHttpServer fe = RawHttpServer.redirectingTo(server.getBaseUrl());
        closeables.add(fe);
        try (CloseableHttpClient client = HttpClients.custom()
                .setConnectionManager(new PoolingHttpClientConnectionManager())
                .setRedirectStrategy(DefaultStreamLoader.redirectAnyMethod())
                .build()) {
            for (int i = 0; i < 2; i++) {
                HttpPut load = new HttpPut(fe.getBaseUrl() + "/api/transaction/load");
                load.setConfig(RequestConfig.custom().setExpectContinueEnabled(true).build());
                load.addHeader("Authorization", StreamLoadUtils.getBasicAuthHeader(USER, PASS));
                load.addHeader("label", "label-stock-client");
                load.addHeader("db", DB);
                load.addHeader("table", TABLE);
                load.setEntity(jsonRow());
                try (CloseableHttpResponse response = client.execute(load)) {
                    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
                    EntityUtils.consume(response.getEntity());
                }
            }
        }
        List<List<String>> requestsByConnection = fe.getRequestsByConnection();
        Assert.assertEquals("both loads should have gone over one connection: " + requestsByConnection,
                1, requestsByConnection.size());
        Assert.assertEquals(2, requestsByConnection.get(0).size());
    }

    @Test(timeout = 60_000)
    public void testPoolLetsEveryIoThreadLoadAtOnce() throws Exception {
        int ioThreads = 4;
        TransactionStreamLoader loader = startedLoader(
                properties(server.getBaseUrl(), true).ioThreadCount(ioThreads).build());
        Assert.assertEquals(ioThreads + 2, loader.getKeepAliveClient().getMaxConnectionsPerHost());
        server.setTxnLoadDelayMs(500);
        server.resetCounters();

        String label = "label-parallel";
        Assert.assertTrue(loader.beginTransaction(label, DB));
        List<LoadRegion> regions = new ArrayList<>();
        List<Future<StreamLoadResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 2 * ioThreads; i++) {
            LoadRegion region = new LoadRegion(label);
            regions.add(region);
            futures.add(loader.send(region));
        }
        for (Future<StreamLoadResponse> future : futures) {
            future.get(60, TimeUnit.SECONDS);
        }
        for (LoadRegion region : regions) {
            region.assertCompleted();
        }

        // HttpClient allows 2 connections per host by default, which would let only 2 loads run at once
        Assert.assertEquals(ioThreads, server.getMaxTxnLoadConcurrency());
        Assert.assertTrue("connections: " + server.getConnectionCount(),
                server.getConnectionCount() <= ioThreads + 2);
        Assert.assertEquals(0, loader.getKeepAliveClient().getTotalStats().getLeased());
    }

    @Test(timeout = 60_000)
    public void testIdleConnectionIsClosedAfterTheIdleTimeout() throws Exception {
        TransactionStreamLoader loader = startedLoader(
                properties(server.getBaseUrl(), true).httpKeepAliveIdleTimeoutMs(300).build());
        server.resetCounters();
        Assert.assertTrue(loader.beginTransaction("label-idle-1", DB));
        KeepAliveHttpClient client = loader.getKeepAliveClient();
        Assert.assertEquals(1, client.getTotalStats().getAvailable());

        waitUntil(() -> client.getTotalStats().getAvailable() == 0, 10_000);

        Assert.assertTrue(loader.beginTransaction("label-idle-2", DB));
        Assert.assertEquals("the second begin should open a new connection", 2, server.getConnectionCount());
    }

    @Test(timeout = 60_000)
    public void testConnectionClosedByTheServerWhileIdleIsNotUsed() throws Exception {
        // The server closes every connection after its response, without saying so in the response, so
        // the pool keeps the connection. Sent on it, a begin would fail: a POST that has been sent is not
        // retried. The check before reusing an idle connection finds that it was closed.
        RawHttpServer fe = RawHttpServer.closingAfterEachResponse();
        closeables.add(fe);
        TransactionStreamLoader loader = startedLoader(properties(fe.getBaseUrl(), true).build());

        Assert.assertTrue(loader.beginTransaction("label-closed-1", DB));
        Thread.sleep(KeepAliveHttpClient.VALIDATE_AFTER_INACTIVITY_MS + 500L);
        Assert.assertTrue(loader.beginTransaction("label-closed-2", DB));

        Assert.assertEquals(2, fe.getRequestsByConnection().size());
    }

    @Test(timeout = 60_000)
    public void testRequestsAfterCloseUseAClientOfTheirOwn() {
        TransactionStreamLoader loader = startedLoader(properties(server.getBaseUrl(), true).build());
        Assert.assertTrue(loader.beginTransaction("label-close", DB));
        loader.close();
        Assert.assertNull(loader.getKeepAliveClient());

        // e.g. a rollback that a failing sink sends after closing the loader
        Assert.assertTrue(loader.rollback(new StreamLoadSnapshot.Transaction(DB, null, "label-close", true)));
    }

    @Test(timeout = 60_000)
    public void testLostCommitResponseIsStillReconciled() {
        TransactionStreamLoader loader = startedLoader(properties(server.getBaseUrl(), true).build());
        String label = "label-lost-commit";
        // A non-200 from an intermediary, while the transaction did commit
        MockedStarRocksHttpServer.ResponseOverride lost = new MockedStarRocksHttpServer.ResponseOverride();
        lost.httpCode = 500;
        lost.status = StreamLoadConstants.RESULT_STATUS_FAILED;
        server.setCommitOverride(lost);
        server.setLabelState(DB, TABLE, label, TransactionStatus.VISIBLE);

        Assert.assertTrue(loader.commit(new StreamLoadSnapshot.Transaction(DB, TABLE, label, true)));
        Assert.assertEquals("the connection of the failed response should be closed, not leaked",
                0, loader.getKeepAliveClient().getTotalStats().getLeased());
    }

    @Test(timeout = 60_000)
    public void testMultiTableTransactionsShareConnections() throws Exception {
        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(server.getBaseUrl())
                .username(USER)
                .password(PASS)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .labelPrefix("test-keep-alive-mtxn-")
                .defaultTableProperties(StreamLoadTableProperties.builder()
                        .database(DB)
                        .table("orders")
                        .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                        .maxBufferRows(100000)
                        .build())
                .expectDelayTime(100)
                .scanningFrequency(50)
                .ioThreadCount(IO_THREADS)
                .enableHttpKeepAlive(true)
                .build();
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();
        try {
            for (int txn = 0; txn < 3; txn++) {
                manager.write(0, DB, "orders", "{\"order_id\":" + txn + "}");
                manager.write(0, DB, "order_items", "{\"item_id\":" + txn + ", \"order_id\":" + txn + "}");
                manager.setCommitAllowed(0, true);
                manager.flush();
                Assert.assertNull("no exception expected", manager.getException());
            }
            Assert.assertTrue("expected a commit", server.getCommitCount() >= 1);
            Assert.assertTrue("expected loads of both tables", server.getLoadCount() >= 2);
            Assert.assertTrue("connections: " + server.getConnectionCount(),
                    server.getConnectionCount() <= DefaultStreamLoader.maxConnectionsPerHost(properties));
        } finally {
            manager.close();
        }
    }

    private static HttpEntity jsonRow() {
        return new ByteArrayEntity("{\"order_id\":1}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                Assert.fail("condition not met within " + timeoutMs + " ms");
            }
            Thread.sleep(20);
        }
    }

    /** A region with one JSON row to load under {@code label}, which records how its load ended. */
    private static final class LoadRegion implements TableRegion {
        private final String label;
        private final AtomicReference<StreamLoadResponse> completed = new AtomicReference<>();
        private final AtomicReference<Throwable> failed = new AtomicReference<>();

        LoadRegion(String label) {
            this.label = label;
        }

        void assertCompleted() {
            Throwable failure = failed.get();
            if (failure != null) {
                throw new AssertionError("the load failed", failure);
            }
            Assert.assertNotNull("the load did not complete", completed.get());
        }

        @Override public StreamLoadTableProperties getProperties() { return null; }
        @Override public Map<String, String> getHeaders() {
            Map<String, String> headers = new HashMap<>();
            headers.put("db", DB);
            headers.put("table", TABLE);
            return headers;
        }
        @Override public String getUniqueKey() { return "P0-" + DB + "." + TABLE; }
        @Override public String getDatabase() { return DB; }
        @Override public String getTable() { return TABLE; }
        @Override public LabelGenerator getLabelGenerator() {
            throw new AssertionError("a region that carries a label must not mint another one");
        }
        @Override public void setLabel(String label) {
            throw new AssertionError("the label of the region must not change");
        }
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
        @Override public void fail(Throwable e) { failed.set(e); }
        @Override public void complete(StreamLoadResponse response) { completed.set(response); }
        @Override public void setResult(Future<?> result) {}
        @Override public Future<?> getResult() { return null; }
        @Override public boolean isReadable() { return false; }
        @Override public boolean isFlushing() { return true; }
        @Override public HttpEntity getHttpEntity() { return jsonRow(); }
    }

    /**
     * A bare HTTP/1.1 server for behavior the JDK server does not let a test control. It answers each
     * request as soon as it has read the request headers, without reading a body, and records the
     * request line of every request, per connection.
     */
    private static final class RawHttpServer implements Closeable {

        private final ServerSocket serverSocket;
        /** The full response to a request, from its target (path and query). */
        private final Function<String, String> responder;
        private final boolean closeAfterResponse;
        private final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "raw-http-server");
            thread.setDaemon(true);
            return thread;
        });
        private final List<List<String>> requestsByConnection = new CopyOnWriteArrayList<>();

        /**
         * Redirects every request to {@code target} with a 307 and keeps the connection open, as the FE
         * does for a transaction load.
         */
        static RawHttpServer redirectingTo(String target) throws IOException {
            return new RawHttpServer(path -> "HTTP/1.1 307 Temporary Redirect\r\n"
                    + "Location: " + target + path + "\r\n"
                    + "Content-Length: 0\r\n"
                    + "Connection: keep-alive\r\n"
                    + "\r\n", false);
        }

        /** Answers every request with {@code Status: OK} and then closes the connection without saying so. */
        static RawHttpServer closingAfterEachResponse() throws IOException {
            String body = "{\"Status\":\"OK\",\"Message\":\"\"}";
            return new RawHttpServer(path -> "HTTP/1.1 200 OK\r\n"
                    + "Content-Type: application/json\r\n"
                    + "Content-Length: " + body.length() + "\r\n"
                    + "\r\n"
                    + body, true);
        }

        private RawHttpServer(Function<String, String> responder, boolean closeAfterResponse) throws IOException {
            this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            this.responder = responder;
            this.closeAfterResponse = closeAfterResponse;
            executor.execute(this::acceptConnections);
        }

        String getBaseUrl() {
            return "http://127.0.0.1:" + serverSocket.getLocalPort();
        }

        /** The request lines that arrived on each connection, in the order the connections were accepted. */
        List<List<String>> getRequestsByConnection() {
            List<List<String>> copy = new ArrayList<>();
            for (List<String> requests : requestsByConnection) {
                synchronized (requests) {
                    copy.add(new ArrayList<>(requests));
                }
            }
            return copy;
        }

        private void acceptConnections() {
            while (!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    List<String> requests = Collections.synchronizedList(new ArrayList<>());
                    requestsByConnection.add(requests);
                    executor.execute(() -> serve(socket, requests));
                } catch (IOException e) {
                    return;
                }
            }
        }

        private void serve(Socket socket, List<String> requests) {
            try (Socket connection = socket;
                 BufferedReader in = new BufferedReader(
                         new InputStreamReader(connection.getInputStream(), StandardCharsets.ISO_8859_1))) {
                OutputStream out = connection.getOutputStream();
                String requestLine;
                while ((requestLine = in.readLine()) != null) {
                    requests.add(requestLine);
                    String header;
                    do {
                        header = in.readLine();
                    } while (header != null && !header.isEmpty());
                    String[] parts = requestLine.split(" ");
                    if (parts.length < 2) {
                        return;
                    }
                    out.write(responder.apply(parts[1]).getBytes(StandardCharsets.ISO_8859_1));
                    out.flush();
                    if (closeAfterResponse) {
                        return;
                    }
                }
            } catch (IOException e) {
                // the client closed the connection
            }
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
            executor.shutdownNow();
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
