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

package com.starrocks.data.load.stream.mergecommit;

import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TableTest {

    // ==================== Mock classes for basic tests ====================

    /**
     * Mock MergeCommitLoader for testing
     */
    private static class MockMergeCommitLoader extends MergeCommitLoader {
        @Override
        public ScheduledFuture<?> scheduleFlush(Table table, long chunkId, int delayMs) {
            return null;
        }

        @Override
        public void sendLoad(LoadRequest.RequestRun requestRun, int delayMs) {
            // no-op
        }

        @Override
        public void start(StreamLoadProperties properties, StreamLoadManager manager) {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /**
     * Mock MergeCommitManager for testing
     */
    private static class MockMergeCommitManager extends MergeCommitManager {
        public MockMergeCommitManager() {
            super(createMinimalProperties());
        }

        private static StreamLoadProperties createMinimalProperties() {
            return StreamLoadProperties.builder()
                    .loadUrls("http://localhost:8030")
                    .username("root")
                    .password("")
                    .defaultTableProperties(StreamLoadTableProperties.builder()
                            .database("db").table("tbl").build())
                    .build();
        }

        @Override
        public void onLoadStart(Table table, long dataSize, int numRows) {
            // no-op
        }

        @Override
        public void onLoadSuccess(Table table, LoadRequest request) {
            // no-op
        }

        @Override
        public void onLoadFailure(Table table, LoadRequest request, Throwable throwable) {
            // no-op
        }

        @Override
        public void releaseCache(LoadRequest request) {
            // no-op
        }
    }

    // ==================== Mockito-based test infrastructure ====================

    private MergeCommitManager manager;
    private MergeCommitLoader loader;
    private SendLoadSpy sendLoadSpy;

    @FunctionalInterface
    private interface SendLoadObserver {
        void onSend(LoadRequest.RequestRun requestRun, int count);
    }

    private static class SendLoadSpy {
        private final List<LoadRequest.RequestRun> capturedRequests =
                Collections.synchronizedList(new ArrayList<>());
        private volatile SendLoadObserver observer = (requestRun, count) -> {};

        public void setObserver(SendLoadObserver observer) {
            this.observer = observer == null ? (requestRun, count) -> {} : observer;
        }

        public void install(MergeCommitLoader loader) {
            doAnswer(invocation -> {
                LoadRequest.RequestRun requestRun = invocation.getArgument(0);
                int count;
                synchronized (capturedRequests) {
                    capturedRequests.add(requestRun);
                    count = capturedRequests.size();
                }
                observer.onSend(requestRun, count);
                return null;
            }).when(loader).sendLoad(any(), anyInt());
        }

        public int size() {
            synchronized (capturedRequests) {
                return capturedRequests.size();
            }
        }

        public LoadRequest.RequestRun get(int index) {
            synchronized (capturedRequests) {
                return capturedRequests.get(index);
            }
        }
    }

    @Before
    public void setUp() {
        manager = mock(MergeCommitManager.class);
        loader = mock(MergeCommitLoader.class);
        sendLoadSpy = new SendLoadSpy();

        doNothing().when(manager).onLoadStart(any(), anyLong(), anyInt());
        doNothing().when(manager).releaseCache(any());
        doNothing().when(manager).onLoadSuccess(any(), any());

        doReturn(mock(ScheduledFuture.class))
                .when(loader).scheduleFlush(any(), anyLong(), anyInt());

        sendLoadSpy.install(loader);
    }

    // ==================== Helper methods ====================

    private Map<String, String> createMap(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private Table createTable(StreamLoadTableProperties properties) {
        return new Table(
                properties.getDatabase(),
                properties.getTable(),
                new MockMergeCommitManager(),
                new MockMergeCommitLoader(),
                properties,
                3,      // maxRetries
                1000,   // retryIntervalInMs
                5000,   // flushIntervalMs
                1024 * 1024, // chunkSize
                10      // maxInflightRequests
        );
    }

    private Table createTableWithMockito(int maxConcurrentRequests) {
        StreamLoadTableProperties props = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_tbl")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .chunkLimit(100)
                .build();
        return new Table(
                "test_db", "test_tbl", manager, loader, props,
                0,      // maxRetries
                0,      // retryIntervalInMs
                10000,  // flushIntervalMs
                100,    // chunkSize
                maxConcurrentRequests);
    }

    private void completeRequest(LoadRequest.RequestRun requestRun) {
        requestRun.loadResult = new StreamLoadResponse();
        requestRun.loadRequest.getTable().loadFinish(requestRun, null);
    }

    private void writeDataToTriggerFlush(Table table) {
        // Write enough data to trigger chunk flush (chunkSize = 100)
        byte[] data = new byte[150];
        table.write(data);
    }

    private void triggerFlushes(Table table, int times) {
        for (int i = 0; i < times; i++) {
            writeDataToTriggerFlush(table);
        }
    }

    private Thread startWriteThread(Table table, CountDownLatch startedLatch) {
        Thread t = new Thread(() -> {
            startedLatch.countDown();
            writeDataToTriggerFlush(table);
        });
        t.start();
        return t;
    }

    /**
     * Wait until there is a waiter on the flushCondition, indicating a thread is blocked.
     */
    private void waitForWaiterOnFlushCondition(Table table, long timeoutMs) throws Exception {
        ReentrantLock lock = table.getFlushLockForTest();

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            lock.lock();
            try {
                if (lock.hasWaiters(table.getFlushConditionForTest())) {
                    return;
                }
            } finally {
                lock.unlock();
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Timeout waiting for waiter on flushCondition");
    }

    // ==================== Basic property tests ====================

    @Test
    public void testGetLoadTimeoutMsWithDefaultTimeout() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        Table table = createTable(properties);

        // Default timeout is 600 seconds = 600000 ms
        assertEquals(LoadParameters.DEFAULT_TIMEOUT_SECONDS * 1000, table.getLoadTimeoutMs());
    }

    @Test
    public void testGetLoadTimeoutMsWithCustomTimeout() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addProperty("timeout", "300")
                .build();

        Table table = createTable(properties);

        // Custom timeout is 300 seconds = 300000 ms
        assertEquals(300 * 1000, table.getLoadTimeoutMs());
    }

    @Test
    public void testGetLoadTimeoutMsWithTimeoutInCommonProperties() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addCommonProperties(createMap("timeout", "120"))
                .build();

        Table table = createTable(properties);

        // Timeout from common properties is 120 seconds = 120000 ms
        assertEquals(120 * 1000, table.getLoadTimeoutMs());
    }

    @Test
    public void testGetLoadParameters() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addProperty("key1", "value1")
                .build();

        Table table = createTable(properties);

        Map<String, String> params = table.getLoadParameters();
        assertEquals("value1", params.get("key1"));
        assertEquals("db", params.get("db"));
        assertEquals("table", params.get("table"));
    }

    @Test
    public void testIsMergeCommitAsyncDefault() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        Table table = createTable(properties);

        // Default should be true
        assertTrue(table.isMergeCommitAsync());
    }

    @Test
    public void testIsMergeCommitAsyncTrue() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addProperty("merge_commit_async", "true")
                .build();

        Table table = createTable(properties);

        assertTrue(table.isMergeCommitAsync());
    }

    @Test
    public void testGetDatabaseAndTable() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        Table table = createTable(properties);

        assertEquals("test_db", table.getDatabase());
        assertEquals("test_table", table.getTable());
    }

    @Test
    public void testGetProperties() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("table")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        Table table = createTable(properties);

        assertEquals(properties, table.getProperties());
    }

    // ==================== Concurrent request tests ====================

    /**
     * Test: maxConcurrentRequests = -1 disables rate limiting
     * Expected: All requests are sent immediately without waiting
     */
    @Test
    public void testMaxConcurrentRequestsDisabled() {
        Table table = createTableWithMockito(-1);

        // Trigger multiple flushes
        triggerFlushes(table, 3);

        // All 3 requests should be sent immediately
        assertEquals(3, sendLoadSpy.size());

        verify(loader, times(3)).sendLoad(any(), anyInt());
    }

    /**
     * Test: maxConcurrentRequests = 0 serializes requests
     * Expected: Second request blocks until first completes
     */
    @Test
    public void testMaxConcurrentRequestsZero() throws Exception {
        Table table = createTableWithMockito(0);

        CountDownLatch firstRequestSent = new CountDownLatch(1);
        CountDownLatch secondWriteStarted = new CountDownLatch(1);
        CountDownLatch secondRequestSent = new CountDownLatch(1);
        AtomicReference<LoadRequest.RequestRun> firstRequest = new AtomicReference<>();

        sendLoadSpy.setObserver((requestRun, count) -> {
            if (count == 1) {
                firstRequest.set(requestRun);
                firstRequestSent.countDown();
            } else if (count == 2) {
                secondRequestSent.countDown();
            }
        });

        // First write triggers first request
        writeDataToTriggerFlush(table);
        assertTrue(firstRequestSent.await(60, TimeUnit.SECONDS));

        // Second write in separate thread (should block)
        Thread secondWriteThread = startWriteThread(table, secondWriteStarted);
        assertTrue(secondWriteStarted.await(60, TimeUnit.SECONDS));

        // Wait until the second write thread is blocked on flushCondition
        waitForWaiterOnFlushCondition(table, 60000);

        // Verify only 1 request sent so far
        assertEquals(1, sendLoadSpy.size());

        // Complete first request
        completeRequest(firstRequest.get());

        // Second request should now be sent
        assertTrue(secondRequestSent.await(1, TimeUnit.SECONDS));

        assertEquals(2, sendLoadSpy.size());

        secondWriteThread.join(1000);
    }

    /**
     * Test: maxConcurrentRequests = 2 limits concurrent requests
     * New semantics: inflightLoadRequests.size() cannot exceed maxConcurrentRequests
     * So maxConcurrentRequests=2 allows up to 2 inflight requests,
     * and blocks when trying to add the 3rd (size=2 >= 2)
     */
    @Test
    public void testMaxConcurrentRequestsPositive() throws Exception {
        Table table = createTableWithMockito(2);

        CountDownLatch twoRequestsSent = new CountDownLatch(2);
        CountDownLatch thirdWriteStarted = new CountDownLatch(1);
        CountDownLatch thirdRequestSent = new CountDownLatch(1);
        AtomicReference<LoadRequest.RequestRun> firstRequest = new AtomicReference<>();

        sendLoadSpy.setObserver((requestRun, count) -> {
            if (count == 1) {
                firstRequest.set(requestRun);
            }
            if (count <= 2) {
                twoRequestsSent.countDown();
            } else if (count == 3) {
                thirdRequestSent.countDown();
            }
        });

        // First two writes - should pass (size < maxConcurrentRequests)
        writeDataToTriggerFlush(table);
        writeDataToTriggerFlush(table);
        assertTrue(twoRequestsSent.await(60, TimeUnit.SECONDS));

        // Third write in separate thread (should block since size=2 >= 2)
        Thread thirdWriteThread = startWriteThread(table, thirdWriteStarted);
        assertTrue(thirdWriteStarted.await(1, TimeUnit.SECONDS));

        // Wait until the third write thread is blocked on flushCondition
        waitForWaiterOnFlushCondition(table, 60000);

        // Verify only 2 requests sent so far
        assertEquals(2, sendLoadSpy.size());

        // Complete first request (size becomes 1, which is < 2, so unblocks)
        completeRequest(firstRequest.get());

        // Third request should now be sent
        assertTrue(thirdRequestSent.await(60, TimeUnit.SECONDS));

        assertEquals(3, sendLoadSpy.size());

        thirdWriteThread.join(1000);
    }

    /**
     * Regression test: flush(true) should wait until inflight requests finish.
     *
     * Before the fix, flush(true) called waitInflightRequests(0). With the new {@code >=} semantics,
     * that condition never becomes false (size is never < 0), causing flush(true) to wait until timeout.
     */
    @Test
    public void testFlushWaitsUntilInflightFinished() throws Exception {
        Table table = createTableWithMockito(2);

        // Trigger 1 inflight request.
        writeDataToTriggerFlush(table);
        assertEquals(1, sendLoadSpy.size());
        LoadRequest.RequestRun requestRun = sendLoadSpy.get(0);

        CountDownLatch flushThreadStarted = new CountDownLatch(1);
        CountDownLatch flushThreadFinished = new CountDownLatch(1);
        AtomicReference<Throwable> flushThrowable = new AtomicReference<>();
        AtomicBoolean flushReturned = new AtomicBoolean(false);

        Thread flushThread = new Thread(() -> {
            flushThreadStarted.countDown();
            try {
                table.flush(true);
                flushReturned.set(true);
            } catch (Throwable t) {
                flushThrowable.set(t);
            } finally {
                flushThreadFinished.countDown();
            }
        });
        flushThread.start();
        assertTrue(flushThreadStarted.await(10, TimeUnit.SECONDS));

        // Ensure flush thread is actually waiting on the condition.
        waitForWaiterOnFlushCondition(table, 60000);
        assertTrue(flushThread.isAlive());

        // Complete inflight request; flush(true) should return.
        completeRequest(requestRun);

        if (!flushThreadFinished.await(10, TimeUnit.SECONDS)) {
            // Avoid hanging the test suite even if regression happens again.
            flushThread.interrupt();
            flushThread.join(5000);
            throw new AssertionError("flush(true) didn't return after inflight finished");
        }

        assertTrue(flushReturned.get());
        assertNull(flushThrowable.get());
    }

    // ==================== Flush timer tests ====================

    /**
     * Runs the flush timers on a real executor, as {@link MergeCommitHttpLoader} does.
     * The loader mocked in {@link #setUp()} never runs them.
     */
    private static class FlushTimers {
        // One latch per scheduled timer, in the order the chunks are created
        private final List<CountDownLatch> timerRuns = Collections.synchronizedList(new ArrayList<>());

        public void install(MergeCommitLoader loader, ScheduledExecutorService executor) {
            doAnswer(invocation -> {
                Table table = invocation.getArgument(0);
                long chunkId = invocation.getArgument(1);
                int delayMs = invocation.getArgument(2);
                CountDownLatch timerRun = new CountDownLatch(1);
                timerRuns.add(timerRun);
                return executor.schedule(() -> {
                    try {
                        table.checkFlushInterval(chunkId);
                    } finally {
                        timerRun.countDown();
                    }
                }, delayMs, TimeUnit.MILLISECONDS);
            }).when(loader).scheduleFlush(any(), anyLong(), anyInt());
        }

        // Waits until the timer of the chunk with the given index (in creation order) has run
        public void awaitRun(int chunkIndex) throws InterruptedException {
            assertTrue("Timer of chunk " + chunkIndex + " did not run",
                    timerRuns.get(chunkIndex).await(60, TimeUnit.SECONDS));
        }
    }

    private Table createSerialTable(String tableName, int flushIntervalMs) {
        StreamLoadTableProperties props = StreamLoadTableProperties.builder()
                .database("test_db")
                .table(tableName)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                // bound the wait for a request slot if a regression blocks it
                .addProperty("timeout", "30")
                .build();
        return new Table(
                "test_db", tableName, manager, loader, props,
                0,      // maxRetries
                0,      // retryIntervalInMs
                flushIntervalMs,
                Long.MAX_VALUE, // chunkSize, so only the timer, flush() and cacheEvict() switch chunks
                0);     // maxConcurrentRequests, serial loading
    }

    private static void waitForWaitersOnFlushCondition(Table table, int numWaiters) throws InterruptedException {
        ReentrantLock lock = table.getFlushLockForTest();
        waitUntil(() -> {
            lock.lock();
            try {
                return lock.getWaitQueueLength(table.getFlushConditionForTest()) >= numWaiters;
            } finally {
                lock.unlock();
            }
        }, "Timeout waiting for " + numWaiters + " waiters on flushCondition");
    }

    private static byte[] row(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    // Must be called before the request completes, which releases the rows
    private static List<String> rowsOf(LoadRequest.RequestRun requestRun) {
        List<String> rows = new ArrayList<>();
        for (byte[] row : requestRun.loadRequest.getChunk().rows()) {
            rows.add(new String(row, StandardCharsets.UTF_8));
        }
        return rows;
    }

    private int numSentRows(Table table) {
        int numRows = 0;
        for (int i = 0; i < sendLoadSpy.size(); i++) {
            LoadRequest loadRequest = sendLoadSpy.get(i).loadRequest;
            if (loadRequest.getTable() == table) {
                numRows += loadRequest.getChunk().numRows();
            }
        }
        return numRows;
    }

    private static void waitUntil(BooleanSupplier condition, String message) throws InterruptedException {
        long deadlineMs = System.currentTimeMillis() + 60000;
        while (!condition.getAsBoolean()) {
            assertTrue(message, System.currentTimeMillis() < deadlineMs);
            Thread.sleep(10);
        }
    }

    // Runs flush(true) in another thread, so that a regression fails the test instead of hanging it
    private static void flushWithTimeout(Table table, long timeoutMs) throws Exception {
        AtomicReference<Throwable> flushThrowable = new AtomicReference<>();
        Thread flushThread = new Thread(() -> {
            try {
                table.flush(true);
            } catch (Throwable t) {
                flushThrowable.set(t);
            }
        });
        flushThread.setDaemon(true);
        flushThread.start();
        flushThread.join(timeoutMs);
        if (flushThread.isAlive()) {
            flushThread.interrupt();
            throw new AssertionError("flush(true) did not return within " + timeoutMs + " ms");
        }
        if (flushThrowable.get() != null) {
            throw new AssertionError("flush(true) failed", flushThrowable.get());
        }
    }

    /**
     * Regression test: with max-concurrent-requests set, a flush timer that fires while no request
     * slot is free must not lose the chunk.
     *
     * Before the fix, the timer switched the chunk out and cancelled itself with interruption, so
     * its wait for a request slot threw at once. The chunk was then neither active nor inflight, the
     * exception went into the timer's future, and the next flush succeeded without the chunk.
     */
    @Test
    public void testDelayedFlushWhileRequestInflightKeepsChunk() throws Exception {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        try {
            FlushTimers timers = new FlushTimers();
            timers.install(loader, executor);
            Table table = createSerialTable("test_tbl", 100);

            // The timer flushes row1, whose request takes the only slot
            table.write(row("row1"));
            timers.awaitRun(0);
            assertEquals(1, sendLoadSpy.size());

            // The timer of row2 fires while the slot is taken
            table.write(row("row2"));
            timers.awaitRun(1);
            assertEquals(1, sendLoadSpy.size());

            // Freeing the slot flushes row2
            completeRequest(sendLoadSpy.get(0));
            assertEquals(2, sendLoadSpy.size());
            assertEquals(Collections.singletonList("row2"), rowsOf(sendLoadSpy.get(1)));
            completeRequest(sendLoadSpy.get(1));

            flushWithTimeout(table, 10000);
            assertEquals(2, sendLoadSpy.size());
            // Every chunk released its cache
            verify(manager, times(2)).releaseCache(any());
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Regression test: flush timers must not wait for a request slot. The loader's executor runs both
     * the timers and the completion of requests, so timers waiting on as many tables as executor
     * threads would keep any request from completing until the load timeout.
     */
    @Test
    public void testDelayedFlushDoesNotBlockSharedExecutor() throws Exception {
        // As in MergeCommitHttpLoader, timers and request completions share the executor threads
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        try {
            new FlushTimers().install(loader, executor);
            sendLoadSpy.setObserver((requestRun, count) ->
                    executor.schedule(() -> completeRequest(requestRun), 100, TimeUnit.MILLISECONDS));
            List<Table> tables = Arrays.asList(createSerialTable("tbl1", 20), createSerialTable("tbl2", 20));

            int numRows = 0;
            long endTimeMs = System.currentTimeMillis() + 1000;
            while (System.currentTimeMillis() < endTimeMs) {
                for (Table table : tables) {
                    table.write(row("row" + numRows));
                }
                numRows++;
                Thread.sleep(5);
            }
            for (Table table : tables) {
                flushWithTimeout(table, 10000);
            }

            for (Table table : tables) {
                assertEquals(numRows, numSentRows(table));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * A delayed flush must not overtake an earlier chunk that waits for a request slot, otherwise
     * a serial table (max-concurrent-requests = 0) loads the chunks out of order.
     */
    @Test
    public void testDelayedFlushDoesNotOvertakeWaitingChunk() throws Exception {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        Thread evictThread = null;
        try {
            FlushTimers timers = new FlushTimers();
            timers.install(loader, executor);
            Table table = createSerialTable("test_tbl", 500);

            // row1 takes the only request slot
            table.write(row("row1"));
            table.flush(false);
            assertEquals(1, sendLoadSpy.size());

            // Cache eviction switches out row2, which waits for the slot
            table.write(row("row2"));
            evictThread = new Thread(table::cacheEvict);
            evictThread.setDaemon(true);
            evictThread.start();
            waitForWaiterOnFlushCondition(table, 60000);

            // The timer of row3 fires while row2 is waiting
            table.write(row("row3"));
            timers.awaitRun(2);
            assertEquals(1, sendLoadSpy.size());

            // The slot goes to row2 first
            completeRequest(sendLoadSpy.get(0));
            waitUntil(() -> sendLoadSpy.size() >= 2, "The next chunk was not sent");
            assertEquals(Collections.singletonList("row2"), rowsOf(sendLoadSpy.get(1)));
            evictThread.join(60000);
            assertFalse(evictThread.isAlive());

            // Then to row3
            completeRequest(sendLoadSpy.get(1));
            assertEquals(3, sendLoadSpy.size());
            assertEquals(Collections.singletonList("row3"), rowsOf(sendLoadSpy.get(2)));
            completeRequest(sendLoadSpy.get(2));
            flushWithTimeout(table, 10000);
        } finally {
            executor.shutdownNow();
            if (evictThread != null) {
                evictThread.interrupt();
            }
        }
    }

    /**
     * A switched chunk that fails to flush is neither active nor inflight, so the table must fail
     * instead of losing the chunk silently, and the next flush must throw.
     */
    @Test
    public void testFailedFlushOfSwitchedChunkFailsTable() throws Exception {
        Table table = createTableWithMockito(0);
        writeDataToTriggerFlush(table);
        assertEquals(1, sendLoadSpy.size());

        // The next full chunk waits for the request slot, and the wait is interrupted
        AtomicReference<Throwable> writeThrowable = new AtomicReference<>();
        Thread writeThread = new Thread(() -> {
            try {
                writeDataToTriggerFlush(table);
            } catch (Throwable t) {
                writeThrowable.set(t);
            }
        });
        writeThread.start();
        waitForWaiterOnFlushCondition(table, 60000);
        writeThread.interrupt();
        writeThread.join(60000);
        assertNotNull(writeThrowable.get());

        completeRequest(sendLoadSpy.get(0));
        try {
            table.flush(true);
            fail("flush should fail after a switched chunk failed to flush");
        } catch (RuntimeException e) {
            // expected
        }
        assertEquals(1, sendLoadSpy.size());
    }

    /**
     * Regression test: flush(true) must also wait for a chunk that another thread switched out and that
     * still waits for a request slot. Otherwise the checkpoint completes before the chunk is loaded.
     */
    @Test
    public void testFlushWaitsForSwitchedChunkWaitingForSlot() throws Exception {
        // the flush timers never run, see setUp()
        Table table = createSerialTable("test_tbl", 100);

        // row1 takes the only request slot
        table.write(row("row1"));
        table.flush(false);
        assertEquals(1, sendLoadSpy.size());

        // flush(true) waits for row1
        CountDownLatch flushReturned = new CountDownLatch(1);
        AtomicReference<Throwable> flushThrowable = new AtomicReference<>();
        Thread flushThread = new Thread(() -> {
            try {
                table.flush(true);
            } catch (Throwable t) {
                flushThrowable.set(t);
            } finally {
                flushReturned.countDown();
            }
        });
        flushThread.setDaemon(true);
        flushThread.start();
        waitForWaitersOnFlushCondition(table, 1);

        // Meanwhile, cache eviction switches out row2, which waits for the slot
        table.write(row("row2"));
        Thread evictThread = new Thread(table::cacheEvict);
        evictThread.setDaemon(true);
        evictThread.start();
        waitForWaitersOnFlushCondition(table, 2);

        // Completing row1 wakes up flush(true) first, and then hands the slot to row2
        completeRequest(sendLoadSpy.get(0));
        waitUntil(() -> sendLoadSpy.size() >= 2, "row2 was not sent");
        assertFalse("flush(true) returned before row2 was loaded", flushReturned.await(500, TimeUnit.MILLISECONDS));

        completeRequest(sendLoadSpy.get(1));
        assertTrue("flush(true) did not return", flushReturned.await(10, TimeUnit.SECONDS));
        assertNull(flushThrowable.get());
    }
}
