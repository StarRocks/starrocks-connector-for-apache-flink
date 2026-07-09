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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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

    @Test
    public void testArrowWriteFlushesImmediately() {
        StreamLoadTableProperties properties = StreamLoadTableProperties.builder()
                .database("db")
                .table("tbl")
                .streamLoadDataFormat(StreamLoadDataFormat.ARROW)
                .chunkLimit(1024 * 1024)
                .build();
        Table table = new Table(
                properties.getDatabase(),
                properties.getTable(),
                manager,
                loader,
                properties,
                3,
                100,
                30000,
                1024 * 1024,
                10
        );

        byte[] payload1 = new byte[]{1, 2, 3};
        byte[] payload2 = new byte[]{4, 5, 6};

        table.write(payload1);
        table.write(payload2);

        // Verify that 2 writes resulted in 2 separate HTTP sendLoad requests (one per payload)
        assertEquals(2, sendLoadSpy.size());
        assertEquals(1, sendLoadSpy.capturedRequests.get(0).loadRequest.getChunk().numRows());
        assertEquals(1, sendLoadSpy.capturedRequests.get(1).loadRequest.getChunk().numRows());
    }
}
