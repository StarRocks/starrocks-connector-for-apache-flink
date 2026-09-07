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

import com.starrocks.data.load.stream.Chunk;
import com.starrocks.data.load.stream.compress.CompressionCodec;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.starrocks.data.load.stream.mergecommit.LoadParameters.DEFAULT_TIMEOUT_SECONDS;
import static com.starrocks.data.load.stream.mergecommit.LoadParameters.TIMEOUT;

public class Table {

    private static final Logger LOG = LoggerFactory.getLogger(Table.class);

    enum FlushChunkReason {
        CHUNK_FULL,
        CHUNK_DELAY,
        CACHE_EVICT,
        FLUSH
    }

    private final String database;
    private final String table;
    private final MergeCommitManager manager;
    protected final MergeCommitLoader mergeCommitLoader;
    private final StreamLoadTableProperties properties;
    private final int maxRetries;
    private final int retryIntervalInMs;
    private final int flushIntervalMs;
    private final long chunkSize;
    private final int maxConcurrentRequests;
    private final Optional<CompressionCodec> compressionCodec;
    private final AtomicLong chunkIdGenerator;
    private volatile Chunk activeChunk;
    // chunk id -> request
    private final Map<Long, LoadRequest> inflightLoadRequests;
    private final AtomicLong cacheBytes = new AtomicLong();
    private final AtomicLong cacheRows = new AtomicLong();
    private final ReentrantLock lock;
    private final Condition flushCondition;
    private volatile ScheduledFuture<?> timer;
    private final AtomicReference<Throwable> tableThrowable;
    private final Map<String, String> loadParameters;
    private final boolean mergeCommitAsync;
    private final int loadTimeoutMs;

    public Table(
            String database,
            String table,
            MergeCommitManager manager,
            MergeCommitLoader mergeCommitLoader,
            StreamLoadTableProperties properties,
            int maxRetries,
            int retryIntervalInMs,
            int flushIntervalMs,
            long chunkSize,
            int maxConcurrentRequests) {
        this.database = database;
        this.table = table;
        this.manager = manager;
        this.mergeCommitLoader = mergeCommitLoader;
        this.properties = properties;
        this.maxRetries = maxRetries;
        this.retryIntervalInMs = retryIntervalInMs;
        this.flushIntervalMs = flushIntervalMs;
        this.chunkSize = chunkSize;
        this.maxConcurrentRequests = maxConcurrentRequests >= 0 ? Math.max(1, maxConcurrentRequests) : Integer.MAX_VALUE;
        this.compressionCodec = CompressionCodec.createCompressionCodec(
                properties.getDataFormat(),
                properties.getProperty("compression"),
                properties.getTableProperties());
        this.chunkIdGenerator = new AtomicLong();
        this.inflightLoadRequests = new ConcurrentHashMap<>();
        this.lock = new ReentrantLock();
        this.flushCondition = lock.newCondition();
        this.tableThrowable = new AtomicReference<>();
        this.loadParameters = LoadParameters.getParameters(properties);
        this.mergeCommitAsync = Boolean.parseBoolean(
                loadParameters.getOrDefault(LoadParameters.MERGE_COMMIT_ASYNC, "true"));
        this.loadTimeoutMs = Integer.parseInt(
                loadParameters.getOrDefault(TIMEOUT, String.valueOf(DEFAULT_TIMEOUT_SECONDS))) * 1000;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public StreamLoadTableProperties getProperties() {
        return properties;
    }

    public Map<String, String> getLoadParameters() {
        return loadParameters;
    }

    public boolean isMergeCommitAsync() {
        return mergeCommitAsync;
    }

    public int write(byte[] row) {
        lock.lock();
        try {
            Chunk chunk = getActiveChunk();
            chunk.addRow(row);
            cacheBytes.addAndGet(row.length);
            cacheRows.incrementAndGet();
            if (!properties.getDataFormat().supportsBatching()) {
                switchChunk(FlushChunkReason.FLUSH);
            } else {
                switchChunk(FlushChunkReason.CHUNK_FULL);
            }
            return row.length;
        } finally {
            lock.unlock();
        }
    }

    public void flush(boolean wait) throws Exception {
        lock.lock();
        try {
            switchChunk(FlushChunkReason.FLUSH);
            if (!wait) {
                return;
            }
            // wait until all inflight requests finished
            waitInflightRequests(1);
        } finally {
            lock.unlock();
        }
    }

    public void checkFlushInterval(long chunkId) {
        lock.lock();
        try {
            if (activeChunk == null || activeChunk.getChunkId() != chunkId) {
                return;
            }
            switchChunk(FlushChunkReason.CHUNK_DELAY);
        } finally {
            lock.unlock();
        }
    }

    public long cacheEvict() {
        lock.lock();
        try {
            if (activeChunk == null || activeChunk.numRows() == 0) {
                return 0;
            }
            long size = activeChunk.rowBytes();
            switchChunk(FlushChunkReason.CACHE_EVICT);
            return size;
        } finally {
            lock.unlock();
        }
    }

    private Chunk getActiveChunk() {
        if (activeChunk == null) {
            activeChunk = new Chunk(properties.getDataFormat(), chunkIdGenerator.incrementAndGet());
            if (timer != null) {
                timer.cancel(true);
            }
            timer = mergeCommitLoader.scheduleFlush(this, activeChunk.getChunkId(), flushIntervalMs);
        }
        return activeChunk;
    }

    private void switchChunk(FlushChunkReason reason) {
        if (activeChunk == null ||
                (reason == FlushChunkReason.CHUNK_FULL && activeChunk.estimateChunkSize() < chunkSize)) {
            return;
        }

        Chunk inactiveChunk = activeChunk;
        activeChunk = null;
        if (timer != null) {
            timer.cancel(true);
            timer = null;
        }
        if (inactiveChunk.numRows() > 0) {
            flushChunk(inactiveChunk, reason);
        }
    }

    private void flushChunk(Chunk chunk, FlushChunkReason reason) {
        waitInflightRequests(maxConcurrentRequests);
        LoadRequest request = new LoadRequest(this, chunk, maxRetries, retryIntervalInMs);
        inflightLoadRequests.put(chunk.getChunkId(), request);
        manager.onLoadStart(this, chunk.rowBytes(), chunk.numRows());
        LoadRequest.RequestRun requestRun = request.newRun();
        mergeCommitLoader.sendLoad(requestRun, 0);
        LOG.info("Flush chunk, db: {}, table: {}, chunkId: {}, rows: {}, bytes: {}, reason: {}",
                database, table, chunk.getChunkId(), chunk.numRows(), chunk.rowBytes(), reason);
    }

    private void waitInflightRequests(int threshold) {
        final long startTimeMs = System.currentTimeMillis();
        final long startTimeNs = System.nanoTime();
        final long timeoutNs = loadTimeoutMs >= 0
                ? TimeUnit.MILLISECONDS.toNanos(loadTimeoutMs)
                : Long.MAX_VALUE;
        final long awakeTimeoutNs = TimeUnit.SECONDS.toNanos(10);
        while (inflightLoadRequests.size() >= threshold && tableThrowable.get() == null) {
            try {
                long elapsedNs = System.nanoTime() - startTimeNs;
                long remainingNs = timeoutNs - elapsedNs;
                if (remainingNs <= 0) {
                    RuntimeException timeoutEx = new RuntimeException(String.format(
                            "Flush timeout when waiting inflight requests, db: %s, table: %s, " +
                                    "elapsed: %d ms, timeout: %d ms, current inflight requests: %d, target size: %d",
                            database, table, System.currentTimeMillis() - startTimeMs, loadTimeoutMs,
                            inflightLoadRequests.size(), threshold));
                    tableThrowable.compareAndSet(null, timeoutEx);
                    throw timeoutEx;
                }

                long notElapsedNs = flushCondition.awaitNanos(Math.min(awakeTimeoutNs, remainingNs));
                if (notElapsedNs <= 0) {
                    LOG.info("Waiting inflight requests, db: {}, table: {}, current inflight requests: {}," +
                            " target size: {}, elapsed: {} ms, timeout: {} ms, {}", database, table,
                            inflightLoadRequests.size(), threshold, System.currentTimeMillis() - startTimeMs,
                            loadTimeoutMs, loadRequestsSummary());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Fail to wait inflight requests, db: {}, table: {}, current inflight requests: {}," +
                            " target size: {}, elapsed: {} ms, {}", database, table, inflightLoadRequests.size(),
                                System.currentTimeMillis() - startTimeMs, threshold, loadRequestsSummary(), e);
                throw new RuntimeException(String.format("Fail to wait inflight requests, db: %s, table: %s", database, table), e);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                LOG.error("Fail to wait inflight requests, db: {}, table: {}, current inflight requests: {}," +
                                " target size: {}, elapsed: {} ms, {}", database, table, inflightLoadRequests.size(),
                        System.currentTimeMillis() - startTimeMs, threshold, loadRequestsSummary(), e);
                throw new RuntimeException(String.format(
                        "Fail to wait inflight requests, db: %s, table: %s", database, table), e);
            }
        }
        if (tableThrowable.get() != null) {
            LOG.error("Fail to wait inflight requests, db: {}, table: {}, current inflight requests: {}," +
                            " target size: {}, elapsed: {} ms, {}", database, table, inflightLoadRequests.size(),
                    System.currentTimeMillis() - startTimeMs, threshold, loadRequestsSummary(), tableThrowable.get());
            throw new RuntimeException(
                    String.format("Exception happened when waiting inflight requests db: %s, table: %s",
                            database, table), tableThrowable.get());
        }
    }

    private void releaseBuffer(LoadRequest request) {
        if (request.isBufferReleased()) {
            return;
        }
        Chunk chunk = request.getChunk();
        chunk.release();
        cacheBytes.addAndGet(-chunk.rowBytes());
        cacheRows.addAndGet(-chunk.numRows());
        manager.releaseCache(request);
        request.setBufferReleased();
    }

    public void loadSyncFinish(LoadRequest.RequestRun requestRun) {
        if (requestRun.loadRequest.canRetry()) {
            return;
        }
        releaseBuffer(requestRun.loadRequest);
        LOG.debug("Sync load finished, db: {}, table: {}, chunkId: {}, rows: {}, bytes: {}",
                database, table, requestRun.loadRequest.getChunk().getChunkId(),
                requestRun.loadRequest.getChunk().numRows(), requestRun.loadRequest.getChunk().rowBytes());
    }

    public void loadFinish(LoadRequest.RequestRun requestRun, Throwable throwable) {
        requestRun.state = throwable == null ? LoadRequest.State.SUCCESS : LoadRequest.State.FAILED;
        requestRun.finishTime = System.currentTimeMillis();
        requestRun.throwable = throwable;
        LoadRequest request = requestRun.loadRequest;
        if (throwable != null) {
            requestRun.throwable = throwable;
            int retryIntervalMs = request.nextRetryIntervalMs();
            if (retryIntervalMs > 0) {
                LoadRequest.RequestRun nextRun = request.newRun();
                mergeCommitLoader.sendLoad(nextRun, retryIntervalMs);
                LOG.warn("Retry to flush chunk, db: {}, table: {}, chunkId: {}, retries: {}, retry interval: {} ms, " +
                        "last exception", database, table, request.getChunk().getChunkId(), request.getNumRuns() - 1,
                        retryIntervalMs,  throwable);
                return;
            }

            tableThrowable.compareAndSet(null, throwable);
            manager.onLoadFailure(this, request, throwable);
        } else {
            releaseBuffer(requestRun.loadRequest);
            Chunk chunk = request.getChunk();
            requestRun.loadResult.setFlushBytes(chunk.rowBytes());
            requestRun.loadResult.setFlushRows(chunk.numRows());
            manager.onLoadSuccess(this, request);
        }
        request.logRequestTrace();
        lock.lock();
        try {
            inflightLoadRequests.remove(request.getChunk().getChunkId());
            flushCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public Optional<CompressionCodec> getCompressionCodec() {
        return compressionCodec;
    }

    public String loadRequestsSummary() {
        StringBuilder builder = new StringBuilder();
        builder.append("db: ").append(database)
                .append(", table: ").append(table)
                .append(", num inflight requests: ").append(inflightLoadRequests.size());
        int count = 0;
        for (LoadRequest request : inflightLoadRequests.values()) {
            builder.append(", request ").append(count)
                    .append(": {");
            request.stateSummary(builder);
            builder.append("}");
            count += 1;
        }
        return builder.toString();
    }

    public int getLoadTimeoutMs() {
        return loadTimeoutMs;
    }

    ReentrantLock getFlushLockForTest() {
        return lock;
    }

    Condition getFlushConditionForTest() {
        return flushCondition;
    }
}
