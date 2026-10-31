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
    // Guarded by lock. Set when the flush interval of the active chunk elapses but the chunk
    // cannot be flushed without waiting for a request slot. loadFinish flushes it later.
    private boolean delayFlushPending;
    // Guarded by lock. Number of switched chunks waiting in flushChunk for a request slot.
    private int numChunksWaitingForSlot;
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
            switchChunk(FlushChunkReason.CHUNK_FULL);
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
            // Wait until all inflight requests finished, and also the chunks that other threads
            // switched out and that still wait for a request slot, for example in cacheEvict()
            waitInflightRequests(1, true);
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
            // The next write() or flush() reports a failed table, and a closed loader cannot send
            if (tableThrowable.get() != null || manager.isClosed()) {
                return;
            }
            // The timer runs on the loader's executor, which also completes the inflight requests,
            // so it must not wait for a request slot. Keep the chunk active, and loadFinish will
            // flush it once a slot frees up.
            if (canFlushWithoutWaiting()) {
                switchChunk(FlushChunkReason.CHUNK_DELAY);
            } else {
                delayFlushPending = true;
                LOG.debug("Delay flush until a request slot frees up, db: {}, table: {}, chunkId: {}, " +
                        "inflight requests: {}, chunks waiting for slot: {}", database, table, chunkId,
                        inflightLoadRequests.size(), numChunksWaitingForSlot);
            }
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
                timer.cancel(false);
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
        delayFlushPending = false;
        if (timer != null) {
            // Do not interrupt: when called from checkFlushInterval(), the timer is the task running this method.
            timer.cancel(false);
            timer = null;
        }
        if (inactiveChunk.numRows() > 0) {
            try {
                flushChunk(inactiveChunk, reason);
            } catch (Throwable e) {
                // The chunk is no longer active and may not be inflight, so fail the table instead
                // of losing the chunk silently
                failTable(inactiveChunk, reason, e);
                throw e;
            }
        }
    }

    // Whether a chunk switched now is sent without waiting: a request slot is free, and no
    // earlier chunk is waiting for one, which keeps the chunks in order
    private boolean canFlushWithoutWaiting() {
        return numChunksWaitingForSlot == 0
                && hasFreeSlot(maxConcurrentRequests)
                && tableThrowable.get() == null;
    }

    // Whether fewer than limit requests are inflight. canFlushWithoutWaiting() and the wait in flushChunk()
    // must use the same test, or the timer or loadFinish() could block on the loader's executor again.
    private boolean hasFreeSlot(int limit) {
        return inflightLoadRequests.size() < limit;
    }

    // Called with the lock held after a request slot frees up
    private void flushDelayedChunk() {
        // On shutdown, the loader's executor is gone, and sending would only log a misleading failure
        if (!delayFlushPending || manager.isClosed() || !canFlushWithoutWaiting()) {
            return;
        }
        try {
            switchChunk(FlushChunkReason.CHUNK_DELAY);
        } catch (Throwable e) {
            // switchChunk has failed the table. Do not throw into the completion of another request.
        }
    }

    private void failTable(Chunk chunk, FlushChunkReason reason, Throwable throwable) {
        // A timeout in waitInflightRequests() has already set tableThrowable to the thrown exception
        Throwable previous = tableThrowable.get();
        tableThrowable.compareAndSet(null, throwable);
        // wake up the threads waiting in waitInflightRequests() to fail them
        flushCondition.signalAll();
        manager.onTableFailure(this, throwable);
        if (previous != null && previous != throwable) {
            // The table failed before, and waitInflightRequests() has logged the cause
            LOG.warn("Fail to flush chunk because the table has failed, db: {}, table: {}, chunkId: {}, " +
                    "rows: {}, bytes: {}, reason: {}", database, table, chunk.getChunkId(), chunk.numRows(),
                    chunk.rowBytes(), reason);
        } else {
            LOG.error("Fail to flush chunk, db: {}, table: {}, chunkId: {}, rows: {}, bytes: {}, reason: {}",
                    database, table, chunk.getChunkId(), chunk.numRows(), chunk.rowBytes(), reason, throwable);
        }
    }

    private void flushChunk(Chunk chunk, FlushChunkReason reason) {
        numChunksWaitingForSlot++;
        try {
            waitInflightRequests(maxConcurrentRequests, false);
        } finally {
            numChunksWaitingForSlot--;
        }
        LoadRequest request = new LoadRequest(this, chunk, maxRetries, retryIntervalInMs);
        inflightLoadRequests.put(chunk.getChunkId(), request);
        manager.onLoadStart(this, chunk.rowBytes(), chunk.numRows());
        LoadRequest.RequestRun requestRun = request.newRun();
        mergeCommitLoader.sendLoad(requestRun, 0);
        LOG.info("Flush chunk, db: {}, table: {}, chunkId: {}, rows: {}, bytes: {}, reason: {}",
                database, table, chunk.getChunkId(), chunk.numRows(), chunk.rowBytes(), reason);
    }

    // Waits until fewer than threshold requests are inflight. If waitSwitchedChunks is true, also waits
    // until no switched chunk waits for a request slot.
    private void waitInflightRequests(int threshold, boolean waitSwitchedChunks) {
        final long startTimeMs = System.currentTimeMillis();
        final long startTimeNs = System.nanoTime();
        final long timeoutNs = loadTimeoutMs >= 0
                ? TimeUnit.MILLISECONDS.toNanos(loadTimeoutMs)
                : Long.MAX_VALUE;
        final long awakeTimeoutNs = TimeUnit.SECONDS.toNanos(10);
        while ((!hasFreeSlot(threshold) || (waitSwitchedChunks && numChunksWaitingForSlot > 0))
                && tableThrowable.get() == null) {
            try {
                long elapsedNs = System.nanoTime() - startTimeNs;
                long remainingNs = timeoutNs - elapsedNs;
                if (remainingNs <= 0) {
                    RuntimeException timeoutEx = new RuntimeException(String.format(
                            "Flush timeout when waiting inflight requests, db: %s, table: %s, " +
                                    "elapsed: %d ms, timeout: %d ms, current inflight requests: %d, target size: %d, " +
                                    "chunks waiting for slot: %d",
                            database, table, System.currentTimeMillis() - startTimeMs, loadTimeoutMs,
                            inflightLoadRequests.size(), threshold, numChunksWaitingForSlot));
                    tableThrowable.compareAndSet(null, timeoutEx);
                    throw timeoutEx;
                }

                long notElapsedNs = flushCondition.awaitNanos(Math.min(awakeTimeoutNs, remainingNs));
                if (notElapsedNs <= 0) {
                    LOG.info("Waiting inflight requests, db: {}, table: {}, current inflight requests: {}," +
                            " target size: {}, chunks waiting for slot: {}, elapsed: {} ms, timeout: {} ms, {}",
                            database, table, inflightLoadRequests.size(), threshold, numChunksWaitingForSlot,
                            System.currentTimeMillis() - startTimeMs, loadTimeoutMs, loadRequestsSummary());
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
            flushDelayedChunk();
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
