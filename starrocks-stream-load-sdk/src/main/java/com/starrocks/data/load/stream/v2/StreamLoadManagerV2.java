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

import com.starrocks.data.load.stream.DefaultStreamLoader;
import com.starrocks.data.load.stream.EnvUtils;
import com.starrocks.data.load.stream.LoadMetrics;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.TransactionStreamLoader;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of {@link StreamLoadManager}. In this manager, you can use normal stream load or
 * transaction stream load to load data to StarRocks. You can control which to use when constructing
 * the manager with parameter **properties**. If {@link StreamLoadProperties#isEnableTransaction()}
 * is true, transaction stream load will be used, otherwise the normal stream load. You can also control
 * how to commit the transaction stream load by parameter **enableAutoCommit**. If it's true, the
 * manager will commit the load automatically, otherwise you need to commit the load manually. Note that
 * this parameter should always be true for the normal stream load currently.
 * The usage for manual commit should like this
 *     manager.write(); // write some recodes
 *     manager.flush();    // ensure the data is flushed to StarRocks, and the transaction is prepared
 *     manager.snapshot(); // take a snapshot the current transactions, mainly recording the labels
 *     manager.commit();   // commit those snapshots
 */
public class StreamLoadManagerV2 implements StreamLoadManager, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamLoadManagerV2.class);

    private static final long serialVersionUID = 1L;

    enum State {
        ACTIVE,
        INACTIVE
    }

    private final StreamLoadProperties properties;
    private final boolean enableAutoCommit;
    private final StreamLoader streamLoader;
    private final int maxRetries;
    private final int retryIntervalInMs;
    // threshold to trigger flush
    private final long maxCacheBytes;
    // threshold to block write
    private final long maxWriteBlockCacheBytes;
    private final Map<String, TableRegion> regions = new HashMap<>();
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    private final AtomicLong totalFlushRows = new AtomicLong(0L);

    private final AtomicLong numberTotalRows = new AtomicLong(0L);
    private final AtomicLong numberLoadRows = new AtomicLong(0L);

    private final FlushAndCommitStrategy flushAndCommitStrategy;
    private final long scanningFrequency;
    private Thread current;
    private Thread manager;
    private volatile boolean savepoint = false;
    private volatile boolean allRegionsCommitted;

    private final Lock lock = new ReentrantLock();
    private final Condition writable = lock.newCondition();
    private final Condition flushable = lock.newCondition();

    private final AtomicReference<State> state = new AtomicReference<>(State.INACTIVE);
    private volatile Throwable e;

    private final Queue<TableRegion> flushQ = new LinkedList<>();

    /**
     * Whether write() has triggered a flush after currentCacheBytes > maxCacheBytes.
     * This flag is set true after the flush is triggered in writer(), and set false
     * after the flush completed in callback(). During this period, there is no need
     * to re-trigger a flush.
     */
    private transient AtomicBoolean writeTriggerFlush;
    private transient LoadMetrics loadMetrics;
    private transient StreamLoadListener streamLoadListener;
    public StreamLoadManagerV2(StreamLoadProperties properties, boolean enableAutoCommit) {
        this.properties = properties;
        if (!enableAutoCommit && !properties.isEnableTransaction()) {
            throw new IllegalArgumentException("You must use transaction stream load if not enable auto-commit");
        }
        this.enableAutoCommit = enableAutoCommit;
        if (!enableAutoCommit) {
            streamLoader = new TransactionStreamLoader();
            maxRetries = 0;
            retryIntervalInMs = 0;
        } else {
            // TODO transaction stream load can't support retry currently
            streamLoader = (properties.getMaxRetries() > 0 || !properties.isEnableTransaction())
                    ? new DefaultStreamLoader() : new TransactionStreamLoader();
            maxRetries = properties.getMaxRetries();
            retryIntervalInMs = properties.getRetryIntervalInMs();
        }
        this.maxCacheBytes = properties.getMaxCacheBytes();
        this.maxWriteBlockCacheBytes = 2 * maxCacheBytes;
        this.scanningFrequency = properties.getScanningFrequency();
        this.flushAndCommitStrategy = new FlushAndCommitStrategy(properties, enableAutoCommit);
    }

    @Override
    public void init() {
        this.writeTriggerFlush = new AtomicBoolean(false);
        this.loadMetrics = new LoadMetrics();
        if (state.compareAndSet(State.INACTIVE, State.ACTIVE)) {
            this.manager = new Thread(() -> {
                long lastPrintTimestamp = -1;
                LOG.info("manager running, scanningFrequency : {}", scanningFrequency);
                while (true) {
                    lock.lock();
                    try {
                        flushable.await(scanningFrequency, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        if (savepoint) {
                            savepoint = false;
                            LockSupport.unpark(current);
                        }
                        break;
                    } finally {
                        lock.unlock();
                    }

                    if (lastPrintTimestamp == -1 || System.currentTimeMillis() - lastPrintTimestamp > 10000) {
                        lastPrintTimestamp = System.currentTimeMillis();
                        LOG.debug("Audit information: {}, {}", loadMetrics, flushAndCommitStrategy);
                    }

                    if (savepoint) {
                        for (TableRegion region : flushQ) {
                            boolean flush = region.flush();
                            LOG.debug("Trigger flush table region {} because of savepoint, region cache bytes: {}, flush: {}",
                                    region.getUniqueKey(), region.getCacheBytes(), flush);
                        }

                        // should ensure all data is committed for auto-commit mode
                        if (enableAutoCommit) {
                            int committedRegions = 0;
                            for (TableRegion region : flushQ) {
                                // savepoint makes sure no more data is written, so these conditions
                                // can guarantee commit after all data has been written to StarRocks
                                if (region.getCacheBytes() == 0 && !region.isFlushing()) {
                                    boolean success = ((TransactionTableRegion) region).commit();
                                    if (success) {
                                        committedRegions += 1;
                                        region.resetAge();
                                    }
                                    LOG.debug("Commit region {} for savepoint, success: {}", region.getUniqueKey(), success);
                                }
                            }

                            if (committedRegions == flushQ.size()) {
                                allRegionsCommitted = true;
                                LOG.info("All regions committed for savepoint, number of regions: {}", committedRegions);
                            }
                        }
                        LockSupport.unpark(current);
                    } else {
                        for (TableRegion region : flushQ) {
                            region.getAndIncrementAge();
                            if (flushAndCommitStrategy.shouldCommit(region)) {
                                boolean success = ((TransactionTableRegion) region).commit();
                                if (success) {
                                    region.resetAge();
                                }
                                LOG.debug("Commit region {} for normal, success: {}", region.getUniqueKey(), success);
                            }
                        }

                        for (TableRegion region : flushAndCommitStrategy.selectFlushRegions(flushQ, currentCacheBytes.get())) {
                            boolean flush = region.flush();
                            LOG.debug("Trigger flush table region {} because of selection, region cache bytes: {}," +
                                    " flush: {}", region.getUniqueKey(), region.getCacheBytes(), flush);
                        }
                    }
                }
            }, "Flink-StarRocks-Sink-Manager");
            manager.setDaemon(true);
            manager.start();
            manager.setUncaughtExceptionHandler((t, ee) -> {
                LOG.error("StarRocks-Sink-Manager error", ee);
                e = ee;
            });
            LOG.info("Flink-StarRocks-Sink-Manager start, enableAutoCommit: {}, streamLoader: {}, {}",
                    enableAutoCommit, streamLoader.getClass().getName(), EnvUtils.getGitInformation());

            streamLoader.start(properties, this);
        }
    }

    public void setStreamLoadListener(StreamLoadListener streamLoadListener) {
        this.streamLoadListener = streamLoadListener;
    }

    @Override
    public void write(String uniqueKey, String database, String table, String... rows) {
        TableRegion region = getCacheRegion(uniqueKey, database, table);
        for (String row : rows) {
            AssertNotException();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Write uniqueKey {}, database {}, table {}, row {}",
                        uniqueKey == null ? "null" : uniqueKey, database, table, row);
            }
            int bytes = region.write(row.getBytes(StandardCharsets.UTF_8));
            long cachedBytes = currentCacheBytes.addAndGet(bytes);
            if (cachedBytes >= maxWriteBlockCacheBytes) {
                long startTime = System.nanoTime();
                lock.lock();
                try {
                    int idx = 0;
                    while (currentCacheBytes.get() >= maxWriteBlockCacheBytes) {
                        AssertNotException();
                        LOG.info("Cache full, wait flush, currentBytes: {}, maxWriteBlockCacheBytes: {}",
                                currentCacheBytes.get(), maxWriteBlockCacheBytes);
                        flushable.signal();
                        writable.await(Math.min(++idx, 5), TimeUnit.SECONDS);
                    }
                } catch (InterruptedException ex) {
                    this.e = ex;
                    throw new RuntimeException(ex);
                } finally {
                    lock.unlock();
                }
                loadMetrics.updateWriteBlock(1, System.nanoTime() - startTime);
            } else if (cachedBytes >= maxCacheBytes && writeTriggerFlush.compareAndSet(false, true)) {
                lock.lock();
                try {
                    flushable.signal();
                } finally {
                    lock.unlock();
                }
                loadMetrics.updateWriteTriggerFlush(1);
                LOG.info("Trigger flush, currentBytes: {}, maxCacheBytes: {}", cachedBytes, maxCacheBytes);
            }
        }
    }

    @Override
    public void callback(StreamLoadResponse response) {
        long cacheByteBeforeFlush = response.getFlushBytes() != null ? currentCacheBytes.getAndAdd(-response.getFlushBytes()) : currentCacheBytes.get();
        if (response.getFlushRows() != null) {
            totalFlushRows.addAndGet(response.getFlushRows());
        }
        writeTriggerFlush.set(false);

        LOG.info("Receive load response, cacheByteBeforeFlush: {}, currentCacheBytes: {}, totalFlushRows : {}",
                cacheByteBeforeFlush, currentCacheBytes.get(), totalFlushRows.get());

        lock.lock();
        try {
            writable.signal();
        } finally {
            lock.unlock();
        }

        if (response.getException() != null) {
            LOG.error("Stream load failed", response.getException());
            this.e = response.getException();
        }

        if (response.getBody() != null) {
            if (response.getBody().getNumberTotalRows() != null) {
                numberTotalRows.addAndGet(response.getBody().getNumberTotalRows());
            }
            if (response.getBody().getNumberLoadedRows() != null) {
                numberLoadRows.addAndGet(response.getBody().getNumberLoadedRows());
            }
        }

        if (response.getException() != null) {
            this.loadMetrics.updateFailedLoad();
        } else {
            this.loadMetrics.updateSuccessLoad(response);
        }

        if (streamLoadListener != null) {
            streamLoadListener.onResponse(response);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("{}", loadMetrics);
        }
    }

    @Override
    public void callback(Throwable e) {
        LOG.error("Stream load failed", e);
        this.e = e;
    }

    public Throwable getException() {
        return e;
    }

    @Override
    public void flush() {
        LOG.info("Stream load manager flush");
        savepoint = true;
        allRegionsCommitted = false;
        current = Thread.currentThread();
        while (!isSavepointFinished()) {
            AssertNotException();
            lock.lock();
            try {
                flushable.signal();
            } finally {
                lock.unlock();
            }
            LockSupport.park(current);
            if (!savepoint) {
                break;
            }
            try {
                for (TableRegion tableRegion : regions.values()) {
                    Future<?> result = tableRegion.getResult();
                    if (result != null) {
                        result.get();
                    }
                }
            } catch (ExecutionException | InterruptedException ex) {
                LOG.warn("Flush get result failed", ex);
            }
        }
        savepoint = false;
    }

    @Override
    public StreamLoadSnapshot snapshot() {
        StreamLoadSnapshot snapshot = StreamLoadSnapshot.snapshot(regions.values());
        for (TableRegion region : regions.values()) {
            region.setLabel(null);
        }
        return snapshot;
    }

    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        return streamLoader.prepare(snapshot);
    }

    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        return streamLoader.commit(snapshot);
    }

    @Override
    public boolean abort(StreamLoadSnapshot snapshot) {
        return streamLoader.rollback(snapshot);
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.ACTIVE, State.INACTIVE)) {
            LOG.info("StreamLoadManagerV2 close, loadMetrics: {}, flushAndCommit: {}",
                    loadMetrics, flushAndCommitStrategy);
            manager.interrupt();
            streamLoader.close();
        }
    }

    private boolean isSavepointFinished() {
        return currentCacheBytes.compareAndSet(0L, 0L) && (!enableAutoCommit || allRegionsCommitted);
    }

    private void AssertNotException() {
        if (e != null) {
            LOG.error("catch exception, wait rollback ", e);
            streamLoader.rollback(snapshot());
            close();
            throw new RuntimeException(e);
        }
    }

    protected TableRegion getCacheRegion(String uniqueKey, String database, String table) {
        if (uniqueKey == null) {
            uniqueKey = StreamLoadUtils.getTableUniqueKey(database, table);
        }

        TableRegion region = regions.get(uniqueKey);
        if (region == null) {
            synchronized (regions) {
                region = regions.get(uniqueKey);
                if (region == null) {
                    StreamLoadTableProperties tableProperties = properties.getTableProperties(uniqueKey);
                    region = new TransactionTableRegion(uniqueKey, database, table, this,
                            tableProperties, streamLoader, maxRetries, retryIntervalInMs);
                    regions.put(uniqueKey, region);
                    flushQ.offer(region);
                }
            }
        }
        return region;
    }
}
