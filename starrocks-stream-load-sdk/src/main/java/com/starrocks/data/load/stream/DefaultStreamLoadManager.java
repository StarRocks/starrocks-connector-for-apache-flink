/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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

public class DefaultStreamLoadManager implements StreamLoadManager, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamLoadManager.class);

    enum State {
        ACTIVE,
        INACTIVE
    }

    private static final Logger log = LoggerFactory.getLogger(DefaultStreamLoadManager.class);

    private final StreamLoadProperties properties;
    private final StreamLoader streamLoader;
    // threshold to trigger flush
    private final long maxCacheBytes;
    // threshold to block write
    private final long maxWriteBlockCacheBytes;
    private final Map<String, TableRegion> regions = new HashMap<>();
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    private final AtomicLong totalFlushRows = new AtomicLong(0L);

    private final AtomicLong numberTotalRows = new AtomicLong(0L);
    private final AtomicLong numberLoadRows = new AtomicLong(0L);

    private final StreamLoadStrategy loadStrategy;
    private final long scanningFrequency;
    private Thread current;
    private Thread manager;
    private volatile boolean savepoint = false;

    private final Lock lock = new ReentrantLock();
    private final Condition writable = lock.newCondition();
    private final Condition flushable = lock.newCondition();

    private final AtomicReference<State> state = new AtomicReference<>(State.INACTIVE);
    private volatile Throwable e;

    private final Queue<TableRegion> waitQ = new ConcurrentLinkedQueue<>();
    private final Queue<TableRegion> prepareQ = new LinkedList<>();
    private final Queue<TableRegion> commitQ = new LinkedList<>();
    private transient LabelGeneratorFactory labelGeneratorFactory;
    /**
     * Whether write() has triggered a flush after currentCacheBytes > maxCacheBytes.
     * This flag is set true after the flush is triggered in writer(), and set false
     * after the flush completed in callback(). During this period, there is no need
     * to re-trigger a flush.
     */
    private transient AtomicBoolean writeTriggerFlush;
    private transient LoadMetrics loadMetrics;

    public DefaultStreamLoadManager(StreamLoadProperties properties) {
        this(properties, new StreamLoadStrategy.DefaultLoadStrategy(properties));
    }

    public DefaultStreamLoadManager(StreamLoadProperties properties, StreamLoadStrategy loadStrategy) {
        this.properties = properties;
        this.streamLoader = properties.isEnableTransaction() ? new TransactionStreamLoader(false) : new DefaultStreamLoader();
        this.maxCacheBytes = properties.getMaxCacheBytes();
        this.maxWriteBlockCacheBytes = 2 * maxCacheBytes;
        this.scanningFrequency = properties.getScanningFrequency();
        this.loadStrategy = loadStrategy;
    }

    @Override
    public void init() {
        this.labelGeneratorFactory = new LabelGeneratorFactory.DefaultLabelGeneratorFactory(properties.getLabelPrefix());
        this.writeTriggerFlush = new AtomicBoolean(false);
        this.loadMetrics = new LoadMetrics();
        if (state.compareAndSet(State.INACTIVE, State.ACTIVE)) {
            this.manager = new Thread(() -> {
                Long lastPrintTimestamp = null;
                log.info("manager running, scanningFrequency : {}", scanningFrequency);
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

                    if (lastPrintTimestamp == null || System.currentTimeMillis() - lastPrintTimestamp > 4999) {
                        log.info("manager report, current Bytes : {},  waitQ : {}, prepareQ : {}, commitQ : {}",
                                currentCacheBytes.get(), waitQ.size(), prepareQ.size(), commitQ.size());
                        lastPrintTimestamp = System.currentTimeMillis();
                    }

                    Iterator<TableRegion> iterator = waitQ.iterator();
                    while (iterator.hasNext()) {
                        TableRegion region = iterator.next();
                        if (region.isReadable()) {
                            prepareQ.offer(region);
                            iterator.remove();
                            LOG.debug("Move table region {}.{} from waitQ to prepareQ", region.getDatabase(), region.getTable());
                        } else {
                            region.getAndIncrementAge();
                            LOG.debug("Increment age of table region {}.{} in waitQ", region.getDatabase(), region.getTable());
                        }
                    }

                    iterator = prepareQ.iterator();
                    while (iterator.hasNext()) {
                        TableRegion region = iterator.next();
                        if (region.testPrepare()) {
                            if (!region.isReadable()) {
                                region.cancel();
                                waitQ.offer(region);
                                iterator.remove();
                                LOG.debug("Move table region {}.{} from prepareQ to waitQ", region.getDatabase(), region.getTable());
                                continue;
                            }
                            if (region.prepare()) {
                                commitQ.offer(region);
                                iterator.remove();
                                LOG.debug("Move table region {}.{} from prepareQ to commitQ", region.getDatabase(), region.getTable());
                            }
                        }
                    }

                    boolean flushingCommit = false;
                    if (savepoint) {
                        iterator = commitQ.iterator();
                        while (iterator.hasNext()) {
                            TableRegion region = iterator.next();
                            region.getAndIncrementAge();
                            if (region.flush()) {
                                waitQ.offer(region);
                                if (region.isFlushing()) {
                                    flushingCommit = true;
                                }
                                iterator.remove();
                                LOG.debug("Move table region {}.{} from commitQ to waitQ because of savepoint",
                                        region.getDatabase(), region.getTable());
                            }
                        }
                    } else {
                        for (TableRegion region : loadStrategy.select(commitQ)) {
                            if (region.flush()) {
                                waitQ.offer(region);
                                commitQ.remove(region);
                                if (region.isFlushing()) {
                                    flushingCommit = true;
                                }
                                LOG.debug("Move table region {}.{} from commitQ to waitQ for normal",
                                        region.getDatabase(), region.getTable());
                            }
                        }
                    }

                    if (!flushingCommit && currentCacheBytes.get() >= maxCacheBytes) {
                        TableRegion maxFlushRegion = commitQ.stream()
                                .max((r1, r2) -> {
                                    if (r1.getFlushBytes() != r2.getFlushBytes()) {
                                        return Long.compare(r2.getFlushBytes(), r1.getFlushBytes());
                                    }
                                    return Long.compare(r2.getCacheBytes(), r1.getCacheBytes());
                                })
                                .orElse(null);
                        if (maxFlushRegion != null) {
                            if (maxFlushRegion.flush()) {
                                commitQ.remove(maxFlushRegion);
                                waitQ.offer(maxFlushRegion);
                                LOG.debug("Move table region {}.{} from commitQ to waitQ for max cache bytes",
                                        maxFlushRegion.getDatabase(), maxFlushRegion.getTable());
                            }
                        }
                    }

                    if (savepoint) {
                        LockSupport.unpark(current);
                    }
                }
            }, "StarRocks-Sink-Manager");
            manager.setDaemon(true);
            manager.start();
            manager.setUncaughtExceptionHandler((t, ee) -> {
                log.error("StarRocks-Sink-Manager error", ee);
                e = ee;
            });
            log.info("StarRocks-Sink-Manager start, {}", EnvUtils.getGitInformation());

            streamLoader.start(properties, this);
        }
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
                lock.lock();
                try {
                    int idx = 0;
                    while (currentCacheBytes.get() >= maxWriteBlockCacheBytes) {
                        AssertNotException();
                        log.info("Cache full, wait flush, currentBytes: {}, maxWriteBlockCacheBytes: {}",
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
            } else if (cachedBytes >= maxCacheBytes && writeTriggerFlush.compareAndSet(false, true)) {
                lock.lock();
                try {
                    flushable.signal();
                } finally {
                    lock.unlock();
                }
                LOG.info("Trigger flush, currentBytes: {}, maxCacheBytes: {}", cachedBytes, maxCacheBytes);
            }
        }
    }

    @Override
    public void callback(StreamLoadResponse response) {

        long currentBytes = response.getFlushBytes() != null ? currentCacheBytes.getAndAdd(-response.getFlushBytes()) : currentCacheBytes.get();
        if (response.getFlushRows() != null) {
            totalFlushRows.addAndGet(response.getFlushRows());
        }
        writeTriggerFlush.set(false);

        log.info("pre bytes : {}, current bytes : {}, totalFlushRows : {}", currentBytes, currentCacheBytes.get(), totalFlushRows.get());

        lock.lock();
        try {
            writable.signal();
        } finally {
            lock.unlock();
        }

        if (response.getException() != null) {
            log.error("Stream load failed", response.getException());
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("{}", loadMetrics);
        }
    }

    @Override
    public void callback(Throwable e) {
        log.error("Stream load failed", e);
        this.e = e;
    }

    @Override
    public void flush() {
        log.info("Stream load manager flush");
        savepoint = true;
        current = Thread.currentThread();
        while (!check()) {
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
                log.warn("Flush get result failed", ex);
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
            log.info("Stream load manger close, current bytes : {}, flush rows : {}" +
                            ", numberTotalRows : {}, numberLoadRows : {}, loadMetrics: {}",
                    currentCacheBytes.get(), totalFlushRows.get(), numberTotalRows.get(), numberLoadRows.get(), loadMetrics);
            manager.interrupt();
            streamLoader.close();
        }
    }

    private boolean check() {
        return currentCacheBytes.compareAndSet(0L, 0L);
    }

    private void AssertNotException() {
        if (e != null) {
            log.error("catch exception, wait rollback ", e);
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
                    StreamLoadTableProperties tableProperties = properties.getTableProperties(uniqueKey, database, table);
                    LabelGenerator labelGenerator = labelGeneratorFactory.create(database, table);
                    region = new BatchTableRegion(uniqueKey, database, table, this, tableProperties,
                            streamLoader, labelGenerator);
                    regions.put(uniqueKey, region);
                    waitQ.offer(region);
                }
            }
        }
        return region;
    }

    static boolean useBatchTableRegion(StreamLoadDataFormat format, boolean enableTransaction, StarRocksVersion version) {
        // csv format always use StreamTableRegion
        if (format instanceof StreamLoadDataFormat.CSVFormat) {
            return false;
        }

        // always use BatchTableRegion in exactly-once mode for json format. In the history,
        // transaction stream load does not support to send json format data using http chunk
        if (enableTransaction) {
            return true;
        }

        // In at-least-once mode, if starrocks version is unknown, use StreamTableRegion by default
        if (version == null) {
            return false;
        }

        // For starrocks <= 2.2, stream load does not support to send json format data
        // using http chunk, so should use BatchTableRegion
        return version.getMajor() < 2 || (version.getMajor() == 2 && version.getMinor() <= 2);
    }
}
