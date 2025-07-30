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

import com.starrocks.data.load.stream.EnvUtils;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MergeCommitManager implements StreamLoadManager, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(MergeCommitManager.class);

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_FLUSH_TIMEOUT_MS = 180000;

    private final StreamLoadProperties properties;
    private final MergeCommitLoader mergeCommitLoader;
    private final int maxRetries;
    private final int retryIntervalInMs;
    private final int flushIntervalMs;
    private final int flushTimeoutMs;
    private final long maxCacheBytes;
    private final long maxWriteBlockCacheBytes;
    private final Map<TableId, Table> tables = new ConcurrentHashMap<>();
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    private final AtomicLong inflightBytes = new AtomicLong(0L);
    private final Lock lock = new ReentrantLock();
    private final Condition writable = lock.newCondition();
    private final AtomicReference<Throwable> exception;
    private transient Thread cacheMonitorThread;
    private transient AtomicBoolean closed;
    private transient MetricListener metricListener;

    public MergeCommitManager(StreamLoadProperties properties) {
        this.properties = properties;
        this.mergeCommitLoader = "brpc".equalsIgnoreCase(properties.getProtocol())
                ? new MergeCommitBrpcLoader() : new MergeCommitHttpLoader();
        this.maxRetries = properties.getMaxRetries();
        this.retryIntervalInMs = properties.getRetryIntervalInMs();
        this.flushIntervalMs = (int) properties.getExpectDelayTime();
        String timeout = properties.getHeaders().get("timeout");
        this.flushTimeoutMs = timeout != null ? Integer.parseInt(timeout) * 1000 : DEFAULT_FLUSH_TIMEOUT_MS;
        this.maxCacheBytes = properties.getMaxCacheBytes();
        this.maxWriteBlockCacheBytes = 2 * maxCacheBytes;
        this.exception = new AtomicReference<>();
    }

    @Override
    public void setMetricListener(MetricListener metricListener) {
        this.metricListener = metricListener;
    }

    @Override
    public void init() {
        this.metricListener = metricListener == null ? new EmptyMetricListener() : metricListener;
        this.closed = new AtomicBoolean(false);
        this.mergeCommitLoader.start(properties, this);
        this.cacheMonitorThread = new Thread(this::monitorCache, "starrocks-cache-monitor");
        this.cacheMonitorThread.setDaemon(true);
        this.cacheMonitorThread.start();
        LOG.info("Init merge commit manager, {}", EnvUtils.getGitInformation());
    }

    @Override
    public void write(String uniqueKey, String database, String tableName, String... rows) {
        Table table = getTable(database, tableName);
        int totalBytes = 0;
        for (String row : rows) {
            checkException();
            byte[] data = row.getBytes(StandardCharsets.UTF_8);
            if (properties.isBlackhole()) {
                totalBytes += data.length;
            } else {
                int bytes = table.write(data);
                totalBytes += bytes;
                currentCacheBytes.addAndGet(bytes);
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("Write record, database {}, table {}, row {}", database, tableName, row);
            }
            checkCacheFull();
        }
        metricListener.onWrite(rows.length, totalBytes);
        metricListener.onCacheChange(maxWriteBlockCacheBytes, currentCacheBytes.get());
    }

    private void checkCacheFull() {
        if (currentCacheBytes.get() < maxWriteBlockCacheBytes) {
            return;
        }
        long startTime = System.currentTimeMillis();
        lock.lock();
        try {
            int idx = 0;
            while (currentCacheBytes.get() >= maxWriteBlockCacheBytes) {
                checkException();
                LOG.info("Cache full, wait flush, currentBytes: {}, maxWriteBlockCacheBytes: {}",
                        currentCacheBytes.get(), maxWriteBlockCacheBytes);
                writable.await(Math.min(++idx, 5), TimeUnit.SECONDS);
            }
        } catch (InterruptedException ex) {
            exception.compareAndSet(null, ex);
            throw new RuntimeException(ex);
        } finally {
            lock.unlock();
        }
        metricListener.onCacheFull(System.currentTimeMillis() - startTime);
    }

    private void monitorCache() {
        List<Table> cachedTables = new ArrayList<>();
        while (!closed.get()) {
            long memoryBytes = currentCacheBytes.get() - inflightBytes.get();
            if (memoryBytes >= maxCacheBytes * 3 / 2) {
                if (tables.size() > cachedTables.size()) {
                    cachedTables.clear();
                    cachedTables.addAll(tables.values());
                }
                Collections.shuffle(cachedTables);
                long expectEvictBytes = memoryBytes - maxCacheBytes;
                LOG.debug("Start to evict cache, maxCacheBytes: {}, cacheBytes: {}, inflightBytes: {}, expectEvictBytes: {}",
                        maxWriteBlockCacheBytes, currentCacheBytes.get(), inflightBytes.get(), expectEvictBytes);
                long evictedSize = 0;
                int numTables = 0;
                for (Table table : cachedTables) {
                    if (evictedSize < expectEvictBytes) {
                        long size = table.cacheEvict();
                        evictedSize += size;
                        numTables += 1;
                        LOG.debug("Evict table, db: {}, table: {}, bytes: {}", table.getDatabase(), table.getTable(), size);
                    } else {
                        break;
                    }
                }
                LOG.debug("Finish to evict cache, maxCacheBytes: {}, cacheBytes: {}, inflightBytes: {}, " +
                                "expectEvictBytes: {}, actualEvictBytes: {}, numTables: {}",
                        maxWriteBlockCacheBytes, currentCacheBytes.get(), inflightBytes.get(), expectEvictBytes,
                        evictedSize, numTables);
            }
            try {
                Thread.sleep(200);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public void onLoadStart(Table table, long dataSize, int numRows) {
        inflightBytes.addAndGet(dataSize);
        metricListener.onLoadStart(dataSize, numRows);
        LOG.debug("Receive load start, db: {}, table: {}, dataSize: {}, inflightBytes: {}",
                table.getDatabase(), table.getTable(), dataSize, inflightBytes.get());
    }

    public void releaseCache(LoadRequest request) {
        long cacheByteBeforeFlush = currentCacheBytes.getAndAdd(-request.getChunk().rowBytes());
        lock.lock();
        try {
            writable.signal();
        } finally {
            lock.unlock();
        }
        metricListener.onCacheChange(maxWriteBlockCacheBytes, currentCacheBytes.get());
        Table table = request.getTable();
        LOG.debug("Release cache, db: {}, table: {}, cacheByteBeforeFlush: {}, currentCacheBytes: {}",
                table.getDatabase(), table.getTable(), cacheByteBeforeFlush, currentCacheBytes.get());
    }

    public void onLoadSuccess(Table table, LoadRequest loadRequest) {
        LoadRequest.RequestRun requestRun = loadRequest.getLastRun();
        StreamLoadResponse response = requestRun.loadResult;
        inflightBytes.addAndGet(-response.getFlushBytes());
        metricListener.onLoadSuccess(loadRequest.getChunk().rowBytes(), loadRequest.getChunk().numRows(),
                loadRequest.getNumRuns() - 1, loadRequest.getTotalTimeMs(), requestRun.getServerTimeMs());
        LOG.debug("Receive load success, db: {}, table: {}, currentCacheBytes: {}",
                table.getDatabase(), table.getTable(), currentCacheBytes.get());
    }

    public void onLoadFailure(Table table, LoadRequest request, Throwable throwable) {
        this.exception.compareAndSet(null, throwable);
        metricListener.onLoadFailure(request.getChunk().rowBytes(), request.getChunk().numRows(),
                request.getNumRuns() - 1, request.getTotalTimeMs());
        lock.lock();
        try {
            writable.signal();
        } finally {
            lock.unlock();
        }
        LOG.debug("Receive load failure, db: {}, table: {}", table.getDatabase(), table.getTable(), throwable);
    }

    public Throwable getException() {
        return exception.get();
    }

    @Override
    public void flush() {
        long totalTriggerTime = 0;
        long maxTriggerTime = Long.MIN_VALUE;
        long minTriggerTime = Long.MAX_VALUE;
        for (Table table : tables.values()) {
            long start = System.currentTimeMillis();
            try {
                table.flush(false);
            } catch (Exception e) {
                exception.compareAndSet(null, e);
                throw new RuntimeException(e);
            }
            long duration = System.currentTimeMillis() - start;
            totalTriggerTime += duration;
            maxTriggerTime = Math.max(maxTriggerTime, duration);
            minTriggerTime = Math.min(minTriggerTime, duration);
        }

        long totalWaitTime = 0;
        long maxWaitTime = Long.MIN_VALUE;
        long minWaitTime = Long.MAX_VALUE;
        for (Table table : tables.values()) {
            long start = System.currentTimeMillis();
            try {
                table.flush(true);
            } catch (Exception e) {
                exception.compareAndSet(null, e);
                throw new RuntimeException(e);
            }
            long duration = System.currentTimeMillis() - start;
            totalWaitTime += duration;
            maxWaitTime = Math.max(maxWaitTime, duration);
            minWaitTime = Math.min(minWaitTime, duration);
        }
        metricListener.onFlush(tables.size(), totalTriggerTime + totalWaitTime);
        LOG.info("Flush {} tables, total cost: {} ms, trigger cost: {} ms, wait cost: {} ms, " +
            "min trigger cost: {} ms, max trigger cost: {} ms, min wait cost: {} ms, max wait cost: {} ms",
                tables.size(), totalTriggerTime + totalWaitTime, totalTriggerTime, totalWaitTime, minTriggerTime,
                maxTriggerTime, minWaitTime, maxWaitTime);
    }

    @Override
    public void close() {
        closed.set(true);
        if (cacheMonitorThread != null) {
            cacheMonitorThread.interrupt();
        }
        mergeCommitLoader.close();
        LOG.info("Close merge commit manager");
    }

    private void checkException() {
        if (exception.get() != null) {
            throw new RuntimeException(exception.get());
        }
    }

    private Table getTable(String database, String tableName) {
        TableId tableId = TableId.of(database, tableName);
        Table table = tables.get(tableId);
        if (table == null) {
            StreamLoadTableProperties tableProperties = properties.getTableProperties(
                    StreamLoadUtils.getTableUniqueKey(database, tableName), database, tableName);
            table = new Table(database, tableName, this, mergeCommitLoader,
                    tableProperties, maxRetries, retryIntervalInMs, flushIntervalMs, flushTimeoutMs,
                    properties.getDefaultTableProperties().getChunkLimit(), properties.getMaxInflightRequests());
            tables.put(tableId, table);
        }
        return table;
    }

    public StreamLoader getStreamLoader() {
        return mergeCommitLoader;
    }

    @Override
    public StreamLoadSnapshot snapshot() {
        return new StreamLoadSnapshot();
    }

    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        return true;
    }

    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        return true;
    }

    @Override
    public boolean abort(StreamLoadSnapshot snapshot) {
        return true;
    }

    @Override
    public void setLabelGeneratorFactory(LabelGeneratorFactory labelGeneratorFactory) {
        // ignore
    }

    @Override
    public void setStreamLoadListener(StreamLoadListener streamLoadListener) {
    }

    @Override
    public void callback(StreamLoadResponse response) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void callback(Throwable e) {
        throw new UnsupportedOperationException();

    }
}
