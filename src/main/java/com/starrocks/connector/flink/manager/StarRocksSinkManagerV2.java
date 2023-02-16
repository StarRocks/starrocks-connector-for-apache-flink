package com.starrocks.connector.flink.manager;

import com.alibaba.fastjson.JSON;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.DefaultStreamLoader;
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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Preconditions;
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

public class StarRocksSinkManagerV2 implements StreamLoadManager, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkManagerV2.class);

    private static final long serialVersionUID = 1L;

    enum State {
        ACTIVE,
        INACTIVE
    }

    private final StreamLoadProperties properties;
    private final boolean enableAutoCommit;
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

    private final FlinkStreamLoadStrategy loadStrategy;
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
    private transient StarRocksSinkRuntimeContext runtimeContext;

    public StarRocksSinkManagerV2(StreamLoadProperties properties, boolean enableAutoCommit) {
        this.properties = properties;
        this.enableAutoCommit = enableAutoCommit;
        this.streamLoader = properties.isEnableTransaction() ? new TransactionStreamLoader() : new DefaultStreamLoader();
        Preconditions.checkArgument(enableAutoCommit || properties.isEnableTransaction(),
                "Normal stream load only support auto-commit mode");
        this.maxCacheBytes = properties.getMaxCacheBytes();
        this.maxWriteBlockCacheBytes = 2 * maxCacheBytes;
        this.scanningFrequency = properties.getScanningFrequency();
        this.loadStrategy = new FlinkStreamLoadStrategy(properties);
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

                    if (lastPrintTimestamp == -1 || System.currentTimeMillis() - lastPrintTimestamp > 4999) {
                        LOG.info("manager report, current Bytes : {}, flushQ : {}", currentCacheBytes.get(), flushQ.size());
                        lastPrintTimestamp = System.currentTimeMillis();
                    }

                    if (savepoint) {
                        for (TableRegion region : flushQ) {
                            boolean flush = region.flush();
                            LOG.debug("Trigger flush table region {} because of savepoint, region cache bytes: {}, flush: {}",
                                    region.getUniqueKey(), region.getCacheBytes(), flush);
                        }

                        // should ensure all data is committed for auto-commit mode
                        if (enableAutoCommit) {
                            int commitedRegions = 0;
                            for (TableRegion region : flushQ) {
                                // savepoint makes sure no more data is written, so these conditions
                                // can guarantee commit after all data has been written to StarRocks
                                if (region.getCacheBytes() == 0 && !region.isFlushing()) {
                                    ((TransactionTableRegion) region).commit();
                                    commitedRegions += 1;
                                    LOG.debug("Commit region {} for savepoint", region.getUniqueKey());
                                }
                            }

                            if (commitedRegions == flushQ.size()) {
                                allRegionsCommitted = true;
                                LOG.info("All regions committed for savepoint, number of regions: {}", commitedRegions);
                            }
                        }
                        LockSupport.unpark(current);
                    } else {
                        for (TableRegion region : flushQ) {
                            region.getAndIncrementAge();
                            if (enableAutoCommit && loadStrategy.shouldCommit(region)) {
                                ((TransactionTableRegion) region).commit();
                                LOG.debug("Commit region {} for normal", region.getUniqueKey());
                            }
                        }

                        if (currentCacheBytes.get() >= maxCacheBytes) {
                            for (TableRegion region : loadStrategy.select(flushQ)) {
                                boolean flush = region.flush();
                                LOG.debug("Trigger flush table region {} because of selection, region cache bytes: {}," +
                                        " flush: {}", region.getUniqueKey(), region.getCacheBytes(), flush);
                            }
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
            LOG.info("Flink-StarRocks-Sink-Manager start, enableAutoCommit: {}, streamLoader: {}",
                    enableAutoCommit, streamLoader.getClass().getName());

            streamLoader.start(properties, this);
        }
    }

    public void setRuntimeContext(RuntimeContext runtimeContext, StarRocksSinkOptions sinkOptions) {
        this.runtimeContext = new StarRocksSinkRuntimeContext(runtimeContext, sinkOptions);
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
            LOG.error("Stream load failed, body : " + JSON.toJSONString(response.getBody()), response.getException());
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

        if (response.getException() != null) {
            StarRocksSinkRuntimeContext.flushFailedRecord(runtimeContext);
        } else {
            StarRocksSinkRuntimeContext.flushSucceedRecord(runtimeContext, response);
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
            LOG.info("Stream load manger close, current bytes : {}, flush rows : {}" +
                            ", numberTotalRows : {}, numberLoadRows : {}, loadMetrics: {}",
                    currentCacheBytes.get(), totalFlushRows.get(), numberTotalRows.get(), numberLoadRows.get(), loadMetrics);
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
                    region = new TransactionTableRegion(uniqueKey, database, table, this, tableProperties, streamLoader);
                    regions.put(uniqueKey, region);
                    flushQ.offer(region);
                }
            }
        }
        return region;
    }
}
