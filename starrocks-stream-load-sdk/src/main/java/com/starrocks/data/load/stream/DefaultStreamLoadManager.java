package com.starrocks.data.load.stream;

import com.alibaba.fastjson.JSON;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultStreamLoadManager implements StreamLoadManager, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamLoadManager.class);

    enum State {
        ACTIVE,
        INACTIVE
    }

    private static final Logger log = LoggerFactory.getLogger(DefaultStreamLoadManager.class);

    private final StreamLoadProperties properties;
    private final StreamLoader streamLoader;
    private final long maxCacheBytes;
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

    public DefaultStreamLoadManager(StreamLoadProperties properties) {
        this(properties, new StreamLoadStrategy.DefaultLoadStrategy(properties));
    }

    public DefaultStreamLoadManager(StreamLoadProperties properties, StreamLoadStrategy loadStrategy) {
        this.properties = properties;
        this.streamLoader = properties.isEnableTransaction() ? new TransactionStreamLoader() : new DefaultStreamLoader();
        this.maxCacheBytes = properties.getMaxCacheBytes();
        this.scanningFrequency = properties.getScanningFrequency();
        this.loadStrategy = loadStrategy;
    }

    @Override
    public void init() {
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
                        } else {
                            region.getAndIncrementAge();
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
                                continue;
                            }
                            if (region.prepare()) {
                                commitQ.offer(region);
                                iterator.remove();
                            }
                        }
                    }

                    boolean flushingCommit = false;
                    if (savepoint) {
                        iterator = commitQ.iterator();
                        while (iterator.hasNext()) {
                            TableRegion region = iterator.next();
                            region.getAndIncrementAge();
                            if (region.commit()) {
                                waitQ.offer(region);
                                if (region.isFlushing()) {
                                    flushingCommit = true;
                                }
                                iterator.remove();
                            }
                        }
                    } else {
                        for (TableRegion region : loadStrategy.select(commitQ)) {
                            if (region.commit()) {
                                waitQ.offer(region);
                                commitQ.remove(region);
                                if (region.isFlushing()) {
                                    flushingCommit = true;
                                }
                            }
                        }
                    }

                    if (!flushingCommit && currentCacheBytes.get() >= maxCacheBytes) {
                        TableRegion maxFlushRegion = commitQ.stream()
                                .filter(TableRegion::isFlushing)
                                .max((r1, r2) -> {
                                    if (r1.getFlushBytes() != r2.getFlushBytes()) {
                                        return Long.compare(r2.getFlushBytes(), r1.getFlushBytes());
                                    }
                                    return Long.compare(r2.getCacheBytes(), r1.getCacheBytes());
                                })
                                .orElse(null);
                        if (maxFlushRegion != null) {
                            if (maxFlushRegion.commit()) {
                                commitQ.remove(maxFlushRegion);
                                waitQ.offer(maxFlushRegion);
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
            log.info("StarRocks-Sink-Manager start");

            streamLoader.start(properties, this);
        }
    }

    @Override
    public void write(String uniqueKey, String database, String table, String... rows) {
        TableRegion region = getCacheRegion(uniqueKey, database, table);
        for (String row : rows) {
            AssertNotException();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Write uniqueKey {}, database {}, table {}, row {}",
                        uniqueKey == null ? "null" : uniqueKey, database, table, row);
            }
            int bytes = region.write(row.getBytes(StandardCharsets.UTF_8));
            if (currentCacheBytes.addAndGet(bytes) >= maxCacheBytes) {
                int idx = 0;
                lock.lock();
                try {
                    while (currentCacheBytes.get() >= maxCacheBytes) {
                        AssertNotException();
                        log.info("Cache full, wait flush, currentBytes :{}", currentCacheBytes.get());
                        flushable.signal();
                        writable.await(Math.min(++idx, 5), TimeUnit.SECONDS);
                    }
                } catch (InterruptedException ex) {
                    this.e = ex;
                    throw new RuntimeException(ex);
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    @Override
    public void callback(StreamLoadResponse response) {

        long currentBytes = response.getFlushBytes() != null ? currentCacheBytes.getAndAdd(-response.getFlushBytes()) : currentCacheBytes.get();
        if (response.getFlushRows() != null) {
            totalFlushRows.addAndGet(response.getFlushRows());
        }

        log.info("pre bytes : {}, current bytes : {}, totalFlushRows : {}", currentBytes, currentCacheBytes.get(), totalFlushRows.get());

        lock.lock();
        try {
            writable.signal();
        } finally {
            lock.unlock();
        }

        if (response.getException() != null) {
            log.error("Stream load failed, body : " + JSON.toJSONString(response.getBody()), response.getException());
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
                            ", numberTotalRows : {}, numberLoadRows : {}",
                    currentCacheBytes.get(), totalFlushRows.get(), numberTotalRows.get(), numberLoadRows.get());
            printRegionDetails();
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
                    StreamLoadTableProperties tableProperties = properties.getTableProperties(uniqueKey);
                    if (useBatchTableRegion(tableProperties.getDataFormat(), properties.isEnableTransaction(), properties.getStarRocksVersion())) {
                        region = new BatchTableRegion(uniqueKey, database, table, this, tableProperties, streamLoader);
                    } else {
                        region = new StreamTableRegion(uniqueKey, database, table, this, tableProperties, streamLoader);
                    }
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

    private void printRegionDetails() {
        StringBuilder builder = new StringBuilder();

        for (TableRegion region : regions.values()) {
            builder.append(JSON.toJSONString(region));
        }

        log.info("Regions details : {}", builder);
    }
}
