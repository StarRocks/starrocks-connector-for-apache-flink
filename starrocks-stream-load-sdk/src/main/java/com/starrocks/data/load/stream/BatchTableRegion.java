package com.starrocks.data.load.stream;

import com.starrocks.data.load.stream.http.StreamLoadEntityMeta;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class BatchTableRegion implements TableRegion {
    enum State {
        ACTIVE,
        PREPARE,
        COMMIT
    }

    private static final Logger log = LoggerFactory.getLogger(BatchTableRegion.class);

    private final StreamLoadManager manager;
    private final StreamLoader streamLoader;

    private final String uniqueKey;
    private final String database;
    private final String table;
    private final StreamLoadTableProperties properties;
    private final StreamLoadDataFormat dataFormat;

    private final AtomicLong age = new AtomicLong(0L);

    private final AtomicLong cacheBytes = new AtomicLong();
    private final AtomicLong flushBytes = new AtomicLong();
    private final AtomicLong flushRows = new AtomicLong();

    private final AtomicReference<State> state;
    private final AtomicBoolean ctl = new AtomicBoolean(false);

    private volatile Queue<byte[]> outBuffer = new LinkedList<>();
    private volatile Queue<byte[]> inBuffer;
    private volatile StreamLoadEntityMeta entityMeta;

    private volatile String label;
    private volatile Future<?> responseFuture;
    private volatile long lastWriteTimeMillis = Long.MAX_VALUE;
    private volatile boolean flushing;

    public BatchTableRegion(String uniqueKey,
                            String database,
                            String table,
                            StreamLoadManager manager,
                            StreamLoadTableProperties properties,
                            StreamLoader streamLoader) {
        this.uniqueKey = uniqueKey;
        this.database = database;
        this.table = table;
        this.manager = manager;
        this.properties = properties;
        this.dataFormat = properties.getDataFormat();
        this.streamLoader = streamLoader;
        this.state = new AtomicReference<>(State.ACTIVE);
    }

    @Override
    public StreamLoadTableProperties getProperties() {
        return properties;
    }

    @Override
    public String getUniqueKey() {
        return uniqueKey;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public long getCacheBytes() {
        return cacheBytes.get();
    }

    @Override
    public long getFlushBytes() {
        return flushBytes.get();
    }

    @Override
    public StreamLoadEntityMeta getEntityMeta() {
        return entityMeta;
    }

    @Override
    public long getLastWriteTimeMillis() {
        return lastWriteTimeMillis;
    }

    @Override
    public void resetAge() {
        age.set(0);
    }

    @Override
    public long getAndIncrementAge() {
        return age.getAndIncrement();
    }

    @Override
    public long getAge() {
        return age.get();
    }

    @Override
    public int write(Record record) {
        throw new RuntimeException("DefaultStreamLoadManager does not support this method");
    }

    @Override
    public int write(byte[] row) {
        if (row == null) {
            return 0;
        }

        int c;
        if (ctl.compareAndSet(false, true)) {
            c = write0(row);
        } else {
            for (;;) {
                if (ctl.compareAndSet(false, true)) {
                    c = write0(row);
                    break;
                }
            }
        }
        ctl.set(false);
        return c;
    }

    protected int write0(byte[] row) {
        if (outBuffer == null) {
            outBuffer = new LinkedList<>();
        }
        outBuffer.offer(row);
        cacheBytes.addAndGet(row.length);
        lastWriteTimeMillis = System.currentTimeMillis();
        return row.length;
    }

    @Override
    public byte[] read() {
        if (flushRows.get() == entityMeta.getRows()) {
            flushing = false;
            return null;
        }

        byte[] row = inBuffer.poll();

        if (row == null) {
            flushing = false;
            return null;
        }

        if (!flushing) {
            flushing = true;
        }
        cacheBytes.addAndGet(-row.length);
        flushBytes.addAndGet(row.length);
        flushRows.incrementAndGet();
        return row;
    }

    @Override
    public boolean testPrepare() {
        return state.compareAndSet(State.ACTIVE, State.PREPARE);
    }

    @Override
    public boolean prepare() {
        return state.get() == State.PREPARE;
    }

    @Override
    public boolean flush() {
        if (state.compareAndSet(State.PREPARE, State.COMMIT)) {
            for (;;) {
                if (ctl.compareAndSet(false, true)) {
                    log.info("uk : {}, label : {}, bytes : {} commit", uniqueKey, label, cacheBytes.get());

                    inBuffer = outBuffer;
                    outBuffer = null;
                    ctl.set(false);
                    break;
                }
            }
            resetAge();
            streamLoad();
            return true;
        }
        return false;
    }

    @Override
    public boolean cancel() {
        //            if (responseFuture != null && !responseFuture.isDone()) {
        //                responseFuture.cancel(true);
        //            }
        return state.compareAndSet(State.PREPARE, State.ACTIVE);
    }

    @Override
    public void callback(StreamLoadResponse response) {
        manager.callback(response);
    }

    @Override
    public void callback(Throwable e) {
        manager.callback(e);
    }

    @Override
    public void complete(StreamLoadResponse response) {
        response.setFlushBytes(flushBytes.get());
        response.setFlushRows(flushRows.get());
        callback(response);

        log.info("Stream load flushed, label : {}", label);
        if (!inBuffer.isEmpty()) {
            log.info("Stream load continue");
            streamLoad();
            return;
        }
        if (state.compareAndSet(State.COMMIT, State.ACTIVE)) {
            log.info("Stream load completed");
        }
    }

    @Override
    public void setResult(Future<?> result) {
        responseFuture = result;
    }

    @Override
    public Future<?> getResult() {
        return responseFuture;
    }

    @Override
    public boolean isReadable() {
        return cacheBytes.get() > 0;
    }

    @Override
    public boolean isFlushing() {
        return flushing;
    }

    protected void flip() {
        flushBytes.set(0L);
        flushRows.set(0L);
        responseFuture = null;

        StreamLoadEntityMeta chunkMeta = genEntityMeta();
        this.entityMeta = chunkMeta;
        log.info("total rows : {}, part rows : {}, part bytes : {}", inBuffer.size(), chunkMeta.getRows(), chunkMeta.getBytes());
    }

    protected void streamLoad() {
        try {
            flip();
            setResult(streamLoader.send(this));
        } catch (Exception e) {
            callback(e);
        }
    }

    protected StreamLoadEntityMeta genEntityMeta() {
        long chunkBytes = 0;
        long chunkRows = 0;

        int delimiter = dataFormat.delimiter() == null ? 0 : dataFormat.delimiter().length;
        if (dataFormat.first() != null) {
            chunkBytes += dataFormat.first().length;
        }
        if (dataFormat.end() != null) {
            chunkBytes += dataFormat.end().length;
        }

        boolean first = true;
        for (byte[] bytes : inBuffer) {
            int d = first ? 0 : delimiter;
            first = false;
            if (chunkBytes + d + bytes.length > properties.getChunkLimit()) {
                break;
            }
            chunkBytes += bytes.length + d;
            chunkRows++;
        }

        return new StreamLoadEntityMeta(chunkBytes, chunkRows);
    }
}
