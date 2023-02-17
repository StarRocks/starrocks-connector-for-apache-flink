package com.starrocks.data.load.stream;

import com.starrocks.data.load.stream.http.StreamLoadEntityMeta;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class StreamTableRegion implements TableRegion, Serializable {

    enum State {
        ACTIVE,
        PREPARE,
        COMMIT
    }

    private static final Logger log = LoggerFactory.getLogger(StreamTableRegion.class);

    private static final byte[] END_STREAM = new byte[0];

    private final StreamLoader streamLoader;

    private final String uniqueKey;
    private final String database;
    private final String table;
    private final StreamLoadManager manager;
    private final StreamLoadTableProperties properties;
    private final long chunkLimit;
    private final StreamLoadDataFormat dataFormat;

    private final AtomicLong age = new AtomicLong(0L);

    private final BlockingQueue<byte[]> buffer = new LinkedTransferQueue<>();
    private final AtomicLong cacheBytes = new AtomicLong();
    private final AtomicLong flushBytes = new AtomicLong();
    private final AtomicLong flushRows = new AtomicLong();

    private final AtomicReference<State> state;

    private volatile String label;
    private volatile Future<?> responseFuture;
    private volatile long lastWriteTimeMillis = Long.MAX_VALUE;
    private volatile boolean flushing;

    public StreamTableRegion(String uniqueKey,
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
        this.chunkLimit = properties.getChunkLimit();
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
        return StreamLoadEntityMeta.CHUNK_ENTITY_META;
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
    public int write(byte[] row) {
        try {
            buffer.put(row);
            if (row != END_STREAM) {
                cacheBytes.addAndGet(row.length);
                lastWriteTimeMillis = System.currentTimeMillis();
            } else {
                log.info("Write EOF");
            }
            return row.length;
        } catch (InterruptedException ignored) {
        }
        return 0;
    }

    private final AtomicLong totalFlushBytes = new AtomicLong();
    private volatile boolean endStream;
    private volatile byte[] next;

    @Override
    public byte[] read() {
        if (state.get() != State.ACTIVE) {
            if (!flushing) {
                flushing = true;
            }
            try {
                byte[] row;
                if (next == null) {
                    row = buffer.take();
                } else {
                    row = next;
                }
                if (row == END_STREAM) {
                    endStream = true;
                    flushing = false;
                    log.info("Read EOF");
                    return null;
                }
                final int delimiterL = dataFormat.delimiter() == null ? 0 : dataFormat.delimiter().length;

                if (totalFlushBytes.get() + row.length + delimiterL > chunkLimit) {
                    next = row;
                    flushing = false;
                    log.info("Read part EOF");
                    return null;
                }
                next = null;
                totalFlushBytes.addAndGet(row.length + delimiterL);
                cacheBytes.addAndGet(-row.length);
                flushBytes.addAndGet(row.length);
                flushRows.incrementAndGet();
                return row;
            } catch (InterruptedException e) {
                log.info("read queue interrupted, msg : {}", e.getMessage());
            }
        }
        return null;
    }

    protected void flip() {
        flushBytes.set(0L);
        flushRows.set(0L);
        responseFuture = null;

        final int initSize = (dataFormat.first() == null ? 0 : dataFormat.first().length)
                + (dataFormat.end() == null ? 0 : dataFormat.end().length)
                - (dataFormat.delimiter() == null ? 0 : dataFormat.delimiter().length);
        totalFlushBytes.set(initSize);
        endStream = false;
    }

    @Override
    public boolean testPrepare() {
        return state.compareAndSet(State.ACTIVE, State.PREPARE);
    }

    @Override
    public boolean prepare() {
        if (streamLoad()) {
            return true;
        }
        cancel();
        return false;
    }

    @Override
    public boolean flush() {
        if (state.compareAndSet(State.PREPARE, State.COMMIT)) {
            write(END_STREAM);
            resetAge();
            log.info("uk : {}, label : {} commit", uniqueKey, label);
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
        if (!endStream) {
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

    protected boolean streamLoad() {
        try {
            flip();
            setResult(streamLoader.send(this));
            return true;
        } catch (Exception e) {
            callback(e);
        }

        return false;
    }
}
