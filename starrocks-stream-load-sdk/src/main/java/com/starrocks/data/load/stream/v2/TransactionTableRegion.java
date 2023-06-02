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

import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
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

public class TransactionTableRegion implements TableRegion {

    enum State {
        ACTIVE,
        FLUSHING,
        COMMITTING
    }

    private static final Logger LOG = LoggerFactory.getLogger(TransactionTableRegion.class);

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

    private volatile long lastCommitTimeMills;

    public TransactionTableRegion(String uniqueKey,
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
        this.lastCommitTimeMills = System.currentTimeMillis();
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
    public boolean isFlushing() {
        return state.get() == State.FLUSHING;
    }

    @Override
    public boolean flush() {
        if (state.compareAndSet(State.ACTIVE, State.FLUSHING)) {
            for (;;) {
                if (ctl.compareAndSet(false, true)) {
                    LOG.info("Flush uniqueKey : {}, label : {}, bytes : {}", uniqueKey, label, cacheBytes.get());
                    inBuffer = outBuffer;
                    outBuffer = null;
                    ctl.set(false);
                    break;
                }
            }
            if (inBuffer != null && !inBuffer.isEmpty()) {
                streamLoad();
                return true;
            } else {
                state.compareAndSet(State.FLUSHING, State.ACTIVE);
                return false;
            }
        }
        return false;
    }

    public boolean commit() {
        if (!state.compareAndSet(State.ACTIVE, State.COMMITTING)) {
            return false;
        }

        boolean commitSuccess;
        if (label != null) {
            StreamLoadSnapshot.Transaction transaction = new StreamLoadSnapshot.Transaction(database, table, label);
            try {
                if (!streamLoader.prepare(transaction)) {
                    String errorMsg = "Failed to prepare transaction, please check taskmanager log for details, " + transaction;
                    throw new StreamLoadFailException(errorMsg);
                }

                if (!streamLoader.commit(transaction)) {
                    String errorMsg = "Failed to commit transaction, please check taskmanager log for details, " + transaction;
                    throw new StreamLoadFailException(errorMsg);
                }
            } catch (Exception e) {
                LOG.error("TransactionTableRegion commit failed, db: {}, table: {}, label: {}", database, table, label, e);
                callback(e);
                return false;
            }

            label = null;
            long commitTime = System.currentTimeMillis();
            long commitDuration = commitTime - lastCommitTimeMills;
            lastCommitTimeMills = commitTime;
            commitSuccess = true;
            LOG.info("Success to commit transaction: {}, duration: {} ms", transaction, commitDuration);
        } else {
            // if the data has never been flushed (label == null), the commit should fail so that StarRocksSinkManagerV2#init
            // will schedule to flush the data first, and then trigger commit again
            commitSuccess = cacheBytes.get() == 0;
        }

        state.compareAndSet(State.COMMITTING, State.ACTIVE);
        return commitSuccess;
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

        LOG.info("Stream load flushed, db: {}, table: {}, label : {}", database, table, label);
        if (!inBuffer.isEmpty()) {
            LOG.info("Stream load continue, db: {}, table: {}, label : {}", database, table, label);
            streamLoad();
            return;
        }
        if (state.compareAndSet(State.FLUSHING, State.ACTIVE)) {
            LOG.info("Stream load completed, db: {}, table: {}, label : {}", database, table, label);
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

    protected void flip() {
        flushBytes.set(0L);
        flushRows.set(0L);
        responseFuture = null;

        StreamLoadEntityMeta chunkMeta = genEntityMeta();
        this.entityMeta = chunkMeta;
        LOG.info("Generate entity meta, db: {}, table: {}, total rows : {}, entity rows : {}, entity bytes : {}",
                database, table, inBuffer.size(), chunkMeta.getRows(), chunkMeta.getBytes());
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

    @Override
    public boolean testPrepare() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean prepare() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadable() {
        throw new UnsupportedOperationException();
    }
}
