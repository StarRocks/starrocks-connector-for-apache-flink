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

import com.starrocks.data.load.stream.Chunk;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.http.StreamLoadEntityMeta;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
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
    private final AtomicLong age = new AtomicLong(0L);
    private final AtomicLong cacheBytes = new AtomicLong();
    private final AtomicReference<State> state;
    private final AtomicBoolean ctl = new AtomicBoolean(false);
    private volatile Chunk activeChunk;
    private final ConcurrentLinkedQueue<Chunk> inactiveChunks = new ConcurrentLinkedQueue<>();
    private volatile String label;
    private volatile Future<?> responseFuture;
    private volatile long lastCommitTimeMills;
    private final int maxRetries;
    private final int retryIntervalInMs;
    private volatile int numRetries;
    private volatile long lastFailTimeMs;

    public TransactionTableRegion(String uniqueKey,
                            String database,
                            String table,
                            StreamLoadManager manager,
                            StreamLoadTableProperties properties,
                            StreamLoader streamLoader,
                            int maxRetries,
                            int retryIntervalInMs) {
        this.uniqueKey = uniqueKey;
        this.database = database;
        this.table = table;
        this.manager = manager;
        this.properties = properties;
        this.streamLoader = streamLoader;
        this.state = new AtomicReference<>(State.ACTIVE);
        this.lastCommitTimeMills = System.currentTimeMillis();
        this.activeChunk = new Chunk(properties.getDataFormat());
        this.maxRetries = maxRetries;
        this.retryIntervalInMs = retryIntervalInMs;
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

    private void switchChunk() {
        if (activeChunk == null) {
            return;
        }
        inactiveChunks.add(activeChunk);
        activeChunk = new Chunk(properties.getDataFormat());
    }

    protected int write0(byte[] row) {
        if (activeChunk.estimateChunkSize(row) > properties.getChunkLimit()) {
            switchChunk();
        }

        activeChunk.addRow(row);
        cacheBytes.addAndGet(row.length);
        return row.length;
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
                    if (activeChunk.numRows() > 0) {
                        switchChunk();
                    }
                    ctl.set(false);
                    break;
                }
            }
            if (!inactiveChunks.isEmpty()) {
                streamLoad(0);
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
                fail(e);
                return false;
            }

            label = null;
            long commitTime = System.currentTimeMillis();
            long commitDuration = commitTime - lastCommitTimeMills;
            lastCommitTimeMills = commitTime;
            commitSuccess = true;
            LOG.info("Success to commit transaction: {}, duration: {} ms", transaction, commitDuration);
        } else {
            // if the data has never been flushed (label == null), the commit should fail so that StreamLoadManagerV2#init
            // will schedule to flush the data first, and then trigger commit again
            commitSuccess = cacheBytes.get() == 0;
        }

        state.compareAndSet(State.COMMITTING, State.ACTIVE);
        return commitSuccess;
    }

    @Override
    public void fail(Throwable e) {
        if (numRetries >= maxRetries) {
            manager.callback(e);
            return;
        }
        responseFuture = null;
        numRetries += 1;
        lastFailTimeMs = System.currentTimeMillis();
        LOG.warn("Failed to flush data for db: {}, table: {}, and will retry for {} times after {} ms",
                database, table, numRetries, retryIntervalInMs, e);
        streamLoad(retryIntervalInMs);
    }

    @Override
    public void complete(StreamLoadResponse response) {
        Chunk chunk = inactiveChunks.remove();
        cacheBytes.addAndGet(-chunk.rowBytes());
        response.setFlushBytes(chunk.rowBytes());
        response.setFlushRows(chunk.numRows());
        manager.callback(response);
        numRetries = 0;

        LOG.info("Stream load flushed, db: {}, table: {}, label : {}", database, table, label);
        if (!inactiveChunks.isEmpty()) {
            LOG.info("Stream load continue, db: {}, table: {}, label : {}", database, table, label);
            streamLoad(0);
            return;
        }
        if (state.compareAndSet(State.FLUSHING, State.ACTIVE)) {
            LOG.info("Stream load completed, db: {}, table: {}, label : {}", database, table, label);
        }
    }

    @Override
    public Future<?> getResult() {
        return responseFuture;
    }

    protected void streamLoad(int delayMs) {
        try {
            Chunk chunk = inactiveChunks.peek();
            LOG.info("Stream load chunk, db: {}, table: {}, numRows: {}, rowBytes: {}, chunkBytes: {}",
                    database, table, chunk.numRows(), chunk.rowBytes(), chunk.chunkBytes());
            responseFuture = streamLoader.send(this, delayMs);
        } catch (Exception e) {
            fail(e);
        }
    }

    @Override
    public HttpEntity getHttpEntity() {
        return new ChunkHttpEntity(uniqueKey, inactiveChunks.peek());
    }

    @Override
    public long getLastWriteTimeMillis() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setResult(Future<?> result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void callback(StreamLoadResponse response) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFlushBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] read() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamLoadEntityMeta getEntityMeta() {
        throw new UnsupportedOperationException();
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
