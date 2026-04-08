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
import com.starrocks.data.load.stream.LabelGenerator;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.compress.CompressionCodec;
import com.starrocks.data.load.stream.compress.CompressionHttpEntity;
import com.starrocks.data.load.stream.compress.LZ4FrameCompressionCodec;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.http.StreamLoadEntityMeta;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static com.starrocks.data.load.stream.exception.ErrorUtils.isRetryable;

public class TransactionTableRegion implements TableRegion {

    enum State {
        ACTIVE,
        FLUSHING,
        COMMITTING
    }

    private static final Logger LOG = LoggerFactory.getLogger(TransactionTableRegion.class);

    private final StreamLoadManager manager;
    private final StreamLoader streamLoader;
    private final LabelGenerator labelGenerator;
    private final String uniqueKey;
    private final String database;
    private final String table;
    private final StreamLoadTableProperties properties;
    private final Map<String, String> headers = new HashMap<>();
    private final Optional<CompressionCodec> compressionCodec;
    private final AtomicLong age = new AtomicLong(0L);
    private final AtomicLong cacheBytes = new AtomicLong();
    private final AtomicLong cacheRows = new AtomicLong();
    private final AtomicReference<State> state;
    private final AtomicBoolean writeLock = new AtomicBoolean(false);
    private final AtomicLong chunkIdGenerator = new AtomicLong(0);
    private volatile Chunk activeChunk;
    private final ConcurrentLinkedQueue<Chunk> inactiveChunks = new ConcurrentLinkedQueue<>();
    private volatile String label;
    private volatile Future<?> responseFuture;
    private volatile long lastCommitTimeMills;
    private final int maxRetries;
    private final int retryIntervalInMs;
    private volatile int numRetries;

    // First exception if retry many times
    private volatile Throwable firstException;

    // Multi-table transaction mode flag
    private final boolean multiTableTransactionEnabled;

    // Minimum interval (ms) between two switchChunkForCommit calls on this region.
    // Only meaningful when multiTableTransactionEnabled. Used to batch multiple
    // source transactions into a single inactive chunk, reducing HTTP request
    // overhead and chunk metadata fragmentation.
    private final long miniSwitchIntervalMs;

    // Timestamp (epoch ms) of the last switchChunkForCommit on this region.
    // Initial value 0 ensures the first txnEnd always triggers a switch.
    // Only meaningful when multiTableTransactionEnabled.
    private volatile long lastSwitchTimeMs = 0L;

    // Whether activeChunk is currently at a "clean transaction boundary".
    // - true: the most recent task-thread event on this region was either
    //   a txnEnd (setCommitAllowed) or a switch; all data in activeChunk
    //   belongs to fully-committed source transactions and is safe to freeze.
    // - false: a write occurred after the most recent txnEnd; activeChunk may
    //   contain data from an in-progress source transaction and must NOT be
    //   switched until the next txnEnd arrives.
    // Only meaningful when multiTableTransactionEnabled.
    private volatile boolean activeChunkCleanBoundary = true;

    public TransactionTableRegion(String uniqueKey,
                            String database,
                            String table,
                            StreamLoadManager manager,
                            StreamLoadTableProperties properties,
                            StreamLoader streamLoader,
                            LabelGenerator labelGenerator,
                            int maxRetries,
                            int retryIntervalInMs) {
        this(uniqueKey, database, table, manager, properties, streamLoader,
                labelGenerator, maxRetries, retryIntervalInMs, false, 0L);
    }

    public TransactionTableRegion(String uniqueKey,
                            String database,
                            String table,
                            StreamLoadManager manager,
                            StreamLoadTableProperties properties,
                            StreamLoader streamLoader,
                            LabelGenerator labelGenerator,
                            int maxRetries,
                            int retryIntervalInMs,
                            boolean multiTableTransactionEnabled,
                            long miniSwitchIntervalMs) {
        this.uniqueKey = uniqueKey;
        this.database = database;
        this.table = table;
        this.manager = manager;
        this.properties = properties;
        this.streamLoader = streamLoader;
        this.labelGenerator = labelGenerator;
        initHeaders(properties);
        this.compressionCodec = CompressionCodec.createCompressionCodec(
                properties.getDataFormat(),
                properties.getProperty("compression"),
                properties.getTableProperties());
        this.state = new AtomicReference<>(State.ACTIVE);
        this.lastCommitTimeMills = System.currentTimeMillis();
        this.activeChunk = new Chunk(properties.getDataFormat(), chunkIdGenerator.getAndIncrement());
        this.maxRetries = maxRetries;
        this.retryIntervalInMs = retryIntervalInMs;
        this.multiTableTransactionEnabled = multiTableTransactionEnabled;
        this.miniSwitchIntervalMs = miniSwitchIntervalMs;
    }

    private void initHeaders(StreamLoadTableProperties properties) {
        headers.putAll(properties.getProperties());
        // Include db and table headers so that transaction stream load
        // (/api/transaction/load) routes data to the correct table.
        // In multi-table transaction mode, each region targets a different
        // table under the same shared label, so these headers are required.
        headers.put("db", database);
        headers.put("table", table);
        Optional<String> compressionType = properties.getProperty("compression");
        // To enable csv compression, at the connector side, the user need to set two properties:
        // "format = csv" and "compression = <compression type>". It needs to be converted to one
        // header "format = <compression type>" which matches the server usage. In the future, the
        // server will be refactored to configure the compression type in the same way as the connector,
        // and this conversion will be removed.
        if (properties.getDataFormat() instanceof StreamLoadDataFormat.CSVFormat && compressionType.isPresent()) {
            // You can see the format name for different compression types here
            // https://github.com/StarRocks/starrocks/blob/main/be/src/http/action/stream_load.cpp#L96
            if (LZ4FrameCompressionCodec.NAME.equalsIgnoreCase(compressionType.get())) {
                headers.put("format", "lz4");
            } else {
                throw new UnsupportedOperationException(
                        "CSV format does not support compression type: " + compressionType.get());
            }

        }
    }

    @Override
    public StreamLoadTableProperties getProperties() {
        return properties;
    }

    @Override
    public Map<String, String> getHeaders() {
        return headers;
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
    public LabelGenerator getLabelGenerator() {
        return labelGenerator;
    }

    @Override
    public synchronized void setLabel(String label) {
        // When a retry is in progress (numRetries > 0), skip setting a new non-null label
        // to avoid overwriting the label being used by the in-flight retry.
        //
        // In the non-multi-table path, TransactionStreamLoader.begin() checks
        // (label == null) before calling setLabel(), so this branch is normally
        // unreachable. It serves as a safety net in case of unexpected concurrent
        // access from the manager thread (e.g. ensureSharedTransaction in multi-table mode).
        //
        // We use synchronized (consistent with fail() and isRetrying()) to make the
        // numRetries check and label assignment atomic, and log a warning for
        // debuggability, but do NOT throw — throwing would be a behavior change that
        // could break the non-multi-table retry path if reached under rare timing.
        if (numRetries > 0 && label != null) {
            LOG.warn("setLabel called with label={} while numRetries={}, existing label={}. "
                    + "Skipping to preserve retry consistency.", label, numRetries, this.label);
            return;
        }
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

    /** Returns the current state as a string for logging purposes. */
    public String getStateForLog() {
        return state.get().name();
    }

    /** Returns {@code true} if the region is currently retrying a failed HTTP load. */
    public synchronized boolean isRetrying() {
        return numRetries > 0;
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

    private static final int MAX_SPIN_ATTEMPTS = 10;
    private static final long SPIN_BACKOFF_NANOS = 1000L; // 1 microsecond initial backoff

    @Override
    public int write(byte[] row) {
        if (row == null) {
            return 0;
        }

        int spins = 0;
        for (;;) {
            if (writeLock.compareAndSet(false, true)) {
                try {
                    return write0(row);
                } finally {
                    writeLock.set(false);
                }
            }
            if (spins < MAX_SPIN_ATTEMPTS) {
                Thread.yield();
                spins++;
            } else {
                LockSupport.parkNanos(SPIN_BACKOFF_NANOS * (spins - MAX_SPIN_ATTEMPTS + 1));
                spins++;
            }
        }
    }

    private void switchChunk() {
        if (activeChunk == null || activeChunk.numRows() == 0) {
            return;
        }
        inactiveChunks.add(activeChunk);
        activeChunk = new Chunk(properties.getDataFormat(), chunkIdGenerator.getAndIncrement());
    }

    /**
     * Moves the active chunk to the inactive queue so the manager thread can flush it.
     *
     * <p>Called from two sites:
     * <ul>
     *   <li><b>Task thread</b> — on txnEnd marker (multi-table mode)</li>
     *   <li><b>Manager thread</b> — during savepoint in {@code DefaultStreamLoadManager}.
     *       At savepoint time the task thread is blocked in {@code flush()},
     *       so there is no concurrent {@link #write(byte[])} call.</li>
     * </ul>
     *
     * <p>The {@code writeLock} is still acquired for safety.
     */
    public void switchChunkForCommit() {
        int spins = 0;
        for (;;) {
            if (writeLock.compareAndSet(false, true)) {
                try {
                    switchChunk();
                    if (multiTableTransactionEnabled) {
                        // Record the switch time for miniInterval bookkeeping.
                        // The new activeChunk (created by switchChunk) is empty,
                        // so it is trivially at a clean transaction boundary.
                        lastSwitchTimeMs = System.currentTimeMillis();
                        activeChunkCleanBoundary = true;
                    }
                } finally {
                    writeLock.set(false);
                }
                LOG.debug("[MultiTxn] switchChunkForCommit: db={}, table={}, inactiveChunks={}",
                        database, table, inactiveChunks.size());
                return;
            }
            if (spins < MAX_SPIN_ATTEMPTS) {
                Thread.yield();
                spins++;
            } else {
                LockSupport.parkNanos(SPIN_BACKOFF_NANOS * (spins - MAX_SPIN_ATTEMPTS + 1));
                spins++;
            }
        }
    }

    /**
     * Called by the manager thread during the commitInFlight phase.
     *
     * <p>If the region has inactive chunks that have not yet been sent (state is ACTIVE),
     * transitions to FLUSHING and starts the HTTP load.  This is different from
     * {@link #flush(FlushReason)} in that it does NOT call {@link #switchChunk()} —
     * the task thread has already done that via {@link #switchChunkForCommit()}.
     *
     * @return {@code true} if a load was triggered, {@code false} otherwise
     */
    public boolean triggerLoadIfNeeded() {
        if (inactiveChunks.isEmpty()) {
            // No data to load (region had no data when txnEnd arrived)
            return false;
        }
        if (state.compareAndSet(State.ACTIVE, State.FLUSHING)) {
            LOG.info("[MultiTxn] triggerLoadIfNeeded: db={}, table={}, label={}, inactiveChunks={}, cacheBytes={}",
                    database, table, label, inactiveChunks.size(), cacheBytes.get());
            streamLoad(0);
            return true;
        }
        // Already FLUSHING or COMMITTING — load is in progress
        return false;
    }

    /**
     * Called on the task thread when a source txnEnd is received for the
     * partition owning this region. Performs three bookkeeping steps:
     *
     * <ol>
     *   <li>Mark the activeChunk as being at a "clean transaction boundary":
     *       regardless of whether a switch actually happens, the most recent
     *       event on this region is now a txnEnd, which means all data in
     *       the current activeChunk belongs to fully-committed source
     *       transactions.</li>
     *   <li>If the miniInterval has elapsed since the last switch AND there
     *       is data to freeze, call {@link #switchChunkForCommit()} to move
     *       the activeChunk into the inactive queue where it can be drained
     *       by autonomous flush.</li>
     *   <li>Otherwise, leave activeChunk untouched — additional source
     *       transactions can still accumulate into it, batching multiple
     *       complete transactions into a single HTTP request when the next
     *       switch does happen.</li>
     * </ol>
     *
     * <p>Only meaningful in multi-table transaction mode. Callers must not
     * invoke this method when {@code multiTableTransactionEnabled} is false.
     */
    public void tryMiniIntervalSwitch() {
        // Step 1: The task thread has just observed a txnEnd for this region.
        // Mark the current activeChunk as clean so that the manager-thread
        // fallback can force-switch it later if the source goes idle.
        // This must happen BEFORE the switch decision because switchChunkForCommit
        // itself sets cleanBoundary=true on the new activeChunk, but if we skip
        // the switch we still want the old activeChunk flagged as clean.
        activeChunkCleanBoundary = true;

        // Step 2: Only switch if miniInterval has elapsed AND the activeChunk
        // has data. An empty activeChunk would produce an empty inactive chunk
        // which wastes a chunk slot and an HTTP request.
        long now = System.currentTimeMillis();
        if (now - lastSwitchTimeMs >= miniSwitchIntervalMs
                && activeChunk != null && activeChunk.numRows() > 0) {
            switchChunkForCommit();
        }
    }

    /**
     * Called by the manager thread during its normal scan loop. If the
     * activeChunk is at a clean transaction boundary, has data, and the
     * miniInterval has elapsed since the last switch, force-switch it so
     * the data can be drained by autonomous flush and committed on the
     * next commit cycle.
     *
     * <p>This is the "source idle" fallback: when the upstream source pauses
     * after a few txnEnds and no further task-thread events arrive, the
     * manager thread takes over the responsibility of freezing completed
     * transaction data into inactiveChunks.
     *
     * <p><b>Safety:</b> The method acquires writeLock and re-checks the
     * clean-boundary flag under the lock. This guarantees that if a task
     * thread write is concurrently in progress, either (a) the task thread
     * runs first and the re-check sees {@code false} (we abort), or (b) the
     * force-switch runs first and the task thread's subsequent write targets
     * a fresh activeChunk.
     *
     * <p>Only meaningful in multi-table transaction mode.
     *
     * @return {@code true} if a switch was performed, {@code false} otherwise
     */
    public boolean tryForceCleanSwitch() {
        // Fast path checks without acquiring the lock.
        // Note: cacheRows covers both activeChunk and inactiveChunks, so it is
        // a coarser filter than we need here. Check activeChunk.numRows()
        // directly so a region whose inactiveChunks still have pending data but
        // whose activeChunk is empty doesn't force us to acquire the lock just
        // to bail out inside it.
        if (!activeChunkCleanBoundary) {
            return false;
        }
        Chunk snapshot = activeChunk;  // volatile load
        if (snapshot == null || snapshot.numRows() == 0) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (now - lastSwitchTimeMs < miniSwitchIntervalMs) {
            return false;
        }

        // Slow path: acquire writeLock and re-check under the lock
        if (!writeLock.compareAndSet(false, true)) {
            // Task thread holds the lock, retry on next scan
            return false;
        }
        try {
            // Re-check clean boundary under the lock: if a task thread write
            // happened between the fast-path check and now, cleanBoundary will
            // be false and we must abort to avoid freezing partial transaction
            // data.
            if (!activeChunkCleanBoundary) {
                return false;
            }
            if (activeChunk == null || activeChunk.numRows() == 0) {
                return false;
            }
            switchChunk();
            lastSwitchTimeMs = System.currentTimeMillis();
            activeChunkCleanBoundary = true;
            LOG.debug("[MultiTxn] tryForceCleanSwitch: db={}, table={}, inactiveChunks={}",
                    database, table, inactiveChunks.size());
            return true;
        } finally {
            writeLock.set(false);
        }
    }

    /**
     * Returns {@code true} if the activeChunk is currently at a clean
     * transaction boundary (all data belongs to completed source transactions).
     */
    public boolean isActiveChunkCleanBoundary() {
        return activeChunkCleanBoundary;
    }

    /** Returns {@code true} if {@code inactiveChunks} is non-empty. */
    public boolean hasInactiveChunks() {
        return !inactiveChunks.isEmpty();
    }

    /** Returns the timestamp (epoch ms) of the last switchChunkForCommit. */
    public long getLastSwitchTimeMs() {
        return lastSwitchTimeMs;
    }

    protected int write0(byte[] row) {
        if (!multiTableTransactionEnabled) {
            // Non-multi-table: original behavior — switch when a single row would
            // exceed chunk size or row limits, so individual HTTP requests stay
            // bounded.
            if (activeChunk.estimateChunkSize(row) > properties.getChunkLimit()
                    || activeChunk.numRows() >= properties.getMaxBufferRows()) {
                switchChunk();
            }
        }
        // Multi-table mode: do NOT switch mid-transaction. A switch at this point
        // would move partial source-transaction data into inactiveChunks, which
        // the manager's commit path may then load under the shared label before
        // the source transaction has reached its txnEnd. Instead, activeChunk
        // grows until the next setCommitAllowed (txnEnd) triggers a clean switch.
        // Memory is bounded by blockIfCacheFull via maxWriteBlockCacheBytes.

        activeChunk.addRow(row);
        cacheBytes.addAndGet(row.length);
        cacheRows.incrementAndGet();

        if (multiTableTransactionEnabled) {
            // A write after a clean boundary transitions the region to "dirty":
            // activeChunk now holds at least one row from a source transaction
            // whose txnEnd has not yet arrived. The manager thread must not
            // force-switch in this state.
            activeChunkCleanBoundary = false;
        }
        return row.length;
    }

    @Override
    public boolean isFlushing() {
        return state.get() == State.FLUSHING;
    }

    public FlushReason shouldFlush() {
        if (state.get() != State.ACTIVE) {
            return FlushReason.NONE;
        }
        return cacheRows.get() >= properties.getMaxBufferRows() ? FlushReason.BUFFER_ROWS_REACH_LIMIT : FlushReason.NONE;
    }

    public boolean flush(FlushReason reason) {
        LOG.debug("Try to flush db: {}, table: {}, label: {}, cacheBytes: {}, cacheRows: {}, reason: {}",
                database, table, label, cacheBytes, cacheRows, reason);
        if (state.compareAndSet(State.ACTIVE, State.FLUSHING)) {
            if (!multiTableTransactionEnabled) {
                // Non-multi-table: original behavior — acquire writeLock and
                // optionally switch activeChunk into the inactive queue before
                // streaming.
                int spins = 0;
                for (;;) {
                    if (writeLock.compareAndSet(false, true)) {
                        try {
                            if (reason != FlushReason.BUFFER_ROWS_REACH_LIMIT ||
                                    activeChunk.numRows() >= properties.getMaxBufferRows()) {
                                switchChunk();
                            }
                        } finally {
                            writeLock.set(false);
                        }
                        break;
                    }
                    if (spins < MAX_SPIN_ATTEMPTS) {
                        Thread.yield();
                        spins++;
                    } else {
                        LockSupport.parkNanos(SPIN_BACKOFF_NANOS * (spins - MAX_SPIN_ATTEMPTS + 1));
                        spins++;
                    }
                }
            }
            // Multi-table mode: never touch activeChunk here. Autonomous flush
            // only drains already-frozen inactiveChunks (produced by
            // switchChunkForCommit at txnEnd or manager-thread clean-boundary
            // fallback). This preserves the invariant that every chunk reaching
            // StarRocks under the shared label comes from a completed source
            // transaction.
            if (!inactiveChunks.isEmpty()) {
                LOG.debug("Flush db: {}, table: {}, label: {}, cacheBytes: {}, cacheRows: {}, reason: {}",
                        database, table, label, cacheBytes.get(), cacheRows.get(), reason);
                streamLoad(0);
                return true;
            } else {
                state.compareAndSet(State.FLUSHING, State.ACTIVE);
                return false;
            }
        }
        return false;
    }

    // Commit the load asynchronously
    // 1. commit() should not be called concurrently
    // 2. because commit is executed asynchronously, the caller should poll the
    //    method to see if it executes successfully
    // 3. a true returned value indicates a successful commit, and a false value
    //    indicates the commit should not be triggered, such as it is FLUSHING,
    //    or it's still doing commit asynchronously
    public boolean commit() {
        LOG.debug("Try to commit, db: {}, table: {}, label: {}", database, table, label);
        boolean commitTriggered = false;
        if (!state.compareAndSet(State.ACTIVE, State.COMMITTING)) {
            if (state.get() != State.COMMITTING) {
                return false;
            }
            commitTriggered = true;
        }

        if (commitTriggered) {
            // label will be set to null after commit executes successfully
            if (label == null) {
                state.compareAndSet(State.COMMITTING, State.ACTIVE);
                LOG.debug("Success to commit, db: {}, table: {}", database, table);
                return true;
            } else {
                // wait for the commit to finish
                return false;
            }
        }

        if (label == null) {
            // if the data has never been flushed (label == null), the commit should fail
            // so that DefaultStreamLoadManager#init will schedule to flush the data first, and
            // then trigger commit again
            boolean commitSuccess = cacheBytes.get() == 0;
            state.compareAndSet(State.COMMITTING, State.ACTIVE);
            if (commitSuccess) {
                LOG.debug("Success to commit, db: {}, table: {}", database, table);
            }
            return commitSuccess;
        }

        try {
            streamLoader.getExecutorService().submit(this::doCommit);
        } catch (Exception e) {
            LOG.error("Failed to submit commit task, db: {}, table: {}, label: {}", database, table, label, e);
            throw e;
        }

        // wait for the commit to finish
        return false;
    }

    private void doCommit() {
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
        } catch (Throwable e) {
            LOG.error("TransactionTableRegion commit failed, db: {}, table: {}, label: {}", database, table, label, e);
            fail(e);
            return;
        }

        long commitTime = System.currentTimeMillis();
        long commitDuration = commitTime - lastCommitTimeMills;
        lastCommitTimeMills = commitTime;
        label = null;
        LOG.info("Success to commit transaction: {}, duration: {} ms", transaction, commitDuration);
    }

    @Override
    public void fail(Throwable e) {
        if (firstException == null) {
            firstException = e;
        }

        // Synchronize on 'this' so that the numRetries increment is atomic with
        // respect to the check in setLabel(), preventing label injection mid-retry.
        synchronized (this) {
            if (numRetries >= maxRetries || !isRetryable(e)) {
                LOG.error("Failed to flush data for db: {}, table: {} after {} times retry, the last exception is",
                        database, table, numRetries, e);
                manager.callback(firstException);
                return;
            }
            responseFuture = null;
            numRetries += 1;
        }
        LOG.warn("Failed to flush data for db: {}, table: {}, and will retry for {} times after {} ms",
                database, table, numRetries, retryIntervalInMs, e);
        streamLoad(retryIntervalInMs);
    }

    @Override
    public void complete(StreamLoadResponse response) {
        Chunk chunk = inactiveChunks.remove();
        cacheBytes.addAndGet(-chunk.rowBytes());
        cacheRows.addAndGet(-chunk.numRows());
        response.setFlushBytes(chunk.rowBytes());
        response.setFlushRows(chunk.numRows());
        manager.callback(response);
        synchronized (this) {
            numRetries = 0;
            firstException = null;
        }

        if (!inactiveChunks.isEmpty()) {
            LOG.debug("Stream load continue, db: {}, table: {}, label: {}, cacheBytes: {}, cacheRows: {}",
                    database, table, label, cacheBytes, cacheRows);
            streamLoad(0);
            return;
        }
        if (state.compareAndSet(State.FLUSHING, State.ACTIVE)) {
            LOG.debug("Stream load completed, db: {}, table: {}, label: {}, cacheBytes: {}, cacheRows: {}",
                    database, table, label, cacheBytes, cacheRows);
        }
    }

    @Override
    public Future<?> getResult() {
        return responseFuture;
    }

    protected void streamLoad(int delayMs) {
        try {
            Chunk chunk = inactiveChunks.peek();
            if (chunk == null) {
                LOG.warn("No inactive chunk available for stream load, db: {}, table: {}", database, table);
                state.compareAndSet(State.FLUSHING, State.ACTIVE);
                return;
            }
            LOG.debug("Stream load chunk, db: {}, table: {}, numRows: {}, rowBytes: {}, chunkBytes: {}",
                    database, table, chunk.numRows(), chunk.rowBytes(), chunk.chunkBytes());
            responseFuture = streamLoader.send(this, delayMs);
        } catch (Exception e) {
            state.compareAndSet(State.FLUSHING, State.ACTIVE);
            fail(e);
        }
    }

    @Override
    public HttpEntity getHttpEntity() {
        Chunk chunk = inactiveChunks.peek();
        if (chunk == null) {
            throw new IllegalStateException("No inactive chunk available for HTTP entity, db: " + database + ", table: " + table);
        }
        ChunkHttpEntity entity = new ChunkHttpEntity(uniqueKey, chunk);
        return compressionCodec
                .map(codec -> (HttpEntity) new CompressionHttpEntity(entity, codec))
                .orElse(entity);
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

    @Override
    public boolean flush() { throw new UnsupportedOperationException(); }
}
