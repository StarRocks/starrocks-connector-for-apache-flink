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

import com.starrocks.data.load.stream.exception.ErrorUtils;

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

    // Hard upper bound (bytes) for activeChunk while an in-progress source
    // transaction is being accumulated. Only meaningful in multi-table mode.
    //
    // Multi-table mode deliberately disables size/row-based chunk switching
    // (see write0) to keep a source transaction atomic under the shared
    // label, which means activeChunk cannot be drained until the next txnEnd
    // arrives. If a single region's activeChunk grows past the task thread's
    // blockIfCacheFull hard threshold (maxWriteBlockCacheBytes), deadlock is
    // inevitable because the manager has no inactiveChunks to flush. Fail
    // fast when this threshold is exceeded so the user gets a clear error
    // instead of a silent hang.
    //
    // A value of 0 disables the check (used by the legacy constructor).
    private final long multiTableSingleTxnMaxBytes;

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

    // Bytes written into activeChunk since the most recent clean-boundary
    // transition (true -> false), i.e. bytes that belong to the currently
    // in-progress source transaction on this region. Updated under writeLock in
    // write0() and in the boundary-restoring code paths.
    //
    // Every increment here is mirrored (+delta) onto the manager's aggregate
    // in-progress counter, and every time this region transitions back to a
    // clean boundary the accumulated value is mirrored (-delta) back to that
    // counter before being reset to 0. See DefaultStreamLoadManager.
    //   aggregateInProgressTxnBytes for the deadlock-avoidance motivation.
    //
    // Only meaningful when multiTableTransactionEnabled.
    private long inProgressTxnBytes = 0L;

    // Non-null when {@link #manager} is a DefaultStreamLoadManager. Resolved
    // once in the constructor so the aggregate-tracking hot path does not need
    // an instanceof check per row. When null (production builds never hit this
    // — all regions are created by DefaultStreamLoadManager), aggregate tracking
    // degrades gracefully and the deadlock guard is skipped.
    private final DefaultStreamLoadManager aggregateTracker;

    // Per-(db,table) load serialization gate shared across all partition-regions
    // of the same table (multi-table transaction mode). The server serves one
    // in-flight /api/transaction/load per (label, table) channel — the FE
    // multi-statement task holds a single-channel sub-task per table, and the
    // BE guards the txn context with a per-label try_lock — so concurrent sends
    // from sibling partition-regions are rejected with the transient
    // TXN_IN_PROCESSING status. Acquired when this region enters FLUSHING,
    // released when it returns to ACTIVE. Null disables serialization
    // (non-multi-table mode).
    private volatile AtomicBoolean tableLoadGate;

    // Highest chunkId ever moved into inactiveChunks on this region. Used as the
    // source value for the commit watermark. Only meaningful when
    // multiTableTransactionEnabled.
    private volatile long lastSwitchedChunkId = -1L;

    // Commit watermark: while a multi-table commit cycle is in flight, only
    // inactive chunks with chunkId <= watermark may be loaded — chunks switched
    // after the commit cut belong to the NEXT shared transaction. Long.MAX_VALUE
    // means "no restriction" (no commit in flight, or savepoint drain).
    private volatile long commitWatermarkChunkId = Long.MAX_VALUE;

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
                labelGenerator, maxRetries, retryIntervalInMs, false, 0L, 0L);
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
                            long miniSwitchIntervalMs,
                            long multiTableSingleTxnMaxBytes) {
        this.uniqueKey = uniqueKey;
        this.database = database;
        this.table = table;
        this.manager = manager;
        this.aggregateTracker = manager instanceof DefaultStreamLoadManager
                ? (DefaultStreamLoadManager) manager
                : null;
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
        this.multiTableSingleTxnMaxBytes = multiTableSingleTxnMaxBytes;
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
        // Multi-table callers that need to detect the skip must use
        // {@link #trySetLabel(String)} instead.
        trySetLabel(label);
    }

    /**
     * Atomically sets the label iff no retry is in progress, returning whether
     * the label was applied.
     *
     * <p>Unlike {@link #setLabel(String)}, the outcome is surfaced to the caller
     * rather than silently swallowed. This is used by multi-table mode's
     * {@code ensureSharedTransaction()} so that a retry starting between the
     * bulk {@code isRetrying()} check and label injection is detected and the
     * shared-transaction setup is rolled back — preventing the manager from
     * treating a region as "joined the shared transaction" when the region
     * actually still holds its previous (stale or null) label.
     *
     * @param label the label to set (may be {@code null} to clear)
     * @return {@code true} if the label was applied; {@code false} if a retry
     *         is in progress and the non-null label assignment was skipped.
     *         Clearing the label ({@code label == null}) always returns
     *         {@code true}.
     */
    public synchronized boolean trySetLabel(String label) {
        if (numRetries > 0 && label != null) {
            LOG.warn("setLabel called with label={} while numRetries={}, existing label={}. "
                    + "Skipping to preserve retry consistency.", label, numRetries, this.label);
            return false;
        }
        this.label = label;
        return true;
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
        lastSwitchedChunkId = activeChunk.getChunkId();
        inactiveChunks.add(activeChunk);
        activeChunk = new Chunk(properties.getDataFormat(), chunkIdGenerator.getAndIncrement());
    }

    /**
     * Flushes the in-progress-byte accounting back to the manager's aggregate
     * counter. Called at every transition of {@code activeChunkCleanBoundary}
     * from {@code false} to {@code true} (a txnEnd completing an in-progress
     * source transaction on this region), so that the bytes that were tracked
     * as "in-progress" stop inflating the global guard. Must be called under
     * the same serialization context that mutates the boundary flag (either
     * under {@code writeLock} or on the task thread while no concurrent write
     * is in flight).
     */
    private void releaseInProgressBytes() {
        if (!multiTableTransactionEnabled || inProgressTxnBytes == 0L) {
            return;
        }
        long delta = inProgressTxnBytes;
        inProgressTxnBytes = 0L;
        if (aggregateTracker != null) {
            aggregateTracker.addAggregateInProgressTxnBytes(-delta);
        }
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
                        // If this call freezes in-progress bytes (e.g. savepoint
                        // path switches an unfinished chunk), release them from
                        // the manager's aggregate guard now that the bytes have
                        // moved to inactiveChunks and can be flushed.
                        releaseInProgressBytes();
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
        if (!headChunkEligible()) {
            // No data to load: either the queue is empty (region had no data
            // when txnEnd arrived) or the head chunk was switched after the
            // commit cut and belongs to the next shared transaction.
            return false;
        }
        if (tryEnterFlushing()) {
            LOG.info("[MultiTxn] triggerLoadIfNeeded: db={}, table={}, label={}, inactiveChunks={}, cacheBytes={}",
                    database, table, label, inactiveChunks.size(), cacheBytes.get());
            streamLoad(0);
            return true;
        }
        // Already FLUSHING/COMMITTING, or a sibling region of the same table
        // holds the table gate — the load will be retried on the next scan.
        return false;
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

    // -----------------------------------------------------------------------
    // Per-(db,table) load serialization (multi-table transaction mode)
    // -----------------------------------------------------------------------

    /** Injects the shared per-(db,table) gate. Called once at region creation. */
    public void setTableLoadGate(AtomicBoolean gate) {
        this.tableLoadGate = gate;
    }

    /**
     * Atomically acquires the table gate (if enabled) and transitions
     * ACTIVE -> FLUSHING. All load-starting paths must go through this so that
     * at most one region per (db, table) has an in-flight load at any time.
     */
    private boolean tryEnterFlushing() {
        AtomicBoolean gate = tableLoadGate;
        if (gate != null && !gate.compareAndSet(false, true)) {
            // A sibling region of the same table is loading; retry next scan.
            return false;
        }
        if (state.compareAndSet(State.ACTIVE, State.FLUSHING)) {
            return true;
        }
        if (gate != null) {
            gate.set(false);
        }
        return false;
    }

    /** Transitions FLUSHING -> ACTIVE and releases the table gate (if held). */
    private void exitFlushing() {
        state.compareAndSet(State.FLUSHING, State.ACTIVE);
        AtomicBoolean gate = tableLoadGate;
        if (gate != null) {
            gate.set(false);
        }
    }

    // -----------------------------------------------------------------------
    // Commit watermark (multi-table transaction mode)
    // -----------------------------------------------------------------------

    /**
     * Freezes the set of chunks eligible for the in-flight commit cycle to
     * everything switched so far. Chunks switched after this call belong to
     * the next shared transaction. Called by the manager thread right after
     * the commit cut (lockstep switch of all clean-boundary partitions).
     */
    public void setCommitWatermark() {
        commitWatermarkChunkId = lastSwitchedChunkId;
    }

    /** Lifts the commit watermark. Called when the commit cycle ends. */
    public void clearCommitWatermark() {
        commitWatermarkChunkId = Long.MAX_VALUE;
    }

    /** Whether a commit watermark is currently frozen on this region. */
    public boolean hasCommitWatermark() {
        return commitWatermarkChunkId != Long.MAX_VALUE;
    }

    /** Whether the head inactive chunk is eligible under the current watermark. */
    private boolean headChunkEligible() {
        Chunk head = inactiveChunks.peek();
        return head != null && head.getChunkId() <= commitWatermarkChunkId;
    }

    /**
     * Whether this region still has watermark-eligible chunks that have not
     * been loaded. Used by the manager to decide when the commit cut has been
     * fully drained.
     */
    public boolean hasEligiblePendingChunks() {
        return headChunkEligible();
    }

    // -----------------------------------------------------------------------
    // Lockstep switching primitives (multi-table transaction mode)
    //
    // The commit cut and the manager-thread idle fallback must freeze the
    // activeChunks of ALL regions of a partition at the SAME source-transaction
    // boundary. The manager first try-locks every region of the partition,
    // re-checks the clean-boundary flag under the locks, then switches all of
    // them before releasing — so the task thread cannot advance the boundary
    // between two sibling switches.
    // -----------------------------------------------------------------------

    /** Try-acquire this region's write lock. Pair with {@link #unlockWrite()}. */
    public boolean tryLockWrite() {
        return writeLock.compareAndSet(false, true);
    }

    /** Releases the write lock acquired via {@link #tryLockWrite()}. */
    public void unlockWrite() {
        writeLock.set(false);
    }

    /** Must be called while holding the write lock. */
    public boolean isCleanBoundaryUnderLock() {
        return activeChunkCleanBoundary;
    }

    /** Must be called while holding the write lock. */
    public boolean hasActiveDataUnderLock() {
        Chunk snapshot = activeChunk;
        return snapshot != null && snapshot.numRows() > 0;
    }

    /**
     * Switches the activeChunk for commit. Must be called while holding the
     * write lock and only when {@link #isCleanBoundaryUnderLock()} is true.
     */
    public void switchForCommitUnderLock() {
        switchChunk();
        lastSwitchTimeMs = System.currentTimeMillis();
        activeChunkCleanBoundary = true;
        releaseInProgressBytes();
    }

    /**
     * Marks the activeChunk as being at a clean transaction boundary and
     * releases the in-progress byte accounting. Called on the task thread for
     * every txnEnd, regardless of whether a switch happens — the switch
     * decision is made at partition level by the manager via
     * {@code lockstepSwitchPartition()}.
     */
    public void markCleanBoundary() {
        activeChunkCleanBoundary = true;
        releaseInProgressBytes();
    }

    /**
     * Whether the activeChunk has accumulated at least half of the write-block
     * threshold. Used by the partition-level switch decision to override
     * miniInterval batching before headroom runs out (replaces the legacy
     * per-region protective switch in write0).
     */
    public boolean isActiveChunkHalfFull() {
        if (multiTableSingleTxnMaxBytes <= 0) {
            return false;
        }
        Chunk snapshot = activeChunk;
        return snapshot != null && snapshot.numRows() > 0
                && snapshot.chunkBytes() >= multiTableSingleTxnMaxBytes / 2;
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
        } else if (multiTableSingleTxnMaxBytes > 0) {
            // Multi-table mode.
            //
            // NOTE: the legacy per-region "clean-boundary safe switch" that used
            // to live here was removed: a single region switching on its own
            // breaks the cross-table alignment of the commit cut (sibling
            // regions of the same partition must freeze at the same source
            // transaction boundary). Headroom protection is now handled at
            // partition level: the txnEnd path overrides miniInterval batching
            // and lockstep-switches ALL regions of the partition when any of
            // them reports isActiveChunkHalfFull().
            //
            // (2) Fail-fast on a single oversized in-progress transaction.
            //     After any clean-boundary safe switch above, if adding this
            //     row would still push activeChunk past the write-block hard
            //     cap, the current in-progress source transaction alone is
            //     too large. Multi-table mode cannot switch activeChunk
            //     mid-transaction, so blockIfCacheFull would stall the task
            //     thread while the manager has no inactiveChunks to drain —
            //     a silent deadlock. Surface a clear error instead.
            if (activeChunk.estimateChunkSize(row) > multiTableSingleTxnMaxBytes) {
                throw new IllegalStateException(
                        "In-progress source transaction for db=" + database + ", table=" + table
                                + " exceeded the multi-table transaction write-block threshold ("
                                + multiTableSingleTxnMaxBytes + " bytes). Multi-table mode cannot "
                                + "switch activeChunk mid-transaction, so a single source transaction "
                                + "must fit within 2 * sink.transaction.multi-table.buffer-size. "
                                + "Reduce the source transaction size or increase the buffer size.");
            }
            // (3) Aggregate fail-fast across regions. Even when each region
            //     stays under multiTableSingleTxnMaxBytes, a single source
            //     transaction spanning many tables can collectively push the
            //     manager-level cache past maxWriteBlockCacheBytes. Because no
            //     region has reached a clean boundary yet, no chunk can be
            //     switched/flushed and blockIfCacheFull would park the task
            //     thread indefinitely. Guard against that aggregate deadlock
            //     here by checking the manager's in-progress byte counter
            //     before we extend activeChunk.
            if (aggregateTracker != null) {
                long projected = aggregateTracker.getAggregateInProgressTxnBytes() + row.length;
                long writeBlockLimit = aggregateTracker.getMaxWriteBlockCacheBytes();
                if (projected > writeBlockLimit) {
                    throw new IllegalStateException(
                            "Aggregate in-progress source-transaction bytes across all tables ("
                                    + projected + " bytes) would exceed the multi-table write-block "
                                    + "threshold (" + writeBlockLimit + " bytes). Multi-table mode "
                                    + "cannot switch activeChunks mid-transaction, so the combined "
                                    + "payload of a single source transaction across all tables "
                                    + "must fit within 2 * sink.transaction.multi-table.buffer-size. "
                                    + "Reduce the source transaction size or increase the buffer "
                                    + "size. Offending region: db=" + database + ", table=" + table
                                    + ".");
                }
            }
        }
        // Multi-table mode: outside the two branches above we do NOT switch
        // mid-transaction. A switch inside an in-progress source transaction
        // would move partial transaction data into inactiveChunks, which the
        // manager's commit path may then load under the shared label before
        // the transaction has reached its txnEnd — breaking source-transaction
        // atomicity. Instead, activeChunk grows until the next setCommitAllowed
        // (txnEnd) triggers a clean switch, or the clean-boundary safe switch
        // above drains previously-completed batched transactions, or the
        // fail-fast above rejects a single oversized transaction to avoid the
        // blockIfCacheFull deadlock.

        activeChunk.addRow(row);
        cacheBytes.addAndGet(row.length);
        cacheRows.incrementAndGet();

        if (multiTableTransactionEnabled) {
            // A write after a clean boundary transitions the region to "dirty":
            // activeChunk now holds at least one row from a source transaction
            // whose txnEnd has not yet arrived. The manager thread must not
            // force-switch in this state.
            activeChunkCleanBoundary = false;
            // Track this row as in-progress and mirror to the manager's
            // aggregate counter so cross-region accumulation can be bounded.
            inProgressTxnBytes += row.length;
            if (aggregateTracker != null) {
                aggregateTracker.addAggregateInProgressTxnBytes(row.length);
            }
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
        if (multiTableTransactionEnabled && !headChunkEligible()) {
            // Nothing eligible to drain (empty queue, or all pending chunks are
            // beyond the commit watermark and belong to the next transaction).
            // Check before acquiring the table gate to avoid churning it.
            return false;
        }
        if (tryEnterFlushing()) {
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
                exitFlushing();
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
            state.compareAndSet(State.COMMITTING, State.ACTIVE);
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
            // Handle commit errors directly instead of routing through fail(),
            // which is designed for the flush state machine. fail()'s retry
            // path calls streamLoad() — wrong for a commit failure — and would
            // leave the region stuck in COMMITTING. Commit failures are always
            // terminal: release COMMITTING and propagate to the manager.
            if (firstException == null) {
                firstException = e;
            }
            state.compareAndSet(State.COMMITTING, State.ACTIVE);
            manager.callback(firstException);
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

        // In multi-table transaction mode, only the transient TXN_IN_PROCESSING
        // rejection is safe to retry: the FE rejects it at channel-acquisition
        // time before ingesting any data, so a re-send cannot duplicate rows in
        // the shared transaction. Generic failures may have partially ingested
        // data into the transaction and must stay terminal.
        boolean retryableError = multiTableTransactionEnabled
                ? ErrorUtils.isTxnInProcessing(e)
                : isRetryable(e);
        // Synchronize on 'this' so that the numRetries increment is atomic with
        // respect to the check in setLabel(), preventing label injection mid-retry.
        synchronized (this) {
            if (numRetries >= maxRetries || !retryableError) {
                LOG.error("Failed to flush data for db: {}, table: {} after {} times retry, the last exception is",
                        database, table, numRetries, e);
                // Terminal failure: no further retry will re-drive the state
                // machine back to ACTIVE via complete(). Release FLUSHING (and
                // the table gate) now so the manager's final drain / rollback
                // paths observe a non-busy region. The manager will see this.e
                // from callback() below and stop scheduling new work.
                exitFlushing();
                manager.callback(firstException);
                return;
            }
            responseFuture = null;
            numRetries += 1;
        }
        // Retry path: keep state=FLUSHING so the manager cannot CAS(ACTIVE ->
        // FLUSHING) on the same region while this retry is pending, which
        // would cause a duplicate send of the same inactive chunk.
        // TXN_IN_PROCESSING is a sub-second transient (the shared transaction
        // channel is busy with another in-flight load), so use a short bounded
        // backoff instead of the full retry interval to avoid stalling the
        // commit cycle.
        int delayMs = ErrorUtils.isTxnInProcessing(e)
                ? Math.min(retryIntervalInMs, 500 * numRetries)
                : retryIntervalInMs;
        LOG.warn("Failed to flush data for db: {}, table: {}, and will retry for {} times after {} ms",
                database, table, numRetries, delayMs, e);
        streamLoad(delayMs);
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

        if (multiTableTransactionEnabled ? headChunkEligible() : !inactiveChunks.isEmpty()) {
            LOG.debug("Stream load continue, db: {}, table: {}, label: {}, cacheBytes: {}, cacheRows: {}",
                    database, table, label, cacheBytes, cacheRows);
            streamLoad(0);
            return;
        }
        // Queue drained, or the next chunk is beyond the commit watermark and
        // belongs to the next shared transaction. Return to ACTIVE and release
        // the table gate so sibling regions of the same table can load.
        exitFlushing();
        LOG.debug("Stream load completed, db: {}, table: {}, label: {}, cacheBytes: {}, cacheRows: {}",
                database, table, label, cacheBytes, cacheRows);
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
                exitFlushing();
                return;
            }
            LOG.debug("Stream load chunk, db: {}, table: {}, numRows: {}, rowBytes: {}, chunkBytes: {}",
                    database, table, chunk.numRows(), chunk.rowBytes(), chunk.chunkBytes());
            responseFuture = streamLoader.send(this, delayMs);
        } catch (Exception e) {
            // Do NOT reset state to ACTIVE here. fail() may schedule a retry
            // via streamLoad(retryIntervalInMs); while that retry is pending
            // the region must remain FLUSHING so the manager thread cannot
            // CAS(ACTIVE -> FLUSHING) and trigger a second concurrent flush
            // on the same inactive chunk. fail() itself resets to ACTIVE only
            // when retries are exhausted or the exception is non-retryable.
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
