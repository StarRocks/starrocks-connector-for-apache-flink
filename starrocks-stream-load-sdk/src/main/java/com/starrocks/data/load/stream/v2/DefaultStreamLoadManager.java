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

import com.starrocks.data.load.stream.DefaultStreamLoader;
import com.starrocks.data.load.stream.EnvUtils;
import com.starrocks.data.load.stream.LabelGenerator;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.LoadMetrics;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.TransactionStreamLoader;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
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

/**
 * An implementation of {@link StreamLoadManager}. In this manager, you can use normal stream load or
 * transaction stream load to load data to StarRocks. You can control which to use when constructing
 * the manager with parameter **properties**. If {@link StreamLoadProperties#isEnableTransaction()}
 * is true, transaction stream load will be used, otherwise the normal stream load. You can also control
 * how to commit the transaction stream load by parameter **enableAutoCommit**. If it's true, the
 * manager will commit the load automatically, otherwise you need to commit the load manually. Note that
 * this parameter should always be true for the normal stream load currently.
 * The usage for manual commit should like this
 *     manager.write(); // write some recodes
 *     manager.flush();    // ensure the data is flushed to StarRocks, and the transaction is prepared
 *     manager.snapshot(); // take a snapshot the current transactions, mainly recording the labels
 *     manager.commit();   // commit those snapshots
 */
public class DefaultStreamLoadManager implements StreamLoadManager, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamLoadManager.class);

    private static final long serialVersionUID = 1L;

    enum State {
        ACTIVE,
        INACTIVE
    }

    private final StreamLoadProperties properties;
    private final boolean enableAutoCommit;
    private final StreamLoader streamLoader;
    private final int maxRetries;
    private final int retryIntervalInMs;
    // threshold to trigger flush
    private final long maxCacheBytes;
    // threshold to block write
    private final long maxWriteBlockCacheBytes;
    private final Map<String, TableRegion> regions = new ConcurrentHashMap<>();
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    private final AtomicLong totalFlushRows = new AtomicLong(0L);

    private final AtomicLong numberTotalRows = new AtomicLong(0L);
    private final AtomicLong numberLoadRows = new AtomicLong(0L);

    private final FlushAndCommitStrategy flushAndCommitStrategy;
    private final long scanningFrequency;
    private Thread current;
    private Thread manager;
    private volatile boolean savepoint = false;
    private volatile boolean allRegionsCommitted;
    private long flushTimeoutMs = 660000L; // default stream load timeout is 600s, 1.1x for flush
    /**
     * Maximum time (ms) a shared transaction can stay open before being proactively
     * recycled to avoid StarRocks server-side timeout. Defaults to 80% of 600s = 480s.
     */
    private long sharedTxnMaxIdleMs = 480_000L;

    private final AtomicBoolean commitInFlight = new AtomicBoolean(false);
    /** Timestamp (ms) when commitInFlight was set to true, for timeout detection. */
    private volatile long commitInFlightStartMs;

    private final Lock lock = new ReentrantLock();
    private final Condition writable = lock.newCondition();
    private final Condition flushable = lock.newCondition();

    private final AtomicReference<State> state = new AtomicReference<>(State.INACTIVE);
    private volatile Throwable e;

    private final Queue<TransactionTableRegion> flushQ = new ConcurrentLinkedQueue<>();

    /** Per-partition region index for multi-table transaction mode. */
    private final Map<Integer, List<TransactionTableRegion>> partitionRegions = new ConcurrentHashMap<>();

    private final boolean multiTableTransactionEnabled;

    /** Tracks per-partition transaction boundaries (multi-table mode only). */
    private transient PartitionCommitTracker partitionTracker;

    /** Coordinates shared label begin/prepare/commit (multi-table mode only). */
    private transient SharedTransactionCoordinator txnCoordinator;

    /**
     * Whether write() has triggered a flush after currentCacheBytes > maxCacheBytes.
     * This flag is set true after the flush is triggered in writer(), and set false
     * after the flush completed in callback(). During this period, there is no need
     * to re-trigger a flush.
     */
    private transient AtomicBoolean writeTriggerFlush;
    private transient LoadMetrics loadMetrics;
    private transient StreamLoadListener streamLoadListener;
    private transient LabelGeneratorFactory labelGeneratorFactory;

    public DefaultStreamLoadManager(StreamLoadProperties properties, boolean enableAutoCommit) {
        this.properties = properties;
        if (!enableAutoCommit && !properties.isEnableTransaction()) {
            throw new IllegalArgumentException("You must use transaction stream load if not enable auto-commit");
        }
        this.enableAutoCommit = enableAutoCommit;
        if (!enableAutoCommit) {
            streamLoader = new TransactionStreamLoader(false);
            maxRetries = 0;
            retryIntervalInMs = 0;
        } else {
            // TODO transaction stream load can't support retry currently
            streamLoader = (properties.getMaxRetries() > 0 || !properties.isEnableTransaction())
                    ? new DefaultStreamLoader() : new TransactionStreamLoader(true);
            maxRetries = properties.getMaxRetries();
            retryIntervalInMs = properties.getRetryIntervalInMs();
        }
        if (properties.isEnableMultiTableTransaction() && !(streamLoader instanceof TransactionStreamLoader)) {
            throw new IllegalArgumentException(
                    "Multi-table transaction mode requires TransactionStreamLoader. " +
                    "Retry (maxRetries > 0) is not supported with multi-table transactions.");
        }
        if (properties.isEnableMultiTableTransaction() && properties.getMultiTableTransactionBufferSize() > 0) {
            this.maxCacheBytes = properties.getMultiTableTransactionBufferSize();
        } else {
            this.maxCacheBytes = properties.getMaxCacheBytes();
        }
        this.maxWriteBlockCacheBytes = 2 * maxCacheBytes;
        this.scanningFrequency = properties.getScanningFrequency();
        this.multiTableTransactionEnabled = properties.isEnableMultiTableTransaction();
        this.flushAndCommitStrategy = new FlushAndCommitStrategy(properties, enableAutoCommit);
        // get timeout from properties's header
        String timeoutStr = properties.getHeaders().get("timeout");
        if (timeoutStr != null) {
            try {
                long timeoutSec = Long.parseLong(timeoutStr);
                this.flushTimeoutMs = timeoutSec * 1100; // 1.1x for flush
                // 80% of server-side timeout as safety margin for shared transaction recycling
                this.sharedTxnMaxIdleMs = timeoutSec * 800;
            } catch (NumberFormatException ex) {
                LOG.warn("Invalid timeout value in properties header: {}, using default", timeoutStr);
            }
        }
    }

    @Override
    public void init() {
        if (labelGeneratorFactory == null) {
            this.labelGeneratorFactory =
                    new LabelGeneratorFactory.DefaultLabelGeneratorFactory(properties.getLabelPrefix());
        }
        this.writeTriggerFlush = new AtomicBoolean(false);
        this.loadMetrics = new LoadMetrics();
        if (multiTableTransactionEnabled) {
            this.partitionTracker = new PartitionCommitTracker(properties.getExpectDelayTime());
        }
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

                    if (lastPrintTimestamp == -1 || System.currentTimeMillis() - lastPrintTimestamp > 10000) {
                        lastPrintTimestamp = System.currentTimeMillis();
                        LOG.debug("Audit information: {}, {}", loadMetrics, flushAndCommitStrategy);
                    }

                    if (savepoint) {
                        if (multiTableTransactionEnabled && txnCoordinator != null) {
                          try {
                            LOG.info("[MultiTxn] Savepoint: completing shared transaction");

                            // Upstream must ensure all source transactions are complete
                            // before the checkpoint barrier arrives. If any partition is
                            // still ACTIVE (no txnEnd received), it means upstream violated
                            // this contract — fail fast rather than silently committing
                            // partial transaction data.
                            List<Integer> activePartitions = partitionTracker.getActivePartitions();
                            if (!activePartitions.isEmpty()) {
                                throw new IllegalStateException(
                                        "[MultiTxn] Partitions " + activePartitions + " still have " +
                                        "uncommitted transaction data at checkpoint. Upstream must " +
                                        "ensure all transactions are complete before checkpoint barrier.");
                            }

                            // Ensure a shared transaction is open (may not be if no data
                            // was written yet, or if we just finished a commit cycle).
                            if (!txnCoordinator.isActive() && !flushQ.isEmpty()) {
                                ensureSharedTransaction();
                            }

                            // Wait for any in-flight loads to complete
                            for (TransactionTableRegion region : flushQ) {
                                Future<?> result = region.getResult();
                                if (result != null) {
                                    try {
                                        result.get();
                                    } catch (Exception ignored) {
                                        // errors will be handled by the callback
                                    }
                                }
                            }

                            // Switch and trigger loads for regions that have been through
                            // trySwitchAndCommit() (TXN_END_RECEIVED or SWITCHED) but
                            // may still have inactive chunks not yet sent.
                            for (TransactionTableRegion region : flushQ) {
                                region.switchChunkForCommit();
                            }
                            // Trigger loads for regions with pending data
                            for (TransactionTableRegion region : flushQ) {
                                if (region.triggerLoadIfNeeded()) {
                                    txnCoordinator.markDataLoaded();
                                }
                            }
                            // Wait for triggered loads to complete.
                            boolean allLoadsDone = false;
                            while (!allLoadsDone && this.e == null) {
                                allLoadsDone = true;
                                for (TransactionTableRegion region : flushQ) {
                                    if (region.isFlushing()) {
                                        allLoadsDone = false;
                                        break;
                                    }
                                }
                                if (!allLoadsDone && this.e == null) {
                                    LockSupport.parkNanos(1_000_000L);
                                }
                            }
                            // Commit or rollback
                            String anyTable = null;
                            for (TransactionTableRegion region : flushQ) {
                                if (anyTable == null) {
                                    anyTable = region.getTable();
                                }
                            }
                            if (allLoadsDone && anyTable != null && txnCoordinator.isActive()) {
                                try {
                                    if (txnCoordinator.hasDataLoaded()) {
                                        txnCoordinator.prepareAndCommit(anyTable);
                                        LOG.info("[MultiTxn] Shared transaction committed during savepoint");
                                    } else {
                                        LOG.info("[MultiTxn] No data loaded; rolling back empty txn during savepoint");
                                        txnCoordinator.reset();
                                    }
                                    allRegionsCommitted = true;
                                } catch (Exception ex) {
                                    LOG.error("[MultiTxn] Failed to commit shared transaction during savepoint", ex);
                                    this.e = ex;
                                }
                            } else if (anyTable == null) {
                                // No regions at all — nothing to commit
                                allRegionsCommitted = true;
                            }
                          } catch (Exception ex) {
                            LOG.error("[MultiTxn] Savepoint shared transaction failed", ex);
                            this.e = ex;
                          } finally {
                            if (txnCoordinator.isActive()) {
                                txnCoordinator.reset();
                            }
                            commitInFlight.set(false);
                            if (partitionTracker != null) {
                                partitionTracker.reset();
                            }
                            for (TransactionTableRegion region : flushQ) {
                                if (!region.isRetrying()) {
                                    region.setLabel(null);
                                }
                            }
                          }
                        } else {
                            // Non-multi-table path: flush and commit each region independently.
                            // This block must NOT run after a multi-table savepoint commit,
                            // because it would open new independent transactions for any
                            // residual data, breaking the atomicity guarantee.
                            for (TransactionTableRegion region : flushQ) {
                                boolean flush = region.flush(FlushReason.FORCE);
                                LOG.debug("Trigger flush table region {} because of savepoint, region cache bytes: {}, flush: {}",
                                        region.getUniqueKey(), region.getCacheBytes(), flush);
                            }

                            // should ensure all data is committed for auto-commit mode
                            if (enableAutoCommit) {
                                int committedRegions = 0;
                                for (TransactionTableRegion region : flushQ) {
                                    // savepoint makes sure no more data is written, so these conditions
                                    // can guarantee commit after all data has been written to StarRocks
                                    boolean success = region.commit();
                                    if (success && region.getCacheBytes() == 0) {
                                        committedRegions += 1;
                                        region.resetAge();
                                    }
                                    LOG.debug("Commit region {} for savepoint, success: {}", region.getUniqueKey(), success);
                                }

                                if (committedRegions == flushQ.size()) {
                                    allRegionsCommitted = true;
                                    LOG.info("All regions committed for savepoint, number of regions: {}", committedRegions);
                                } else {
                                    LOG.debug("Some regions not committed for savepoint, expected num: {}, actual num: {}",
                                            flushQ.size(), committedRegions);
                                }
                            }
                        }
                        LockSupport.unpark(current);
                    } else if (commitInFlight.get()) {
                        // Multi-table coordinator-based commit (manager thread)
                        processMultiTableCommit();
                    } else {
                        // Normal timer-driven path (non-multi-table, or multi-table between commits)

                        // In multi-table mode, ensure a shared transaction is always open so
                        // that autonomous flushes use the shared label instead of independent labels.
                        // This eliminates the data-loss window where an independent-label flush
                        // could be orphaned when a shared transaction later overwrites the label.
                        if (multiTableTransactionEnabled && txnCoordinator != null && !flushQ.isEmpty()) {
                            if (!txnCoordinator.isActive()) {
                                try {
                                    ensureSharedTransaction();
                                } catch (Exception beginEx) {
                                    LOG.error("[MultiTxn] Failed to eagerly open shared transaction", beginEx);
                                    this.e = beginEx;
                                    // Skip flush/commit to avoid creating orphan independent transactions.
                                    continue;
                                }
                            } else if (txnCoordinator.getElapsedMs() >= sharedTxnMaxIdleMs) {
                                // Recycle the shared transaction before server-side timeout.
                                try {
                                    recycleSharedTransaction();
                                } catch (Exception recycleEx) {
                                    LOG.error("[MultiTxn] Failed to recycle shared transaction", recycleEx);
                                    this.e = recycleEx;
                                }
                                // Must skip the flush-selection loop below: recycleSharedTransaction()
                                // clears region labels then re-opens via ensureSharedTransaction().
                                // If the re-open silently returned (e.g. a region started retrying),
                                // regions have null labels; falling through would create orphan
                                // independent-label loads that break atomicity.
                                continue;
                            }
                        }

                        // In multi-table mode, check if partitions are ready to
                        // switch and commit.  This runs on the manager thread (not
                        // the task thread) so that the task thread has had time to
                        // process subsequent records and register all partitions in
                        // the tracker before allSwitched() is evaluated.
                        if (multiTableTransactionEnabled && partitionTracker != null) {
                            trySwitchAndCommit();
                            if (commitInFlight.get()) {
                                // All partitions switched; skip autonomous flush and
                                // enter processMultiTableCommit() on the next iteration.
                                continue;
                            }
                        }

                        for (TransactionTableRegion region : flushQ) {
                            region.getAndIncrementAge();
                            if (flushAndCommitStrategy.shouldCommit(region)) {
                                boolean success = region.commit();
                                if (success) {
                                    region.resetAge();
                                }
                                LOG.debug("Commit region {} for normal, success: {}", region.getUniqueKey(), success);
                            }
                        }

                        for (FlushAndCommitStrategy.SelectFlushResult result : flushAndCommitStrategy.selectFlushRegions(flushQ, currentCacheBytes.get())) {
                            TransactionTableRegion region = result.getRegion();
                            boolean flush = region.flush(result.getReason());
                            if (flush && multiTableTransactionEnabled && txnCoordinator != null) {
                                txnCoordinator.markDataLoaded();
                            }
                            LOG.debug("Trigger flush table region {} because of selection, region cache bytes: {}," +
                                    " flush: {}", region.getUniqueKey(), region.getCacheBytes(), flush);
                        }
                    }
                }
            }, "StarRocks-Sink-Manager");
            manager.setDaemon(true);
            manager.setUncaughtExceptionHandler((t, ee) -> {
                LOG.error("StarRocks-Sink-Manager error", ee);
                e = ee;
            });
            streamLoader.start(properties, this);

            if (multiTableTransactionEnabled) {
                this.txnCoordinator = new SharedTransactionCoordinator(streamLoader, labelGeneratorFactory);
                LOG.info("[MultiTxn] Multi-table transaction mode enabled");
            }

            // Start the manager thread AFTER streamLoader and txnCoordinator
            // are fully initialized, so the thread has guaranteed visibility
            // of these fields (Thread.start() provides happens-before).
            manager.start();
            LOG.info("StarRocks-Sink-Manager start, enableAutoCommit: {}, streamLoader: {}, {}",
                    enableAutoCommit, streamLoader.getClass().getName(), EnvUtils.getGitInformation());
        }
    }

    @Override
    public void setCommitAllowed(boolean allowed) {
        // Legacy no-partition variant: no-op in multi-table mode
    }

    @Override
    public void setCommitAllowed(int partition, boolean allowed) {
        if (!multiTableTransactionEnabled) {
            return;
        }
        if (!allowed) {
            return;
        }

        // Record the txnEnd and immediately switch chunks on the task thread.
        // This must happen synchronously before returning, because the very next
        // record from the source may belong to a new transaction.  If we deferred
        // the switch to the manager thread, the new transaction's write() would
        // call onWrite() and reset the partition back to ACTIVE, causing the
        // previous transaction's commit to be lost.
        boolean transitioned = partitionTracker.onTxnEnd(partition);
        if (transitioned) {
            List<TransactionTableRegion> pRegions = partitionRegions.get(partition);
            if (pRegions != null) {
                for (TransactionTableRegion region : pRegions) {
                    region.switchChunkForCommit();
                }
            }
            partitionTracker.markSwitched(partition);
            LOG.debug("[MultiTxn] txnEnd for partition={}, regions immediately switched", partition);
        } else {
            LOG.debug("[MultiTxn] txnEnd for partition={}, deferred (already switched)", partition);
        }
    }

    /**
     * Attempts to switch ready partitions and trigger a commit if all are switched.
     * Called on the manager thread (from the normal timer-driven path) so that the
     * task thread has had time to process subsequent records and register all
     * partitions before allSwitched() is evaluated.
     */
    private void trySwitchAndCommit() {
        List<Integer> readyPartitions = partitionTracker.getReadyToSwitch();
        for (int p : readyPartitions) {
            List<TransactionTableRegion> pRegions = partitionRegions.get(p);
            if (pRegions != null) {
                for (TransactionTableRegion region : pRegions) {
                    region.switchChunkForCommit();
                }
            }
            partitionTracker.markSwitched(p);
            LOG.debug("[MultiTxn] partition {} switched, regions={}", p,
                    pRegions == null ? 0 : pRegions.size());
        }

        if (partitionTracker.allSwitched() && commitInFlight.compareAndSet(false, true)) {
            commitInFlightStartMs = System.currentTimeMillis();
            // No flushable.signal() needed — this method already runs on the
            // manager thread.  The caller will see commitInFlight==true and
            // `continue` to the next iteration which enters processMultiTableCommit().
            LOG.info("[MultiTxn] All partitions switched, commitInFlight=true");
        }
    }

    /**
     * Processes a multi-table commit cycle using the SharedTransactionCoordinator.
     * Called on the manager thread when commitInFlight=true.
     *
     * <p>Because the shared transaction is eagerly opened (by {@link #ensureSharedTransaction()})
     * before any autonomous flush, all in-flight HTTP loads already use the shared label.
     * This method simply:
     * <ol>
     *   <li>Waits for all in-flight loads to complete</li>
     *   <li>Triggers loads for any remaining inactive chunks (from switchChunkForCommit)</li>
     *   <li>Waits again for those loads</li>
     *   <li>Executes unified prepare + commit</li>
     *   <li>Opens a new shared transaction for the next cycle</li>
     * </ol>
     */
    private void processMultiTableCommit() {
        // If a prior error occurred (e.g. failed HTTP load), abort the commit
        // cycle immediately so the error surfaces via checkAndThrowException().
        if (this.e != null) {
            LOG.error("[MultiTxn] Aborting commit cycle due to prior error: {}", this.e.getMessage());
            commitInFlight.set(false);
            partitionTracker.reset();
            return;
        }

        // Timeout detection: if the commit cycle is stuck (e.g. a region's HTTP load
        // keeps failing and retrying), abort to avoid stalling the pipeline indefinitely.
        long commitElapsedMs = System.currentTimeMillis() - commitInFlightStartMs;
        if (commitElapsedMs > flushTimeoutMs) {
            LOG.error("[MultiTxn] Commit-in-flight timeout: elapsed {}ms, timeout {}ms",
                    commitElapsedMs, flushTimeoutMs);
            txnCoordinator.reset();
            for (TransactionTableRegion region : flushQ) {
                if (!region.isRetrying()) {
                    region.setLabel(null);
                }
            }
            commitInFlight.set(false);
            partitionTracker.reset();
            this.e = new RuntimeException(String.format(
                    "[MultiTxn] Commit-in-flight timeout: elapsed %dms, timeout %dms",
                    commitElapsedMs, flushTimeoutMs));
            return;
        }

        final List<TransactionTableRegion> regionSnapshot =
                Collections.unmodifiableList(new ArrayList<>(flushQ));
        try {
            if (!txnCoordinator.isActive()) {
                // Edge case: commitInFlight was set but no shared txn is open yet
                // (e.g. first write + immediate txnEnd before manager thread ran).
                // Ensure the shared transaction exists before proceeding.
                ensureSharedTransaction();
            }

            // Wait for any in-flight autonomous flushes to complete.
            for (TransactionTableRegion region : regionSnapshot) {
                if (region.isFlushing() || region.isRetrying()) {
                    LOG.debug("[MultiTxn] Region {} still flushing/retrying, will retry next scan",
                            region.getUniqueKey());
                    return;
                }
            }

            // Trigger loads for regions that have inactive chunks from switchChunkForCommit
            // but haven't been flushed yet (data arrived between the last autonomous flush
            // and the txnEnd marker).
            boolean triggeredNew = false;
            for (TransactionTableRegion region : regionSnapshot) {
                if (region.triggerLoadIfNeeded()) {
                    txnCoordinator.markDataLoaded();
                    triggeredNew = true;
                    LOG.debug("[MultiTxn] triggered load for region={}", region.getUniqueKey());
                }
            }
            if (triggeredNew) {
                // New loads were triggered, wait for them to complete on next scan.
                return;
            }

            // All loads are done. Commit.
            String anyTable = null;
            for (TransactionTableRegion region : regionSnapshot) {
                if (anyTable == null) {
                    anyTable = region.getTable();
                }
            }

            if (anyTable != null) {
                if (txnCoordinator.hasDataLoaded()) {
                    txnCoordinator.prepareAndCommit(anyTable);
                } else {
                    // No data was loaded — rollback the empty transaction.
                    LOG.info("[MultiTxn] No data loaded in shared transaction; rolling back empty txn");
                    txnCoordinator.reset();
                }
            } else {
                txnCoordinator.reset();
            }

            // Clear labels and reset state
            for (TransactionTableRegion region : regionSnapshot) {
                region.setLabel(null);
                region.resetAge();
            }
            commitInFlight.set(false);
            partitionTracker.reset();
            LOG.info("[MultiTxn] Shared transaction cycle completed; commitInFlight=false");

            // Immediately open a new shared transaction for the next cycle,
            // so any subsequent autonomous flushes are under the new shared label.
            // This is best-effort: if it fails, the normal path will retry on the next scan.
            if (!flushQ.isEmpty()) {
                try {
                    ensureSharedTransaction();
                } catch (Exception beginEx) {
                    LOG.warn("[MultiTxn] Failed to eagerly open next shared transaction after commit; " +
                            "will retry on next scan: {}", beginEx.getMessage());
                    txnCoordinator.reset();
                    for (TransactionTableRegion region : regionSnapshot) {
                        if (!region.isRetrying()) {
                            region.setLabel(null);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("[MultiTxn] Shared transaction commit failed", ex);
            txnCoordinator.reset();
            commitInFlight.set(false);
            partitionTracker.reset();
            for (TransactionTableRegion region : regionSnapshot) {
                if (!region.isRetrying()) {
                    region.setLabel(null);
                }
            }
            this.e = ex;
        }
    }

    /**
     * Opens a new shared transaction and injects its label into all current regions.
     * Called on the manager thread to ensure autonomous flushes always use the shared label.
     *
     * <p>This must only be called when no shared transaction is currently active.
     */
    private void ensureSharedTransaction() {
        String anyDb = null;
        String anyTable = null;
        for (TransactionTableRegion region : flushQ) {
            if (anyDb == null) {
                anyDb = region.getDatabase();
                anyTable = region.getTable();
            } else if (!anyDb.equals(region.getDatabase())) {
                throw new IllegalStateException(
                        "All regions in a multi-table commit must share the same database. " +
                        "Found databases: '" + anyDb + "' and '" + region.getDatabase() + "'");
            }
        }
        if (anyDb == null) {
            return;
        }

        // Ensure no region is retrying — setLabel() silently skips when numRetries > 0,
        // which would leave the region with a stale label, breaking atomicity.
        for (TransactionTableRegion region : flushQ) {
            if (region.isFlushing() || region.isRetrying()) {
                LOG.debug("[MultiTxn] Cannot open shared txn: region {} is flushing/retrying",
                        region.getUniqueKey());
                return;
            }
        }

        txnCoordinator.begin(anyDb, anyTable);

        List<TransactionTableRegion> injected = new ArrayList<>();
        for (TransactionTableRegion region : flushQ) {
            // Re-check retrying: a region may have entered retry (via fail() on the
            // executor thread) between the bulk check above and this point.
            if (region.isRetrying()) {
                LOG.warn("[MultiTxn] Region {} started retrying during ensureSharedTransaction; "
                        + "rolling back and clearing {} already-injected labels",
                        region.getUniqueKey(), injected.size());
                txnCoordinator.reset();
                for (TransactionTableRegion r : injected) {
                    r.setLabel(null);
                }
                return;
            }
            region.setLabel(txnCoordinator.getSharedLabel());
            injected.add(region);
        }
        LOG.info("[MultiTxn] Eagerly opened shared transaction: label={}",
                txnCoordinator.getSharedLabel());
    }

    /**
     * Recycles (commit-or-rollback + reopen) the current shared transaction to prevent
     * it from hitting the StarRocks server-side timeout. Must only be called from the
     * manager thread when no region is actively flushing.
     *
     * <p><b>IMPORTANT:</b> The caller must {@code continue} to the next loop iteration
     * after calling this method, skipping the flush-selection loop. Between clearing
     * region labels and re-opening the shared transaction, any autonomous flush would
     * use a null/stale label and break atomicity.
     */
    private void recycleSharedTransaction() {
        // Do not recycle if a commit cycle is pending — the switched chunks
        // must be committed under the current shared transaction to preserve atomicity.
        if (commitInFlight.get()) {
            LOG.debug("[MultiTxn] Cannot recycle shared txn: commitInFlight=true");
            return;
        }

        // Wait for any in-flight loads before recycling.
        for (TransactionTableRegion region : flushQ) {
            if (region.isFlushing() || region.isRetrying()) {
                LOG.debug("[MultiTxn] Cannot recycle shared txn: region {} still flushing/retrying",
                        region.getUniqueKey());
                return;
            }
        }

        LOG.info("[MultiTxn] Recycling shared transaction approaching timeout: label={}, elapsed={}ms",
                txnCoordinator.getSharedLabel(), txnCoordinator.getElapsedMs());

        // If any partition still has uncommitted transaction data (ACTIVE state),
        // we must NOT commit — that would break cross-table atomicity by exposing
        // a partial source transaction. Instead, fail fast so Flink restarts the
        // job from the last checkpoint, re-consuming all data since that checkpoint.
        if (partitionTracker != null) {
            List<Integer> activePartitions = partitionTracker.getActivePartitions();
            if (!activePartitions.isEmpty()) {
                LOG.error("[MultiTxn] Shared transaction approaching timeout but partitions {} "
                        + "still ACTIVE (no txnEnd received). Rolling back and failing fast. "
                        + "Upstream must complete source transactions within {}ms. "
                        + "label={}", activePartitions, sharedTxnMaxIdleMs,
                        txnCoordinator.getSharedLabel());
                txnCoordinator.reset();
                for (TransactionTableRegion region : flushQ) {
                    if (!region.isRetrying()) {
                        region.setLabel(null);
                    }
                }
                commitInFlight.set(false);
                partitionTracker.reset();
                this.e = new StreamLoadFailException(
                        "[MultiTxn] Multi-table transaction timeout: upstream must complete " +
                        "source transactions within " + sharedTxnMaxIdleMs + "ms. " +
                        "Active partitions without txnEnd: " + activePartitions);
                return;
            }
        }

        String anyTable = null;
        for (TransactionTableRegion region : flushQ) {
            if (anyTable == null) {
                anyTable = region.getTable();
            }
        }

        if (anyTable != null && txnCoordinator.hasDataLoaded()) {
            txnCoordinator.prepareAndCommit(anyTable);
            LOG.info("[MultiTxn] Recycled shared transaction committed");
        } else {
            txnCoordinator.reset();
            LOG.info("[MultiTxn] Recycled empty shared transaction (rolled back)");
        }

        // Clear labels and open a fresh shared transaction.
        for (TransactionTableRegion region : flushQ) {
            region.setLabel(null);
        }
        ensureSharedTransaction();
    }

    public void setStreamLoadListener(StreamLoadListener streamLoadListener) {
        this.streamLoadListener = streamLoadListener;
    }

    public void setLabelGeneratorFactory(LabelGeneratorFactory labelGeneratorFactory) {
        this.labelGeneratorFactory = labelGeneratorFactory;
    }

    @Override
    public void write(String uniqueKey, String database, String table, String... rows) {
        TableRegion region = getCacheRegion(uniqueKey, database, table);
        for (String row : rows) {
            checkAndThrowException();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Write uniqueKey {}, database {}, table {}, row {}",
                        uniqueKey == null ? "null" : uniqueKey, database, table, row);
            }
            int bytes = region.write(row.getBytes(StandardCharsets.UTF_8));
            blockIfCacheFull(currentCacheBytes.addAndGet(bytes));
        }
    }

    /**
     * Blocks the calling (task) thread if the write-side cache is full, and signals
     * the manager thread to flush when the soft threshold is reached.
     *
     * <p>Extracted to avoid duplication between the two {@code write()} overloads.
     */
    private void blockIfCacheFull(long cachedBytes) {
        if (cachedBytes >= maxWriteBlockCacheBytes) {
            long startTime = System.nanoTime();
            lock.lock();
            try {
                int idx = 0;
                while (currentCacheBytes.get() >= maxWriteBlockCacheBytes) {
                    checkAndThrowException();
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
            loadMetrics.updateWriteBlock(1, System.nanoTime() - startTime);
        } else if (cachedBytes >= maxCacheBytes && writeTriggerFlush.compareAndSet(false, true)) {
            lock.lock();
            try {
                flushable.signal();
            } finally {
                lock.unlock();
            }
            loadMetrics.updateWriteTriggerFlush(1);
            LOG.info("Trigger flush, currentBytes: {}, maxCacheBytes: {}", cachedBytes, maxCacheBytes);
        }
    }

    @Override
    public void callback(StreamLoadResponse response) {
        long cacheByteBeforeFlush = response.getFlushBytes() != null ? currentCacheBytes.getAndAdd(-response.getFlushBytes()) : currentCacheBytes.get();
        if (response.getFlushRows() != null) {
            totalFlushRows.addAndGet(response.getFlushRows());
        }
        writeTriggerFlush.set(false);

        LOG.debug("Receive load response, cacheByteBeforeFlush: {}, currentCacheBytes: {}, totalFlushRows : {}",
                cacheByteBeforeFlush, currentCacheBytes.get(), totalFlushRows.get());

        lock.lock();
        try {
            writable.signal();
        } finally {
            lock.unlock();
        }

        if (response.getException() != null) {
            LOG.error("Stream load failed", response.getException());
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

        if (streamLoadListener != null) {
            streamLoadListener.onResponse(response);
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

    public Throwable getException() {
        return e;
    }

    @Override
    public void flush() {
        LOG.info("Stream load manager flush start - currentCacheBytes: {}, maxCacheBytes: {}",
                currentCacheBytes.get(), maxCacheBytes);

        initializeFlushState();

        long startTime = System.currentTimeMillis();
        long waitTime = 100; // Initial wait time: 100ms

        try {
            while (!isSavepointFinished()) {
                checkFlushTimeout(startTime);

                triggerFlushSignal();
                LockSupport.park(current);

                if (!savepoint) {
                    break;
                }

                waitForRegionResults(waitTime);
                waitTime = calculateNextWaitTime(waitTime);
            }

            finishFlush();
        } finally {
            // Ensure the savepoint flag is always cleared even if checkFlushTimeout()
            // or finishFlush() throw, so the manager thread does not keep acting on
            // a stale savepoint signal after flush() has already returned.
            savepoint = false;
        }
    }

    private void initializeFlushState() {
        savepoint = true;
        allRegionsCommitted = false;
        current = Thread.currentThread();
    }

    private void checkFlushTimeout(long startTime) {
        long elapsedMs = System.currentTimeMillis() - startTime;
        if (elapsedMs > flushTimeoutMs) {
            String errorMsg = String.format(
                    "Stream load manager flush timeout: elapsed %dms, timeout %dms, " +
                            "currentCacheBytes: %d, allRegionsCommitted: %s, savepoint: %s",
                    elapsedMs, flushTimeoutMs, currentCacheBytes.get(), allRegionsCommitted, savepoint);

            LOG.error(errorMsg);
            throw new RuntimeException(String.format(
                    "Stream load manager flush timeout: elapsed %dms, timeout %dms", elapsedMs, flushTimeoutMs));
        }
    }

    private void triggerFlushSignal() {
        lock.lock();
        try {
            flushable.signal();
        } finally {
            lock.unlock();
        }
    }

    private void waitForRegionResults(long waitTime) {
        try {
            for (TableRegion tableRegion : regions.values()) {
                Future<?> result = tableRegion.getResult();
                if (result != null) {
                    result.get();
                }
            }

            if (waitTime > 200) {
                LockSupport.parkNanos(waitTime * 1_000_000L);
                LOG.info("Stream load manager flush waiting: {}ms", waitTime);
            }
        } catch (ExecutionException | InterruptedException ex) {
            LOG.warn("Stream load manager flush get result failed", ex);
            throw new RuntimeException(ex);
        }
    }

    private long calculateNextWaitTime(long currentWaitTime) {
        return Math.min(currentWaitTime * 2, 10000); // Max wait time: 10s
    }

    private void finishFlush() {
        LOG.info("Stream load manager flush finished - currentCacheBytes: {}, maxCacheBytes: {}, allRegionsCommitted: {}",
                currentCacheBytes.get(), maxCacheBytes, allRegionsCommitted);
        checkAndThrowException();
        // savepoint is cleared by the finally block in flush(), not here.
    }

    @Override
    public StreamLoadSnapshot snapshot() {
        StreamLoadSnapshot snapshot = StreamLoadSnapshot.snapshot(regions.values());
        for (TableRegion region : regions.values()) {
            region.setLabel(null);
        }
        return snapshot;
    }

    public StreamLoader getStreamLoader() {
        return streamLoader;
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
            LOG.info("StreamLoadManagerV2 close, loadMetrics: {}, flushAndCommit: {}",
                    loadMetrics, flushAndCommitStrategy);
            // Clean up the shared transaction to avoid server-side timeout warnings.
            if (txnCoordinator != null && txnCoordinator.isActive()) {
                try {
                    txnCoordinator.reset();
                } catch (Exception ex) {
                    LOG.warn("Failed to rollback shared transaction during close", ex);
                }
            }
            manager.interrupt();
            streamLoader.close();
        }
    }

    private boolean isSavepointFinished() {
        if (e != null) {
            return true;
        }
        return currentCacheBytes.get() == 0L && (!enableAutoCommit || allRegionsCommitted);
    }

    private void checkAndThrowException() {
        if (e != null) {
            LOG.error("catch exception, wait rollback ", e);
            streamLoader.rollback(snapshot());
            close();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(int partition, String database, String table, String... rows) {
        if (!multiTableTransactionEnabled) {
            write(null, database, table, rows);
            return;
        }
        String uniqueKey = "P" + partition + "-" + StreamLoadUtils.getTableUniqueKey(database, table);
        partitionTracker.onWrite(partition);
        TableRegion region = getCacheRegion(uniqueKey, database, table, partition);
        for (String row : rows) {
            checkAndThrowException();
            int bytes = region.write(row.getBytes(StandardCharsets.UTF_8));
            blockIfCacheFull(currentCacheBytes.addAndGet(bytes));
        }
    }

    protected TableRegion getCacheRegion(String uniqueKey, String database, String table) {
        return getCacheRegion(uniqueKey, database, table, -1);
    }

    protected TableRegion getCacheRegion(String uniqueKey, String database, String table, int partition) {
        if (uniqueKey == null) {
            uniqueKey = StreamLoadUtils.getTableUniqueKey(database, table);
        }

        TableRegion region = regions.get(uniqueKey);
        if (region == null) {
            synchronized (regions) {
                region = regions.get(uniqueKey);
                if (region == null) {
                    // For per-partition regions, look up table properties by the real table key
                    String tableKey = StreamLoadUtils.getTableUniqueKey(database, table);
                    StreamLoadTableProperties tableProperties = properties.getTableProperties(tableKey, database, table);
                    LabelGenerator labelGenerator = labelGeneratorFactory.create(database, table);
                    TransactionTableRegion newRegion = new TransactionTableRegion(
                            uniqueKey, database, table, this,
                            tableProperties, streamLoader, labelGenerator, maxRetries, retryIntervalInMs);
                    if (multiTableTransactionEnabled) {
                        newRegion.getHeaders().put("transaction_type", "multi");
                        // If a shared transaction is already open, inject its label so that
                        // the first flush of this region uses the shared label.
                        if (txnCoordinator != null && txnCoordinator.isActive()) {
                            newRegion.setLabel(txnCoordinator.getSharedLabel());
                        }
                    }
                    regions.put(uniqueKey, newRegion);
                    flushQ.offer(newRegion);
                    if (partition >= 0) {
                        partitionRegions.computeIfAbsent(partition, k -> new CopyOnWriteArrayList<>()).add(newRegion);
                    }
                    region = newRegion;
                }
            }
        }
        return region;
    }
}