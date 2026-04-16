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

    // Multi-table mode only. Aggregate bytes currently held in activeChunks that
    // belong to in-progress (not-yet-ended) source transactions across all
    // regions. A single source transaction can span several tables; the task
    // thread accumulates rows into each region's activeChunk until the per-table
    // txnEnd arrives, at which point the corresponding region transitions to a
    // clean boundary and its in-progress contribution is subtracted from this
    // counter.
    //
    // The per-region fail-fast in TransactionTableRegion only catches the case
    // where a single region's in-progress chunk exceeds maxWriteBlockCacheBytes.
    // When a single source transaction splits its payload across many regions,
    // each region can stay under the per-region cap while the aggregate pushes
    // currentCacheBytes past the write-block threshold — blockIfCacheFull then
    // parks the task thread, which in turn cannot deliver the txnEnd marker,
    // resulting in a silent deadlock. This aggregate counter lets us fail fast
    // in write() before the deadlock window opens.
    private final AtomicLong aggregateInProgressTxnBytes = new AtomicLong(0L);

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

    /**
     * Commit interval (ms) cached from {@code properties.getExpectDelayTime()}
     * at construction time. Read on every manager scan via
     * {@link #shouldTriggerCommit()}, so we cache it to avoid repeated property
     * lookups (and to make the contract immutable — the interval is a fixed
     * value for the lifetime of a manager instance). Only meaningful in
     * multi-table transaction mode.
     */
    private long commitIntervalMs;

    /**
     * Minimum interval (ms) between two {@code switchChunkForCommit} calls on the
     * same region in multi-table mode. Computed as
     * {@code min(1000, max(100, commitInterval/10))} — small enough to batch
     * frequent txnEnds but capped at 1s to keep data freshness reasonable.
     */
    private long miniSwitchIntervalMs;

    /**
     * Timestamp (epoch ms) of the last successful commit (or construction time).
     * Used by the manager thread's time-driven commit decision. Only meaningful
     * in multi-table transaction mode.
     */
    private volatile long lastCommitTimeMs;
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
        // Multi-table mode is incompatible with SDK manual-commit mode. The manager's
        // timer-driven path (tryStartTimerDrivenCommit -> processMultiTableCommit) autonomously
        // commits the shared transaction once commitIntervalMs has elapsed and data is present,
        // so an external 2PC caller would see data published before its explicit snapshot/commit
        // step. Reject the combination at construction time rather than silently auto-publishing.
        if (properties.isEnableMultiTableTransaction() && !enableAutoCommit) {
            throw new IllegalArgumentException(
                    "Multi-table transaction mode requires enableAutoCommit=true. " +
                    "The manager autonomously drives shared-transaction commits on a timer, " +
                    "which is incompatible with external manual-commit (2PC) control.");
        }
        if (properties.isEnableMultiTableTransaction() && properties.getMultiTableTransactionBufferSize() > 0) {
            this.maxCacheBytes = properties.getMultiTableTransactionBufferSize();
        } else {
            this.maxCacheBytes = properties.getMaxCacheBytes();
        }
        this.maxWriteBlockCacheBytes = 2 * maxCacheBytes;
        this.scanningFrequency = properties.getScanningFrequency();
        this.multiTableTransactionEnabled = properties.isEnableMultiTableTransaction();
        // Pass the (possibly overridden) maxCacheBytes so the strategy's
        // cache-full flush threshold stays aligned with the manager's
        // write-block threshold. See review comment P1 on PR #487.
        this.flushAndCommitStrategy = new FlushAndCommitStrategy(properties, enableAutoCommit, this.maxCacheBytes);
        // Cache commit interval and compute miniSwitchIntervalMs for multi-table
        // mode. miniInterval is capped between 100 ms and 1000 ms, targeting
        // commitInterval / 10 as a sensible default so that a 1 s commit interval
        // yields 100 ms batching and a 30 s commit interval caps at 1 s batching.
        // lastCommitTimeMs is initialized in init() (right before the manager
        // thread starts), so it reflects the start of the scan loop rather than
        // construction time.
        this.commitIntervalMs = properties.getExpectDelayTime();
        this.miniSwitchIntervalMs = Math.min(1000L, Math.max(100L, this.commitIntervalMs / 10L));
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
            this.partitionTracker = new PartitionCommitTracker();
            this.lastCommitTimeMs = System.currentTimeMillis();
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
                            // before the checkpoint barrier arrives. Two conditions must
                            // hold for every region at savepoint time:
                            //   (a) The owning partition has received at least one txnEnd
                            //       (no partitions in the ACTIVE state of the tracker).
                            //   (b) Every region's activeChunk is at a clean transaction
                            //       boundary, i.e., no write has occurred since its most
                            //       recent txnEnd. If activeChunk is dirty, the in-progress
                            //       source transaction has not yet completed.
                            // Either violation indicates the upstream broke its contract;
                            // fail fast rather than silently committing partial data.
                            List<Integer> partitionsWithoutTxnEnd = partitionTracker.getPartitionsWithoutTxnEnd();
                            if (!partitionsWithoutTxnEnd.isEmpty()) {
                                throw new IllegalStateException(
                                        "[MultiTxn] Partitions " + partitionsWithoutTxnEnd + " have written " +
                                        "data but never received txnEnd at checkpoint. Upstream must " +
                                        "ensure all transactions are complete before checkpoint barrier.");
                            }
                            for (TransactionTableRegion region : flushQ) {
                                if (!region.isActiveChunkCleanBoundary()) {
                                    throw new IllegalStateException(
                                            "[MultiTxn] Region " + region.getUniqueKey() + " has in-progress " +
                                            "transaction data (writes after latest txnEnd) at checkpoint. " +
                                            "Upstream must ensure all transactions are complete before " +
                                            "checkpoint barrier.");
                                }
                            }

                            // Wait for all regions to quiesce (neither flushing nor
                            // retrying) before opening the shared transaction. Without
                            // this wait, ensureSharedTransaction() would return false on
                            // any transient in-flight load and force the checkpoint to
                            // fail — a common race under load. The check mirrors the
                            // allLoadsDone loop below, extended with isRetrying() (so
                            // we don't race trySetLabel() against a region currently
                            // between retry attempts), and capped by flushTimeoutMs so
                            // a non-converging retry cannot block the savepoint forever.
                            long waitDeadline = System.currentTimeMillis() + flushTimeoutMs;
                            while (true) {
                                if (this.e != null) {
                                    // Some callback failed the manager while we were
                                    // waiting. Preserve the original root cause rather
                                    // than letting ensureSharedTransaction() below throw
                                    // a secondary exception that the outer catch would
                                    // assign to this.e, masking the real error.
                                    throw new IllegalStateException(
                                            "[MultiTxn] Manager errored while waiting for "
                                            + "regions to quiesce during savepoint", this.e);
                                }
                                boolean anyBusy = false;
                                for (TransactionTableRegion region : flushQ) {
                                    if (region.isFlushing() || region.isRetrying()) {
                                        anyBusy = true;
                                        break;
                                    }
                                }
                                if (!anyBusy) {
                                    break;
                                }
                                if (System.currentTimeMillis() > waitDeadline) {
                                    throw new IllegalStateException(
                                            "[MultiTxn] Savepoint timed out after " + flushTimeoutMs
                                            + "ms waiting for regions to quiesce before opening "
                                            + "shared transaction");
                                }
                                LockSupport.parkNanos(1_000_000L);
                            }

                            // Ensure a shared transaction is open (may not be if no data
                            // was written yet, or if we just finished a commit cycle).
                            // After the quiesce wait above this will almost always
                            // succeed; the throw covers the microsecond-window race
                            // where trySetLabel() loses to a late-arriving retry.
                            if (!txnCoordinator.isActive() && !flushQ.isEmpty()) {
                                if (!ensureSharedTransaction()) {
                                    throw new IllegalStateException(
                                            "[MultiTxn] Could not open shared transaction during "
                                            + "savepoint even after quiesce wait (likely trySetLabel "
                                            + "lost a race with a late retry). Failing the checkpoint "
                                            + "to preserve atomicity; Flink will retry.");
                                }
                            }

                            // Force switch remaining activeChunks: since we verified
                            // above that every region is at a clean boundary, any data
                            // still in activeChunk belongs to completed source
                            // transactions and is safe to freeze for commit.
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
                                    // Keep lastCommitTimeMs in sync with actual commit
                                    // activity so the next shouldTriggerCommit() check on
                                    // the normal path starts its countdown from the
                                    // savepoint commit, not from the previous regular
                                    // commit. This is cosmetic (downstream hasDataLoaded
                                    // / hasInactiveChunks checks prevent spurious commits),
                                    // but keeps the time accounting accurate.
                                    lastCommitTimeMs = System.currentTimeMillis();
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
                                boolean opened;
                                try {
                                    opened = ensureSharedTransaction();
                                } catch (Exception beginEx) {
                                    LOG.error("[MultiTxn] Failed to eagerly open shared transaction", beginEx);
                                    this.e = beginEx;
                                    // Skip flush/commit to avoid creating orphan independent transactions.
                                    continue;
                                }
                                if (!opened) {
                                    // ensureSharedTransaction() silently skipped (e.g. a region is
                                    // flushing/retrying, or trySetLabel lost a race with a concurrent
                                    // retry). Regions may hold null/stale labels, so an autonomous
                                    // flush here would create an orphan independent transaction and
                                    // break multi-table atomicity. Retry on the next scan.
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

                        // In multi-table mode, first give the manager-thread
                        // fallback a chance to force-switch any region whose
                        // activeChunk is at a clean transaction boundary and has
                        // been idle long enough (handles the "source paused
                        // after txnEnd" case). Then check whether the commit
                        // interval has elapsed AND there is data to commit.
                        if (multiTableTransactionEnabled && partitionTracker != null) {
                            managerForceSwitchCleanBoundaryRegions();
                            tryStartTimerDrivenCommit();
                            if (commitInFlight.get()) {
                                // Commit interval elapsed with data available; skip
                                // autonomous flush and enter processMultiTableCommit()
                                // on the next iteration.
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

        // For each region owned by this partition, invoke tryMiniIntervalSwitch:
        // - It marks the region's activeChunk as being at a clean transaction
        //   boundary (since the most recent task-thread event on the region is
        //   now a txnEnd).
        // - It performs switchChunkForCommit only if the miniInterval has
        //   elapsed since the last switch AND activeChunk has data. Otherwise
        //   the current source transaction batches into the existing activeChunk
        //   together with previously-completed source transactions, amortizing
        //   the HTTP-load overhead across multiple txns (N:1 mapping).
        //
        // Both the cleanBoundary mark and the conditional switch MUST run on
        // the task thread (not deferred to the manager thread) because the task
        // thread is the sole serializer of write() and setCommitAllowed()
        // events. Marking cleanBoundary=true here guarantees that the flag
        // reflects the MOST RECENT task-thread event: if the next event is
        // another write, write0() will flip it back to false before the manager
        // thread's scan observes it, preserving the invariant that
        // "cleanBoundary=true" means "activeChunk contains only data from
        // completed source transactions". Deferring to the manager thread would
        // open a window where a write and a manager-thread force-switch could
        // race, potentially freezing partial transaction data.
        List<TransactionTableRegion> pRegions = partitionRegions.get(partition);
        int regionCount = pRegions == null ? 0 : pRegions.size();
        if (pRegions != null) {
            for (TransactionTableRegion region : pRegions) {
                region.tryMiniIntervalSwitch();
            }
        }
        partitionTracker.onTxnEnd(partition);
        if (LOG.isDebugEnabled()) {
            LOG.debug("[MultiTxn] txnEnd recorded for partition={}, regions={}, miniInterval={}ms",
                    partition, regionCount, miniSwitchIntervalMs);
        }
    }

    /**
     * Manager-thread fallback: force-switch any region whose activeChunk is at a
     * clean transaction boundary, has data, and has been idle (no task-thread
     * switch) for at least {@code miniSwitchIntervalMs}.
     *
     * <p>This handles the "source paused after a few txnEnds" case: the task
     * thread has processed a txnEnd without switching (because the previous
     * switch was too recent), and then no further task-thread events arrive.
     * Without this fallback, the completed-but-not-yet-frozen data would sit in
     * activeChunk until the next source write + txnEnd, which could be an
     * unbounded delay.
     *
     * <p>{@link TransactionTableRegion#tryForceCleanSwitch()} handles all the
     * write-lock acquisition and double-checking of the clean-boundary flag
     * under the lock, so this loop is racy-safe.
     */
    private void managerForceSwitchCleanBoundaryRegions() {
        for (TransactionTableRegion region : flushQ) {
            region.tryForceCleanSwitch();
        }
    }

    /**
     * Decide whether the manager thread should trigger a commit. Called on every
     * scan cycle of the manager's main loop. Returns {@code true} only when:
     *
     * <ol>
     *   <li>The commit interval has elapsed since the last successful commit.</li>
     *   <li>There is data that could be committed: either the shared transaction
     *       coordinator has already recorded a load (via autonomous flush) or at
     *       least one region has inactive chunks pending.</li>
     * </ol>
     *
     * <p>If the interval has elapsed but there is no data, we simply skip the
     * cycle without consuming the interval — the next time data becomes
     * available, it will be committed immediately.
     */
    private boolean shouldTriggerCommit() {
        if (commitInFlight.get()) {
            return false;
        }
        if (System.currentTimeMillis() - lastCommitTimeMs < commitIntervalMs) {
            return false;
        }
        if (txnCoordinator != null && txnCoordinator.hasDataLoaded()) {
            return true;
        }
        for (TransactionTableRegion region : flushQ) {
            if (region.hasInactiveChunks()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Attempts to start a time-driven commit cycle. Called on the manager thread
     * from the normal scan path. Does NOT do any per-region switching — that is
     * handled by (a) the task thread's {@code tryMiniIntervalSwitch} on each
     * txnEnd and (b) {@link #managerForceSwitchCleanBoundaryRegions()} for the
     * source-idle fallback.
     *
     * <p>Sets {@code commitInFlight=true} if {@link #shouldTriggerCommit()}
     * returns true; the main loop will then enter {@code processMultiTableCommit()}
     * on the next iteration to drive the actual commit protocol.
     */
    private void tryStartTimerDrivenCommit() {
        if (shouldTriggerCommit() && commitInFlight.compareAndSet(false, true)) {
            commitInFlightStartMs = System.currentTimeMillis();
            // No flushable.signal() needed — this method already runs on the
            // manager thread. The caller will see commitInFlight==true and
            // `continue` to the next iteration which enters processMultiTableCommit().
            LOG.info("[MultiTxn] Commit interval elapsed with data available, commitInFlight=true");
        }
    }

    /**
     * Processes a multi-table commit cycle using the SharedTransactionCoordinator.
     * Called on the manager thread when {@code commitInFlight=true}.
     *
     * <p>Because the shared transaction is eagerly opened (by
     * {@link #ensureSharedTransaction()}) before any autonomous flush, all
     * in-flight HTTP loads already use the shared label. This method simply:
     * <ol>
     *   <li>Waits for any in-flight loads to complete (defers to the next scan
     *       cycle if any region is still FLUSHING/retrying)</li>
     *   <li>Triggers loads for any remaining inactive chunks — those produced by
     *       {@code switchChunkForCommit} or the manager-thread clean-boundary
     *       fallback that were not yet drained by autonomous flush</li>
     *   <li>On the next scan cycle, once those triggered loads complete,
     *       executes a unified commit via the coordinator (multi-table
     *       transactions skip the prepare step, which StarRocks does not
     *       support in multi-table mode)</li>
     *   <li>Resets state ({@code commitInFlight}, {@code partitionTracker},
     *       region labels, {@code lastCommitTimeMs}) and opens a new shared
     *       transaction for the next cycle</li>
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
                if (!ensureSharedTransaction()) {
                    // A region is flushing/retrying, or label injection lost a race.
                    // Bail out and retry on the next scan; triggerLoadIfNeeded() below
                    // would otherwise generate independent labels and break atomicity.
                    LOG.debug("[MultiTxn] Could not open shared txn in commit path; retrying next scan");
                    return;
                }
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
            // Record the commit time so shouldTriggerCommit() re-starts its
            // interval countdown from this point.
            lastCommitTimeMs = System.currentTimeMillis();
            LOG.info("[MultiTxn] Shared transaction cycle completed; commitInFlight=false");

            // Immediately open a new shared transaction for the next cycle,
            // so any subsequent autonomous flushes are under the new shared label.
            // This is best-effort: if it cannot be opened now (exception, or a region
            // is flushing/retrying), the normal manager-thread path guards against
            // autonomous flushes by checking isActive() + ensureSharedTransaction()
            // again before selecting regions to flush.
            if (!flushQ.isEmpty()) {
                try {
                    if (!ensureSharedTransaction()) {
                        LOG.debug("[MultiTxn] Could not eagerly open next shared transaction after commit; "
                                + "will retry on next scan");
                    }
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
     *
     * @return {@code true} if a shared transaction was opened and every region in
     *         {@code flushQ} received the shared label; {@code false} if the attempt
     *         was skipped or rolled back due to a transient condition (empty flushQ,
     *         a region was flushing/retrying, or label injection lost a race with a
     *         concurrent retry). Callers MUST skip flush/commit work when this
     *         returns {@code false}, since regions may hold null/stale labels and
     *         an autonomous flush would create an orphan independent transaction
     *         that breaks multi-table atomicity. The next manager scan will retry.
     */
    private boolean ensureSharedTransaction() {
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
            return false;
        }

        // Ensure no region is retrying — setLabel() silently skips when numRetries > 0,
        // which would leave the region with a stale label, breaking atomicity.
        for (TransactionTableRegion region : flushQ) {
            if (region.isFlushing() || region.isRetrying()) {
                LOG.debug("[MultiTxn] Cannot open shared txn: region {} is flushing/retrying",
                        region.getUniqueKey());
                return false;
            }
        }

        txnCoordinator.begin(anyDb, anyTable);

        List<TransactionTableRegion> injected = new ArrayList<>();
        for (TransactionTableRegion region : flushQ) {
            // Re-check retrying: a region may have entered retry (via fail() on the
            // executor thread) between the bulk check above and this point.
            //
            // The isRetrying() check below is still a useful fast-path, but it does
            // NOT close the race by itself because the monitor is released between
            // isRetrying() and trySetLabel(). trySetLabel() re-checks numRetries
            // under the same synchronized block that fail() uses to increment it,
            // so if a retry starts in that window it returns false and we treat
            // the region exactly like the isRetrying() branch: roll back the
            // already-injected labels and re-drive ensureSharedTransaction on the
            // next scan. This preserves the invariant that every region in
            // `injected` has actually received the shared label.
            if (region.isRetrying()
                    || !region.trySetLabel(txnCoordinator.getSharedLabel())) {
                LOG.warn("[MultiTxn] Region {} started retrying during ensureSharedTransaction; "
                        + "rolling back and clearing {} already-injected labels",
                        region.getUniqueKey(), injected.size());
                txnCoordinator.reset();
                for (TransactionTableRegion r : injected) {
                    r.setLabel(null);
                }
                return false;
            }
            injected.add(region);
        }
        LOG.info("[MultiTxn] Eagerly opened shared transaction: label={}",
                txnCoordinator.getSharedLabel());
        return true;
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

        // If any region has in-progress source transaction data, we must NOT
        // commit the recycled transaction — that would expose a partial source
        // transaction in StarRocks. Two conditions trigger fail-fast:
        //   (a) Any partition has written data but never received txnEnd
        //       (ACTIVE in the tracker). This means the source started a
        //       transaction and never closed it within the timeout window.
        //   (b) Any region's activeChunk is not at a clean boundary. This
        //       means a write occurred after the latest txnEnd but before the
        //       current source transaction completed.
        // In either case, fail so Flink restarts from the last checkpoint.
        if (partitionTracker != null) {
            List<Integer> partitionsWithoutTxnEnd = partitionTracker.getPartitionsWithoutTxnEnd();
            if (!partitionsWithoutTxnEnd.isEmpty()) {
                LOG.error("[MultiTxn] Shared transaction approaching timeout but partitions {} "
                        + "have written data without receiving txnEnd. Rolling back and failing fast. "
                        + "Upstream must complete source transactions within {}ms. "
                        + "label={}", partitionsWithoutTxnEnd, sharedTxnMaxIdleMs,
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
                        "Partitions without txnEnd: " + partitionsWithoutTxnEnd);
                return;
            }
        }
        List<String> dirtyRegions = null;
        for (TransactionTableRegion region : flushQ) {
            if (!region.isActiveChunkCleanBoundary()) {
                if (dirtyRegions == null) {
                    dirtyRegions = new ArrayList<>();
                }
                dirtyRegions.add(region.getUniqueKey());
            }
        }
        if (dirtyRegions != null) {
            LOG.error("[MultiTxn] Shared transaction approaching timeout but regions {} "
                    + "have in-progress transaction data (writes after latest txnEnd). "
                    + "Rolling back and failing fast. label={}",
                    dirtyRegions, txnCoordinator.getSharedLabel());
            txnCoordinator.reset();
            for (TransactionTableRegion region : flushQ) {
                if (!region.isRetrying()) {
                    region.setLabel(null);
                }
            }
            commitInFlight.set(false);
            if (partitionTracker != null) {
                partitionTracker.reset();
            }
            this.e = new StreamLoadFailException(
                    "[MultiTxn] Multi-table transaction timeout: regions with in-progress " +
                    "source transactions: " + dirtyRegions);
            return;
        }

        // All regions were observed clean in the pre-check above. Now freeze
        // their activeChunks atomically using tryForceCleanSwitch(), which
        // re-checks cleanBoundary under the writeLock. This closes a race
        // window: between the unlocked pre-check and the actual switch, the
        // task thread could call write() on a region and flip its
        // cleanBoundary to false. Using switchChunkForCommit() here would
        // unconditionally freeze that now-dirty activeChunk (containing an
        // in-progress source transaction), violating the safety invariant.
        //
        // If tryForceCleanSwitch() returns false for a region, one of:
        //   (a) a concurrent write raced and made the region dirty — its
        //       data stays in activeChunk and will be committed in a future
        //       cycle (under the new shared label opened after this recycle);
        //   (b) activeChunk was already empty — nothing to do;
        //   (c) miniInterval has not yet elapsed — cannot happen here because
        //       recycle fires only after sharedTxnMaxIdleMs which is far
        //       larger than any miniInterval.
        // In all three cases, skipping the region is safe.
        for (TransactionTableRegion region : flushQ) {
            boolean switched = region.tryForceCleanSwitch();
            if (!switched && !region.isActiveChunkCleanBoundary()) {
                LOG.warn("[MultiTxn] Recycle: region {} became dirty between pre-check and switch; " +
                        "its data will be committed in a future cycle", region.getUniqueKey());
            }
        }
        // Drain any newly-frozen inactive chunks into the shared transaction
        // before committing. We wait synchronously for each region's load to
        // avoid racing with the subsequent label-clear step.
        for (TransactionTableRegion region : flushQ) {
            if (region.triggerLoadIfNeeded()) {
                txnCoordinator.markDataLoaded();
            }
        }
        // Wait for in-flight loads with a bounded timeout. This point is
        // reached only after sharedTxnMaxIdleMs has elapsed, so we are already
        // operating on a timeout budget; an unbounded wait could deadlock the
        // manager thread if a load hangs (e.g., network stall or a region stuck
        // in a long retry loop). Cap the total wait at flushTimeoutMs and fail
        // fast on timeout so Flink can restart the job from the last checkpoint.
        long recycleWaitStartMs = System.currentTimeMillis();
        for (TransactionTableRegion region : flushQ) {
            while (region.isFlushing() || region.isRetrying()) {
                if (System.currentTimeMillis() - recycleWaitStartMs > flushTimeoutMs) {
                    LOG.error("[MultiTxn] Recycle wait timeout ({}ms) for region {}, " +
                            "failing fast. label={}",
                            flushTimeoutMs, region.getUniqueKey(),
                            txnCoordinator.getSharedLabel());
                    txnCoordinator.reset();
                    for (TransactionTableRegion r : flushQ) {
                        if (!r.isRetrying()) {
                            r.setLabel(null);
                        }
                    }
                    commitInFlight.set(false);
                    if (partitionTracker != null) {
                        partitionTracker.reset();
                    }
                    this.e = new StreamLoadFailException(
                            "[MultiTxn] Recycle wait timeout: region " + region.getUniqueKey() +
                            " did not complete its in-flight load within " + flushTimeoutMs + "ms");
                    return;
                }
                LockSupport.parkNanos(1_000_000L);
            }
        }

        String anyTable = null;
        for (TransactionTableRegion region : flushQ) {
            if (anyTable == null) {
                anyTable = region.getTable();
            }
        }

        boolean actuallyCommitted = false;
        if (anyTable != null && txnCoordinator.hasDataLoaded()) {
            txnCoordinator.prepareAndCommit(anyTable);
            actuallyCommitted = true;
            LOG.info("[MultiTxn] Recycled shared transaction committed");
        } else {
            txnCoordinator.reset();
            LOG.info("[MultiTxn] Recycled empty shared transaction (rolled back)");
        }

        // Clear labels and reset cycle state before opening a fresh shared transaction.
        for (TransactionTableRegion region : flushQ) {
            region.setLabel(null);
        }
        if (partitionTracker != null) {
            partitionTracker.reset();
        }
        // Only advance lastCommitTimeMs on a real commit. A rollback-empty
        // recycle (no data was ever loaded) is not a commit, and we must not
        // delay the next commit-interval countdown by pretending one happened.
        // Otherwise a burst of data immediately after an idle recycle would
        // have to wait an extra commitInterval before becoming visible.
        if (actuallyCommitted) {
            lastCommitTimeMs = System.currentTimeMillis();
        }
        // Best-effort: re-open a fresh shared transaction so the next autonomous
        // flush uses the new shared label. If a region is flushing/retrying now
        // we'll skip silently — the normal manager-thread path re-checks
        // isActive() and retries ensureSharedTransaction() before any flush,
        // so no orphan independent transaction can slip through.
        if (!ensureSharedTransaction()) {
            LOG.debug("[MultiTxn] Could not open fresh shared transaction after recycle; "
                    + "will retry on next scan");
        }
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
     * Applies {@code delta} to the multi-table aggregate in-progress txn byte
     * counter and returns the resulting value.
     *
     * <p>Called from {@link TransactionTableRegion} when:
     * <ul>
     *   <li>a write extends an in-progress source transaction ({@code delta > 0});</li>
     *   <li>a region transitions back to a clean transaction boundary,
     *       freezing previously in-progress bytes as committed ({@code delta < 0}).</li>
     * </ul>
     *
     * <p>Meaningful only when multi-table transaction mode is enabled.
     */
    long addAggregateInProgressTxnBytes(long delta) {
        return aggregateInProgressTxnBytes.addAndGet(delta);
    }

    /** Returns the current value of the aggregate in-progress txn byte counter. */
    long getAggregateInProgressTxnBytes() {
        return aggregateInProgressTxnBytes.get();
    }

    /**
     * Returns the effective write-block threshold. When the multi-table aggregate
     * in-progress bytes plus an incoming row would exceed this threshold, the
     * task thread would be blocked by {@link #blockIfCacheFull} with no way for
     * any region to flush (no region has reached a clean boundary yet) — a
     * silent deadlock. Regions call this to know when to fail fast.
     */
    long getMaxWriteBlockCacheBytes() {
        return maxWriteBlockCacheBytes;
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
            try {
                manager.interrupt();
                streamLoader.close();
            } finally {
                // Defensive: drop any residual in-progress byte accounting so a
                // later reuse of this instance (or a snapshot taken post-close)
                // cannot observe a stale non-zero aggregate that would produce
                // false-positive fail-fasts in write0's aggregate guard. The
                // reset must run even if streamLoader.close() throws — otherwise
                // the "defensive" framing in the comment above would only hold
                // on the happy path.
                aggregateInProgressTxnBytes.set(0L);
            }
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
                    // In multi-table mode, pass maxWriteBlockCacheBytes (= 2 * multi-table
                    // buffer size) as the per-region hard cap for an in-progress source
                    // transaction. This is the exact threshold at which blockIfCacheFull
                    // would start blocking the task thread; if a single region's activeChunk
                    // alone exceeds it, deadlock is inevitable because the manager has no
                    // inactiveChunks to flush (multi-table mode cannot switch activeChunk
                    // until the next txnEnd arrives). Failing fast here gives a clear error
                    // instead of a silent hang.
                    long singleTxnMaxBytes = multiTableTransactionEnabled ? maxWriteBlockCacheBytes : 0L;
                    TransactionTableRegion newRegion = new TransactionTableRegion(
                            uniqueKey, database, table, this,
                            tableProperties, streamLoader, labelGenerator, maxRetries, retryIntervalInMs,
                            multiTableTransactionEnabled, miniSwitchIntervalMs, singleTxnMaxBytes);
                    if (multiTableTransactionEnabled) {
                        newRegion.getHeaders().put("transaction_type", "multi");
                        // If a shared transaction is already open, inject its label so that
                        // the first flush of this region uses the shared label.
                        //
                        // Shared transactions are single-database by construction
                        // (see ensureSharedTransaction()), so a region whose database
                        // does not match the active shared txn must never receive that
                        // label — otherwise its first flush would POST to its own
                        // database with a label that belongs to a different one, which
                        // StarRocks rejects and which would abort the sink task with
                        // an opaque HTTP error. Fail fast at routing time instead, so
                        // the exception points directly at the mixed-database write.
                        if (txnCoordinator != null && txnCoordinator.isActive()) {
                            String txnDb = txnCoordinator.getDatabase();
                            if (txnDb != null && !txnDb.equals(database)) {
                                throw new IllegalStateException(
                                        "[MultiTxn] Cannot route write for database '" + database
                                        + "' while a shared transaction is active for database '"
                                        + txnDb + "'. Multi-table shared transactions require all "
                                        + "regions to share the same database; configure a separate "
                                        + "sink for each database.");
                            }
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