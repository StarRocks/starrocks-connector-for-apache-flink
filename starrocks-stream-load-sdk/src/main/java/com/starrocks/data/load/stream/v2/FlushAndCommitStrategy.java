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

import com.starrocks.data.load.stream.StreamLoadStrategy;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class FlushAndCommitStrategy implements StreamLoadStrategy {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlushAndCommitStrategy.class);

    private final long expectDelayTime;
    private final long scanFrequency;
    private final long ageThreshold;
    private final long maxCacheBytes;
    private final boolean enableAutoCommit;

    private final AtomicLong numAgeTriggerFlush = new AtomicLong(0);
    private final AtomicLong numCacheTriggerFlush = new AtomicLong(0);
    private final AtomicLong numTableTriggerFlush = new AtomicLong(0);

    private final boolean multiTableTransactionEnabled;

    public FlushAndCommitStrategy(StreamLoadProperties properties, boolean enableAutoCommit) {
        this(properties, enableAutoCommit, properties.getMaxCacheBytes());
    }

    /**
     * Constructor that accepts an explicit {@code maxCacheBytes} threshold.
     *
     * <p>In multi-table transaction mode, {@code DefaultStreamLoadManager} overrides
     * its internal {@code maxCacheBytes} to the multi-table buffer size, but the
     * {@code FlushAndCommitStrategy} was previously still constructed from
     * {@code properties.getMaxCacheBytes()}, causing the cache-full flush threshold
     * in {@link #selectFlushRegions(Queue, long)} to be misaligned with the
     * manager's write-block threshold. This constructor lets the caller pass the
     * aligned value explicitly.
     */
    public FlushAndCommitStrategy(StreamLoadProperties properties, boolean enableAutoCommit, long maxCacheBytes) {
        this.expectDelayTime = properties.getExpectDelayTime();
        this.scanFrequency = properties.getScanningFrequency();
        // Integer division: commit may trigger slightly earlier than expectDelayTime
        // when expectDelayTime is not evenly divisible by scanFrequency
        this.ageThreshold = expectDelayTime / scanFrequency;
        this.maxCacheBytes = maxCacheBytes;
        this.enableAutoCommit = enableAutoCommit;
        this.multiTableTransactionEnabled = properties.isEnableMultiTableTransaction();

        LOG.info("{}", this);
    }

    @Override
    public List<TableRegion> select(Iterable<TableRegion> regions) {
       throw new UnsupportedOperationException();
    }

    public List<SelectFlushResult> selectFlushRegions(Queue<TransactionTableRegion> regions, long currentCacheBytes) {
        List<SelectFlushResult> flushRegions = new ArrayList<>();

        if (multiTableTransactionEnabled) {
            // Multi-table mode: selectFlushRegions only drains already-frozen
            // inactiveChunks. The activeChunk is never flushed by this path —
            // it can only be switched by switchChunkForCommit (on txnEnd or the
            // manager's clean-boundary fallback). Age-based commit and
            // row-count-triggered flush are both meaningless here because:
            //   - commit is driven by commit interval + shouldTriggerCommit
            //   - activeChunk never moves to inactive via flush()
            // So the selection rule collapses to "flush any region whose
            // inactive queue has data".
            for (TransactionTableRegion region : regions) {
                if (region.hasInactiveChunks()) {
                    numTableTriggerFlush.getAndIncrement();
                    flushRegions.add(new SelectFlushResult(FlushReason.INACTIVE_DRAIN, region));
                    LOG.debug("[MultiTxn] Choose region {} to drain inactive chunks",
                            region.getUniqueKey());
                }
            }
            return flushRegions;
        }

        // Non-multi-table mode: original behavior unchanged.
        for (TransactionTableRegion region : regions) {
            if (shouldCommit(region)) {
                numAgeTriggerFlush.getAndIncrement();
                flushRegions.add(new SelectFlushResult(FlushReason.COMMIT, region));
                LOG.debug("Choose region {} to flush because the region should commit, age: {}, " +
                            "threshold: {}, scanFreq: {}, expectDelayTime: {}", region.getUniqueKey(),
                                region.getAge(), ageThreshold, scanFrequency, expectDelayTime);
            } else {
                FlushReason reason = region.shouldFlush();
                if (reason != FlushReason.NONE) {
                    numTableTriggerFlush.getAndIncrement();
                    flushRegions.add(new SelectFlushResult(reason, region));
                    LOG.debug("Choose region {} to flush because the region itself decide to flush, age: {}, " +
                                    "threshold: {}, scanFreq: {}, expectDelayTime: {}, reason: {}", region.getUniqueKey(),
                            region.getAge(), ageThreshold, scanFrequency, expectDelayTime, reason);
                }
            }
        }

        // simply choose the region with maximum bytes
        if (flushRegions.isEmpty() && currentCacheBytes >= maxCacheBytes) {
            TransactionTableRegion region = regions.stream()
                    .max(Comparator.comparingLong(TableRegion::getCacheBytes)).orElse(null);
            if (region != null) {
                numCacheTriggerFlush.getAndIncrement();
                flushRegions.add(new SelectFlushResult(FlushReason.CACHE_FULL, region));
                LOG.debug("Choose region {} to flush because it's force flush, age: {}, " +
                            "threshold: {}, scanFreq: {}, expectDelayTime: {}", region.getUniqueKey(),
                                region.getAge(), ageThreshold, scanFrequency, expectDelayTime);
            }
        }

        return flushRegions;
    }
    
    /**
     * In multi-table mode, age-based commit is disabled (commits are event-driven
     * via PartitionCommitTracker). In normal mode, standard age-based commit applies.
     */
    public boolean shouldCommit(TableRegion region) {
        return enableAutoCommit && !multiTableTransactionEnabled && region.getAge() > ageThreshold;
    }

    public boolean isMultiTableTransactionEnabled() {
        return multiTableTransactionEnabled;
    }

    @Override
    public String toString() {
        return "FlushAndCommitStrategy{" +
                "expectDelayTime=" + expectDelayTime +
                ", scanFrequency=" + scanFrequency +
                ", ageThreshold=" + ageThreshold +
                ", maxCacheBytes=" + maxCacheBytes +
                ", enableAutoCommit=" + enableAutoCommit +
                ", multiTableTransactionEnabled=" + multiTableTransactionEnabled +
                ", numAgeTriggerFlush=" + numAgeTriggerFlush +
                ", numCacheTriggerFlush=" + numCacheTriggerFlush +
                ", numTableTriggerFlush=" + numTableTriggerFlush +
                '}';
    }

    public static class SelectFlushResult {

        private final FlushReason reason;
        private TransactionTableRegion region;

        public SelectFlushResult(FlushReason reason, TransactionTableRegion region) {
            this.reason = reason;
            this.region = region;
        }

        public FlushReason getReason() {
            return reason;
        }

        public TransactionTableRegion getRegion() {
            return region;
        }
    }
}
