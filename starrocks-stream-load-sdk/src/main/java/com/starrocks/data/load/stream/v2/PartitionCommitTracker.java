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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tracks per-partition transaction boundary state for multi-table transaction mode.
 *
 * <p>In the current design, commit timing is driven by the commit interval and
 * per-region {@code activeChunkCleanBoundary} flags. The tracker's role has been
 * reduced to <em>informational/safety</em> bookkeeping:
 *
 * <ul>
 *   <li>Remember which partitions have written data in the current commit cycle
 *       (used by {@link #getPartitionsWithoutTxnEnd()} to detect upstream contract
 *       violations at savepoint/recycle time).</li>
 *   <li>Remember which partitions have received at least one {@code txnEnd} since
 *       the last commit (the {@code TXN_END_SEEN} state is <em>sticky</em> — it is
 *       not cleared by subsequent writes).</li>
 * </ul>
 *
 * <p>The tracker no longer drives the switch/commit state machine. It does not
 * track per-partition "switched" state, does not manage pending txnEnd signals,
 * and does not decide when to commit. Those responsibilities live in
 * {@code DefaultStreamLoadManager} (commit timing) and
 * {@code TransactionTableRegion} (switch timing + clean-boundary flag).
 */
public class PartitionCommitTracker {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionCommitTracker.class);

    enum PartitionState {
        /** Partition has written data but has not yet received a txnEnd in the current cycle. */
        ACTIVE,
        /**
         * Partition has received at least one txnEnd since the last {@link #reset()}.
         * <p>This state is <em>sticky</em>: subsequent writes do not transition it
         * back to {@code ACTIVE}. The purpose is to let savepoint/recycle safety
         * checks distinguish "partition that never saw a txnEnd" (upstream contract
         * violation) from "partition that is between source transactions".
         */
        TXN_END_SEEN
    }

    private final Map<Integer, PartitionState> partitions = new LinkedHashMap<>();

    public PartitionCommitTracker() {
    }

    /**
     * Called when data is written for a partition. Registers the partition as
     * {@code ACTIVE} if it is not already tracked; otherwise leaves the state
     * unchanged (crucially, does not reset {@code TXN_END_SEEN} to {@code ACTIVE}).
     */
    public synchronized void onWrite(int partition) {
        if (!partitions.containsKey(partition)) {
            partitions.put(partition, PartitionState.ACTIVE);
        }
        // If already ACTIVE or TXN_END_SEEN: leave unchanged. TXN_END_SEEN is
        // sticky so that getPartitionsWithoutTxnEnd() continues to report "this
        // partition has seen at least one txnEnd" across subsequent writes.
    }

    /**
     * Called when a {@code txnEnd} marker arrives for a partition. Transitions
     * the partition to {@code TXN_END_SEEN} (registering it if needed).
     */
    public synchronized void onTxnEnd(int partition) {
        partitions.put(partition, PartitionState.TXN_END_SEEN);
    }

    /**
     * Returns partitions that have written data but never received a {@code txnEnd}
     * in the current commit cycle. This indicates an incomplete source transaction:
     *
     * <ul>
     *   <li>At <b>savepoint</b> time, this is an upstream contract violation — the
     *       connector must fail fast rather than commit partial transaction data.</li>
     *   <li>At <b>recycle</b> time (shared transaction approaching timeout), this
     *       indicates the upstream has not completed its transaction within the
     *       server-side timeout window — fail fast for the same reason.</li>
     * </ul>
     */
    public synchronized List<Integer> getPartitionsWithoutTxnEnd() {
        List<Integer> result = new ArrayList<>();
        for (Map.Entry<Integer, PartitionState> entry : partitions.entrySet()) {
            if (entry.getValue() == PartitionState.ACTIVE) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    /** Returns {@code true} if at least one partition has received a {@code txnEnd}. */
    public synchronized boolean hasAnyTxnEndSeen() {
        for (PartitionState state : partitions.values()) {
            if (state == PartitionState.TXN_END_SEEN) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resets state after a successful commit. All tracked partitions are cleared,
     * so the next commit cycle starts from an empty tracker. Partitions that
     * continue to produce data will be re-registered by the next {@link #onWrite}.
     */
    public synchronized void reset() {
        partitions.clear();
        LOG.info("[MultiTxn] PartitionCommitTracker reset");
    }

    public synchronized boolean isEmpty() {
        return partitions.isEmpty();
    }

    @Override
    public synchronized String toString() {
        return "PartitionCommitTracker{" +
                "partitions=" + partitions +
                '}';
    }
}
