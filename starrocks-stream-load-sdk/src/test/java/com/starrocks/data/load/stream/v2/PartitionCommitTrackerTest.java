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

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for the simplified {@link PartitionCommitTracker}. In the current design
 * the tracker is an informational/safety aid only: it tracks which partitions
 * have written data and which have seen at least one {@code txnEnd} since the
 * last commit. It no longer drives commit timing or switch decisions.
 */
public class PartitionCommitTrackerTest {

    /**
     * Verifies the basic state transitions: a partition starts in ACTIVE after
     * the first {@code onWrite}, then transitions to TXN_END_SEEN after
     * {@code onTxnEnd}.
     */
    @Test
    public void testBasicStateTransitions() {
        PartitionCommitTracker tracker = new PartitionCommitTracker();

        // Initially empty
        Assert.assertTrue(tracker.isEmpty());
        Assert.assertFalse(tracker.hasAnyTxnEndSeen());

        // Write → ACTIVE (partition not yet seen a txnEnd)
        tracker.onWrite(0);
        Assert.assertFalse(tracker.isEmpty());
        List<Integer> withoutTxnEnd = tracker.getPartitionsWithoutTxnEnd();
        Assert.assertEquals(1, withoutTxnEnd.size());
        Assert.assertEquals(Integer.valueOf(0), withoutTxnEnd.get(0));
        Assert.assertFalse(tracker.hasAnyTxnEndSeen());

        // txnEnd → TXN_END_SEEN
        tracker.onTxnEnd(0);
        Assert.assertTrue("hasAnyTxnEndSeen should be true after onTxnEnd",
                tracker.hasAnyTxnEndSeen());
        Assert.assertTrue("No partitions without txnEnd after onTxnEnd",
                tracker.getPartitionsWithoutTxnEnd().isEmpty());
    }

    /**
     * Verifies the sticky property: once a partition is TXN_END_SEEN, subsequent
     * {@code onWrite} calls must not demote it back to ACTIVE. This is important
     * so {@code getPartitionsWithoutTxnEnd()} correctly distinguishes "partition
     * that never saw a txnEnd" from "partition that is between source transactions".
     */
    @Test
    public void testTxnEndSeenIsSticky() {
        PartitionCommitTracker tracker = new PartitionCommitTracker();

        tracker.onWrite(0);
        tracker.onTxnEnd(0);
        Assert.assertTrue(tracker.hasAnyTxnEndSeen());
        Assert.assertTrue(tracker.getPartitionsWithoutTxnEnd().isEmpty());

        // Subsequent writes must NOT move the partition back to ACTIVE
        tracker.onWrite(0);
        tracker.onWrite(0);
        tracker.onWrite(0);

        Assert.assertTrue("TXN_END_SEEN state must be sticky across onWrite calls",
                tracker.hasAnyTxnEndSeen());
        Assert.assertTrue("Partition must not reappear in without-txnEnd list",
                tracker.getPartitionsWithoutTxnEnd().isEmpty());
    }

    /**
     * Verifies that {@code onTxnEnd} registers a previously-unknown partition
     * directly into TXN_END_SEEN (no prior {@code onWrite} required).
     */
    @Test
    public void testTxnEndForUnknownPartition() {
        PartitionCommitTracker tracker = new PartitionCommitTracker();

        tracker.onTxnEnd(42);
        Assert.assertFalse("Tracker should not be empty", tracker.isEmpty());
        Assert.assertTrue("Partition 42 should be TXN_END_SEEN",
                tracker.hasAnyTxnEndSeen());
        Assert.assertTrue("Partition 42 should not appear in without-txnEnd list",
                tracker.getPartitionsWithoutTxnEnd().isEmpty());
    }

    /**
     * Verifies that multiple partitions can be tracked independently and that
     * {@code getPartitionsWithoutTxnEnd()} returns only those still lacking a txnEnd.
     */
    @Test
    public void testMultiplePartitions() {
        PartitionCommitTracker tracker = new PartitionCommitTracker();

        tracker.onWrite(0);
        tracker.onWrite(1);
        tracker.onWrite(2);

        List<Integer> withoutTxnEnd = tracker.getPartitionsWithoutTxnEnd();
        Assert.assertEquals("All three partitions start in ACTIVE", 3, withoutTxnEnd.size());

        // Only partition 1 sees a txnEnd
        tracker.onTxnEnd(1);

        withoutTxnEnd = tracker.getPartitionsWithoutTxnEnd();
        Assert.assertEquals("Partitions 0 and 2 still lack txnEnd", 2, withoutTxnEnd.size());
        Assert.assertTrue(withoutTxnEnd.contains(0));
        Assert.assertTrue(withoutTxnEnd.contains(2));
        Assert.assertFalse(withoutTxnEnd.contains(1));
    }

    /**
     * Verifies that {@code reset()} clears all tracked partitions so the next
     * commit cycle starts from an empty tracker.
     */
    @Test
    public void testResetClearsAllPartitions() {
        PartitionCommitTracker tracker = new PartitionCommitTracker();

        tracker.onWrite(0);
        tracker.onWrite(1);
        tracker.onTxnEnd(0);
        tracker.onTxnEnd(1);

        Assert.assertFalse(tracker.isEmpty());
        Assert.assertTrue(tracker.hasAnyTxnEndSeen());

        tracker.reset();

        Assert.assertTrue("Tracker should be empty after reset", tracker.isEmpty());
        Assert.assertFalse("hasAnyTxnEndSeen should be false after reset",
                tracker.hasAnyTxnEndSeen());
        Assert.assertTrue("No partitions without txnEnd after reset",
                tracker.getPartitionsWithoutTxnEnd().isEmpty());
    }

    /**
     * Verifies that {@code onWrite} on an existing ACTIVE partition is idempotent
     * (does not accidentally register new entries).
     */
    @Test
    public void testRepeatedWritesOnSamePartition() {
        PartitionCommitTracker tracker = new PartitionCommitTracker();

        for (int i = 0; i < 100; i++) {
            tracker.onWrite(5);
        }

        List<Integer> withoutTxnEnd = tracker.getPartitionsWithoutTxnEnd();
        Assert.assertEquals("Only one partition should be tracked",
                1, withoutTxnEnd.size());
        Assert.assertEquals(Integer.valueOf(5), withoutTxnEnd.get(0));
    }

    /**
     * Verifies the "multiple complete source transactions within one commit cycle"
     * scenario (N:1 mapping). Multiple {@code onTxnEnd} calls on the same partition
     * should leave it in TXN_END_SEEN and remain consistent.
     */
    @Test
    public void testMultipleTxnEndsSamePartition() {
        PartitionCommitTracker tracker = new PartitionCommitTracker();

        tracker.onWrite(0);
        tracker.onTxnEnd(0);
        tracker.onWrite(0);
        tracker.onTxnEnd(0);
        tracker.onWrite(0);
        tracker.onTxnEnd(0);

        Assert.assertTrue(tracker.hasAnyTxnEndSeen());
        Assert.assertTrue(tracker.getPartitionsWithoutTxnEnd().isEmpty());
    }
}
