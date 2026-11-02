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

import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests the first-row time and latency that the coordinator reports per shared transaction. */
public class SharedTransactionCoordinatorTest {

    private static final LabelGeneratorFactory LABELS =
            new LabelGeneratorFactory.DefaultLabelGeneratorFactory("test-");

    private StreamLoader streamLoader;
    private RecordingListener listener;
    private SharedTransactionCoordinator coordinator;

    @Before
    public void setUp() {
        streamLoader = mock(StreamLoader.class);
        when(streamLoader.beginTransaction(anyString(), anyString())).thenReturn(true);
        when(streamLoader.commit(any(StreamLoadSnapshot.Transaction.class))).thenReturn(true);
        listener = new RecordingListener();
        coordinator = new SharedTransactionCoordinator(streamLoader, LABELS, () -> listener);
    }

    @Test
    public void testReportsEarliestFirstRowTime() {
        coordinator.begin("db", "t1");
        String label = coordinator.getSharedLabel();
        long now = System.currentTimeMillis();
        coordinator.recordLoadedChunk(label, now - 2000);
        coordinator.recordLoadedChunk(label, now - 5000);
        coordinator.recordLoadedChunk(label, now - 3000);
        coordinator.markDataLoaded();

        long beforeCommit = System.currentTimeMillis();
        coordinator.prepareAndCommit("t1");
        long afterCommit = System.currentTimeMillis();

        assertEquals(1, listener.calls.size());
        Call call = listener.calls.get(0);
        assertEquals(label, call.label);
        assertEquals(now - 5000, call.firstRowTimeMs);
        assertTrue(call.latencyMs >= beforeCommit - (now - 5000));
        assertTrue(call.latencyMs <= afterCommit - (now - 5000));
        assertFalse(coordinator.isActive());
    }

    @Test
    public void testIgnoresLoadsOfOtherLabels() {
        coordinator.begin("db", "t1");
        String label = coordinator.getSharedLabel();
        long now = System.currentTimeMillis();
        coordinator.recordLoadedChunk(label, now - 1000);
        // e.g. a late response for a label that was rolled back
        coordinator.recordLoadedChunk(label + "-stale", now - 60_000);
        coordinator.prepareAndCommit("t1");

        assertEquals(now - 1000, listener.calls.get(0).firstRowTimeMs);
    }

    @Test
    public void testCountsLoadWithUnknownLabel() {
        coordinator.begin("db", "t1");
        long now = System.currentTimeMillis();
        coordinator.recordLoadedChunk(coordinator.getSharedLabel(), now - 1000);
        coordinator.recordLoadedChunk(null, now - 4000);
        coordinator.prepareAndCommit("t1");

        assertEquals(now - 4000, listener.calls.get(0).firstRowTimeMs);
    }

    @Test
    public void testNoCallbackWhenFirstRowTimeIsUnknown() {
        // Recorded while no shared transaction is open: must not leak into the next one.
        coordinator.recordLoadedChunk("test-x", System.currentTimeMillis() - 1000);
        coordinator.begin("db", "t1");
        coordinator.recordLoadedChunk(coordinator.getSharedLabel(), -1L);
        coordinator.prepareAndCommit("t1");

        assertTrue(listener.calls.isEmpty());
        assertFalse(coordinator.isActive());
    }

    @Test
    public void testEachTransactionStartsFresh() {
        long now = System.currentTimeMillis();
        coordinator.begin("db", "t1");
        coordinator.recordLoadedChunk(coordinator.getSharedLabel(), now - 9000);
        coordinator.prepareAndCommit("t1");

        coordinator.begin("db", "t1");
        String second = coordinator.getSharedLabel();
        coordinator.recordLoadedChunk(second, now - 1000);
        coordinator.prepareAndCommit("t1");

        assertEquals(2, listener.calls.size());
        assertEquals(second, listener.calls.get(1).label);
        assertEquals(now - 1000, listener.calls.get(1).firstRowTimeMs);
    }

    @Test
    public void testRollbackDiscardsFirstRowTime() {
        coordinator.begin("db", "t1");
        coordinator.recordLoadedChunk(coordinator.getSharedLabel(), System.currentTimeMillis() - 9000);
        coordinator.reset();

        coordinator.begin("db", "t1");
        coordinator.prepareAndCommit("t1");

        assertTrue(listener.calls.isEmpty());
    }

    @Test
    public void testFailedCommitDoesNotNotify() {
        when(streamLoader.commit(any(StreamLoadSnapshot.Transaction.class))).thenReturn(false);
        coordinator.begin("db", "t1");
        coordinator.recordLoadedChunk(coordinator.getSharedLabel(), System.currentTimeMillis() - 1000);
        try {
            coordinator.prepareAndCommit("t1");
            fail("Expected the failed commit to throw");
        } catch (StreamLoadFailException expected) {
            // the manager rolls the transaction back with reset()
        }

        assertTrue(listener.calls.isEmpty());
        assertTrue(coordinator.isActive());
    }

    @Test
    public void testListenerFailureDoesNotFailCommit() {
        coordinator = new SharedTransactionCoordinator(streamLoader, LABELS, () -> new StreamLoadListener() {
            @Override
            public void onResponse(StreamLoadResponse response) {
            }

            @Override
            public void onSharedTransactionCommitted(String label, long firstRowTimeMs, long latencyMs) {
                throw new IllegalStateException("listener failure");
            }
        });
        coordinator.begin("db", "t1");
        coordinator.recordLoadedChunk(coordinator.getSharedLabel(), System.currentTimeMillis() - 1000);
        coordinator.prepareAndCommit("t1");

        verify(streamLoader).commit(any(StreamLoadSnapshot.Transaction.class));
        assertFalse(coordinator.isActive());
    }

    @Test
    public void testWorksWithoutListener() {
        coordinator = new SharedTransactionCoordinator(streamLoader, LABELS, () -> null);
        coordinator.begin("db", "t1");
        coordinator.recordLoadedChunk(coordinator.getSharedLabel(), System.currentTimeMillis() - 1000);
        coordinator.prepareAndCommit("t1");

        assertFalse(coordinator.isActive());
    }

    private static class Call {
        final String label;
        final long firstRowTimeMs;
        final long latencyMs;

        Call(String label, long firstRowTimeMs, long latencyMs) {
            this.label = label;
            this.firstRowTimeMs = firstRowTimeMs;
            this.latencyMs = latencyMs;
        }
    }

    private static class RecordingListener implements StreamLoadListener {
        final List<Call> calls = new ArrayList<>();

        @Override
        public void onResponse(StreamLoadResponse response) {
        }

        @Override
        public void onSharedTransactionCommitted(String label, long firstRowTimeMs, long latencyMs) {
            calls.add(new Call(label, firstRowTimeMs, latencyMs));
        }
    }
}
