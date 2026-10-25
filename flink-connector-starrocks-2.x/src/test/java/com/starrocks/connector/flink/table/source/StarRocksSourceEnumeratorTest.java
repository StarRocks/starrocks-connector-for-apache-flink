/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
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

package com.starrocks.connector.flink.table.source;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;

import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StarRocksSourceEnumeratorTest {

    private static final StarRocksSourceSplit SPLIT_0 = split("split-0-be1");
    private static final StarRocksSourceSplit SPLIT_1 = split("split-1-be2");

    @Test
    public void testDiscoversOffTheCoordinatorThreadAndAssignsOnRequest() throws Throwable {
        try (MockSplitEnumeratorContext<StarRocksSourceSplit> context = context(2)) {
            AtomicInteger discoveries = new AtomicInteger();
            StarRocksSourceEnumerator enumerator = new StarRocksSourceEnumerator(
                    context, discoverer(discoveries, SPLIT_0, SPLIT_1), emptyState(false));
            enumerator.start();
            assertEquals("planning must not run on the coordinator thread", 0, discoveries.get());

            enumerator.handleSplitRequest(0, null);
            assertTrue(context.getSplitsAssignmentSequence().isEmpty());
            assertFalse(context.hasNoMoreSplits(0));

            context.runNextOneTimeCallable();
            assertEquals(1, discoveries.get());
            assertEquals(Collections.singletonList(SPLIT_0), assignedTo(context, 0));

            enumerator.handleSplitRequest(1, null);
            assertEquals(Collections.singletonList(SPLIT_1), assignedTo(context, 1));

            enumerator.handleSplitRequest(0, null);
            assertTrue(context.hasNoMoreSplits(0));
            assertFalse(context.hasNoMoreSplits(1));
        }
    }

    @Test
    public void testRestoreFromEmptyStateDoesNotReadTheTableAgain() throws Throwable {
        try (MockSplitEnumeratorContext<StarRocksSourceSplit> context = context(2)) {
            AtomicInteger discoveries = new AtomicInteger();
            // every split was assigned before the checkpoint, so the readers restore them themselves
            StarRocksSourceEnumerator enumerator = new StarRocksSourceEnumerator(
                    context, discoverer(discoveries, SPLIT_0, SPLIT_1), emptyState(true));
            enumerator.start();
            assertTrue(context.getOneTimeCallables().isEmpty());

            enumerator.handleSplitRequest(0, null);
            enumerator.handleSplitRequest(1, null);

            assertEquals(0, discoveries.get());
            assertTrue(context.getSplitsAssignmentSequence().isEmpty());
            assertTrue(context.hasNoMoreSplits(0));
            assertTrue(context.hasNoMoreSplits(1));
        }
    }

    @Test
    public void testRestoreAssignsOnlyThePendingSplits() throws Throwable {
        try (MockSplitEnumeratorContext<StarRocksSourceSplit> context = context(1)) {
            StarRocksSourceEnumerator enumerator = new StarRocksSourceEnumerator(
                    context, failingDiscoverer(),
                    new StarRocksSourceEnumeratorState(true, new ArrayList<>(Collections.singletonList(SPLIT_1))));
            enumerator.start();

            enumerator.handleSplitRequest(0, null);
            assertEquals(Collections.singletonList(SPLIT_1), assignedTo(context, 0));
            enumerator.handleSplitRequest(0, null);
            assertTrue(context.hasNoMoreSplits(0));
        }
    }

    @Test
    public void testRestoreBeforeDiscoveryFinishedPlansAgain() throws Throwable {
        try (MockSplitEnumeratorContext<StarRocksSourceSplit> context = context(1)) {
            AtomicInteger discoveries = new AtomicInteger();
            StarRocksSourceEnumerator enumerator = new StarRocksSourceEnumerator(
                    context, discoverer(discoveries, SPLIT_0), emptyState(false));
            enumerator.start();
            context.runNextOneTimeCallable();
            assertEquals(1, discoveries.get());
            assertTrue(enumerator.snapshotState(1L).isSplitsDiscovered());
        }
    }

    @Test
    public void testAddSplitsBackReassignedToRestartedReader() throws Throwable {
        try (MockSplitEnumeratorContext<StarRocksSourceSplit> context = context(2)) {
            StarRocksSourceEnumerator enumerator = new StarRocksSourceEnumerator(
                    context, failingDiscoverer(), emptyState(true));
            enumerator.start();

            enumerator.addSplitsBack(Collections.singletonList(SPLIT_0), 0);
            StarRocksSourceEnumeratorState snapshot = enumerator.snapshotState(1L);
            assertTrue("returned splits must survive a checkpoint", snapshot.getPendingSplits().contains(SPLIT_0));

            enumerator.handleSplitRequest(0, null);
            assertEquals(Collections.singletonList(SPLIT_0), assignedTo(context, 0));
        }
    }

    @Test
    public void testIgnoresRequestsFromUnregisteredReaders() throws Throwable {
        try (MockSplitEnumeratorContext<StarRocksSourceSplit> context = new MockSplitEnumeratorContext<>(2)) {
            context.registerReader(new ReaderInfo(0, "localhost"));
            StarRocksSourceEnumerator enumerator = new StarRocksSourceEnumerator(
                    context, failingDiscoverer(),
                    new StarRocksSourceEnumeratorState(true, new ArrayList<>(Collections.singletonList(SPLIT_0))));
            enumerator.start();

            enumerator.handleSplitRequest(1, null);
            assertTrue(context.getSplitsAssignmentSequence().isEmpty());
            enumerator.handleSplitRequest(0, null);
            assertEquals(Collections.singletonList(SPLIT_0), assignedTo(context, 0));
        }
    }

    @Test
    public void testDiscoveryFailureFailsTheJob() throws Throwable {
        try (MockSplitEnumeratorContext<StarRocksSourceSplit> context = context(1)) {
            StarRocksSourceEnumerator enumerator = new StarRocksSourceEnumerator(
                    context, failingDiscoverer(), emptyState(false));
            enumerator.start();
            try {
                context.runNextOneTimeCallable();
                fail("a failed plan request must surface instead of leaving readers waiting");
            } catch (Throwable t) {
                assertTrue(String.valueOf(t.getMessage()).contains("Failed to discover splits"));
            }
        }
    }

    private static MockSplitEnumeratorContext<StarRocksSourceSplit> context(int parallelism) {
        MockSplitEnumeratorContext<StarRocksSourceSplit> context = new MockSplitEnumeratorContext<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            context.registerReader(new ReaderInfo(i, "localhost"));
        }
        return context;
    }

    private static StarRocksSourceEnumeratorState emptyState(boolean splitsDiscovered) {
        return new StarRocksSourceEnumeratorState(splitsDiscovered, new ArrayList<>());
    }

    private static Callable<List<StarRocksSourceSplit>> discoverer(
            AtomicInteger discoveries, StarRocksSourceSplit... splits) {
        return () -> {
            discoveries.incrementAndGet();
            return new ArrayList<>(Arrays.asList(splits));
        };
    }

    private static Callable<List<StarRocksSourceSplit>> failingDiscoverer() {
        return () -> {
            throw new IllegalStateException("FE unavailable");
        };
    }

    private static List<StarRocksSourceSplit> assignedTo(
            MockSplitEnumeratorContext<StarRocksSourceSplit> context, int subtaskId) {
        List<StarRocksSourceSplit> assigned = new ArrayList<>();
        for (SplitsAssignment<StarRocksSourceSplit> assignment : context.getSplitsAssignmentSequence()) {
            assigned.addAll(assignment.assignment().getOrDefault(subtaskId, Collections.emptyList()));
        }
        return assigned;
    }

    private static StarRocksSourceSplit split(String splitId) {
        return new StarRocksSourceSplit(new QueryBeXTablets("be1:9060", Arrays.asList(1L, 2L)), splitId, "plan");
    }
}
