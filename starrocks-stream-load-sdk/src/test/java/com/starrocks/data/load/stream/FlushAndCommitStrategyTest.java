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

package com.starrocks.data.load.stream;

import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.v2.FlushAndCommitStrategy;
import com.starrocks.data.load.stream.v2.FlushReason;
import com.starrocks.data.load.stream.v2.TransactionTableRegion;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlushAndCommitStrategyTest {

    @Test
    public void testFlushWithoutCacheFull() {
        StreamLoadProperties properties = StreamLoadProperties.builder()
                .expectDelayTime(1000)
                .scanningFrequency(50)
                .cacheMaxBytes(1024 * 1024)
                .build();
        FlushAndCommitStrategy strategy = new FlushAndCommitStrategy(properties, true);
        // should flush because age is enough
        TransactionTableRegion region1 = mockRegion("u1", 21, 100, FlushReason.NONE);
        // should flush because BUFFER_ROWS_REACH_LIMIT
        TransactionTableRegion region2 = mockRegion("u2", 10, 50, FlushReason.BUFFER_ROWS_REACH_LIMIT);
        // should not flush
        TransactionTableRegion region3 = mockRegion("u3", 10, 120, FlushReason.NONE);

        LinkedList<TransactionTableRegion> queue = new LinkedList<>();
        queue.add(region1);
        queue.add(region2);
        queue.add(region3);
        long cacheBytes = queue.stream().mapToLong(TransactionTableRegion::getCacheBytes).sum();
        List<FlushAndCommitStrategy.SelectFlushResult> resultList = strategy.selectFlushRegions(queue, cacheBytes);
        assertEquals(2, resultList.size());
        assertSame(region1, resultList.get(0).getRegion());
        assertSame(FlushReason.COMMIT, resultList.get(0).getReason());
        assertSame(region2, resultList.get(1).getRegion());
        assertSame(FlushReason.BUFFER_ROWS_REACH_LIMIT, resultList.get(1).getReason());
    }

    @Test
    public void testFlushWithCacheFull() {
        StreamLoadProperties properties = StreamLoadProperties.builder()
                .expectDelayTime(1000)
                .scanningFrequency(50)
                .cacheMaxBytes(100)
                .build();
        FlushAndCommitStrategy strategy = new FlushAndCommitStrategy(properties, true);
        // should not flush
        TransactionTableRegion region1 = mockRegion("u1", 1, 100, FlushReason.NONE);
        // should flush because the cache bytes is the maximum
        TransactionTableRegion region2 = mockRegion("u2", 1, 200, FlushReason.NONE);
        // should not flush
        TransactionTableRegion region3 = mockRegion("u3", 1, 120, FlushReason.NONE);

        LinkedList<TransactionTableRegion> queue = new LinkedList<>();
        queue.add(region1);
        queue.add(region2);
        queue.add(region3);
        long cacheBytes = queue.stream().mapToLong(TransactionTableRegion::getCacheBytes).sum();
        List<FlushAndCommitStrategy.SelectFlushResult> resultList = strategy.selectFlushRegions(queue, cacheBytes);
        assertEquals(1, resultList.size());
        assertSame(region2, resultList.get(0).getRegion());
        assertSame(FlushReason.CACHE_FULL, resultList.get(0).getReason());
    }

    /**
     * In multi-table mode, {@code selectFlushRegions} must ignore age-based
     * commit, {@code shouldFlush()} row-limit signals, and cache-full
     * triggers, and only return regions whose {@code inactiveChunks} is
     * non-empty. The commit cadence is controlled by
     * {@code DefaultStreamLoadManager.shouldTriggerCommit()} instead.
     */
    @Test
    public void testMultiTableModeSelectsOnlyRegionsWithInactiveChunks() {
        StreamLoadProperties properties = StreamLoadProperties.builder()
                .expectDelayTime(1000)
                .scanningFrequency(50)
                .cacheMaxBytes(1024 * 1024)
                .enableMultiTableTransaction()
                .build();
        FlushAndCommitStrategy strategy = new FlushAndCommitStrategy(properties, true);

        // Age-triggered in non-multi-table mode, but should be ignored here.
        TransactionTableRegion oldButEmpty =
                mockMultiTableRegion("u1", 1000, 100, FlushReason.NONE, false);
        // Has inactive chunks → should be selected.
        TransactionTableRegion pendingDrain =
                mockMultiTableRegion("u2", 1, 200, FlushReason.NONE, true);
        // shouldFlush=BUFFER_ROWS_REACH_LIMIT in non-multi-table mode, but
        // this signal is ignored in multi-table mode.
        TransactionTableRegion rowLimitReached =
                mockMultiTableRegion("u3", 1, 300, FlushReason.BUFFER_ROWS_REACH_LIMIT, false);

        LinkedList<TransactionTableRegion> queue = new LinkedList<>();
        queue.add(oldButEmpty);
        queue.add(pendingDrain);
        queue.add(rowLimitReached);
        long cacheBytes = queue.stream().mapToLong(TransactionTableRegion::getCacheBytes).sum();

        List<FlushAndCommitStrategy.SelectFlushResult> resultList =
                strategy.selectFlushRegions(queue, cacheBytes);

        assertEquals("Only regions with inactive chunks should be selected in multi-table mode",
                1, resultList.size());
        assertSame(pendingDrain, resultList.get(0).getRegion());
        assertSame("Multi-table drain should use INACTIVE_DRAIN reason",
                FlushReason.INACTIVE_DRAIN, resultList.get(0).getReason());
    }

    /**
     * In multi-table mode, even when the total cached bytes exceed
     * {@code maxCacheBytes}, {@code selectFlushRegions} must NOT pick a
     * region whose only data is in {@code activeChunk} — the activeChunk may
     * contain in-progress source transaction data that must not be flushed
     * until the next txnEnd arrives. Back-pressure via {@code blockIfCacheFull}
     * is the correct mechanism for limiting memory growth in this case.
     */
    @Test
    public void testMultiTableModeIgnoresCacheFullWhenNoInactive() {
        StreamLoadProperties properties = StreamLoadProperties.builder()
                .expectDelayTime(1000)
                .scanningFrequency(50)
                .cacheMaxBytes(100) // very small so cacheBytes > maxCacheBytes easily
                .enableMultiTableTransaction()
                .build();
        FlushAndCommitStrategy strategy = new FlushAndCommitStrategy(properties, true);

        // Three regions all with only active-chunk data (no inactive chunks);
        // total cacheBytes >> maxCacheBytes.
        TransactionTableRegion r1 = mockMultiTableRegion("u1", 1, 200, FlushReason.NONE, false);
        TransactionTableRegion r2 = mockMultiTableRegion("u2", 1, 300, FlushReason.NONE, false);
        TransactionTableRegion r3 = mockMultiTableRegion("u3", 1, 500, FlushReason.NONE, false);

        LinkedList<TransactionTableRegion> queue = new LinkedList<>();
        queue.add(r1);
        queue.add(r2);
        queue.add(r3);
        long cacheBytes = queue.stream().mapToLong(TransactionTableRegion::getCacheBytes).sum();

        List<FlushAndCommitStrategy.SelectFlushResult> resultList =
                strategy.selectFlushRegions(queue, cacheBytes);

        assertEquals("Cache-full must not pick a region without inactive chunks in multi-table mode",
                0, resultList.size());
    }

    /**
     * Sanity check for the explicit {@code maxCacheBytes} constructor path
     * added to fix the review P1 comment: the strategy must use the value
     * passed in, not {@code properties.getMaxCacheBytes()}.
     */
    @Test
    public void testExplicitMaxCacheBytesConstructor() {
        StreamLoadProperties properties = StreamLoadProperties.builder()
                .expectDelayTime(1000)
                .scanningFrequency(50)
                .cacheMaxBytes(100) // would trigger cache-full with a naive strategy
                .build();
        // Override to a much larger value (simulating the multi-table buffer override).
        long overriddenMaxCacheBytes = 10 * 1024 * 1024L;
        FlushAndCommitStrategy strategy =
                new FlushAndCommitStrategy(properties, true, overriddenMaxCacheBytes);

        // Small current cacheBytes well below the override; cache-full must NOT fire.
        TransactionTableRegion region = mockRegion("u1", 1, 500, FlushReason.NONE);
        LinkedList<TransactionTableRegion> queue = new LinkedList<>();
        queue.add(region);

        List<FlushAndCommitStrategy.SelectFlushResult> resultList =
                strategy.selectFlushRegions(queue, 500);

        assertEquals("No flush expected: 500 bytes is below the overridden maxCacheBytes",
                0, resultList.size());
    }

    private static TransactionTableRegion mockRegion(String uniqueKey, long age, long cacheBytes, FlushReason flushReason) {
        TransactionTableRegion region = mock(TransactionTableRegion.class);
        when(region.getUniqueKey()).thenReturn(uniqueKey);
        when(region.getAge()).thenReturn(age);
        when(region.getCacheBytes()).thenReturn(cacheBytes);
        when(region.shouldFlush()).thenReturn(flushReason);
        return region;
    }

    private static TransactionTableRegion mockMultiTableRegion(
            String uniqueKey, long age, long cacheBytes,
            FlushReason flushReason, boolean hasInactiveChunks) {
        TransactionTableRegion region = mockRegion(uniqueKey, age, cacheBytes, flushReason);
        when(region.hasInactiveChunks()).thenReturn(hasInactiveChunks);
        return region;
    }
}
