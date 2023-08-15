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

    private static TransactionTableRegion mockRegion(String uniqueKey, long age, long cacheBytes, FlushReason flushReason) {
        TransactionTableRegion region = mock(TransactionTableRegion.class);
        when(region.getUniqueKey()).thenReturn(uniqueKey);
        when(region.getAge()).thenReturn(age);
        when(region.getCacheBytes()).thenReturn(cacheBytes);
        when(region.shouldFlush()).thenReturn(flushReason);
        return region;
    }
}
