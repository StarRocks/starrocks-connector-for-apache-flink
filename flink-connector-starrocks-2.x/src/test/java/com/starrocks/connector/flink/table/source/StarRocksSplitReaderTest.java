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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.table.data.GenericRowData;

import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryPlan;
import com.starrocks.connector.flink.table.source.struct.Tablet;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StarRocksSplitReaderTest {

    private static final String BE1 = "be1:9060";
    private static final String BE2 = "be2:9060";

    private final List<String> opened = new ArrayList<>();
    private final Set<String> failingOpens = new HashSet<>();
    private final AtomicInteger plans = new AtomicInteger();

    @Test
    public void testSplitThatOpensIsNotPlannedAgain() throws Exception {
        StarRocksSplitReader reader = reader(1, plan("plan-2", tablet(1, BE1)));
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split(BE1, 1))));

        assertEquals(Collections.singletonList(1L), readAll(reader, "split-0"));
        assertEquals(Collections.singletonList(BE1 + " [1] plan-1"), opened);
        assertEquals(0, plans.get());
    }

    @Test
    public void testSplitThatFailsToOpenIsReadWithANewPlan() throws Exception {
        failingOpens.add(BE1 + " [1, 2] plan-1");
        StarRocksSplitReader reader = reader(1, plan("plan-2", tablet(1, BE1), tablet(2, BE1)));
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split(BE1, 1, 2))));

        assertEquals(Arrays.asList(1L, 2L), readAll(reader, "split-0"));
        assertEquals(Arrays.asList(BE1 + " [1, 2] plan-1", BE1 + " [1, 2] plan-2"), opened);
        assertEquals(1, plans.get());
    }

    @Test
    public void testNewPlanPrefersAnotherReplicaAndSpreadsTheSplitOverBes() throws Exception {
        failingOpens.add(BE1 + " [1, 2, 3] plan-1");
        // tablet 3 has no replica left on BE1, tablet 2 has one on BE1 and BE2, tablet 1 only on BE1
        StarRocksSplitReader reader = reader(1, plan("plan-2", tablet(1, BE1), tablet(2, BE1, BE2), tablet(3, BE2)));
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split(BE1, 1, 2, 3))));

        assertEquals(Arrays.asList(1L, 2L, 3L), readAll(reader, "split-0"));
        assertEquals(Arrays.asList(BE1 + " [1, 2, 3] plan-1", BE1 + " [1] plan-2", BE2 + " [2, 3] plan-2"), opened);
    }

    @Test
    public void testSplitIsRetriedUpToMaxRetriesWithANewPlanEachTime() throws Exception {
        failingOpens.add(BE1 + " [1] plan-1");
        failingOpens.add(BE1 + " [1] plan-2");
        StarRocksSplitReader reader = reader(2, plan("plan-2", tablet(1, BE1)), plan("plan-3", tablet(1, BE1)));
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split(BE1, 1))));

        assertEquals(Collections.singletonList(1L), readAll(reader, "split-0"));
        assertEquals(2, plans.get());
    }

    @Test
    public void testSplitFailsOnceTheRetriesAreUsedUp() throws Exception {
        failingOpens.add(BE1 + " [1] plan-1");
        failingOpens.add(BE1 + " [1] plan-2");
        StarRocksSplitReader reader = reader(1, plan("plan-2", tablet(1, BE1)));
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split(BE1, 1))));

        try {
            readAll(reader, "split-0");
            fail("the split must fail");
        } catch (IOException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("after 1 retries"));
            assertEquals("open " + BE1 + " [1] plan-2", e.getCause().getMessage());
        }
    }

    @Test
    public void testNoRetriesFailOnTheFirstError() throws Exception {
        failingOpens.add(BE1 + " [1] plan-1");
        StarRocksSplitReader reader = reader(0);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split(BE1, 1))));

        try {
            readAll(reader, "split-0");
            fail("the split must fail");
        } catch (IOException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("restart the job without state"));
            assertEquals(0, plans.get());
        }
    }

    @Test
    public void testSplitWhoseTabletsAreGoneFailsWithoutMoreRetries() throws Exception {
        failingOpens.add(BE1 + " [1, 2] plan-1");
        StarRocksSplitReader reader = reader(3, plan("plan-2", tablet(1, BE1)));
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split(BE1, 1, 2))));

        try {
            readAll(reader, "split-0");
            fail("the split must fail");
        } catch (IOException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Tablets [2] of split split-0"));
            assertEquals(1, plans.get());
        }
    }

    @Test
    public void testCountSplitNeedsNoScan() throws Exception {
        StarRocksSplitReader reader = reader(1);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(
                new StarRocksSourceSplit(new QueryBeXTablets("count", new ArrayList<>()), "count-3"))));

        RecordsWithSplitIds<GenericRowData> records = reader.fetch();
        assertEquals("count-3", records.nextSplit());
        int rows = 0;
        while (records.nextRecordFromSplit() != null) {
            rows++;
        }
        assertEquals(3, rows);
        assertEquals(Collections.singleton("count-3"), records.finishedSplits());
        assertTrue(opened.isEmpty());
    }

    private StarRocksSplitReader reader(int maxRetries, QueryPlan... newPlans) {
        Queue<QueryPlan> plansLeft = new ArrayDeque<>(Arrays.asList(newPlans));
        Callable<QueryPlan> planner = () -> {
            plans.incrementAndGet();
            return plansLeft.remove();
        };
        return new StarRocksSplitReader(planner, maxRetries, 0, (beNode, tabletIds, opaquedQueryPlan) -> {
            String open = beNode + " " + tabletIds + " " + opaquedQueryPlan;
            opened.add(open);
            if (failingOpens.contains(open)) {
                throw new RuntimeException("open " + open);
            }
            return new TabletIdReader(tabletIds);
        });
    }

    // reads the split to its end and returns the tablet ids its rows came from
    private static List<Long> readAll(StarRocksSplitReader reader, String splitId) throws IOException {
        List<Long> rows = new ArrayList<>();
        while (true) {
            RecordsWithSplitIds<GenericRowData> records = reader.fetch();
            String id;
            while ((id = records.nextSplit()) != null) {
                assertEquals(splitId, id);
                GenericRowData row;
                while ((row = records.nextRecordFromSplit()) != null) {
                    rows.add(row.getLong(0));
                }
            }
            if (records.finishedSplits().contains(splitId)) {
                return rows;
            }
        }
    }

    private static StarRocksSourceSplit split(String beNode, long... tabletIds) {
        List<Long> ids = new ArrayList<>();
        for (long id : tabletIds) {
            ids.add(id);
        }
        return new StarRocksSourceSplit(new QueryBeXTablets(beNode, ids), "split-0", "plan-1");
    }

    @SafeVarargs
    private static QueryPlan plan(String opaquedQueryPlan, Map.Entry<String, Tablet>... tablets) {
        QueryPlan plan = new QueryPlan();
        plan.setOpaqued_query_plan(opaquedQueryPlan);
        Map<String, Tablet> partitions = new HashMap<>();
        for (Map.Entry<String, Tablet> tablet : tablets) {
            partitions.put(tablet.getKey(), tablet.getValue());
        }
        plan.setPartitions(partitions);
        return plan;
    }

    private static Map.Entry<String, Tablet> tablet(long tabletId, String... routings) {
        Tablet tablet = new Tablet();
        tablet.setRoutings(Arrays.asList(routings));
        return new HashMap.SimpleEntry<>(String.valueOf(tabletId), tablet);
    }

    // one row per tablet, holding the tablet id
    private static class TabletIdReader implements StarRocksSourceDataReader {

        private final Queue<Long> tabletIds;

        TabletIdReader(List<Long> tabletIds) {
            this.tabletIds = new ArrayDeque<>(tabletIds);
        }

        @Override
        public GenericRowData getNext() {
            return GenericRowData.of(tabletIds.remove());
        }

        @Override
        public boolean hasNext() {
            return !tabletIds.isEmpty();
        }

        @Override
        public void close() {
        }
    }
}
