/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StarRocksSourceSplitSerializerTest {

    @Test
    public void testRoundTripDataSplit() throws Exception {
        StarRocksSourceSplit split = new StarRocksSourceSplit(
                new QueryBeXTablets("be1:9060", Arrays.asList(1L, 2L, 3L)),
                "split-0-be1:9060",
                "opaque-plan");

        StarRocksSourceSplit restored = roundTrip(split);

        assertEquals("split-0-be1:9060", restored.splitId());
        assertEquals("be1:9060", restored.getBeXTablets().getBeNode());
        assertEquals(Arrays.asList(1L, 2L, 3L), restored.getBeXTablets().getTabletIds());
        assertEquals("opaque-plan", restored.getOpaquedQueryPlan());
    }

    @Test
    public void testRoundTripQueryPlanOver64KiB() throws Exception {
        StringBuilder plan = new StringBuilder(70000);
        for (int i = 0; i < 70000; i++) {
            plan.append((char) ('a' + i % 26));
        }
        StarRocksSourceSplit split = new StarRocksSourceSplit(
                new QueryBeXTablets("be1:9060", Arrays.asList(1L)),
                "split-0-be1:9060",
                plan.toString());

        StarRocksSourceSplit restored = roundTrip(split);

        assertEquals(plan.toString(), restored.getOpaquedQueryPlan());
    }

    @Test
    public void testRoundTripCountSplit() throws Exception {
        StarRocksSourceSplit split = new StarRocksSourceSplit(
                new QueryBeXTablets("count", Collections.emptyList()), "count-42");

        StarRocksSourceSplit restored = roundTrip(split);

        assertEquals("count-42", restored.splitId());
        assertNull(restored.getOpaquedQueryPlan());
    }

    @Test
    public void testRejectsUnknownVersion() throws Exception {
        StarRocksSourceSplitSerializer serializer = StarRocksSourceSplitSerializer.INSTANCE;
        byte[] bytes = serializer.serialize(new StarRocksSourceSplit(
                new QueryBeXTablets("count", Collections.emptyList()), "count-42"));
        try {
            serializer.deserialize(serializer.getVersion() + 1, bytes);
            fail("a split written by an unknown serializer version must not be read");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("Unknown"));
        }
    }

    @Test
    public void testEnumeratorStateWritesASharedPlanOnce() throws Exception {
        StringBuilder plan = new StringBuilder(70000);
        for (int i = 0; i < 70000; i++) {
            plan.append((char) ('a' + i % 26));
        }
        List<StarRocksSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            splits.add(new StarRocksSourceSplit(
                    new QueryBeXTablets("be" + i + ":9060", Arrays.asList((long) i)), "split-" + i, plan.toString()));
        }
        splits.add(new StarRocksSourceSplit(new QueryBeXTablets("count", Collections.emptyList()), "count-7"));
        StarRocksSourceEnumeratorStateSerializer serializer = StarRocksSourceEnumeratorStateSerializer.INSTANCE;

        byte[] bytes = serializer.serialize(new StarRocksSourceEnumeratorState(true, splits));
        assertTrue("the plan must be stored once, not per split", bytes.length < 2 * plan.length());

        StarRocksSourceEnumeratorState restored = serializer.deserialize(serializer.getVersion(), bytes);
        assertTrue(restored.isSplitsDiscovered());
        assertEquals(4, restored.getPendingSplits().size());
        for (int i = 0; i < 3; i++) {
            StarRocksSourceSplit split = restored.getPendingSplits().get(i);
            assertEquals("split-" + i, split.splitId());
            assertEquals("be" + i + ":9060", split.getBeXTablets().getBeNode());
            assertEquals(Collections.singletonList((long) i), split.getBeXTablets().getTabletIds());
            assertEquals(plan.toString(), split.getOpaquedQueryPlan());
        }
        assertNull(restored.getPendingSplits().get(3).getOpaquedQueryPlan());

        try {
            serializer.deserialize(serializer.getVersion() + 1, bytes);
            fail("enumerator state written by an unknown version must not be read");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("Unknown"));
        }
    }

    private static StarRocksSourceSplit roundTrip(StarRocksSourceSplit split) throws Exception {
        StarRocksSourceSplitSerializer serializer = StarRocksSourceSplitSerializer.INSTANCE;
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
    }
}
