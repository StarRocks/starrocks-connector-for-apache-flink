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

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
    public void testRoundTripCountSplit() throws Exception {
        StarRocksSourceSplit split = new StarRocksSourceSplit(
                new QueryBeXTablets("count", Collections.emptyList()), "count-42");

        StarRocksSourceSplit restored = roundTrip(split);

        assertEquals("count-42", restored.splitId());
        assertNull(restored.getOpaquedQueryPlan());
    }

    private static StarRocksSourceSplit roundTrip(StarRocksSourceSplit split) throws Exception {
        StarRocksSourceSplitSerializer serializer = StarRocksSourceSplitSerializer.INSTANCE;
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
    }
}
