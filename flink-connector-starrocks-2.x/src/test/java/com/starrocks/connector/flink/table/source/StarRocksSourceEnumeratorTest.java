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

import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;

import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StarRocksSourceEnumeratorTest {

    @Test
    public void testAddSplitsBackReassignedToRestartedReader() throws Exception {
        try (MockSplitEnumeratorContext<StarRocksSourceSplit> context = new MockSplitEnumeratorContext<>(2)) {
            StarRocksSourceEnumerator enumerator = createEnumerator(context);
            StarRocksSourceSplit split = new StarRocksSourceSplit(
                    new QueryBeXTablets("be1:9060", new ArrayList<>()), "split-0-be1");

            enumerator.addSplitsBack(Collections.singletonList(split), 0);

            Collection<StarRocksSourceSplit> snapshot = enumerator.snapshotState(1L);
            assertTrue("returned splits must survive a checkpoint", snapshot.contains(split));

            enumerator.addReader(0);
            List<SplitsAssignment<StarRocksSourceSplit>> assignments = context.getSplitsAssignmentSequence();
            assertEquals(1, assignments.size());
            assertEquals(Collections.singletonList(split), assignments.get(0).assignment().get(0));
        }
    }

    private StarRocksSourceEnumerator createEnumerator(MockSplitEnumeratorContext<StarRocksSourceSplit> context) {
        Configuration conf = new Configuration();
        StarRocksSourceOptions options = new StarRocksSourceOptions(conf, conf.toMap());
        return new StarRocksSourceEnumerator(
                context, options, new String[0], null, -1, null, null, new ArrayList<>());
    }
}
