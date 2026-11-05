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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class StarRocksSourceReaderTest {

    @Test
    public void testRequestsASplitOnStartAndAfterEachFinishedSplit() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        try (StarRocksSourceReader reader = reader(context)) {
            reader.start();
            assertEquals(1, context.getNumSplitRequests());

            StarRocksSourceSplit split = new StarRocksSourceSplit(
                    new QueryBeXTablets("count", new ArrayList<>()), "count-1");
            reader.onSplitFinished(Collections.singletonMap(split.splitId(), split));
            assertEquals(2, context.getNumSplitRequests());
        }
    }

    @Test
    public void testRestoredReaderFinishesItsSplitBeforeAskingForMore() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        try (StarRocksSourceReader reader = reader(context)) {
            reader.addSplits(Collections.singletonList(new StarRocksSourceSplit(
                    new QueryBeXTablets("count", new ArrayList<>()), "count-1")));
            reader.start();
            assertEquals(0, context.getNumSplitRequests());
        }
    }

    private static StarRocksSourceReader reader(TestingReaderContext context) {
        Configuration conf = new Configuration();
        StarRocksSourceOptions options = new StarRocksSourceOptions(conf, conf.toMap());
        return new StarRocksSourceReader(options, new ArrayList<>(), null, () -> {
            throw new UnsupportedOperationException();
        }, context);
    }
}
