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

package com.starrocks.connector.flink.manager.source;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.it.source.StarRocksSourceBaseTest;
import com.starrocks.connector.flink.manager.StarRocksQueryPlanVisitor;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;

import org.junit.Test;

public class StarRocksQueryPlanVisitorTest extends StarRocksSourceBaseTest {


    @Test
    public void testGetQueryPlan() throws IOException {
        mockResonsefunc();
        StarRocksQueryPlanVisitor visitor = new StarRocksQueryPlanVisitor(OPTIONS);
        QueryInfo queryInfo = visitor.getQueryInfo(querySQL);
        List<Long> tabletsList = new ArrayList<>();
        List<Integer> countList = new ArrayList<>();
        queryInfo.getBeXTablets().forEach(beXTablets -> {
            tabletsList.addAll(beXTablets.getTabletIds());
            countList.add(beXTablets.getTabletIds().size());
        });

        Collections.sort(tabletsList);
        Collections.sort(countList);
        assertTrue((countList.get(countList.size() - 1) - countList.get(0)) <= 1);
        for (int i = 0; i < tabletCount; i ++) {
            assertTrue(i == tabletsList.get(i));
        }
    }
}
