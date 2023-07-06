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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;


public class StreamLoadResponseTest {

    @Test
    public void testDeserialize() throws Exception {
        String entityContent = "{\n" +
                "    \"TxnId\": 22736752,\n" +
                "    \"Label\": \"119d4ca5-a920-4dbb-84ad-64e062a449c5\",\n" +
                "    \"Status\": \"Success\",\n" +
                "    \"Message\": \"OK\",\n" +
                "    \"NumberTotalRows\": 93,\n" +
                "    \"NumberLoadedRows\": 93,\n" +
                "    \"NumberFilteredRows\": 0,\n" +
                "    \"NumberUnselectedRows\": 0,\n" +
                "    \"LoadBytes\": 17227,\n" +
                "    \"LoadTimeMs\": 17575,\n" +
                "    \"BeginTxnTimeMs\": 0,\n" +
                "    \"StreamLoadPlanTimeMs\": 1,\n" +
                "    \"ReadDataTimeMs\": 0,\n" +
                "    \"WriteDataTimeMs\": 17487,\n" +
                "    \"CommitAndPublishTimeMs\": 86\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        // StreamLoadResponseBody does not contain all fields returned by StarRocks
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // filed names in StreamLoadResponseBody are case-insensitive
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        StreamLoadResponse.StreamLoadResponseBody responseBody =
                objectMapper.readValue(entityContent, StreamLoadResponse.StreamLoadResponseBody.class);

        Assert.assertNotNull(responseBody.getStreamLoadPlanTimeMs());
    }

}