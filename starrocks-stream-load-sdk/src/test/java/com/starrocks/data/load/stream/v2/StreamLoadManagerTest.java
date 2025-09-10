/*
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

package com.starrocks.data.load.stream.v2;

import com.starrocks.data.load.stream.MockedStarRocksHttpServer;
import com.starrocks.data.load.stream.StreamLoadConstants;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StreamLoadManagerTest {

    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    private MockedStarRocksHttpServer mockedServer;

    @Before
    public void setUp() throws Exception {
        mockedServer = MockedStarRocksHttpServer.builder()
                .port(0)
                .enforceAuth(USERNAME, PASSWORD)
                .build();
        mockedServer.start();
    }

    @After
    public void tearDown() {
        if (mockedServer != null) {
            mockedServer.stop();
        }
    }

    @Test
    public void testFlushException() throws Exception {
        String dbName = "db";
        String tblName = "tbl";
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(dbName)
                .table(tblName)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(10)
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("3.5.6")
                .enableTransaction()
                .labelPrefix("test-")
                .defaultTableProperties(tableProps)
                .scanningFrequency(50)
                .ioThreadCount(1)
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();
        manager.write(null, dbName, tblName, "{\"id\":1,\"name\":\"starrocks\"}");

        MockedStarRocksHttpServer.ResponseOverride prepareResponse = new MockedStarRocksHttpServer.ResponseOverride();
        prepareResponse.status = StreamLoadConstants.RESULT_STATUS_FAILED;
        prepareResponse.message = "artificial prepare failed";
        prepareResponse.includeErrorURL = false;
        mockedServer.setPrepareOverride(prepareResponse);

        RuntimeException thrown = null;
        try {
            manager.flush();
        } catch (RuntimeException e) {
            thrown = e;
        } finally {
            manager.close();
        }

        Assert.assertNotNull("flush() should throw when exception has been set", thrown);
        String msg = thrown.getMessage() == null ? "" : thrown.getMessage();
        Assert.assertTrue(msg.contains("artificial prepare failed"));
    }
}


