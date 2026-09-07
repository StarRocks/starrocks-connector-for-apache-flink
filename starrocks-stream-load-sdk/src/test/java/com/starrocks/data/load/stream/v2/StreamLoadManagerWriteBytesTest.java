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
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.mergecommit.EmptyMetricListener;
import com.starrocks.data.load.stream.mergecommit.LoadParameters;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StreamLoadManagerWriteBytesTest {

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
    public void testDefaultStreamLoadManagerWriteBytes() {
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
                .labelPrefix("test-write-bytes-")
                .defaultTableProperties(tableProps)
                .scanningFrequency(50)
                .ioThreadCount(1)
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();
        try {
            byte[] row1 = "{\"id\":1,\"name\":\"row1\"}".getBytes(StandardCharsets.UTF_8);
            byte[] row2 = "{\"id\":2,\"name\":\"row2\"}".getBytes(StandardCharsets.UTF_8);
            // Write normal rows
            manager.writeBytes(null, dbName, tblName, row1, row2);
            // Write with null row element - should skip null safely without exception
            manager.writeBytes(null, dbName, tblName, (byte[]) null, row1);
        } finally {
            manager.close();
        }
    }

    @Test
    public void testDefaultStreamLoadManagerWriteBytesPartition() {
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
                .labelPrefix("test-partition-bytes-")
                .defaultTableProperties(tableProps)
                .scanningFrequency(50)
                .ioThreadCount(1)
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();
        try {
            byte[] row = "{\"id\":10,\"name\":\"partition_row\"}".getBytes(StandardCharsets.UTF_8);
            manager.writeBytes(0, dbName, tblName, row);
            manager.writeBytes(1, dbName, tblName, (byte[]) null, row);
        } finally {
            manager.close();
        }
    }

    @Test
    public void testMergeCommitManagerWriteBytes() {
        String dbName = "db";
        String tblName = "tbl";
        Map<String, String> headers = new HashMap<>();
        headers.put(LoadParameters.ENABLE_MERGE_COMMIT, "true");

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
                .addHeaders(headers)
                .labelPrefix("test-merge-commit-bytes-")
                .defaultTableProperties(tableProps)
                .scanningFrequency(50)
                .ioThreadCount(1)
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        AtomicInteger reportedRows = new AtomicInteger(0);
        AtomicLong reportedBytes = new AtomicLong(0L);

        manager.setMetricListener(new EmptyMetricListener() {
            @Override
            public void onWrite(int numRows, int dataSize) {
                reportedRows.addAndGet(numRows);
                reportedBytes.addAndGet(dataSize);
            }
        });

        manager.init();
        try {
            byte[] row1 = "{\"id\":100}".getBytes(StandardCharsets.UTF_8);
            byte[] row2 = "{\"id\":101}".getBytes(StandardCharsets.UTF_8);
            manager.writeBytes(null, dbName, tblName, row1, row2);
            // Write with null row safely skipped
            manager.writeBytes(null, dbName, tblName, (byte[]) null, row1);

            Assert.assertEquals(3, reportedRows.get());
            Assert.assertEquals(row1.length * 2 + row2.length, reportedBytes.get());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testZeroVarargsAndOverloadSafety() {
        String dbName = "db";
        String tblName = "tbl";
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(dbName)
                .table(tblName)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("3.5.6")
                .enableTransaction()
                .defaultTableProperties(tableProps)
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();
        try {
            // These calls must compile cleanly without JLS §15.12.2.5 varargs ambiguity
            manager.write(null, dbName, tblName);
            manager.writeBytes(null, dbName, tblName);
            manager.write(0, dbName, tblName);
            manager.writeBytes(0, dbName, tblName);
        } finally {
            manager.close();
        }
    }

    @Test
    public void testZeroVarargsAndNullRowsDoNotActivatePartitions() {
        String dbName = "db";
        String tblName = "tbl";
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(dbName)
                .table(tblName)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableTransaction()
                .enableMultiTableTransaction()
                .labelPrefix("test-noop-partition-")
                .defaultTableProperties(tableProps)
                .scanningFrequency(60000)
                .ioThreadCount(1)
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();
        try {
            // Write 0-varargs or null rows across multiple partitions.
            // These partitions must NOT be marked active in the partitionTracker.
            manager.write(1, dbName, tblName);
            manager.write(2, dbName, tblName, (String) null);
            manager.write(2, dbName, tblName, (String[]) null);
            manager.writeBytes(3, dbName, tblName);
            manager.writeBytes(4, dbName, tblName, (byte[]) null);
            manager.writeBytes(4, dbName, tblName, (byte[][]) null);

            // Write actual data only to partition 0, then mark partition 0 complete
            byte[] row = "{\"id\":10,\"name\":\"partition_row\"}".getBytes(StandardCharsets.UTF_8);
            manager.writeBytes(0, dbName, tblName, row);
            manager.setCommitAllowed(0, true);

            // If partitions 1, 2, 3, 4 had been activated by the no-op writes, flush() (savepoint)
            // would fail with IllegalStateException stating partitions 1, 2, 3, 4 never received txnEnd.
            // Because they were deferred until non-null write, flush() succeeds smoothly.
            manager.flush();
            Assert.assertNull("flush() must succeed without exception", manager.getException());
        } finally {
            manager.close();
        }
    }
}
