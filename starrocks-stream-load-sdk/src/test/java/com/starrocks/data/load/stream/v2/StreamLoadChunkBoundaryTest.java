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

import com.starrocks.data.load.stream.BatchTableRegion;
import com.starrocks.data.load.stream.Chunk;
import com.starrocks.data.load.stream.MockedStarRocksHttpServer;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.http.StreamLoadEntityMeta;
import com.starrocks.data.load.stream.mergecommit.MergeCommitLoader;
import com.starrocks.data.load.stream.mergecommit.MergeCommitManager;
import com.starrocks.data.load.stream.mergecommit.Table;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class StreamLoadChunkBoundaryTest {

    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    private MockedStarRocksHttpServer mockedServer;

    public static class TestUnbatchableFormat implements StreamLoadDataFormat, Serializable {
        @Override
        public String name() {
            return "unbatchable";
        }

        @Override
        public boolean supportsBatching() {
            return false;
        }

        @Override
        public byte[] first() {
            return new byte[0];
        }

        @Override
        public byte[] delimiter() {
            return new byte[0];
        }

        @Override
        public byte[] end() {
            return new byte[0];
        }
    }

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
    public void testTransactionTableRegionSinglePayloadChunkSwitching() throws Exception {
        String dbName = "db";
        String tblName = "tbl";
        StreamLoadDataFormat format = new TestUnbatchableFormat();
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(dbName)
                .table(tblName)
                .streamLoadDataFormat(format)
                .maxBufferRows(100)
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("3.5.6")
                .enableTransaction()
                .labelPrefix("test-unbatchable-")
                .defaultTableProperties(tableProps)
                .build();

        DefaultStreamLoadManager manager = new DefaultStreamLoadManager(properties, true);
        manager.init();
        try {
            TransactionTableRegion region = (TransactionTableRegion) manager.getCacheRegion(null, dbName, tblName);
            byte[] row1 = "payload1".getBytes(StandardCharsets.UTF_8);
            byte[] row2 = "payload2".getBytes(StandardCharsets.UTF_8);
            byte[] row3 = "payload3".getBytes(StandardCharsets.UTF_8);

            region.write(row1);
            region.write(row2);
            region.write(row3);

            Field inactiveChunksField = TransactionTableRegion.class.getDeclaredField("inactiveChunks");
            inactiveChunksField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Queue<Chunk> inactiveChunks = (Queue<Chunk>) inactiveChunksField.get(region);

            // Because supportsBatching() is false, row1 and row2 are already switched to inactiveChunks
            Assert.assertEquals(2, inactiveChunks.size());
            for (Chunk chunk : inactiveChunks) {
                Assert.assertEquals(1, chunk.numRows());
            }

            Field activeChunkField = TransactionTableRegion.class.getDeclaredField("activeChunk");
            activeChunkField.setAccessible(true);
            Chunk activeChunk = (Chunk) activeChunkField.get(region);
            Assert.assertEquals(1, activeChunk.numRows());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testTransactionTableRegionMultiTableSplitOnSwitch() throws Exception {
        String dbName = "db";
        String tblName = "tbl";
        StreamLoadDataFormat format = new TestUnbatchableFormat();
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(dbName)
                .table(tblName)
                .streamLoadDataFormat(format)
                .maxBufferRows(100)
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("3.5.6")
                .enableTransaction()
                .labelPrefix("test-multitable-unbatchable-")
                .defaultTableProperties(tableProps)
                .build();

        DefaultStreamLoadManager manager = new DefaultStreamLoadManager(properties, true);
        manager.init();
        try {
            TransactionTableRegion region = (TransactionTableRegion) manager.getCacheRegion(null, dbName, tblName);
            // Enable multi-table transaction mode on region to test split behavior
            Field multiTableEnabledField = TransactionTableRegion.class.getDeclaredField("multiTableTransactionEnabled");
            multiTableEnabledField.setAccessible(true);
            multiTableEnabledField.set(region, true);

            byte[] row1 = "payload1".getBytes(StandardCharsets.UTF_8);
            byte[] row2 = "payload2".getBytes(StandardCharsets.UTF_8);
            byte[] row3 = "payload3".getBytes(StandardCharsets.UTF_8);

            // In multi-table mode, rows accumulate in activeChunk to preserve transaction atomicity
            region.write(row1);
            region.write(row2);
            region.write(row3);

            Field activeChunkField = TransactionTableRegion.class.getDeclaredField("activeChunk");
            activeChunkField.setAccessible(true);
            Chunk activeChunk = (Chunk) activeChunkField.get(region);
            Assert.assertEquals(3, activeChunk.numRows());

            // On txnEnd / commit cut, switchChunkForCommit is invoked
            region.switchChunkForCommit();

            Field inactiveChunksField = TransactionTableRegion.class.getDeclaredField("inactiveChunks");
            inactiveChunksField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Queue<Chunk> inactiveChunks = (Queue<Chunk>) inactiveChunksField.get(region);

            // All 3 rows must be split into distinct single-row chunks
            Assert.assertEquals(3, inactiveChunks.size());
            for (Chunk chunk : inactiveChunks) {
                Assert.assertEquals(1, chunk.numRows());
            }
        } finally {
            manager.close();
        }
    }

    @Test
    public void testMergeCommitTableSinglePayloadFlush() {
        StreamLoadDataFormat format = new TestUnbatchableFormat();
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("db")
                .table("tbl")
                .streamLoadDataFormat(format)
                .chunkLimit(1024 * 1024)
                .build();

        MergeCommitManager manager = mock(MergeCommitManager.class);
        MergeCommitLoader loader = mock(MergeCommitLoader.class);
        AtomicInteger sendCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            sendCount.incrementAndGet();
            return null;
        }).when(loader).sendLoad(any(), anyInt());

        Table table = new Table("db", "tbl", manager, loader, tableProps, 0, 0, 5000, 1024 * 1024, 10);
        byte[] row1 = "payload1".getBytes(StandardCharsets.UTF_8);
        byte[] row2 = "payload2".getBytes(StandardCharsets.UTF_8);

        table.write(row1);
        Assert.assertEquals(1, sendCount.get());
        table.write(row2);
        Assert.assertEquals(2, sendCount.get());
    }

    @Test
    public void testBatchTableRegionSinglePayloadMeta() throws Exception {
        StreamLoadDataFormat format = new TestUnbatchableFormat();
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("db")
                .table("tbl")
                .streamLoadDataFormat(format)
                .chunkLimit(1024 * 1024)
                .build();

        BatchTableRegion region = new BatchTableRegion("key", "db", "tbl", null, tableProps, null, null);
        Queue<byte[]> inBuffer = new LinkedList<>();
        inBuffer.add("payload1".getBytes(StandardCharsets.UTF_8));
        inBuffer.add("payload2".getBytes(StandardCharsets.UTF_8));

        Field inBufferField = BatchTableRegion.class.getDeclaredField("inBuffer");
        inBufferField.setAccessible(true);
        inBufferField.set(region, inBuffer);

        Method genEntityMetaMethod = BatchTableRegion.class.getDeclaredMethod("genEntityMeta");
        genEntityMetaMethod.setAccessible(true);
        StreamLoadEntityMeta meta = (StreamLoadEntityMeta) genEntityMetaMethod.invoke(region);

        Assert.assertEquals(1, meta.getRows());
    }

    @Test
    public void testBatchTableRegionOversizedUnbatchablePayloadMeta() throws Exception {
        StreamLoadDataFormat format = new TestUnbatchableFormat();
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("db")
                .table("tbl")
                .streamLoadDataFormat(format)
                .chunkLimit(50)
                .build();

        BatchTableRegion region = new BatchTableRegion("key", "db", "tbl", null, tableProps, null, null);
        Queue<byte[]> inBuffer = new LinkedList<>();
        byte[] oversizedPayload = new byte[500];
        inBuffer.add(oversizedPayload);
        inBuffer.add(new byte[100]);

        Field inBufferField = BatchTableRegion.class.getDeclaredField("inBuffer");
        inBufferField.setAccessible(true);
        inBufferField.set(region, inBuffer);

        Method genEntityMetaMethod = BatchTableRegion.class.getDeclaredMethod("genEntityMeta");
        genEntityMetaMethod.setAccessible(true);
        StreamLoadEntityMeta meta = (StreamLoadEntityMeta) genEntityMetaMethod.invoke(region);

        Assert.assertEquals(1, meta.getRows());
        Assert.assertEquals(500, meta.getBytes());
    }

    @Test
    public void testBatchTableRegionOversizedPayloadDrainsInBuffer() throws Exception {
        StreamLoadDataFormat format = new TestUnbatchableFormat();
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("db")
                .table("tbl")
                .streamLoadDataFormat(format)
                .chunkLimit(50)
                .build();

        BatchTableRegion region = new BatchTableRegion("key", "db", "tbl", null, tableProps, null, null);
        Queue<byte[]> inBuffer = new LinkedList<>();
        byte[] oversizedPayload = "oversized_payload_exceeding_chunk_limit".getBytes(StandardCharsets.UTF_8);
        inBuffer.add(oversizedPayload);

        Field inBufferField = BatchTableRegion.class.getDeclaredField("inBuffer");
        inBufferField.setAccessible(true);
        inBufferField.set(region, inBuffer);

        Method flipMethod = BatchTableRegion.class.getDeclaredMethod("flip");
        flipMethod.setAccessible(true);
        flipMethod.invoke(region);

        byte[] readRow = region.read();
        Assert.assertArrayEquals(oversizedPayload, readRow);

        byte[] eofRow = region.read();
        Assert.assertNull("read() must return null at chunk EOF", eofRow);

        Assert.assertTrue("inBuffer must be drained to prevent infinite streamLoad loop", inBuffer.isEmpty());
    }

    @Test
    public void testBatchTableRegionOversizedBatchablePayloadMeta() throws Exception {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("db")
                .table("tbl")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .chunkLimit(50)
                .build();

        BatchTableRegion region = new BatchTableRegion("key", "db", "tbl", null, tableProps, null, null);
        Queue<byte[]> inBuffer = new LinkedList<>();
        byte[] oversizedRow = new byte[200];
        byte[] normalRow = new byte[20];
        inBuffer.add(oversizedRow);
        inBuffer.add(normalRow);

        Field inBufferField = BatchTableRegion.class.getDeclaredField("inBuffer");
        inBufferField.setAccessible(true);
        inBufferField.set(region, inBuffer);

        Method genEntityMetaMethod = BatchTableRegion.class.getDeclaredMethod("genEntityMeta");
        genEntityMetaMethod.setAccessible(true);
        StreamLoadEntityMeta meta = (StreamLoadEntityMeta) genEntityMetaMethod.invoke(region);

        // First oversized row must be selected alone, deferring normalRow to the next chunk
        Assert.assertEquals(1, meta.getRows());
        Assert.assertEquals(
                StreamLoadDataFormat.JSON.first().length
                        + StreamLoadDataFormat.JSON.end().length
                        + oversizedRow.length,
                meta.getBytes());
    }
}
