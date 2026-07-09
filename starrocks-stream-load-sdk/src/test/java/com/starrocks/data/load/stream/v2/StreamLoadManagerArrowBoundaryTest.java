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

import com.starrocks.data.load.stream.Chunk;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.mergecommit.MergeCommitManager;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class StreamLoadManagerArrowBoundaryTest {

    private static final String DB = "test_db";
    private static final String TBL = "test_tbl";

    private StreamLoadProperties createProperties(boolean multiTable) {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(DB)
                .table(TBL)
                .streamLoadDataFormat(StreamLoadDataFormat.ARROW)
                .build();

        StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .loadUrls("http://127.0.0.1:8030")
                .username("root")
                .password("")
                .defaultTableProperties(tableProps)
                .scanningFrequency(60000);

        if (multiTable) {
            builder.enableMultiTableTransaction();
        }

        return builder.build();
    }

    @Test
    public void testArrowStringWriteFailsFast() {
        StreamLoadProperties props = createProperties(false);
        DefaultStreamLoadManager manager = new DefaultStreamLoadManager(props, true);
        manager.init();
        try {
            manager.write(null, DB, TBL, "some string row");
            fail("Expected IllegalStateException for String write with Arrow format");
        } catch (IllegalStateException e) {
            assertNotNull(e.getMessage());
            assertEquals(
                    "Arrow format requires the byte[] write API. "
                    + "String-based writes will corrupt the Arrow IPC stream. "
                    + "Use write(uniqueKey, database, table, byte[]...) instead.",
                    e.getMessage());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testArrowPartitionStringWriteFailsFast() {
        StreamLoadProperties props = createProperties(true);
        DefaultStreamLoadManager manager = new DefaultStreamLoadManager(props, true);
        manager.init();
        try {
            manager.write(1, DB, TBL, "some string row");
            fail("Expected IllegalStateException for String write with Arrow format in multi-table mode");
        } catch (IllegalStateException e) {
            assertNotNull(e.getMessage());
            assertEquals(
                    "Arrow format requires the byte[] write API. "
                    + "String-based writes will corrupt the Arrow IPC stream. "
                    + "Use write(partition, database, table, byte[]...) instead.",
                    e.getMessage());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testArrowBinaryWriteWithNullElements() {
        StreamLoadProperties props = createProperties(false);
        DefaultStreamLoadManager manager = new DefaultStreamLoadManager(props, true);
        manager.init();
        try {
            byte[] validPayload = new byte[]{1, 2, 3};
            // Passing null element alongside valid payload should not throw NullPointerException
            manager.write(null, DB, TBL, (byte[]) null, validPayload);

            TransactionTableRegion region = (TransactionTableRegion) manager.getCacheRegion(null, DB, TBL);
            assertNotNull(region);
            assertEquals(validPayload.length, region.getCacheBytes());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testArrowBoundaryPreservationNonMultiTable() {
        StreamLoadProperties props = createProperties(false);
        DefaultStreamLoadManager manager = new DefaultStreamLoadManager(props, true);
        manager.init();
        try {
            byte[] payload1 = new byte[]{1, 2, 3};
            byte[] payload2 = new byte[]{4, 5, 6};

            // Write 1: goes to activeChunk
            manager.write(null, DB, TBL, payload1);
            TransactionTableRegion region = (TransactionTableRegion) manager.getCacheRegion(null, DB, TBL);
            assertEquals(0, region.getInactiveChunksCount());
            assertEquals(payload1.length, region.getCacheBytes());

            // Write 2: since format does not support batching, pre-write switch moves payload 1 to inactiveChunks
            manager.write(null, DB, TBL, payload2);
            assertEquals(1, region.getInactiveChunksCount());

            // Now trigger switch for commit: moves payload 2 to inactiveChunks as well
            region.switchChunkForCommit();
            assertEquals(2, region.getInactiveChunksCount());

            // Verify each inactive chunk contains exactly 1 row (no concatenation)
            ChunkHttpEntity entity = (ChunkHttpEntity) region.getHttpEntity();
            assertEquals(1, entity.getChunk().numRows());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testArrowBoundaryPreservationMultiTableSplit() {
        StreamLoadProperties props = createProperties(true);
        DefaultStreamLoadManager manager = new DefaultStreamLoadManager(props, true);
        manager.init();
        try {
            byte[] payload1 = new byte[]{1, 2, 3};
            byte[] payload2 = new byte[]{4, 5, 6};

            // Write 1 and 2 in multi-table mode: both accumulate in activeChunk during source transaction
            manager.write(0, DB, TBL, payload1);
            manager.write(0, DB, TBL, payload2);

            String uniqueKey = "P0-" + com.starrocks.data.load.stream.StreamLoadUtils.getTableUniqueKey(DB, TBL);
            TransactionTableRegion region = (TransactionTableRegion) manager.getCacheRegion(uniqueKey, DB, TBL, 0);
            // Atomicity preserved mid-transaction: 0 inactive chunks, rows in activeChunk
            assertEquals(0, region.getInactiveChunksCount());
            assertEquals(payload1.length + payload2.length, region.getCacheBytes());

            // When transaction completes, switchChunk splits activeChunk into separate 1-row chunks
            region.switchChunkForCommit();
            assertEquals(2, region.getInactiveChunksCount());

            // Verify each chunk has exactly 1 row
            ChunkHttpEntity entity = (ChunkHttpEntity) region.getHttpEntity();
            assertEquals(1, entity.getChunk().numRows());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testMergeCommitArrowStringWriteFailsFast() {
        StreamLoadProperties props = createProperties(false);
        MergeCommitManager manager = new MergeCommitManager(props);

        try {
            manager.write(null, DB, TBL, "some string row");
            fail("Expected IllegalStateException for String write with Arrow format in MergeCommitManager");
        } catch (IllegalStateException e) {
            assertNotNull(e.getMessage());
            assertEquals(
                    "Arrow format requires the byte[] write API. "
                    + "String-based writes will corrupt the Arrow IPC stream. "
                    + "Use write(uniqueKey, database, table, byte[]...) instead.",
                    e.getMessage());
        }
    }

    @Test
    public void testMergeCommitArrowNullElements() {
        StreamLoadProperties props = createProperties(false);
        MergeCommitManager manager = new MergeCommitManager(props);

        // Passing null elements should be safely skipped without NPE
        manager.write(null, DB, TBL, (byte[]) null);
    }
}
