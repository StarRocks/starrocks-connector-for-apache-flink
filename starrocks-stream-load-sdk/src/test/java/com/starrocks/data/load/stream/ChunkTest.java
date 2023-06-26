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

package com.starrocks.data.load.stream;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ChunkTest {

    @Test
    public void testCsvChunk() {
        testChunkBase(StreamLoadDataFormat.CSV);
    }

    @Test
    public void testJsonChunk() {
        testChunkBase(StreamLoadDataFormat.JSON);
    }

    private void testChunkBase(StreamLoadDataFormat format) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long expectedChunkBytes = format.first().length + format.end().length;
        long expectedRowBytes = 0;
        List<byte[]> expectedData = new ArrayList<>();
        expectedData.add(format.first());
        Chunk chunk = new Chunk(format);
        int numRows = 100;
        for (int i = 0; i < numRows; i++) {
            int len = random.nextInt( 10) + 1;
            byte[] row = new byte[len];
            random.nextBytes(row);

            if (i > 0) {
                expectedData.add(format.delimiter());
                expectedChunkBytes += format.delimiter().length;
            }
            chunk.addRow(row);
            expectedData.add(row);
            expectedChunkBytes += row.length;
            expectedRowBytes += row.length;

            assertEquals(i + 1, chunk.numRows());
            assertEquals(expectedRowBytes, chunk.rowBytes());
            assertEquals(expectedChunkBytes, chunk.chunkBytes());
        }
        expectedData.add(format.end());

        Iterator<byte[]> expectedIterator = expectedData.iterator();
        Iterator<byte[]> actualIterator = chunk.iterator();
        while (expectedIterator.hasNext()) {
            assertTrue(actualIterator.hasNext());
            byte[] expectedItem = expectedIterator.next();
            byte[] actualItem = actualIterator.next();
            assertArrayEquals(expectedItem, actualItem);
        }
        assertFalse(actualIterator.hasNext());
    }
}
