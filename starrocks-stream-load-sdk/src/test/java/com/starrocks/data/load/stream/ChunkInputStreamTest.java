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

import com.starrocks.data.load.stream.v2.ChunkInputStream;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChunkInputStreamTest {

    @Test
    public void testReadOneByteEachTime() throws Exception {
        ChunkMeta chunkMeta = genChunk();
        Chunk chunk = chunkMeta.chunk;
        byte[] actualData = new byte[(int) chunk.chunkBytes()];
        try (ChunkInputStream inputStream = new ChunkInputStream(chunk)) {
            int pos = 0;
            while (pos < actualData.length) {
                int len = inputStream.read(actualData, pos, 1);
                if (len == -1) {
                    break;
                }
                assertEquals(1, len);
                pos += len;
            }
            assertArrayEquals(chunkMeta.expectedData, actualData);
        }
    }

    @Test
    public void testReadAllOnce() throws Exception {
        ChunkMeta chunkMeta = genChunk();
        Chunk chunk = chunkMeta.chunk;
        byte[] actualData = new byte[(int) chunk.chunkBytes()];
        try (ChunkInputStream inputStream = new ChunkInputStream(chunk)) {
            int len = inputStream.read(actualData, 0, actualData.length);
            assertEquals(actualData.length, len);
            assertArrayEquals(chunkMeta.expectedData, actualData);
        }
    }

    @Test
    public void testReadRandomBytesEacheTime() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        ChunkMeta chunkMeta = genChunk();
        Chunk chunk = chunkMeta.chunk;
        byte[] actualData = new byte[(int) chunk.chunkBytes()];
        try (ChunkInputStream inputStream = new ChunkInputStream(chunk)) {
            int pos = 0;
            while (pos < actualData.length) {
                int maxLen = random.nextInt(actualData.length - pos) + 1;
                int len = inputStream.read(actualData, pos, maxLen);
                if (len == -1) {
                    break;
                }
                assertTrue(len > 0 && len <= maxLen);
                pos += len;
            }
            assertArrayEquals(chunkMeta.expectedData, actualData);
        }
    }

    public static ChunkMeta genChunk() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Chunk chunk = new Chunk(StreamLoadDataFormat.CSV);
        for (int i = 0; i < 1000; i++) {
            int len = random.nextInt( 100) + 1;
            byte[] row = new byte[len];
            random.nextBytes(row);
            chunk.addRow(row);
        }

        byte[] expectedData = new byte[(int) chunk.chunkBytes()];
        ByteBuffer buffer = ByteBuffer.wrap(expectedData);
        Iterator<byte[]> iterator = chunk.iterator();
        while (iterator.hasNext()) {
            byte[] item = iterator.next();
            buffer.put(item);
        }

        return new ChunkMeta(chunk, expectedData);
    }

    public static class ChunkMeta {
        Chunk chunk;
        byte[] expectedData;

        public ChunkMeta(Chunk chunk, byte[] expectedData) {
            this.chunk = chunk;
            this.expectedData = expectedData;
        }
    }
}
