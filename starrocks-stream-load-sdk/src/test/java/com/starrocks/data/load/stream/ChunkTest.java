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

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
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

    @Test
    public void testArrowChunk() {
        testChunkBase(StreamLoadDataFormat.ARROW);
    }

    @Test
    public void testEmptyCsvChunk() {
        testEmptyChunkBase(StreamLoadDataFormat.CSV);
    }

    @Test
    public void testEmptyJsonChunk() {
        testEmptyChunkBase(StreamLoadDataFormat.JSON);
    }

    @Test
    public void testEmptyArrowChunk() {
        testEmptyChunkBase(StreamLoadDataFormat.ARROW);
    }

    private void testEmptyChunkBase(StreamLoadDataFormat format) {
        Chunk chunk = new Chunk(format, 1);
        assertEquals(0, chunk.numRows());
        assertEquals(0, chunk.rowBytes());
        assertEquals(format.first().length + format.end().length, chunk.chunkBytes());

        // Verify iterator returns FIRST and END even for empty chunk
        Iterator<byte[]> iterator = chunk.iterator();
        assertTrue(iterator.hasNext());
        assertArrayEquals(format.first(), iterator.next());
        assertTrue(iterator.hasNext());
        assertArrayEquals(format.end(), iterator.next());
        assertFalse(iterator.hasNext());
    }

    private void testChunkBase(StreamLoadDataFormat format) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long expectedChunkBytes = format.first().length + format.end().length;
        long expectedRowBytes = 0;
        List<byte[]> expectedData = new ArrayList<>();
        expectedData.add(format.first());
        Chunk chunk = new Chunk(format, 1);
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

    /**
     * Verifies that arbitrary binary data (including bytes that are invalid
     * UTF-8) survives the Arrow chunk path without corruption.  This is the
     * key invariant that the byte[] write API is designed to protect.
     */
    @Test
    public void testArrowBinaryIntegrity() {
        // Arrow IPC magic bytes and padding that are NOT valid UTF-8
        byte[] arrowPayload = new byte[] {
            (byte) 0x41, (byte) 0x52, (byte) 0x52, (byte) 0x4F, // "ARRO"
            (byte) 0x57, (byte) 0x31, (byte) 0x00, (byte) 0x00, // "W1\0\0"
            (byte) 0xFF, (byte) 0xFE, (byte) 0xFD, (byte) 0x80, // non-UTF-8 bytes
            (byte) 0xC0, (byte) 0xC1, (byte) 0xF5, (byte) 0xF6, // invalid UTF-8 lead bytes
        };

        Chunk chunk = new Chunk(StreamLoadDataFormat.ARROW, 1);
        chunk.addRow(arrowPayload);

        assertEquals(1, chunk.numRows());
        assertEquals(arrowPayload.length, chunk.rowBytes());

        // Arrow format returns empty first/delimiter/end, so chunkBytes == rowBytes
        assertEquals(arrowPayload.length, chunk.chunkBytes());

        // Verify byte-for-byte fidelity through the iterator
        Iterator<byte[]> iter = chunk.iterator();
        assertTrue(iter.hasNext());
        assertArrayEquals(new byte[0], iter.next()); // first()
        assertTrue(iter.hasNext());
        assertArrayEquals(arrowPayload, iter.next()); // the payload
        assertTrue(iter.hasNext());
        assertArrayEquals(new byte[0], iter.next()); // end()
        assertFalse(iter.hasNext());
    }

    @Test
    public void testSupportsBatchingContract() {
        assertTrue(StreamLoadDataFormat.CSV.supportsBatching());
        assertTrue(StreamLoadDataFormat.JSON.supportsBatching());
        assertFalse(StreamLoadDataFormat.ARROW.supportsBatching());
    }

    @Test
    public void testArrowFormatEqualityAndSerialization() throws Exception {
        StreamLoadDataFormat.ArrowFormat format1 = new StreamLoadDataFormat.ArrowFormat();
        StreamLoadDataFormat.ArrowFormat format2 = new StreamLoadDataFormat.ArrowFormat();

        assertEquals(format1, format2);
        assertEquals(format1, StreamLoadDataFormat.ARROW);
        assertEquals(format1.hashCode(), format2.hashCode());
        assertEquals("ArrowFormat{}", format1.toString());

        // Test serialization roundtrip preserves canonical singleton via readResolve
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(format1);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            Object deserialized = ois.readObject();
            assertSame(StreamLoadDataFormat.ARROW, deserialized);
        }
    }

    @Test
    public void testChunkGetRows() {
        Chunk chunk = new Chunk(StreamLoadDataFormat.ARROW, 42);
        byte[] row1 = new byte[]{1, 2, 3};
        byte[] row2 = new byte[]{4, 5, 6};
        chunk.addRow(row1);
        chunk.addRow(row2);

        List<byte[]> rows = chunk.getRows();
        assertEquals(2, rows.size());
        assertArrayEquals(row1, rows.get(0));
        assertArrayEquals(row2, rows.get(1));
    }
}
