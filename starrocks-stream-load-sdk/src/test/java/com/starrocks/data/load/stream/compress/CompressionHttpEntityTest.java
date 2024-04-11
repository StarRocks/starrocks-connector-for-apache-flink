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

package com.starrocks.data.load.stream.compress;

import com.starrocks.data.load.stream.ChunkInputStreamTest;
import com.starrocks.data.load.stream.v2.ChunkHttpEntity;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static com.starrocks.data.load.stream.ChunkInputStreamTest.genChunk;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests for {@link CompressionHttpEntity}. */
public class CompressionHttpEntityTest {

    @Test
    public void testBasic() throws Exception {
        ChunkInputStreamTest.ChunkMeta chunkMeta = genChunk();
        ChunkHttpEntity entity = new ChunkHttpEntity("test", chunkMeta.chunk);
        MockCompressionCodec compressionCodec = new MockCompressionCodec();
        CompressionHttpEntity compressionEntity = new CompressionHttpEntity(entity, compressionCodec);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        compressionEntity.writeTo(outputStream);
        assertArrayEquals(chunkMeta.expectedData, outputStream.toByteArray());
        assertEquals(1, compressionCodec.getStreams().size());
        assertEquals(entity.getContentLength(), compressionCodec.getStreams().get(0).getCount());
    }

    private static class MockCompressionCodec implements CompressionCodec {

        private final List<CountingOutputStream> streams;

        public MockCompressionCodec() {
            this.streams = new ArrayList<>();
        }

        public List<CountingOutputStream> getStreams() {
            return streams;
        }

        @Override
        public OutputStream createCompressionStream(OutputStream rawOutputStream, long contentSize) throws IOException {
            this.streams.add(new CountingOutputStream(rawOutputStream));
            return streams.get(streams.size() - 1);
        }
    }
}
