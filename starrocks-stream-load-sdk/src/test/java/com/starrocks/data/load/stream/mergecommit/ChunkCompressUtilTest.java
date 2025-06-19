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

package com.starrocks.data.load.stream.mergecommit;

import com.starrocks.data.load.stream.ChunkInputStreamTest;
import com.starrocks.data.load.stream.compress.CompressionHttpEntityTest;
import org.junit.Test;

import static com.starrocks.data.load.stream.ChunkInputStreamTest.genChunk;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ChunkCompressUtilTest {

    @Test
    public void testCompress() throws Exception {
        ChunkInputStreamTest.ChunkMeta chunkMeta = genChunk();
        CompressionHttpEntityTest.MockCompressionCodec compressionCodec =
                new CompressionHttpEntityTest.MockCompressionCodec();
        byte[] result = ChunkCompressUtil.compress(chunkMeta.chunk, compressionCodec);
        assertArrayEquals(chunkMeta.expectedData, result);
        assertEquals(1, compressionCodec.getStreams().size());
        assertEquals(chunkMeta.chunk.chunkBytes(), compressionCodec.getStreams().get(0).getCount());
    }
}
