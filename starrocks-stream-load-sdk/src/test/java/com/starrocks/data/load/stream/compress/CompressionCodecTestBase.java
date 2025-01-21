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

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import static com.starrocks.data.load.stream.ChunkInputStreamTest.genChunk;
import static org.junit.Assert.assertArrayEquals;

/** Test base for {@link CompressionCodec}. */
public abstract class CompressionCodecTestBase {

    protected abstract byte[] decompress(byte[] compressedData, int rawSize) throws Exception;

    public void testCompressBase(CompressionCodec compressionCodec) throws Exception {
        ChunkInputStreamTest.ChunkMeta chunkMeta = genChunk();
        ChunkHttpEntity entity = new ChunkHttpEntity("test", chunkMeta.chunk);
        CompressionHttpEntity compressionEntity = new CompressionHttpEntity(entity, compressionCodec);
        ChunkInputStreamTest.ChunkMeta chunkMeta1 = genChunk();
        ChunkHttpEntity entity1 = new ChunkHttpEntity("test", chunkMeta1.chunk);
        CompressionHttpEntity compressionEntity1 = new CompressionHttpEntity(entity1, compressionCodec);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        compressionEntity.writeTo(outputStream);
        compressionEntity1.writeTo(outputStream);
        byte[] result = outputStream.toByteArray();
        byte[] descompressData = decompress(result, (int) (entity.getContentLength() + entity1.getContentLength()));
        assertArrayEquals(chunkMeta.expectedData, Arrays.copyOfRange(descompressData, 0, (int) entity.getContentLength()));
        assertArrayEquals(chunkMeta1.expectedData,
                Arrays.copyOfRange(descompressData, (int) entity.getContentLength(), descompressData.length));
    }
}
