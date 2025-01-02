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

import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LZ4FrameCompressionCodecTest extends CompressionCodecTestBase {

    @Override
    protected byte[] decompress(byte[] compressedData, int rawSize) throws Exception {
        byte[] result = new byte[rawSize];
        LZ4FrameInputStream inputStream = new LZ4FrameInputStream(new ByteArrayInputStream(compressedData));
        int totalRead = 0;
        int n = inputStream.read(result, totalRead, rawSize - totalRead);
        while (n > 0) {
            totalRead += n;
            n = inputStream.read(result, totalRead, rawSize - totalRead);
        }
        inputStream.close();
        return result;
    }

    @Test
    public void testCreate() {
        Map<String, Object> properties = new HashMap<>();
        LZ4FrameCompressionCodec codec1 = LZ4FrameCompressionCodec.create(properties);
        assertEquals(LZ4FrameOutputStream.BLOCKSIZE.SIZE_4MB, codec1.getBlockSize());

        properties.put("compression.lz4.block.size", LZ4FrameOutputStream.BLOCKSIZE.SIZE_1MB);
        LZ4FrameCompressionCodec codec2 = LZ4FrameCompressionCodec.create(properties);
        assertEquals(LZ4FrameOutputStream.BLOCKSIZE.SIZE_1MB, codec2.getBlockSize());
    }

    @Test
    public void testCompress() throws Exception {
        LZ4FrameCompressionCodec codec = LZ4FrameCompressionCodec.create(new HashMap<>());
        testCompressBase(codec);
    }
}
