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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/** Compress data using LZ4_FRAME. */
public class LZ4FrameCompressionCodec implements CompressionCodec {

    public static final String NAME = "LZ4_FRAME";

    private final LZ4FrameOutputStream.BLOCKSIZE blockSize;
    private final boolean blockIndependence;
    private final LZ4Compressor compressor;
    private final XXHash32 hash;

    public LZ4FrameCompressionCodec(LZ4FrameOutputStream.BLOCKSIZE blockSize, boolean blockIndependence) {
        this.blockSize = blockSize;
        this.blockIndependence = blockIndependence;
        this.compressor = LZ4Factory.fastestInstance().fastCompressor();
        this.hash = XXHashFactory.fastestInstance().hash32();
    }

    @Override
    public OutputStream createCompressionStream(OutputStream rawOutputStream, long contentSize) throws IOException {
        if (contentSize < 0) {
            return blockIndependence ?
                    new LZ4FrameOutputStream(rawOutputStream, blockSize, -1, compressor, hash,
                            LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE) :
                    new LZ4FrameOutputStream(rawOutputStream, blockSize, -1, compressor, hash);
        } else {
            return blockIndependence ?
                    new LZ4FrameOutputStream(rawOutputStream, blockSize, contentSize, compressor, hash,
                            LZ4FrameOutputStream.FLG.Bits.CONTENT_SIZE,
                            LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE) :
                    new LZ4FrameOutputStream(rawOutputStream, blockSize, contentSize, compressor, hash,
                            LZ4FrameOutputStream.FLG.Bits.CONTENT_SIZE);
        }
    }

    public static LZ4FrameCompressionCodec create(Map<String, Object> properties) {
        LZ4FrameOutputStream.BLOCKSIZE blockSize = (LZ4FrameOutputStream.BLOCKSIZE) properties.getOrDefault(
                CompressionOptions.LZ4_BLOCK_SIZE, CompressionOptions.DEFAULT_LZ4_BLOCK_SIZE);
        boolean blockIndependence = (boolean) properties.getOrDefault(
                CompressionOptions.LZ4_BLOCK_INDEPENDENCE, CompressionOptions.DEFAULT_LZ4_BLOCK_INDEPENDENCE);
        return new LZ4FrameCompressionCodec(blockSize, blockIndependence);
    }
}
