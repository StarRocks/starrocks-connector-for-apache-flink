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

import com.starrocks.data.load.stream.v2.ChunkHttpEntity;
import org.apache.http.entity.HttpEntityWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Wrapping entity that compresses content when writing. */
public class CompressionHttpEntity extends HttpEntityWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(CompressionHttpEntity.class);

    private final CompressionCodec compressionCodec;

    public CompressionHttpEntity(ChunkHttpEntity entity, CompressionCodec compressionCodec) {
        super(entity);
        this.compressionCodec = compressionCodec;
        entity.setLogAfterWrite(false);
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public boolean isChunked() {
        // force content chunking
        return true;
    }

    @Override
    public InputStream getContent() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(final OutputStream outStream) throws IOException {
        long startTime = System.nanoTime();
        ChunkHttpEntity entity = (ChunkHttpEntity) wrappedEntity;
        final CountingOutputStream countingOutputStream = new CountingOutputStream(outStream);
        final OutputStream compressOutputStream =
                compressionCodec.createCompressionStream(countingOutputStream, entity.getContentLength());
        entity.writeTo(compressOutputStream);
        compressOutputStream.close();
        long rawSize = entity.getContentLength();
        long compressSize = countingOutputStream.getCount();
        float compressRatio = compressSize == 0 ? 1 : (float) rawSize / compressSize;
        LOG.info("Write entity for table {}, chunkId: {}. raw/compressed size:{}/{}, compress ratio:{}, time:{}us",
                entity.getTableUniqueKey(), entity.getChunk().getChunkId(), rawSize, compressSize,
                compressRatio, (System.nanoTime() - startTime) / 1000);
    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
