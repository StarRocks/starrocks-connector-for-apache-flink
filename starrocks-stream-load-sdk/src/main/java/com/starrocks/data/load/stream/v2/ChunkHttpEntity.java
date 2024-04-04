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

package com.starrocks.data.load.stream.v2;

import com.starrocks.data.load.stream.Chunk;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ChunkHttpEntity extends AbstractHttpEntity {

    private static final Logger LOG = LoggerFactory.getLogger(ChunkHttpEntity.class);

    protected static final int OUTPUT_BUFFER_SIZE = 2048;
    private static final Header CONTENT_TYPE =
            new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM.toString());
    private final String tableUniqueKey;
    private final Chunk chunk;
    private final long contentLength;
    private boolean logAfterWrite;

    public ChunkHttpEntity(String tableUniqueKey, Chunk chunk) {
        this.tableUniqueKey = tableUniqueKey;
        this.chunk = chunk;
        this.contentLength = chunk.chunkBytes();
        this.logAfterWrite = true;
    }

    public String getTableUniqueKey() {
        return tableUniqueKey;
    }

    public void setLogAfterWrite(boolean logAfterWrite) {
        this.logAfterWrite = logAfterWrite;
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public boolean isChunked() {
        return false;
    }

    @Override
    public long getContentLength() {
        return contentLength;
    }

    @Override
    public Header getContentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Header getContentEncoding() {
        return null;
    }

    @Override
    public InputStream getContent() {
        return new ChunkInputStream(chunk);
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
        try (InputStream inputStream = new ChunkInputStream(chunk)) {
            final byte[] buffer = new byte[OUTPUT_BUFFER_SIZE];
            int len;
            while ((len = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, len);
            }
        }
        if (logAfterWrite || LOG.isDebugEnabled()) {
            LOG.info("Finish to write entity for table {}, contentLength : {}",
                    tableUniqueKey, contentLength);
        }
    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
