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

package com.starrocks.data.load.stream.http;

import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.io.StreamLoadStream;

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

public class StreamLoadEntity extends AbstractHttpEntity {

    private static final Logger log = LoggerFactory.getLogger(StreamLoadEntity.class);

    protected static final int OUTPUT_BUFFER_SIZE = 2048;

    private static final Header CONTENT_TYPE =
            new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM.toString());

    private final TableRegion region;
    private final InputStream content;

    private final boolean chunked;
    private final long contentLength;

    public StreamLoadEntity(TableRegion region,
                            StreamLoadDataFormat dataFormat,
                            StreamLoadEntityMeta meta) {
        this.region = region;
        this.content = new StreamLoadStream(region, dataFormat);
        this.chunked = meta.getBytes() == -1L;
        this.contentLength = meta.getBytes();
    }

    @Override
    public boolean isRepeatable() {
        return false;
    }

    @Override
    public boolean isChunked() {
        return chunked;
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
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return content;
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
        long total = 0;
        try (InputStream inputStream = this.content) {
            final byte[] buffer = new byte[OUTPUT_BUFFER_SIZE];
            int l;
            while ((l = inputStream.read(buffer)) != -1) {
                total += l;
                outputStream.write(buffer, 0, l);
            }
        }
        log.info("Entity write end, contentLength : {}, total : {}", contentLength, total);
    }

    @Override
    public boolean isStreaming() {
        return true;
    }
}
