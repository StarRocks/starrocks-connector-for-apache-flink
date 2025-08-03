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

import com.starrocks.data.load.stream.StreamLoadDataFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;

/** Compress data with the specified compression algorithm. */
public interface CompressionCodec {

    /**
     * Create an output stream to compress the raw output stream.
     *
     * @param rawOutputStream the output stream to compress
     * @param contentSize the content size of the raw stream. -1 if the size is unknown
     * @return an output stream with compressed data
     */
    OutputStream createCompressionStream(final OutputStream rawOutputStream, long contentSize) throws IOException;

    static Optional<CompressionCodec> createCompressionCodec(StreamLoadDataFormat dataFormat,
                                                             Optional<String> compressionType,
                                                             Map<String, Object> properties) {
        if (!compressionType.isPresent()) {
            return Optional.empty();
        }

        if (LZ4FrameCompressionCodec.NAME.equalsIgnoreCase(compressionType.get())) {
            return Optional.of(LZ4FrameCompressionCodec.create(properties));
        }

        throw new UnsupportedOperationException(
                String.format("Not support to compress format %s with compression type %s",
                        dataFormat.name(), compressionType.get()));
    }
}
