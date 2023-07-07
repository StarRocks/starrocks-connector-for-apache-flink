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

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class ChunkInputStream extends InputStream {

    private final Chunk chunk;
    private final Iterator<byte[]> itemIterator;
    private byte[] currentItem;
    private int currentPos;

    public ChunkInputStream(Chunk chunk) {
        this.chunk = chunk;
        this.itemIterator = chunk.iterator();
    }

    @Override
    public int read() throws IOException {
        byte[] bytes = new byte[1];
        int ws = read(bytes);
        if (ws == -1) {
            return -1;
        }
        return bytes[0];
    }

    @Override
    public int read(byte[] buf) throws IOException {
        return read(buf, 0, buf.length);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        if (!itemIterator.hasNext() && currentItem == null) {
            return -1;
        }

        byte[] item = currentItem;
        int pos = currentPos;
        int readBytes = 0;
        while (readBytes < len && (item != null || itemIterator.hasNext())) {
            if (item == null ) {
                item = itemIterator.next();
                pos = 0;
            }

            int size = Math.min(len - readBytes, item.length - pos);
            System.arraycopy(item, pos, buf, off + readBytes, size);
            readBytes += size;
            pos += size;

            if (pos == item.length) {
                item = null;
                pos = 0;
            }
        }

        currentItem = item;
        currentPos = pos;

        return readBytes;
    }
}
