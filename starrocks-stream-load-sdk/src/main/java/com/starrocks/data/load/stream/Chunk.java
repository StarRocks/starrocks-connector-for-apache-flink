/*
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

package com.starrocks.data.load.stream;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A chunk is the http body that will be sent in one http request. Multiple rows
 * will be assembled as a chunk in csv or json format. So the chunk size includes
 * the rows' size and the delimiters' size.
 */
public class Chunk {

    private final StreamLoadDataFormat format;
    private final LinkedList<byte[]> buffer;
    private final AtomicLong rowBytes;
    private final AtomicLong chunkBytes;

    public Chunk(StreamLoadDataFormat format) {
        this.format = format;
        this.buffer = new LinkedList<>();
        this.rowBytes = new AtomicLong();
        this.chunkBytes = new AtomicLong();
        chunkBytes.addAndGet(format.first().length);
        chunkBytes.addAndGet(format.end().length);
    }

    public void addRow(byte[] data) {
        rowBytes.addAndGet(data.length);
        chunkBytes.addAndGet(data.length + (buffer.isEmpty() ?  0 : format.delimiter().length));
        buffer.add(data);
    }

    public int numRows() {
        return buffer.size();
    }

    public long rowBytes() {
        return rowBytes.get();
    }

    public long chunkBytes() {
        return chunkBytes.get();
    }

    public long estimateChunkSize(byte[] data) {
        return chunkBytes.get() + data.length + format.delimiter().length;
    }

    public Iterator<byte[]> iterator() {
        return new DataIterator();
    }

    enum ItemType {
        NONE,
        FIRST,
        ROW,
        DELIMITER,
        END
    }

    // Iterates the chunk including rows and delimiters
    private class DataIterator implements Iterator<byte[]> {

        private final Iterator<byte[]> rowIterator;
        private int totalItems;
        private ItemType nextItemType;

        public DataIterator() {
            this.totalItems = 2 + buffer.size() + (buffer.size() - 1);
            this.rowIterator = buffer.iterator();
            this.nextItemType = ItemType.FIRST;
        }

        @Override
        public boolean hasNext() {
            return totalItems > 0;
        }

        @Override
        public byte[] next() {
            byte[] item;
            switch (nextItemType) {
                case FIRST:
                    item = format.first();
                    nextItemType = rowIterator.hasNext() ? ItemType.ROW : ItemType.END;
                    break;
                case ROW:
                    item = rowIterator.next();
                    nextItemType = rowIterator.hasNext() ? ItemType.DELIMITER : ItemType.END;
                    break;
                case DELIMITER:
                    item = format.delimiter();
                    nextItemType = ItemType.ROW;
                    break;
                case  END:
                    item = format.end();
                    nextItemType = ItemType.NONE;
                    break;
                default:
                    throw new UnsupportedOperationException("Should not switch to type " + nextItemType);
            }
            totalItems -= 1;
            return item;
        }
    }
}
