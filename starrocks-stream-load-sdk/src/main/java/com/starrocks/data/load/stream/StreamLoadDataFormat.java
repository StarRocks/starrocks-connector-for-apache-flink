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

package com.starrocks.data.load.stream;

import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public interface StreamLoadDataFormat {
    StreamLoadDataFormat JSON = new JSONFormat();
    StreamLoadDataFormat CSV = new CSVFormat();
    StreamLoadDataFormat ARROW = new ArrowFormat();

    default String name() {
        return "";
    }

    byte[] first();
    byte[] delimiter();
    byte[] end();

    class CSVFormat implements StreamLoadDataFormat, Serializable {

        private static final byte[] EMPTY_DELIMITER = new byte[0];
        private static final String DEFAULT_LINE_DELIMITER = "\n";
        private final byte[] delimiter;

        public CSVFormat() {
            this(DEFAULT_LINE_DELIMITER);
        }

        public CSVFormat(String rowDelimiter) {
            if (rowDelimiter == null) {
                throw new IllegalArgumentException("row delimiter can not be null");
            }

            this.delimiter = rowDelimiter.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String name() {
            return "csv";
        }

        @Override
        public byte[] first() {
            return EMPTY_DELIMITER;
        }

        @Override
        public byte[] delimiter() {
            return delimiter;
        }

        @Override
        public byte[] end() {
            // For transaction stream load, need to append the row delimiter to the end of each
            // load, and separate the data in multiple loads. For example, there are 3 columns,
            // column delimiter ",", row delimiter "\n". There are 2 rows (1, 2, 3), (4, 5, 6),
            // and send them in two loads. If not append the end delimiter, the data received by
            // StarRocks will be "1,2,34,5,6", the two rows are not separated by row delimiter, and
            // StarRocks will parse it as one line with 5 columns. After appending the end delimiter,
            // it will be "1,2,3\n4,5,6", which will be parsed correctly
            return delimiter;
        }

        @JsonValue
        @Override
        public String toString() {
            return "CsvFormat{" +
                    "first=" + new String(EMPTY_DELIMITER) +
                    ", delimiter=" + new String(delimiter) +
                    ", end=" + new String(delimiter) +
                    '}';
        }
    }

    class JSONFormat implements StreamLoadDataFormat, Serializable {
        private static final byte[] first = "[".getBytes(StandardCharsets.UTF_8);
        private static final byte[] delimiter = ",".getBytes(StandardCharsets.UTF_8);
        private static final byte[] end = "]".getBytes(StandardCharsets.UTF_8);

        @Override
        public String name() {
            return "json";
        }

        @Override
        public byte[] first() {
            return first;
        }

        @Override
        public byte[] delimiter() {
            return delimiter;
        }

        @Override
        public byte[] end() {
            return end;
        }

        @JsonValue
        @Override
        public String toString() {
            return "JsonFormat{" +
                    "first=" + new String(first) +
                    ", delimiter=" + new String(delimiter) +
                    ", end=" + new String(end) +
                    '}';
        }
    }

    /**
     * ArrowFormat represents the Apache Arrow IPC stream format.
     * <p>
     * Note on validation: This SDK intentionally avoids importing heavyweight Apache Arrow
     * dependencies (such as arrow-vector and arrow-memory) to keep the client lightweight 
     * and performant. Therefore, there is no client-side schema validation of the Arrow 
     * RecordBatches before they are sent. The SDK simply streams the raw bytes directly.
     * <p>
     * Validation is delegated to the StarRocks backend's ArrowScanner, which will natively 
     * parse the Arrow IPC stream and validate its schema against the requested `columns` 
     * mapping HTTP header. If an invalid Arrow byte stream is sent, the stream load 
     * operation will fail and return an error from the backend.
     * <p>
     * Since Arrow IPC streams are self-describing and do not use text delimiters, 
     * `first()`, `delimiter()`, and `end()` all return empty byte arrays to enable 
     * zero-copy appending of pre-serialized RecordBatches.
     */
    class ArrowFormat implements StreamLoadDataFormat, Serializable {
        private static final byte[] EMPTY = new byte[0];

        @Override
        public String name() {
            return "arrow";
        }

        @Override
        public byte[] first() {
            return EMPTY;
        }

        @Override
        public byte[] delimiter() {
            return EMPTY;
        }

        @Override
        public byte[] end() {
            return EMPTY;
        }

        @JsonValue
        @Override
        public String toString() {
            return "ArrowFormat{}";
        }
    }
}
