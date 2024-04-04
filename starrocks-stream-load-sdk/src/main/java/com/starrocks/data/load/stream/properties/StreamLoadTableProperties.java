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

package com.starrocks.data.load.stream.properties;

import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.annotation.Evolving;
import com.starrocks.data.load.stream.compress.CompressionOptions;
import net.jpountz.lz4.LZ4FrameOutputStream;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class StreamLoadTableProperties implements Serializable {

    private final String uniqueKey;
    private final String database;
    private final String table;
    private final StreamLoadDataFormat dataFormat;
    private final Map<String, Object> tableProperties;
    // stream load properties
    private final Map<String, String> properties;
    private final boolean enableUpsertDelete;
    private final long chunkLimit;
    private final int maxBufferRows;
    private final String columns;

    private StreamLoadTableProperties(Builder builder) {
        this.database = builder.database;
        this.table = builder.table;

        this.uniqueKey = builder.uniqueKey == null
                ? StreamLoadUtils.getTableUniqueKey(database, table)
                : builder.uniqueKey;

        this.dataFormat = builder.dataFormat == null
                ? StreamLoadDataFormat.JSON
                : builder.dataFormat;

        this.enableUpsertDelete = builder.enableUpsertDelete;
        if (dataFormat instanceof StreamLoadDataFormat.JSONFormat) {
            chunkLimit = Math.min(3221225472L, builder.chunkLimit);
        } else {
            chunkLimit = Math.min(10737418240L, builder.chunkLimit);
        }
        this.maxBufferRows = builder.maxBufferRows;
        this.tableProperties = new HashMap<>(builder.tableProperties);
        this.properties = new HashMap<>(builder.properties);
        this.columns = builder.columns;
    }

    public String getColumns() {return columns; }

    public String getUniqueKey() {
        return uniqueKey;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public boolean isEnableUpsertDelete() {
        return enableUpsertDelete;
    }

    public StreamLoadDataFormat getDataFormat() {
        return dataFormat;
    }

    public Long getChunkLimit() {
        return chunkLimit;
    }

    public int getMaxBufferRows() {
        return maxBufferRows;
    }

    @Evolving
    public Map<String, Object> getTableProperties() {
        return tableProperties;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String uniqueKey;
        private String database;
        private String table;
        private String columns;
        private boolean enableUpsertDelete;
        private StreamLoadDataFormat dataFormat;
        private long chunkLimit;
        private int maxBufferRows = Integer.MAX_VALUE;

        private final Map<String, Object> tableProperties = new HashMap<>();

        // Stream load properties
        private final Map<String, String> properties = new HashMap<>();

        private Builder() {

        }

        // This function does not copy the uniqueKey and properties attributes because the uniqueKey 
        // is generated in the StreamLoadTableProperties constructor.
        // The properties only contains three elements(database,table,columns), which are automatically
        // populated during the build process.
        // TODO: StreamLoadProperties.headers hold properties common to multiple tables, while
        // StreamLoadTableProperties.properties hold the specific properties of an individual table.
        // This should be taken into consideration during the refactoring.
        public Builder copyFrom(StreamLoadTableProperties streamLoadTableProperties) {
            // TODO: datbase, table, columns are private propertis for an individual table.
            // We may not copy thers private propertis.
            database(streamLoadTableProperties.getDatabase());
            table(streamLoadTableProperties.getTable());
            columns(streamLoadTableProperties.getColumns());
            streamLoadDataFormat(streamLoadTableProperties.getDataFormat());
            chunkLimit(streamLoadTableProperties.getChunkLimit());
            maxBufferRows(streamLoadTableProperties.getMaxBufferRows());
            tableProperties.putAll(streamLoadTableProperties.getTableProperties());
            return this;
        }

        public Builder uniqueKey(String uniqueKey) {
            this.uniqueKey = uniqueKey;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder columns(String columns) {
            this.columns = columns;
            return this;
        }

        public Builder enableUpsertDelete(boolean enableUpsertDelete) {
            this.enableUpsertDelete = enableUpsertDelete;
            return this;
        }

        public Builder streamLoadDataFormat(StreamLoadDataFormat dataFormat) {
            this.dataFormat = dataFormat;
            return this;
        }

        public Builder chunkLimit(long chunkLimit) {
            this.chunkLimit = chunkLimit;
            return this;
        }

        public Builder maxBufferRows(int maxBufferRows) {
            this.maxBufferRows = maxBufferRows;
            return this;
        }

        public Builder addProperties(Map<String, String> properties) {
            this.properties.putAll(properties);
            return this;
        }

        public Builder addProperty(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        public Builder setLZ4BlockSize(LZ4FrameOutputStream.BLOCKSIZE blockSize) {
            tableProperties.put(CompressionOptions.LZ4_BLOCK_SIZE, blockSize);
            return this;
        }

        public Builder enableLZ4BlockIndependence() {
            tableProperties.put(CompressionOptions.LZ4_BLOCK_INDEPENDENCE, true);
            return this;
        }

        public StreamLoadTableProperties build() {
            if (database == null || table == null) {
                throw new IllegalArgumentException(String.format("database `%s` or table `%s` can't be null", database, table));
            }

            addProperty("db", database);
            addProperty("table", table);
            if (columns != null) {
                addProperty("columns", columns);
            }
            return new StreamLoadTableProperties(this);
        }

    }
}
