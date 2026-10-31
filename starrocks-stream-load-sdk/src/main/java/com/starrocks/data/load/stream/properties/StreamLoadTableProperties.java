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
import java.util.Optional;

public class StreamLoadTableProperties implements Serializable {

    private final String uniqueKey;
    private final String database;
    private final String table;
    private final StreamLoadDataFormat dataFormat;
    private final Map<String, Object> tableProperties;
    // individual stream load properties
    private final Map<String, String> properties;
    private final boolean enableUpsertDelete;
    private final long chunkLimit;
    private final int maxBufferRows;
    private final String columns;
    private final Map<String, String> commonProperties;

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
        this.commonProperties = new HashMap<>(builder.commonProperties);
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

    public Map<String, String> getCommonProperties() {
        return commonProperties;
    }

    public Optional<String> getProperty(String name) {
        String value = properties.get(name);
        if (value != null) {
            return Optional.of(value);
        }

        return Optional.ofNullable(commonProperties.get(name));
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

        private final Map<String, String> commonProperties = new HashMap<>();

        private Builder() {

        }

        // uniqueKey is not copied: it is generated from database and table, and build() writes this
        // builder's own database and table into the properties, so the copy names its own table.
        public Builder copyFrom(StreamLoadTableProperties streamLoadTableProperties) {
            database(streamLoadTableProperties.getDatabase());
            table(streamLoadTableProperties.getTable());
            columns(streamLoadTableProperties.getColumns());
            streamLoadDataFormat(streamLoadTableProperties.getDataFormat());
            chunkLimit(streamLoadTableProperties.getChunkLimit());
            maxBufferRows(streamLoadTableProperties.getMaxBufferRows());
            tableProperties.putAll(streamLoadTableProperties.getTableProperties());
            // The properties map holds the per table stream load headers, and nothing re-derives
            // them, so a copy that drops them would write the table differently.
            properties.putAll(streamLoadTableProperties.getProperties());
            // build() re-derives the `columns` header from this builder's own columns field, and it
            // only writes that entry when the field is non-null, so a generated entry copied here
            // would survive an explicit columns(null) and keep loading the source table's columns.
            // Drop the entry build() generated, which is the one equal to the source's columns
            // field. Every other header is copied as is, a `columns` value the caller added as a raw
            // property included, since build() never generates that one. build() drops the case
            // variants of the names it derives when it writes its own, so the copy is left with one
            // header per reserved name.
            String sourceColumns = streamLoadTableProperties.getColumns();
            if (sourceColumns != null && sourceColumns.equals(properties.get("columns"))) {
                properties.remove("columns");
            }
            commonProperties.putAll(streamLoadTableProperties.getCommonProperties());
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

        public Builder addCommonProperties(Map<String, String> properties) {
            this.commonProperties.putAll(properties);
            return this;
        }

        public StreamLoadTableProperties build() {
            if (database == null || table == null) {
                throw new IllegalArgumentException(String.format("database `%s` or table `%s` can't be null", database, table));
            }

            // db, table and columns are the headers this builder derives from its own fields, and
            // HTTP header names are case insensitive: a caller supplied `DB` next to the derived
            // `db` is the same header sent twice, and the server may read either value. The derived
            // field is the explicit destination, so it wins: the conflicting reserved names are
            // dropped whatever their case before the derived ones are written. db and table are
            // always derived, columns only when the field is set, so a raw `columns` property, which
            // the wildcard default relies on, still survives a build that leaves that field null.
            // Only these three reserved names are touched, every other header keeps its spelling.
            removeReservedProperty("db");
            removeReservedProperty("table");
            if (columns != null) {
                removeReservedProperty("columns");
            }
            addProperty("db", database);
            addProperty("table", table);
            if (columns != null) {
                addProperty("columns", columns);
            }
            return new StreamLoadTableProperties(this);
        }

        // HTTP header names are case insensitive, so `Columns` and `columns` are one header. Both
        // maps are cleared because LoadParameters merges the common map before the per table map,
        // which would put a mixed case common entry on the wire next to the derived per table one.
        private void removeReservedProperty(String name) {
            properties.keySet().removeIf(key -> name.equalsIgnoreCase(key));
            commonProperties.keySet().removeIf(key -> name.equalsIgnoreCase(key));
        }

    }
}
