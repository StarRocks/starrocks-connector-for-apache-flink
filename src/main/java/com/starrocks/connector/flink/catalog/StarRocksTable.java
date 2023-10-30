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

package com.starrocks.connector.flink.catalog;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** Describe a StarRocks table. */
public class StarRocksTable implements Serializable {

    enum TableType {
        UNKNOWN,
        DUPLICATE,
        AGGREGATE,
        UNIQUE,
        PRIMARY
    }

    private static final long serialVersionUID = 1L;

    private final String database;

    private final String table;

    private final StarRocksSchema schema;

    private final TableType tableType;

    // Following fields can be nullable if we can not get the metas from StarRocks

    // For different tables, have different meaning. It's duplicate keys for duplicate table,
    // aggregate keys for aggregate table, unique keys for unique table, and primary keys for
    // primary table. The list has been sorted by the ordinal positions of the columns.
    @Nullable
    private final List<String> tableKeys;

    @Nullable
    private final List<String> partitionKeys;

    @Nullable
    private final Integer numBuckets;

    @Nullable
    public final String comment;

    @Nullable
    private final Map<String, String> properties;

    private StarRocksTable(String database, String table, StarRocksSchema schema, TableType tableType,
                          @Nullable List<String> tableKeys, @Nullable List<String> partitionKeys,
                          @Nullable Integer numBuckets, @Nullable String comment,
                           @Nullable Map<String, String> properties) {
        this.database = Preconditions.checkNotNull(database);
        this.table = Preconditions.checkNotNull(table);
        this.schema = Preconditions.checkNotNull(schema);
        this.tableType = Preconditions.checkNotNull(tableType);
        this.tableKeys = tableKeys;
        this.partitionKeys = partitionKeys;
        this.numBuckets = numBuckets;
        this.comment = comment;
        this.properties = properties;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public StarRocksSchema getSchema() {
        return schema;
    }

    @Nullable
    public TableType getTableType() {
        return tableType;
    }

    @Nullable
    public List<String> getTableKeys() {
        return tableKeys;
    }

    @Nullable
    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    @Nullable
    public Integer getNumBuckets() {
        return numBuckets;
    }

    @Nullable
    public String getComment() {
        return comment;
    }

    @Nullable
    public Map<String, String> getProperties() {
        return properties;
    }

    public static class Builder {

        private String database;
        private String table;
        private StarRocksSchema schema;
        private TableType tableType;
        private List<String> tableKeys;
        private List<String> partitionKeys;
        private Integer numBuckets;
        private String comment;
        private Map<String, String> properties;

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public Builder setSchema(StarRocksSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder setTableType(TableType tableType) {
            this.tableType = tableType;
            return this;
        }

        public Builder setTableKeys(List<String> tableKeys) {
            this.tableKeys = new ArrayList<>(tableKeys);
            return this;
        }

        public Builder setPartitionKeys(List<String> partitionKeys) {
            this.partitionKeys = new ArrayList<>(partitionKeys);
            return this;
        }

        public Builder setNumBuckets(int numBuckets) {
            this.numBuckets = numBuckets;
            return this;
        }

        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder setTableProperties(Map<String, String> properties) {
            this.properties = new HashMap<>(properties);
            return this;
        }

        public StarRocksTable build() {
            Preconditions.checkNotNull(database, "database can't be null");
            Preconditions.checkNotNull(table, "table can't be null");
            Preconditions.checkNotNull(schema, "schema can't be null");
            Preconditions.checkNotNull(tableType, "table type can't be null");

            if (tableKeys != null) {
                List<StarRocksColumn> tableKeyColumns = new ArrayList<>(tableKeys.size());
                for (String name : tableKeys) {
                    StarRocksColumn column = schema.getColumn(name);
                    Preconditions.checkNotNull(column,
                            String.format("%s.%s does not contain column %s", database, table, name));
                    tableKeyColumns.add(column);
                }
                tableKeyColumns.sort(Comparator.comparingInt(StarRocksColumn::getOrdinalPosition));
                tableKeys = tableKeyColumns.stream().map(StarRocksColumn::getName).collect(Collectors.toList());
            }

            return new StarRocksTable(database, table, schema, tableType, tableKeys, partitionKeys,
                    numBuckets, comment, properties);
        }
    }
}
