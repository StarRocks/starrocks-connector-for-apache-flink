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

/** Describe the schema of a StarRocks table. */
public class StarRocksSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    // Have been sorted by the ordinal positions of columns.
    private final List<StarRocksColumn> columns;
    // Map column names to the Column structure.
    private final Map<String, StarRocksColumn> columnMap;

    private StarRocksSchema(List<StarRocksColumn> columns) {
        this.columns = new ArrayList<>(columns);
        this.columns.sort(Comparator.comparingInt(StarRocksColumn::getOrdinalPosition));
        this.columnMap = new HashMap<>();
        this.columns.forEach(column -> columnMap.put(column.getName(), column));
    }

    public StarRocksColumn getColumn(String columnName) {
        return columnMap.get(columnName);
    }

    public List<StarRocksColumn> getColumns() {
        return columns;
    }

    public static class Builder {

        private final List<StarRocksColumn> columns = new ArrayList<>();

        public Builder addColumn(StarRocksColumn column) {
            columns.add(column);
            return this;
        }

        public StarRocksSchema build() {
            Preconditions.checkState(!columns.isEmpty(), "There should be at least one column.");
            return new StarRocksSchema(columns);
        }
    }
}
