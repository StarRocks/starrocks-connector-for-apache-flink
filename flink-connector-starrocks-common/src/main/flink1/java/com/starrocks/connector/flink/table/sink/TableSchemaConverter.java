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

package com.starrocks.connector.flink.table.sink;

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Converts the legacy {@link TableSchema} to the {@link ResolvedSchema} that the
 * common module speaks ({@link TableSchema} was removed in Flink 2.0).
 */
public final class TableSchemaConverter {

    private TableSchemaConverter() {
    }

    public static ResolvedSchema toResolvedSchema(TableSchema schema) {
        List<Column> columns = new ArrayList<>();
        for (TableColumn tableColumn : schema.getTableColumns()) {
            columns.add(Column.physical(tableColumn.getName(), tableColumn.getType()));
        }
        UniqueConstraint primaryKey = schema.getPrimaryKey()
                .map(pk -> UniqueConstraint.primaryKey(pk.getName(), pk.getColumns()))
                .orElse(null);
        return new ResolvedSchema(columns, Collections.emptyList(), primaryKey);
    }
}
