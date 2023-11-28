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

package com.starrocks.connector.flink.table.sink.v2;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.tools.JsonWrapper;

/** Serializer for the {@link RowData} record. */
public class RowDataSerializationSchema implements StarRocksRecordSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final String tableName;
    boolean supportUpsertDelete;
    boolean ignoreUpdateBefore;
    private final StarRocksISerializer serializer;
    private final StarRocksIRowTransformer<RowData> rowTransformer;
    private transient DefaultStarRocksRowData reusableRowData;

    public RowDataSerializationSchema(
            String databaseName,
            String tableName,
            boolean supportUpsertDelete,
            boolean ignoreUpdateBefore,
            StarRocksISerializer serializer,
            StarRocksIRowTransformer<RowData> rowTransformer) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.supportUpsertDelete = supportUpsertDelete;
        this.ignoreUpdateBefore = ignoreUpdateBefore;
        this.serializer = serializer;
        this.rowTransformer = rowTransformer;
    }

    @Override
    public void open() {
        JsonWrapper jsonWrapper = new JsonWrapper();
        this.serializer.open(new StarRocksISerializer.SerializerContext(jsonWrapper));
        this.rowTransformer.setRuntimeContext(null);
        this.rowTransformer.setFastJsonWrapper(jsonWrapper);
        this.reusableRowData = new DefaultStarRocksRowData();
        reusableRowData.setDatabase(databaseName);
        reusableRowData.setTable(tableName);
    }

    @Override
    public StarRocksRowData serialize(RowData record) {
        if (RowKind.UPDATE_BEFORE.equals(record.getRowKind()) &&
                (!supportUpsertDelete || ignoreUpdateBefore)) {
            return null;
        }
        if (!supportUpsertDelete && RowKind.DELETE.equals(record.getRowKind())) {
            // let go the UPDATE_AFTER and INSERT rows for tables who have a group of `unique` or `duplicate` keys.
            return null;
        }
        String serializedRow = serializer.serialize(rowTransformer.transform(record, supportUpsertDelete));
        reusableRowData.setRow(serializedRow);
        return reusableRowData;
    }

    @Override
    public void close() {

    }
}
