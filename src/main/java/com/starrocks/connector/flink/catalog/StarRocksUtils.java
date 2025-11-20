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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarRocksUtils {

    public static StarRocksTable toStarRocksTable(
            String catalogName,
            ObjectPath tablePath,
            Configuration tableBaseConfig,
            CatalogBaseTable flinkTable) {
        TableSchema flinkSchema = flinkTable.getSchema();
        if (!flinkSchema.getPrimaryKey().isPresent()) {
            throw new CatalogException(
                    String.format("Catalog %s can't create a non primary key table %s.",
                            catalogName, tablePath.getFullName()));
        }
        RowType rowType = (RowType) flinkSchema.toPhysicalRowDataType().getLogicalType();
        Map<String, RowType.RowField> nameToFieldMap = new HashMap<>();
        for (RowType.RowField field : rowType.getFields()) {
            nameToFieldMap.put(field.getName(), field);
        }
        List<String> primaryKeys = flinkSchema.getPrimaryKey()
                .map(pk -> pk.getColumns())
                .orElse(Collections.emptyList());
        Preconditions.checkState(!primaryKeys.isEmpty());

        List<RowType.RowField> orderedFields = new ArrayList<>();
        for (String primaryKey : primaryKeys) {
            orderedFields.add(nameToFieldMap.get(primaryKey));
        }
        for (RowType.RowField field : rowType.getFields()) {
            if (!primaryKeys.contains(field.getName())) {
                orderedFields.add(field);
            }
        }

        List<StarRocksColumn> starRocksColumns = new ArrayList<>();
        for (int i = 0; i < orderedFields.size(); i++) {
            RowType.RowField field = orderedFields.get(i);
            StarRocksColumn.Builder columnBuilder =
                    new StarRocksColumn.Builder()
                            .setColumnName(field.getName())
                            .setOrdinalPosition(i);
            TypeUtils.toStarRocksType(columnBuilder, field.getType());
            starRocksColumns.add(columnBuilder.build());
        }

        StarRocksTable.Builder starRocksTableBuilder = new StarRocksTable.Builder()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getObjectName())
                .setTableType(StarRocksTable.TableType.PRIMARY_KEY)
                .setColumns(starRocksColumns)
                .setTableKeys(primaryKeys)
                .setDistributionKeys(primaryKeys)
                .setComment(flinkTable.getComment());
        if (tableBaseConfig.contains(CatalogOptions.TABLE_NUM_BUCKETS)) {
            starRocksTableBuilder.setNumBuckets(tableBaseConfig.get(CatalogOptions.TABLE_NUM_BUCKETS));
        }
        starRocksTableBuilder.setTableProperties(
                    ConfigUtils.getPrefixConfigs(
                        CatalogOptions.TABLE_PROPERTIES_PREFIX,
                        tableBaseConfig.toMap(),
                        true
                    )
                );
        return starRocksTableBuilder.build();
    }
}
