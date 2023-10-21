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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class StarRocksUtils {

    private static final String TABLE_SCHEMA_QUERY =
            "SELECT `COLUMN_NAME`, `DATA_TYPE`, `ORDINAL_POSITION`, `COLUMN_SIZE`, `DECIMAL_DIGITS`, " +
                    "`COLUMN_DEFAULT`, `IS_NULLABLE`, `COLUMN_KEY` FROM `information_schema`.`COLUMNS` " +
                    "WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME`=?;";

    public static StarRocksSchema getStarRocksSchema(Connection connection, String database, String table) throws Exception {
        StarRocksSchema.Builder schemaBuilder = new StarRocksSchema.Builder();
        try (PreparedStatement statement = connection.prepareStatement(TABLE_SCHEMA_QUERY)) {
            statement.setObject(1, database);
            statement.setObject(2, table);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String name = resultSet.getString("COLUMN_NAME");
                    String type = resultSet.getString("DATA_TYPE");
                    int position = resultSet.getInt("ORDINAL_POSITION");
                    Integer size = resultSet.getInt("COLUMN_SIZE");
                    if (resultSet.wasNull()) {
                        size = null;
                    }
                    Integer scale = resultSet.getInt("DECIMAL_DIGITS");
                    if (resultSet.wasNull()) {
                        scale = null;
                    }
                    String defaultValue = resultSet.getString("COLUMN_DEFAULT");
                    String isNullable = resultSet.getString("IS_NULLABLE");
                    String columnKey = resultSet.getString("COLUMN_KEY");
                    StarRocksColumn column = new StarRocksColumn.Builder()
                            .setName(name)
                            .setOrdinalPosition(position - 1)
                            .setType(type)
                            .setKey(columnKey)
                            .setSize(size)
                            .setScale(scale)
                            .setDefaultValue(defaultValue)
                            .setNullable(isNullable == null || !isNullable.equalsIgnoreCase("NO"))
                            .setComment(null)
                            .build();
                    schemaBuilder.addColumn(column);
                }
            }
        }

        return schemaBuilder.build();
    }

    public static List<String> getPrimaryKeys(StarRocksSchema starRocksSchema) {
        return starRocksSchema.getColumns().stream()
                .filter(column -> "PRI".equalsIgnoreCase(column.getKey()))
                // In StarRocks, primary keys must be declared in the same order of that in schema
                .sorted(Comparator.comparingInt(StarRocksColumn::getOrdinalPosition))
                .map(StarRocksColumn::getName)
                .collect(Collectors.toList());
    }

    public static StarRocksTable getStarRocksTable(Connection connection, String database, String table) throws Exception {
        StarRocksSchema starRocksSchema = StarRocksUtils.getStarRocksSchema(connection, database, table);
        // could be empty if this is not a primary key table
        List<String> primaryKeys = StarRocksUtils.getPrimaryKeys(starRocksSchema);
        StarRocksTable.TableType tableType = primaryKeys.isEmpty() ?
                StarRocksTable.TableType.UNKNOWN : StarRocksTable.TableType.PRIMARY;
        return new StarRocksTable.Builder()
                .setDatabase(database)
                .setTable(table)
                .setSchema(starRocksSchema)
                .setTableType(tableType)
                .setTableKeys(primaryKeys.isEmpty() ? null : primaryKeys)
                .build();
    }

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
        StarRocksSchema.Builder starRocksSchemaBuilder = new StarRocksSchema.Builder();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            RowType.RowField field = rowType.getFields().get(i);
            StarRocksColumn.Builder columnBuilder =
                    new StarRocksColumn.Builder()
                            .setName(field.getName())
                            .setOrdinalPosition(i);
            TypeUtils.toStarRocksType(columnBuilder, field.getType());
            starRocksSchemaBuilder.addColumn(columnBuilder.build());
        }
        StarRocksSchema starRocksSchema = starRocksSchemaBuilder.build();
        StarRocksTable.Builder starRocksTableBuilder = new StarRocksTable.Builder()
                .setDatabase(tablePath.getDatabaseName())
                .setTable(tablePath.getObjectName())
                .setSchema(starRocksSchema);

        List<String> primaryKeys = flinkSchema.getPrimaryKey()
                .map(pk -> pk.getColumns())
                .orElse(Collections.emptyList());
        // have verified it's a primary key table above
        Preconditions.checkState(!primaryKeys.isEmpty());
        starRocksTableBuilder.setTableType(StarRocksTable.TableType.PRIMARY);
        starRocksTableBuilder.setTableKeys(primaryKeys);
        if (tableBaseConfig.contains(CatalogOptions.TABLE_NUM_BUCKETS)) {
            starRocksTableBuilder.setNumBuckets(tableBaseConfig.get(CatalogOptions.TABLE_NUM_BUCKETS));
        }
        starRocksTableBuilder.setComment(flinkTable.getComment());
        starRocksTableBuilder.setTableProperties(
                ConfigUtils.getPrefixConfigs(CatalogOptions.TABLE_PROPERTIES_PREFIX, tableBaseConfig.toMap()));
        return starRocksTableBuilder.build();
    }

    public static String buildCreateTableSql(StarRocksTable table, boolean ignoreIfExists) {
        Preconditions.checkState(table.getTableType() == StarRocksTable.TableType.PRIMARY);
        StringBuilder builder = new StringBuilder();
        builder.append(
                String.format("CREATE TABLE %s`%s`.`%s`",
                        ignoreIfExists ? "IF NOT EXISTS " : "",
                        table.getDatabase(),
                        table.getTable())
        );
        builder.append(" (\n");
        StarRocksSchema schema = table.getSchema();
        String columnsStmt = schema.getColumns().stream().map(StarRocksUtils::buildColumnStatement)
                .collect(Collectors.joining(",\n"));
        builder.append(columnsStmt);
        builder.append("\n) ");
        String primaryKeys = table.getTableKeys().stream()
                .map(pk -> "`" + pk + "`").collect(Collectors.joining(", "));
        builder.append(String.format("PRIMARY KEY (%s)\n", primaryKeys));
        builder.append(String.format("DISTRIBUTED BY HASH (%s)", primaryKeys));
        if (table.getNumBuckets() != null) {
            builder.append(" BUCKETS ");
            builder.append(table.getNumBuckets());
        }
        if (table.getProperties() != null && !table.getProperties().isEmpty()) {
            builder.append("\nPROPERTIES (\n");
            String properties = table.getProperties().entrySet().stream()
                    .map(entry -> String.format("\"%s\" = \"%s\"", entry.getKey(), entry.getValue()))
                    .collect(Collectors.joining(",\n"));
            builder.append(properties);
            builder.append("\n)");
        }
        builder.append(";");
        return builder.toString();
    }

    public static String buildColumnStatement(StarRocksColumn column) {
        StringBuilder builder = new StringBuilder();
        builder.append("`");
        builder.append(column.getName());
        builder.append("` ");
        builder.append(getFullColumnType(column.getType(), column.getSize(), column.getScale()));
        builder.append(" ");
        builder.append(column.isNullable() ? "NULL" : "NOT NULL");
        if (column.getDefaultValue() != null) {
            builder.append(" DEFAULT '");
            builder.append(column.getDefaultValue());
            builder.append("'");
        }
        if (column.getComment() != null) {
            builder.append(" COMMENT \"");
            builder.append(column.getComment());
            builder.append("\"");
        }
        return builder.toString();
    }

    public static String getFullColumnType(String type, @Nullable Integer size, @Nullable Integer scale) {
        String dataType = type.toUpperCase();
        switch (dataType) {
            case "DECIMAL":
                return String.format("DECIMAL(%d, %s)", size, scale);
            case "CHAR":
            case "VARCHAR":
                return String.format("%s(%d)", dataType, size);
            default:
                return dataType;
        }
    }
}
