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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

public class TypeUtils {

    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String LARGEINT = "BIGINT UNSIGNED";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";
    public static final String JSON = "JSON";

    public static final int MAX_VARCHAR_SIZE = 1048576;
    public static final int STRING_SIZE = 65533;

    public static DataType toFlinkType(
            String starRocksType,
            @Nullable Integer precision,
            @Nullable Integer scale,
            boolean isNull) {
        switch (starRocksType.toUpperCase()) {
            case BOOLEAN:
                return wrapNull(DataTypes.BOOLEAN(), isNull);
            case TINYINT:
                // mysql does not have boolean type, and starrocks `information_schema`.`COLUMNS` will return
                // a "tinyint" data type for both StarRocks BOOLEAN and TINYINT type, We distinguish them by
                // column size, and the size of BOOLEAN is null
                return precision == null
                        ? wrapNull(DataTypes.BOOLEAN(), isNull)
                        : wrapNull(DataTypes.TINYINT(), isNull);
            case SMALLINT:
                return wrapNull(DataTypes.SMALLINT(), isNull);
            case INT:
                return wrapNull(DataTypes.INT(), isNull);
            case BIGINT:
                return wrapNull(DataTypes.BIGINT(), isNull);
            case LARGEINT:
                return wrapNull(DataTypes.STRING(), isNull);
            case FLOAT:
                return wrapNull(DataTypes.FLOAT(), isNull);
            case DOUBLE:
                return wrapNull(DataTypes.DOUBLE(), isNull);
            case DECIMAL:
                Preconditions.checkNotNull(precision, "Precision for StarRocks DECIMAL can't be null.");
                Preconditions.checkNotNull(scale, "Scale for StarRocks DECIMAL can't be null.");
                return wrapNull(DataTypes.DECIMAL(precision, scale), isNull);
            case CHAR:
                Preconditions.checkNotNull(precision, "Precision for StarRocks CHAR can't be null.");
                return wrapNull(DataTypes.CHAR(precision), isNull);
            case VARCHAR:
                Preconditions.checkNotNull(precision, "Precision for StarRocks VARCHAR can't be null.");
                return wrapNull(DataTypes.VARCHAR(precision), isNull);
            case STRING:
                return wrapNull(DataTypes.STRING(), isNull);
            case DATE:
                return wrapNull(DataTypes.DATE(), isNull);
            case DATETIME:
                return wrapNull(DataTypes.TIMESTAMP(0), isNull);
            case JSON:
                return wrapNull(DataTypes.STRING(), isNull);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported StarRocks type %s when mapping StarRocks and Flink tables via Catalog. " +
                                        "You can try to create table directly if you want to map this StarRocks type.",
                                starRocksType));
        }
    }

    private static DataType wrapNull(DataType dataType, boolean isNull) {
        return isNull ? dataType.nullable() : dataType.notNull();
    }

    public static void toStarRocksType(StarRocksColumn.Builder builder, LogicalType flinkType) {
        flinkType.accept(new FlinkLogicalTypeVisitor(builder));
    }

    private static class FlinkLogicalTypeVisitor extends LogicalTypeDefaultVisitor<StarRocksColumn.Builder> {

        private final StarRocksColumn.Builder builder;

        public FlinkLogicalTypeVisitor(StarRocksColumn.Builder builder) {
            this.builder = builder;
        }

        @Override
        public StarRocksColumn.Builder visit(CharType charType) {
            builder.setType(CHAR);
            builder.setSize(charType.getLength());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(VarCharType varCharType) {
            builder.setType(VARCHAR);
            builder.setSize(Math.min(varCharType.getLength(), MAX_VARCHAR_SIZE));
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(BooleanType booleanType) {
            builder.setType(BOOLEAN);
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(DecimalType decimalType) {
            builder.setType(DECIMAL);
            builder.setSize(decimalType.getPrecision());
            builder.setScale(decimalType.getScale());
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(TinyIntType tinyIntType) {
            builder.setType(TINYINT);
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(SmallIntType smallIntType) {
            builder.setType(SMALLINT);
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(IntType intType) {
            builder.setType(INT);
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(BigIntType bigIntType) {
            builder.setType(BIGINT);
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(FloatType floatType) {
            builder.setType(FLOAT);
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(DoubleType doubleType) {
            builder.setType(DOUBLE);
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(DateType dateType) {
            builder.setType(DATE);
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(TimestampType timestampType) {
            builder.setType(DATETIME);
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(ZonedTimestampType zonedTimestampType) {
            builder.setType(DATETIME);
            return builder;
        }

        @Override
        public StarRocksColumn.Builder visit(LocalZonedTimestampType localZonedTimestampType) {
            builder.setType(DATETIME);
            return builder;
        }

        @Override
        protected StarRocksColumn.Builder defaultMethod(LogicalType logicalType) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported StarRocks type %s when mapping StarRocks and Flink tables via Catalog. " +
                                    "You can try to create table directly if you want to map this Flink type.",
                                        logicalType.toString()));
        }
    }
}
