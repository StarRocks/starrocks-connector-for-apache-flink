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

package com.starrocks.connector.flink.row.source;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.starrocks.connector.flink.tools.DataUtil;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Converts arrow field data to Flink data.
public interface ArrowFieldConverter {

    String DATETIME_FORMAT_LONG = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    String DATETIME_FORMAT_SHORT = "yyyy-MM-dd HH:mm:ss";
    String DATE_FORMAT = "yyyy-MM-dd";

    Object convert(Object arrowData);

    static void checkNullable(boolean isNullable, Object obj) {
        if (obj == null && !isNullable) {
            throw new IllegalStateException("The value is null for a non-nullable column");
        }
    }

    // Convert from arrow boolean to flink boolean
    class BooleanConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public BooleanConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(Object arrowData) {
            checkNullable(isNullable, arrowData);
            return ((int) arrowData) != 0;
        }
    }

    // Convert from arrow numeric type to flink numeric type,
    // including tinyint, smallint, int, bigint, float, double
    class NumericConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public NumericConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(Object arrowData) {
            checkNullable(isNullable, arrowData);
            return arrowData;
        }
    }

    // Convert from arrow decimal type to flink decimal
    class DecimalConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public DecimalConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(Object arrowData) {
            checkNullable(isNullable, arrowData);
            BigDecimal value = (BigDecimal) arrowData;
            return DecimalData.fromBigDecimal(value, value.precision(), value.scale());
        }
    }

    // Convert from arrow varchar to flink char/varchar
    class StringConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public StringConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(Object arrowData) {
            checkNullable(isNullable, arrowData);
            return new String((byte[]) arrowData);
        }
    }

    // Convert from arrow varchar to flink date
    class DateConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public DateConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(Object arrowData) {
            checkNullable(isNullable, arrowData);
            String value = new String((byte[]) arrowData);
            LocalDate date = LocalDate.parse(value, DateTimeFormatter.ofPattern(DATE_FORMAT));
            return (int) date.atStartOfDay().toLocalDate().toEpochDay();
        }
    }

    // Convert from arrow varchar to flink timestamp-related type
    class TimestampConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public TimestampConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(Object arrowData) {
            checkNullable(isNullable, arrowData);
            String value = new String((byte[]) arrowData);
            if (value.length() < DATETIME_FORMAT_SHORT.length()) {
                throw new IllegalArgumentException("Date value length shorter than DATETIME_FORMAT_SHORT");
            }
            if (value.length() == DATETIME_FORMAT_SHORT.length()) {
                value = DataUtil.addZeroForNum(value + ".", DATETIME_FORMAT_LONG.length());
            }
            value = DataUtil.addZeroForNum(value, DATETIME_FORMAT_LONG.length());
            DateTimeFormatter df = DateTimeFormatter.ofPattern(DATETIME_FORMAT_LONG);
            LocalDateTime ldt = LocalDateTime.parse(value, df);
            return TimestampData.fromLocalDateTime(ldt);
        }
    }

    // Convert from arrow list to flink arrow
    class ArrayConverter implements ArrowFieldConverter {

        private final boolean isNullable;
        private final ArrowFieldConverter elementConverter;

        public ArrayConverter(boolean isNullable, ArrowFieldConverter elementConverter) {
            this.isNullable = isNullable;
            this.elementConverter = elementConverter;
        }

        @Override
        public Object convert(Object arrowData) {
            checkNullable(isNullable, arrowData);
            List<?> list = (List<?> ) arrowData;
            Object[] data = new Object[list.size()];
            for (int i = 0; i < data.length; i++) {
                Object obj = list.get(i);
                data[i] = obj == null ? null : elementConverter.convert(obj);
            }
            return new GenericArrayData(data);
        }
    }

    // Convert from arrow struct to flink row
    class StructConverter implements ArrowFieldConverter {

        private final boolean isNullable;
        private final List<ArrowFieldConverter> childConverters;

        public StructConverter(boolean isNullable, List<ArrowFieldConverter> childConverters) {
            this.isNullable = isNullable;
            this.childConverters = childConverters;
        }

        @Override
        public Object convert(Object arrowData) {
            checkNullable(isNullable, arrowData);
            List<?> list = (List<?> ) arrowData;
            GenericRowData rowData = new GenericRowData(list.size());
            for (int i = 0; i < list.size(); i++) {
                Object obj = list.get(i);
                rowData.setField(i, obj == null ? null : childConverters.get(i).convert(obj));
            }
            return rowData;
        }
    }

    Map<LogicalTypeRoot, Types.MinorType> FLINK_AND_ARROW_TYPE_MAPPINGS = new HashMap<LogicalTypeRoot, Types.MinorType>() {{
        put(LogicalTypeRoot.BOOLEAN, Types.MinorType.BIT);
        put(LogicalTypeRoot.TINYINT, Types.MinorType.TINYINT);
        put(LogicalTypeRoot.SMALLINT, Types.MinorType.SMALLINT);
        put(LogicalTypeRoot.INTEGER, Types.MinorType.INT);
        put(LogicalTypeRoot.BIGINT, Types.MinorType.BIGINT);
        put(LogicalTypeRoot.FLOAT, Types.MinorType.FLOAT4);
        put(LogicalTypeRoot.DOUBLE, Types.MinorType.FLOAT8);
        put(LogicalTypeRoot.DECIMAL, Types.MinorType.DECIMAL);
        put(LogicalTypeRoot.DATE, Types.MinorType.VARCHAR);
        put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, Types.MinorType.VARCHAR);
        put(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, Types.MinorType.VARCHAR);
        put(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, Types.MinorType.VARCHAR);
        put(LogicalTypeRoot.CHAR, Types.MinorType.VARCHAR);
        put(LogicalTypeRoot.VARCHAR, Types.MinorType.VARCHAR);
        put(LogicalTypeRoot.ARRAY, Types.MinorType.LIST);
        put(LogicalTypeRoot.ROW, Types.MinorType.STRUCT);
    }};

    static void checkTypeCompatible(LogicalType flinkType, Field field) {
        Types.MinorType expectMinorType = FLINK_AND_ARROW_TYPE_MAPPINGS.get(flinkType.getTypeRoot());
        if (expectMinorType == null) {
            throw new UnsupportedOperationException(String.format(
                    "Flink type %s is not supported, and arrow type is %s",
                    flinkType, field.getType()));
        }

        Types.MinorType actualMinorType = Types.getMinorTypeForArrowType(field.getType());
        if (expectMinorType != actualMinorType) {
            throw new IllegalStateException(String.format(
                    "Flink %s should be mapped to arrow %s, but is arrow %s",
                    flinkType, expectMinorType, actualMinorType));
        }
    }

    static ArrowFieldConverter createConverter(LogicalType flinkType, Field arrowField) {
        checkTypeCompatible(flinkType, arrowField);
        LogicalTypeRoot rootType = flinkType.getTypeRoot();
        switch (rootType) {
            case BOOLEAN:
                return new BooleanConverter(flinkType.isNullable());
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return new NumericConverter(flinkType.isNullable());
            case DECIMAL:
                return new DecimalConverter(flinkType.isNullable());
            case DATE:
                return new DateConverter(flinkType.isNullable());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return new TimestampConverter(flinkType.isNullable());
            case CHAR:
            case VARCHAR:
                return new StringConverter(flinkType.isNullable());
            case ARRAY:
                return new ArrayConverter(flinkType.isNullable(),
                        createConverter(((ArrayType) flinkType).getElementType(), arrowField.getChildren().get(0)));
            case ROW:
                RowType rowType = (RowType) flinkType;
                Preconditions.checkState(rowType.getFieldCount() == arrowField.getChildren().size());
                List<ArrowFieldConverter> fieldConverters = new ArrayList<>();
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    fieldConverters.add(createConverter(rowType.getTypeAt(i), arrowField.getChildren().get(i)));
                }
                return new StructConverter(flinkType.isNullable(), fieldConverters);
            default:
                throw new UnsupportedOperationException("Unsupported type " + flinkType);
        }
    }
}