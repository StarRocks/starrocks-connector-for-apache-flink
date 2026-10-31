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
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
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

    /**
     * Convert the data at the given position of the arrow vector to Flink data.
     * @param vector the input arrow vector
     * @param rowIndex the position of the data
     * @return the flink data
     */
    Object convert(FieldVector vector, int rowIndex);

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
        public Object convert(FieldVector vector, int rowIndex) {
            BitVector bitVector = (BitVector) vector;
            Object value = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
            checkNullable(isNullable, value);
            return value;
        }
    }

    // Convert from arrow tinyint to flink tinyint
    class TinyIntConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public TinyIntConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(FieldVector vector, int rowIndex) {
            TinyIntVector tinyIntVector = (TinyIntVector) vector;
            Object value = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
            checkNullable(isNullable, value);
            return value;
        }
    }

    // Convert from arrow smallint to flink smallint
    class SmallIntConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public SmallIntConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(FieldVector vector, int rowIndex) {
            SmallIntVector smallIntVector = (SmallIntVector) vector;
            Object value = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
            checkNullable(isNullable, value);
            return value;
        }
    }

    // Convert from arrow int to flink int
    class IntConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public IntConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(FieldVector vector, int rowIndex) {
            IntVector intVector = (IntVector) vector;
            Object value = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
            checkNullable(isNullable, value);
            return value;
        }
    }

    // Convert from arrow bigint to flink bigint
    class BigIntConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public BigIntConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(FieldVector vector, int rowIndex) {
            BigIntVector bigIntVector = (BigIntVector) vector;
            Object value = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
            checkNullable(isNullable, value);
            return value;
        }
    }

    // Convert from arrow float to flink float
    class FloatConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public FloatConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(FieldVector vector, int rowIndex) {
            Float4Vector floatVector = (Float4Vector) vector;
            Object value = floatVector.isNull(rowIndex) ? null : floatVector.get(rowIndex);
            checkNullable(isNullable, value);
            return value;
        }
    }

    // Convert from arrow double to flink double
    class DoubleConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public DoubleConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(FieldVector vector, int rowIndex) {
            Float8Vector floatVector = (Float8Vector) vector;
            Object value = floatVector.isNull(rowIndex) ? null : floatVector.get(rowIndex);
            checkNullable(isNullable, value);
            return value;
        }
    }

    // Convert from arrow decimal type to flink decimal
    class DecimalConverter implements ArrowFieldConverter {

        private final int precision;
        private final int scale;
        private final boolean isNullable;

        public DecimalConverter(int precision, int scale, boolean isNullable) {
            this.precision = precision;
            this.scale = scale;
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(FieldVector vector, int rowIndex) {
            DecimalVector decimalVector = (DecimalVector) vector;
            BigDecimal value = decimalVector.isNull(rowIndex) ? null : decimalVector.getObject(rowIndex);
            checkNullable(isNullable, value);
            return value == null ? null : DecimalData.fromBigDecimal(value, precision, scale);
        }
    }

    // Convert from arrow varchar to flink char/varchar
    class StringConverter implements ArrowFieldConverter {

        private final boolean isNullable;

        public StringConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(FieldVector vector, int rowIndex) {
            VarCharVector varCharVector = (VarCharVector) vector;
            String value = varCharVector.isNull(rowIndex) ? null : new String(varCharVector.get(rowIndex));
            checkNullable(isNullable, value);
            return StringData.fromString(value);
        }
    }

    // Convert from arrow varchar to flink date
    class DateConverter implements ArrowFieldConverter {

        private static final String DATE_FORMAT = "yyyy-MM-dd";

        private final boolean isNullable;

        public DateConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(FieldVector vector, int rowIndex) {
            VarCharVector varCharVector = (VarCharVector) vector;
            String value = varCharVector.isNull(rowIndex) ? null : new String(varCharVector.get(rowIndex));
            checkNullable(isNullable, value);
            if (value == null) {
                return null;
            }
            LocalDate date = LocalDate.parse(value, DateTimeFormatter.ofPattern(DATE_FORMAT));
            return (int) date.atStartOfDay().toLocalDate().toEpochDay();
        }
    }

    // Convert from arrow varchar to flink timestamp-related type
    class TimestampConverter implements ArrowFieldConverter {

        private static final DateTimeFormatter DATETIME_FORMATTER =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]");

        private final boolean isNullable;

        public TimestampConverter(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public Object convert(FieldVector vector, int rowIndex) {
            VarCharVector varCharVector = (VarCharVector) vector;
            String value = varCharVector.isNull(rowIndex) ? null : new String(varCharVector.get(rowIndex));
            checkNullable(isNullable, value);
            if (value == null) {
                return null;
            }
            LocalDateTime ldt = LocalDateTime.parse(value, DATETIME_FORMATTER);
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
        public Object convert(FieldVector vector, int rowIndex) {
            ListVector listVector = (ListVector) vector;
            if (listVector.isNull(rowIndex)) {
                checkNullable(isNullable, null);
                return null;
            }

            int index = rowIndex * ListVector.OFFSET_WIDTH;
            int start = listVector.getOffsetBuffer().getInt(index);
            int end = listVector.getOffsetBuffer().getInt(index + ListVector.OFFSET_WIDTH);
            Object[] data = new Object[end - start];
            for (int i = 0; i < data.length; i++) {
                data[i] = elementConverter.convert(listVector.getDataVector(), start + i);
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
        public Object convert(FieldVector vector, int rowIndex) {
            StructVector structVector = (StructVector) vector;
            if (structVector.isNull(rowIndex)) {
                checkNullable(isNullable, null);
                return null;
            }

            GenericRowData rowData = new GenericRowData(structVector.size());
            for (int i = 0; i < structVector.size(); i++) {
                Object obj = childConverters.get(i).convert((FieldVector) structVector.getVectorById(i), rowIndex);
                rowData.setField(i, obj);
            }
            return rowData;
        }
    }

    // Convert from arrow map to flink map
    class MapConverter implements ArrowFieldConverter {

        private final boolean isNullable;
        private final ArrowFieldConverter keyConverter;
        private final ArrowFieldConverter valueConverter;

        public MapConverter(boolean isNullable, ArrowFieldConverter keyConverter, ArrowFieldConverter valueConverter) {
            this.isNullable = isNullable;
            this.keyConverter = keyConverter;
            this.valueConverter = valueConverter;
        }

        @Override
        public Object convert(FieldVector vector, int rowIndex) {
            MapVector mapVector = (MapVector) vector;
            if (mapVector.isNull(rowIndex)) {
                checkNullable(isNullable, null);
                return null;
            }

            Map<Object, Object> map = new HashMap<>();
            StructVector structVector = (StructVector) mapVector.getDataVector();
            int index = rowIndex * ListVector.OFFSET_WIDTH;
            int start = mapVector.getOffsetBuffer().getInt(index);
            int end = mapVector.getOffsetBuffer().getInt(index + ListVector.OFFSET_WIDTH);
            for (int i = start; i < end; i++) {
                Object key = keyConverter.convert((FieldVector) structVector.getVectorById(0), i);
                Object value = valueConverter.convert((FieldVector) structVector.getVectorById(1), i);
                map.put(key, value);
            }
            return new GenericMapData(map);
        }
    }

    Map<LogicalTypeRoot, Types.MinorType> FLINK_AND_ARROW_TYPE_MAPPING = new HashMap<LogicalTypeRoot, Types.MinorType>() {{
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
        put(LogicalTypeRoot.MAP, Types.MinorType.MAP);
    }};

    static void checkTypeCompatible(LogicalType flinkType, Field field) {
        Types.MinorType expectMinorType = FLINK_AND_ARROW_TYPE_MAPPING.get(flinkType.getTypeRoot());
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
                return new TinyIntConverter(flinkType.isNullable());
            case SMALLINT:
                return new SmallIntConverter(flinkType.isNullable());
            case INTEGER:
                return new IntConverter(flinkType.isNullable());
            case BIGINT:
                return new BigIntConverter(flinkType.isNullable());
            case FLOAT:
                return new FloatConverter(flinkType.isNullable());
            case DOUBLE:
                return new DoubleConverter(flinkType.isNullable());
            case DECIMAL:
                DecimalType type = (DecimalType) flinkType;
                return new DecimalConverter(type.getPrecision(), type.getScale(), flinkType.isNullable());
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
            case MAP:
                MapType mapType = (MapType) flinkType;
                Field structField = arrowField.getChildren().get(0);
                ArrowFieldConverter keyConverter = createConverter(
                        mapType.getKeyType(), structField.getChildren().get(0));
                ArrowFieldConverter valueConverter = createConverter(
                        mapType.getValueType(), structField.getChildren().get(1));
                return new MapConverter(mapType.isNullable(), keyConverter, valueConverter);
            default:
                throw new UnsupportedOperationException("Unsupported type " + flinkType);
        }
    }
}