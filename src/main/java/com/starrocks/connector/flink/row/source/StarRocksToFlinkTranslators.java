package com.starrocks.connector.flink.row.source;


import com.starrocks.connector.flink.tools.DataUtil;


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

import org.apache.arrow.vector.types.Types;

import org.apache.flink.table.data.DecimalData;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.apache.flink.util.Preconditions;




import java.math.BigDecimal;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;



public class StarRocksToFlinkTranslators {
    

    public static final String DATETIME_FORMAT_LONG = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String DATETIME_FORMAT_SHORT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT = "yyyy-MM-dd";

    public class ToFlinkDate implements StarRocksToFlinkTrans {

        @Override
        public Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable) {
            // beShowDataType.String => Flink Date
            Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.VARCHAR), "");
            Object[] result = new Object[rowCount];
            VarCharVector varCharVector = (VarCharVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex ++) {
                if (varCharVector.isNull(rowIndex)) {
                    if (!nullable) {
                        throwNullableException(colIndex);
                    }
                    result[rowIndex] = null;
                    continue;
                }
                String value = new String(varCharVector.get(rowIndex));
                LocalDate date = LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                int timestamp = (int)date.atStartOfDay().toLocalDate().toEpochDay();
                result[rowIndex] = timestamp;
            }
            return result;
        }

    }

    public class ToFlinkTimestamp implements StarRocksToFlinkTrans {

        @Override
        public Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable) {
            // beShowDataType.String => Flink Timestamp
            Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.VARCHAR), "");
            Object[] result = new Object[rowCount];
            VarCharVector varCharVector = (VarCharVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex ++) {
                if (varCharVector.isNull(rowIndex)) {
                    if (!nullable) {
                        throwNullableException(colIndex);
                    }
                    result[rowIndex] = null;
                    continue;
                }
                String value = new String(varCharVector.get(rowIndex));
                if (value.length() < DATETIME_FORMAT_SHORT.length()) {
                    throw new RuntimeException("");
                }
                if (value.length() == DATETIME_FORMAT_SHORT.length()) {
                    StringBuilder sb = new StringBuilder(value).append(".");
                    value = DataUtil.addZeroForNum(sb.toString(), DATETIME_FORMAT_LONG.length());
                } 
                value = DataUtil.addZeroForNum(value, DATETIME_FORMAT_LONG.length());
                DateTimeFormatter df = DateTimeFormatter.ofPattern(DATETIME_FORMAT_LONG);
                LocalDateTime ldt = LocalDateTime.parse(value, df);
                result[rowIndex] = TimestampData.fromLocalDateTime(ldt);
            }
            return result;
        }

    }

    public class ToFlinkChar implements StarRocksToFlinkTrans {

        @Override
        public Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable) {
            // beShowDataType.String => Flink CHAR/VARCHAR/STRING
            Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.VARCHAR), "");
            Object[] result = new Object[rowCount];
            VarCharVector varCharVector = (VarCharVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex ++) {
                if (varCharVector.isNull(rowIndex)) {
                    if (!nullable) {
                        throwNullableException(colIndex);
                    }
                    result[rowIndex] = null;
                    continue;
                }
                String value = new String(varCharVector.get(rowIndex));
                result[rowIndex] = StringData.fromString(value);
            }
            return result;
        }

    }

    public class ToFlinkBoolean implements StarRocksToFlinkTrans {

        @Override
        public Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable) {
            // beShowDataType.Bit => Flink boolean
            Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.BIT), "");
            Object[] result = new Object[rowCount];
            BitVector bitVector = (BitVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                if (fieldValue == null && !nullable) {
                    throwNullableException(colIndex);
                }
                result[rowIndex] = fieldValue;
            }
            return result;
        }

    }

    public class ToFlinkTinyInt implements StarRocksToFlinkTrans {

        @Override
        public Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable) {
            // beShowDataType.TinyInt => Flink TinyInt
            Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.TINYINT), "");
            Object[] result = new Object[rowCount];
            TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                if (fieldValue == null && !nullable) {
                    throwNullableException(colIndex);
                }
                result[rowIndex] = fieldValue;
            }
            return result;
        }

    }

    public class ToFlinkSmallInt implements StarRocksToFlinkTrans {

        @Override
        public Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable) {
            // beShowDataType.SmalInt => Flink SmalInt
            Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.SMALLINT), "");
            Object[] result = new Object[rowCount];
            SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                if (fieldValue == null && !nullable) {
                    throwNullableException(colIndex);
                }
                result[rowIndex] = fieldValue;
            }
            return result;
        }

    }

    public class ToFlinkInt implements StarRocksToFlinkTrans {

        @Override
        public Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable) {
            // beShowDataType.Int => Flink Int
            Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.INT), "");
            Object[] result = new Object[rowCount];
            IntVector intVector = (IntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                if (fieldValue == null && !nullable) {
                    throwNullableException(colIndex);
                }
                result[rowIndex] = fieldValue;
            }
            return result;
        }

    }

    public class ToFlinkBigInt implements StarRocksToFlinkTrans {

        @Override
        public Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable) {
            // beShowDataType.BigInt => Flink BigInt
            Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.BIGINT), "");
            Object[] result = new Object[rowCount];
            BigIntVector bigIntVector = (BigIntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                if (fieldValue == null && !nullable) {
                    throwNullableException(colIndex);
                }
                result[rowIndex] = fieldValue;
            }
            return result;
        }

    }

    public class ToFlinkFloat implements StarRocksToFlinkTrans {

        @Override
        public Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable) {
            // beShowDataType.Float => Flink Float
            Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.FLOAT4), "");
            Object[] result = new Object[rowCount];
            Float4Vector float4Vector = (Float4Vector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                if (fieldValue == null && !nullable) {
                    throwNullableException(colIndex);
                }
                result[rowIndex] = fieldValue;
            }
            return result;
        }

    }

    public class ToFlinkDouble implements StarRocksToFlinkTrans {

        @Override
        public Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable) {
            // beShowDataType.Double => Flink Double
            Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.FLOAT8), "");
            Object[] result = new Object[rowCount];
            Float8Vector float8Vector = (Float8Vector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                if (fieldValue == null && !nullable) {
                    throwNullableException(colIndex);
                }
                result[rowIndex] = fieldValue;
            }
            return result;
        }

    }

    public class ToFlinkDecimal implements StarRocksToFlinkTrans {

        @Override
        public Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable) {
            // beShowDataType.Decimal => Flink Decimal
            Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.DECIMAL), "");
            Object[] result = new Object[rowCount];
            DecimalVector decimalVector = (DecimalVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                if (decimalVector.isNull(rowIndex)) {
                    if (!nullable) {
                        throwNullableException(colIndex);
                    }
                    result[rowIndex] = null;
                    continue;
                }
                BigDecimal value = decimalVector.getObject(rowIndex);
                result[rowIndex] = DecimalData.fromBigDecimal(value, value.precision(), value.scale());
            }
            return result;
        }
    }

    private void throwNullableException(int colIndex) {
        throw new RuntimeException("Data could not be null. please check create table SQL, column index is: " + colIndex);
    }
}
