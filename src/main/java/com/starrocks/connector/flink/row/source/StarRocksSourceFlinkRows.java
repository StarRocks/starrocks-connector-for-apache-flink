/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;
import com.starrocks.connector.flink.table.source.struct.Const;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.table.source.struct.StarRocksSchema;
import com.starrocks.connector.flink.thrift.TScanBatchResult;
import com.starrocks.connector.flink.tools.DataUtil;

import org.apache.arrow.memory.RootAllocator;

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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class StarRocksSourceFlinkRows {

    private static Logger LOG = LoggerFactory.getLogger(StarRocksSourceFlinkRows.class);

    public static final String DATETIME_FORMAT_LONG = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String DATETIME_FORMAT_SHORT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT = "yyyy-MM-dd";

    private int offsetOfBatchForRead;
    private int rowCountOfBatch;
    private int flinkRowsCount;

    private List<GenericRowData> sourceFlinkRows = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;
    private final List<ColunmRichInfo> colunmRichInfos;
    private final SelectColumn[] selectColumns;
    private final StarRocksSchema starRocksSchema;

    public List<GenericRowData> getFlinkRows() {
        return sourceFlinkRows;
    }

    public StarRocksSourceFlinkRows(TScanBatchResult nextResult, List<ColunmRichInfo> colunmRichInfos, 
                                    StarRocksSchema srSchema, SelectColumn[] selectColumns) {

        this.colunmRichInfos = colunmRichInfos;
        this.selectColumns = selectColumns;
        this.starRocksSchema = srSchema;
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        byte[] bytes = nextResult.getRows();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        this.arrowStreamReader = new ArrowStreamReader(byteArrayInputStream, rootAllocator);
        this.offsetOfBatchForRead = 0;
    }

    public StarRocksSourceFlinkRows genFlinkRowsFromArrow() throws IOException {

        this.root = arrowStreamReader.getVectorSchemaRoot();
        while (arrowStreamReader.loadNextBatch()) {
            fieldVectors = root.getFieldVectors();
            if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                continue;
            }
            rowCountOfBatch = root.getRowCount();
            for (int i = 0; i < rowCountOfBatch; i ++) {
                sourceFlinkRows.add(new GenericRowData(this.selectColumns.length));
            }
            this.genFlinkRows();
            flinkRowsCount += root.getRowCount();
        }
        return this;
    }

    public boolean hasNext() {
        
        if (offsetOfBatchForRead < flinkRowsCount) {
            return true;
        }
        this.close();
        return false;
    }


    public GenericRowData next() {

        if (!hasNext()) {
            LOG.error("offset larger than flinksRowsCount");
            throw new RuntimeException("read offset larger than flinksRowsCount");
        }
        return sourceFlinkRows.get(offsetOfBatchForRead ++);
    }

    public int getReadRowCount() {
        return flinkRowsCount;
    }

    private void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close StarRocksSourceFlinkRows:" + e.getMessage());
            throw new RuntimeException("Failed to close StarRocksSourceFlinkRows:" + e.getMessage());
        }
    }
    
    private void setValueToFlinkRows(int rowIndex, int colunm, Object obj) {
        
        if (rowIndex > rowCountOfBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " + rowCountOfBatch;
            LOG.error("Get row offset: {} larger than row size: {}", rowIndex, rowCountOfBatch);
            throw new NoSuchElementException(errMsg);
        }
        sourceFlinkRows.get(rowIndex).setField(colunm, obj);
    }

    private void genFlinkRows() {

        for (int colIndex = 0; colIndex < this.selectColumns.length; colIndex ++) {

            FieldVector columnVector = fieldVectors.get(colIndex);
            Types.MinorType beShowDataType = columnVector.getMinorType();
            String starrocksType = starRocksSchema.get(colIndex).getType();
            ColunmRichInfo richInfo = colunmRichInfos.get(this.selectColumns[colIndex].getColumnIndexInFlinkTable());
            boolean nullable = richInfo.getDataType().getLogicalType().isNullable();
            LogicalTypeRoot flinkTypeRoot = richInfo.getDataType().getLogicalType().getTypeRoot();
            // starrocksType -> flinkType
            starrocksType = DataUtil.ClearBracket(starrocksType);
            if (!Const.DataTypeRelationMap.containsKey(flinkTypeRoot)) {
                throw new RuntimeException(
                    "Flink type not support when convert data from starrocks to flink, " +
                    "type is -> [" + flinkTypeRoot.toString() + "]"
                );
            }
            if (!Const.DataTypeRelationMap.get(flinkTypeRoot).contains(starrocksType)) {
                throw new RuntimeException(
                    "StarRocks type can not convert to flink type, " +
                    "starrocks type is -> [" + starrocksType + "] " + 
                    "flink type is -> [" + flinkTypeRoot.toString() + "]"
                );
            }
            if (flinkTypeRoot == LogicalTypeRoot.DATE) {
                transToFlinkDate(starrocksType, beShowDataType, columnVector, colIndex, nullable);
                continue;
            }
            if (flinkTypeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE ||
                flinkTypeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE || 
                flinkTypeRoot == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE) {
                transToFlinkTimestamp(starrocksType, beShowDataType, columnVector, colIndex, nullable);
                continue;
            }
            if (flinkTypeRoot == LogicalTypeRoot.CHAR ||
                flinkTypeRoot == LogicalTypeRoot.VARCHAR) {
                transToFlinkChar(starrocksType, beShowDataType, columnVector, colIndex, nullable);
                continue;
            }
            if (flinkTypeRoot == LogicalTypeRoot.BOOLEAN) {
                transToFlinkBoolean(starrocksType, beShowDataType, columnVector, colIndex, nullable);
                continue;
            }
            if (flinkTypeRoot == LogicalTypeRoot.TINYINT) {
                transToFlinkTinyInt(starrocksType, beShowDataType, columnVector, colIndex, nullable);
                continue;
            }
            if (flinkTypeRoot == LogicalTypeRoot.SMALLINT) {
                transToFlinkSmallInt(starrocksType, beShowDataType, columnVector, colIndex, nullable);
                continue;
            }
            if (flinkTypeRoot == LogicalTypeRoot.INTEGER) {
                transToFlinkInt(starrocksType, beShowDataType, columnVector, colIndex, nullable);
                continue;
            }
            if (flinkTypeRoot == LogicalTypeRoot.BIGINT) {
                transToFlinkBigInt(starrocksType, beShowDataType, columnVector, colIndex, nullable);
                continue;
            }
            if (flinkTypeRoot == LogicalTypeRoot.FLOAT) {
                transToFlinkFloat(starrocksType, beShowDataType, columnVector, colIndex, nullable);
                continue;
            }
            if (flinkTypeRoot == LogicalTypeRoot.DOUBLE) {
                transToFlinkDouble(starrocksType, beShowDataType, columnVector, colIndex, nullable);
                continue;
            }
            if (flinkTypeRoot == LogicalTypeRoot.DECIMAL) {
                transToFlinkDecimal(starrocksType, beShowDataType, columnVector, colIndex, nullable);
                continue;
            }
        }
    }

    private void transToFlinkDate(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_DATE:
            srDateToFDate(beShowDataType, curFieldVector, colIndex, nullable);
            break; 
        }
    }

    private void transToFlinkTimestamp(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_DATETIME:
            srDataTimeToFTimestamp(beShowDataType, curFieldVector, colIndex, nullable);
            break; 
        }
    }

    private void transToFlinkChar(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_CHAR:
            srCharToFChar(beShowDataType, curFieldVector, colIndex, nullable);
            case Const.DATA_TYPE_STARROCKS_VARCHAR:
            srCharToFChar(beShowDataType, curFieldVector, colIndex, nullable);
            case Const.DATA_TYPE_STARROCKS_LARGEINT:
            srCharToFChar(beShowDataType, curFieldVector, colIndex, nullable);
            break; 
        }
    }

    private void transToFlinkBoolean(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_BOOLEAN:
            srBoolToFBool(beShowDataType, curFieldVector, colIndex, nullable);
            break;
        }
    }
    
    private void transToFlinkTinyInt(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_TINYINT:
            srTinyIntToFTinyInt(beShowDataType, curFieldVector, colIndex, nullable);
            break;
        }
    }

    private void transToFlinkSmallInt(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_SMALLINT:
            srSmallToFSmalInt(beShowDataType, curFieldVector, colIndex, nullable);
            break;
        }
    }

    private void transToFlinkInt(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_INT:
            srIntToFInt(beShowDataType, curFieldVector, colIndex, nullable);
            break;
        }
    }

    private void transToFlinkBigInt(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_BIGINT:
            srBigIntToFBigInt(beShowDataType, curFieldVector, colIndex, nullable);
            break;
        }
    }

    private void transToFlinkFloat(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_FLOAT:
            srFloatToFloat(beShowDataType, curFieldVector, colIndex, nullable);
            break;
        }
    }

    private void transToFlinkDouble(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_DOUBLE:
            srDoubleToFDouble(beShowDataType, curFieldVector, colIndex, nullable);
            break;
        }
    }

    private void transToFlinkDecimal(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex , boolean nullable) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_DECIMAL128:
            srDecimalToFDecimal(beShowDataType, curFieldVector, colIndex, nullable);
            break;
            case Const.DATA_TYPE_STARROCKS_DECIMALV2:
            srDecimalToFDecimal(beShowDataType, curFieldVector, colIndex, nullable);
            break;
            case Const.DATA_TYPE_STARROCKS_DECIMAL:
            srDecimalToFDecimal(beShowDataType, curFieldVector, colIndex, nullable);
            break;
        }
    }

    private void srDateToFDate(Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        // beShowDataType.String => Flink Date
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.VARCHAR), "");
        VarCharVector varCharVector = (VarCharVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex ++) {
            if (varCharVector.isNull(rowIndex)) {
                if (!nullable) {
                    throwNullableException(colIndex);
                }
                setValueToFlinkRows(rowIndex, colIndex, null);
                continue;
            }
            String value = new String(varCharVector.get(rowIndex));
            LocalDate date = LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            int timestamp = (int)date.atStartOfDay().toLocalDate().toEpochDay();
            setValueToFlinkRows(rowIndex, colIndex, timestamp);
        }
    }

    private void srDataTimeToFTimestamp(Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        // beShowDataType.String => Flink Timestamp
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.VARCHAR), "");
        VarCharVector varCharVector = (VarCharVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex ++) {
            if (varCharVector.isNull(rowIndex)) {
                if (!nullable) {
                    throwNullableException(colIndex);
                }
                setValueToFlinkRows(rowIndex, colIndex, null);
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
            setValueToFlinkRows(rowIndex, colIndex, TimestampData.fromLocalDateTime(ldt));
        }
    }

    private void srCharToFChar(Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        // beShowDataType.String => Flink CHAR/VARCHAR/STRING
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.VARCHAR), "");
        VarCharVector varCharVector = (VarCharVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex ++) {
            if (varCharVector.isNull(rowIndex)) {
                if (!nullable) {
                    throwNullableException(colIndex);
                }
                setValueToFlinkRows(rowIndex, colIndex, null);
                continue;
            }
            String value = new String(varCharVector.get(rowIndex));
            setValueToFlinkRows(rowIndex, colIndex, StringData.fromString(value));
        }
    }

    private void srBoolToFBool(Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        // beShowDataType.Bit => Flink boolean
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.BIT), "");
        BitVector bitVector = (BitVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
            if (fieldValue == null && !nullable) {
                throwNullableException(colIndex);
            }
            setValueToFlinkRows(rowIndex, colIndex, fieldValue);
        }
    }

    private void srTinyIntToFTinyInt(Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        // beShowDataType.TinyInt => Flink TinyInt
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.TINYINT), "");
        TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
            if (fieldValue == null && !nullable) {
                throwNullableException(colIndex);
            }
            setValueToFlinkRows(rowIndex, colIndex, fieldValue);
        }
    }

    private void srSmallToFSmalInt(Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        // beShowDataType.SmalInt => Flink SmalInt
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.SMALLINT), "");
        SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
            if (fieldValue == null && !nullable) {
                throwNullableException(colIndex);
            }
            setValueToFlinkRows(rowIndex, colIndex, fieldValue);
        }
    }

    private void srIntToFInt(Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        // beShowDataType.Int => Flink Int
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.INT), "");
        IntVector intVector = (IntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
            if (fieldValue == null && !nullable) {
                throwNullableException(colIndex);
            }
            setValueToFlinkRows(rowIndex, colIndex, fieldValue);
        }
    }

    private void srBigIntToFBigInt(Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        // beShowDataType.BigInt => Flink BigInt
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.BIGINT), "");
        BigIntVector bigIntVector = (BigIntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
            if (fieldValue == null && !nullable) {
                throwNullableException(colIndex);
            }
            setValueToFlinkRows(rowIndex, colIndex, fieldValue);
        }
    }

    private void srFloatToFloat(Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        // beShowDataType.Float => Flink Float
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.FLOAT4), "");
        Float4Vector float4Vector = (Float4Vector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
            if (fieldValue == null && !nullable) {
                throwNullableException(colIndex);
            }
            setValueToFlinkRows(rowIndex, colIndex, fieldValue);
        }
    }

    private void srDoubleToFDouble(Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        // beShowDataType.Double => Flink Double
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.FLOAT8), "");
        Float8Vector float8Vector = (Float8Vector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
            if (fieldValue == null && !nullable) {
                throwNullableException(colIndex);
            }
            setValueToFlinkRows(rowIndex, colIndex, fieldValue);
        }
    }

    private void srDecimalToFDecimal(Types.MinorType beShowDataType, FieldVector curFieldVector, int colIndex, boolean nullable) {
        // beShowDataType.Decimal => Flink Decimal
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.DECIMAL), "");
        DecimalVector decimalVector = (DecimalVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            if (decimalVector.isNull(rowIndex)) {
                if (!nullable) {
                    throwNullableException(colIndex);
                }
                setValueToFlinkRows(rowIndex, colIndex, null);
                continue;
            }
            BigDecimal value = decimalVector.getObject(rowIndex);
            setValueToFlinkRows(rowIndex, colIndex, DecimalData.fromBigDecimal(value, value.precision(), value.scale()));
        }
    }

    private void throwNullableException(int colIndex) {
        throw new RuntimeException("Data could not be null. please check create table SQL, column index is: " + colIndex);
    }
}