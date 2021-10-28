// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.flink.row;

import com.starrocks.connector.flink.exception.StarRocksException;
import com.starrocks.connector.flink.source.Const;
import com.starrocks.connector.flink.source.StarRocksSchema;
import com.starrocks.connector.flink.thrift.TScanBatchResult;
import com.starrocks.connector.flink.util.DataUtil;

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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * row batch data container.
 */
public class StarRocksSourceFlinkRows {
    private static Logger logger = LoggerFactory.getLogger(StarRocksSourceFlinkRows.class);

    public static class Row {
        private List<Object> cols;

        Row(int colCount) {
            this.cols = new ArrayList<>(colCount);
        }

        public List<Object> getCols() {
            return cols;
        }

        public void put(Object o) {
            cols.add(o);
        }
    }

    // offset for iterate the rowBatch
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private List<Row> rowBatch = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;
    private final DataType[] flinkDataTypes;
    private StarRocksSchema starRocksSchema;

    public List<Row> getRowBatch() {
        return rowBatch;
    }

    public StarRocksSourceFlinkRows(TScanBatchResult nextResult, DataType[] flinkDataTypes, StarRocksSchema srSchema) {
        this.flinkDataTypes = flinkDataTypes;
        this.starRocksSchema = srSchema;
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        byte[] bytes = nextResult.getRows();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        this.arrowStreamReader = new ArrowStreamReader(byteArrayInputStream, rootAllocator);
        this.offsetInRowBatch = 0;
    }

    public StarRocksSourceFlinkRows readArrow() throws StarRocksException {
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                fieldVectors = root.getFieldVectors();
                if (fieldVectors.size() != flinkDataTypes.length) {
                    logger.error("Schema size '{}' is not equal to arrow field size '{}'.",
                            fieldVectors.size(), flinkDataTypes.length);
                    throw new StarRocksException("Load StarRocks data failed, schema size of fetch data is wrong.");
                }
                if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                    logger.debug("One batch in arrow has no data.");
                    continue;
                }
                rowCountInOneBatch = root.getRowCount();
                // init the rowBatch
                for (int i = 0; i < rowCountInOneBatch; ++i) {
                    rowBatch.add(new Row(fieldVectors.size()));
                }
                transToFlinkDataType();
                readRowCount += root.getRowCount();
            }
            return this;
        } catch (Exception e) {
            logger.error("Read StarRocks Data failed because: ", e);
            throw new StarRocksException(e.getMessage());
        } finally {
            close();
        }
    }

    public boolean hasNext() {
        if (offsetInRowBatch < readRowCount) {
            return true;
        }
        return false;
    }

    private void addValueToRow(int rowIndex, Object obj) {
        if (rowIndex > rowCountInOneBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " +
                    rowCountInOneBatch;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        rowBatch.get(readRowCount + rowIndex).put(obj);
    }

    public List<Object> next() throws StarRocksException {
        if (!hasNext()) {
            String errMsg = "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        return rowBatch.get(offsetInRowBatch++).getCols();
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            // do nothing
        }
    }


    private void transToFlinkDataType() throws StarRocksException {

        try {
            for (int colIndex = 0; colIndex < fieldVectors.size(); colIndex ++) {

                FieldVector curFieldVector = fieldVectors.get(colIndex);
                Types.MinorType beShowDataType = curFieldVector.getMinorType();
                String starrocksType = starRocksSchema.get(colIndex).getType();
                String flinkType = flinkDataTypes[colIndex].toString();
                // starrocksType -> flinkType
                flinkType = DataUtil.ClearBracket(flinkType);
                starrocksType = DataUtil.ClearBracket(starrocksType);
                if (!Const.DataTypeRelationMap.containsKey(flinkType)) {
                    throw new StarRocksException("flinkType not found" + flinkType);
                }
                if (!Const.DataTypeRelationMap.get(flinkType).contains(starrocksType)) {
                    throw new StarRocksException("Does not support converting " + starrocksType + " to " + flinkType);
                }

                switch (flinkType) {
                    case Const.DATA_TYPE_FLINK_DATE:
                    transToFlinkDate(starrocksType, beShowDataType, curFieldVector);
                    break;
                    case Const.DATA_TYPE_FLINK_TIMESTAMP:
                    transToFlinkTimestamp(starrocksType, beShowDataType, curFieldVector);
                    break;
                    case Const.DATA_TYPE_FLINK_CHAR:
                    transToFlinkChar(starrocksType, beShowDataType, curFieldVector);
                    break;
                    case Const.DATA_TYPE_FLINK_STRING:
                    transToFlinkString(starrocksType, beShowDataType, curFieldVector);
                    break;
                    case Const.DATA_TYPE_FLINK_BOOLEAN:
                    transToFlinkBoolean(starrocksType, beShowDataType, curFieldVector);
                    break;
                    case Const.DATA_TYPE_FLINK_TINYINT:
                    transToFlinkTinyInt(starrocksType, beShowDataType, curFieldVector);
                    break;
                    case Const.DATA_TYPE_FLINK_SMALLINT:
                    transToFlinkSmallInt(starrocksType, beShowDataType, curFieldVector);
                    break;
                    case Const.DATA_TYPE_FLINK_INT:
                    transToFlinkInt(starrocksType, beShowDataType, curFieldVector);
                    break;
                    case Const.DATA_TYPE_FLINK_BIGINT:
                    transToFlinkBigInt(starrocksType, beShowDataType, curFieldVector);
                    break;
                    case Const.DATA_TYPE_FLINK_FLOAT:
                    transToFlinkFloat(starrocksType, beShowDataType, curFieldVector);
                    break;
                    case Const.DATA_TYPE_FLINK_DOUBLE:
                    transToFlinkDouble(starrocksType, beShowDataType, curFieldVector);
                    break;
                    case Const.DATA_TYPE_FLINK_DECIMAL:
                    transToFlinkDecimal(starrocksType, beShowDataType, curFieldVector);
                    break;
                }
            }
        } catch (StarRocksException e) {
            close();
            throw e;
        }
    }

    private void transToFlinkDate(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_DATE:
            srDateToFDate(beShowDataType, curFieldVector);
            break; 
        }
    }

    private void transToFlinkTimestamp(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_DATETIME:
            srDataTimeToFTimestamp(beShowDataType, curFieldVector);
            break; 
        }
    }

    private void transToFlinkChar(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_CHAR:
            srCharToFChar(beShowDataType, curFieldVector);
            break; 
        }
    }

    private void transToFlinkString(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_CHAR:
            srCharToFChar(beShowDataType, curFieldVector);
            break;
            case Const.DATA_TYPE_STARROCKS_VARCHAR:
            srCharToFChar(beShowDataType, curFieldVector);
            break;
            case Const.DATA_TYPE_STARROCKS_LARGEINT:
            srCharToFChar(beShowDataType, curFieldVector);
            break;
        }
    }

    private void transToFlinkBoolean(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_BOOLEAN:
            srBoolToFBool(beShowDataType, curFieldVector);
            break;
        }
    }
    
    private void transToFlinkTinyInt(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_TINYINT:
            srTinyIntToFTinyInt(beShowDataType, curFieldVector);
            break;
        }
    }

    private void transToFlinkSmallInt(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_SMALLINT:
            srSmallToFSmalInt(beShowDataType, curFieldVector);
            break;
        }
    }

    private void transToFlinkInt(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_INT:
            srIntToFInt(beShowDataType, curFieldVector);
            break;
        }
    }

    private void transToFlinkBigInt(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_BIGINT:
            srBigIntToFBigInt(beShowDataType, curFieldVector);
            break;
        }
    }

    private void transToFlinkFloat(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_FLOAT:
            srFloatToFloat(beShowDataType, curFieldVector);
            break;
        }
    }

    private void transToFlinkDouble(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_DOUBLE:
            srDoubleToFDouble(beShowDataType, curFieldVector);
            break;
        }
    }

    private void transToFlinkDecimal(String starrocksType, Types.MinorType beShowDataType, FieldVector curFieldVector) {
        
        switch (starrocksType) {
            case Const.DATA_TYPE_STARROCKS_DECIMAL128:
            srDecimalToFDecimal(beShowDataType, curFieldVector);
            break;
            case Const.DATA_TYPE_STARROCKS_DECIMALV2:
            srDecimalToFDecimal(beShowDataType, curFieldVector);
            break;
            case Const.DATA_TYPE_STARROCKS_DECIMAL:
            srDecimalToFDecimal(beShowDataType, curFieldVector);
            break;
        }
    }

    private void srDateToFDate(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.String => Flink Date
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.VARCHAR), "");
        VarCharVector varCharVector = (VarCharVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex ++) {
            if (varCharVector.isNull(rowIndex)) {
                addValueToRow(rowIndex, null);
                continue;
            }
            String value = new String(varCharVector.get(rowIndex));
            LocalDate date = LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            int timestamp = (int)date.atStartOfDay(ZoneOffset.ofHours(8)).toLocalDate().toEpochDay();
            addValueToRow(rowIndex, timestamp);
        }
    }

    private void srDataTimeToFTimestamp(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.String => Flink Timestamp
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.VARCHAR), "");
        VarCharVector varCharVector = (VarCharVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex ++) {
            if (varCharVector.isNull(rowIndex)) {
                addValueToRow(rowIndex, null);
                continue;
            }
            String value = new String(varCharVector.get(rowIndex));
            DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            LocalDateTime ldt = LocalDateTime.parse(value, df);
            addValueToRow(rowIndex, TimestampData.fromLocalDateTime(ldt));
        }
    }

    private void srCharToFChar(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.String => Flink CHAR/VARCHAR/STRING
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.VARCHAR), "");
        VarCharVector varCharVector = (VarCharVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex ++) {
            if (varCharVector.isNull(rowIndex)) {
                addValueToRow(rowIndex, null);
                continue;
            }
            String value = new String(varCharVector.get(rowIndex));
            addValueToRow(rowIndex, StringData.fromString(value));
        }
    }

    private void srBoolToFBool(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.Bit => Flink boolean
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.BIT), "");
        BitVector bitVector = (BitVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
            Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
            addValueToRow(rowIndex, fieldValue);
        }
    }

    private void srTinyIntToFTinyInt(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.TinyInt => Flink TinyInt
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.TINYINT), "");
        TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
            Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
            addValueToRow(rowIndex, fieldValue);
        }
    }

    private void srSmallToFSmalInt(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.SmalInt => Flink SmalInt
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.SMALLINT), "");
        SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
            Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
            addValueToRow(rowIndex, fieldValue);
        }
    }

    private void srIntToFInt(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.Int => Flink Int
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.INT), "");
        IntVector intVector = (IntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
            Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
            addValueToRow(rowIndex, fieldValue);
        }
    }

    private void srBigIntToFBigInt(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.BigInt => Flink BigInt
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.BIGINT), "");
        BigIntVector bigIntVector = (BigIntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
            Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
            addValueToRow(rowIndex, fieldValue);
        }
    }

    private void srFloatToFloat(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.Float => Flink Float
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.FLOAT4), "");
        Float4Vector float4Vector = (Float4Vector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
            Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
            addValueToRow(rowIndex, fieldValue);
        }
    }

    private void srDoubleToFDouble(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.Double => Flink Double
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.FLOAT8), "");
        Float8Vector float8Vector = (Float8Vector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
            Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
            addValueToRow(rowIndex, fieldValue);
        }
    }

    private void srDecimalToFDecimal(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.Decimal => Flink Decimal
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.DECIMAL), "");
        DecimalVector decimalVector = (DecimalVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
            if (decimalVector.isNull(rowIndex)) {
                addValueToRow(rowIndex, null);
                continue;
            }
            BigDecimal value = decimalVector.getObject(rowIndex);
            addValueToRow(rowIndex, DecimalData.fromBigDecimal(value, value.precision(), value.scale()));
        }
    }
}