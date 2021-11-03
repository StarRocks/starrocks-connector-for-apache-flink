package com.starrocks.connector.flink.row;

import com.starrocks.connector.flink.exception.StarRocksException;
import com.starrocks.connector.flink.source.ColunmRichInfo;
import com.starrocks.connector.flink.source.Const;
import com.starrocks.connector.flink.source.SelectColumn;
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




public class StarRocksSourceFlinkRows {

    private static Logger LOG = LoggerFactory.getLogger(StarRocksSourceFlinkRows.class);

    private int offsetOfBatchForRead = 0;
    private int rowCountOfBatch = 0;
    
    private int flinksRowsCount = 0;
    private List<StarRocksSourceFlinkRow> sourceFlinkRows = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;
    private List<ColunmRichInfo> colunmRichInfos;
    private final SelectColumn[] selectColumns;
    private StarRocksSchema starRocksSchema;

    public List<StarRocksSourceFlinkRow> getFlinkRows() {
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

    public StarRocksSourceFlinkRows genFlinkRowsFromArrow() throws StarRocksException, IOException {

        this.root = arrowStreamReader.getVectorSchemaRoot();
        while (arrowStreamReader.loadNextBatch()) {

            fieldVectors = root.getFieldVectors();
            if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                continue;
            }
            rowCountOfBatch = root.getRowCount();
            int colunmSize = 0;
            for (int i = 0; i < this.selectColumns.length; i ++) {
                if (selectColumns[i].getNeedBack()) {
                    colunmSize = colunmSize + 1;
                }
            }
            for (int i = 0; i < rowCountOfBatch; ++i) {
                sourceFlinkRows.add(new StarRocksSourceFlinkRow(colunmSize));
            }
            this.genFlinkRows();
            flinksRowsCount += root.getRowCount();
        }
        return this;
    }

    public boolean hasNext() {
        return offsetOfBatchForRead < flinksRowsCount;
    }


    public List<Object> next() throws StarRocksException {

        if (!hasNext()) {
            LOG.error("read offset larger than flinksRowsCount");
            throw new NoSuchElementException("read offset larger than flinksRowsCount");
        }
        return sourceFlinkRows.get(offsetOfBatchForRead ++).getColumns();
    }

    public int getReadRowCount() {
        return flinksRowsCount;
    }

    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }
    
    private void setValueToFlinkRows(int rowIndex, Object obj) {
        
        if (rowIndex > rowCountOfBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " + rowCountOfBatch;
            LOG.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        sourceFlinkRows.get(rowIndex).put(obj);
    }

    private void genFlinkRows() throws StarRocksException {

        for (int colIndex = 0; colIndex < this.selectColumns.length; colIndex ++) {

            if (!this.selectColumns[colIndex].getNeedBack()) {
                continue;
            }

            FieldVector columnVector = fieldVectors.get(colIndex);
            Types.MinorType beShowDataType = columnVector.getMinorType();
            String starrocksType = starRocksSchema.get(colIndex).getType();
            String flinkType = colunmRichInfos.get(this.selectColumns[colIndex].getColumnIndexInFlinkTable()).getDataType().toString();
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
                transToFlinkDate(starrocksType, beShowDataType, columnVector);
                break;
                case Const.DATA_TYPE_FLINK_TIMESTAMP:
                transToFlinkTimestamp(starrocksType, beShowDataType, columnVector);
                break;
                case Const.DATA_TYPE_FLINK_CHAR:
                transToFlinkChar(starrocksType, beShowDataType, columnVector);
                break;
                case Const.DATA_TYPE_FLINK_STRING:
                transToFlinkString(starrocksType, beShowDataType, columnVector);
                break;
                case Const.DATA_TYPE_FLINK_BOOLEAN:
                transToFlinkBoolean(starrocksType, beShowDataType, columnVector);
                break;
                case Const.DATA_TYPE_FLINK_TINYINT:
                transToFlinkTinyInt(starrocksType, beShowDataType, columnVector);
                break;
                case Const.DATA_TYPE_FLINK_SMALLINT:
                transToFlinkSmallInt(starrocksType, beShowDataType, columnVector);
                break;
                case Const.DATA_TYPE_FLINK_INT:
                transToFlinkInt(starrocksType, beShowDataType, columnVector);
                break;
                case Const.DATA_TYPE_FLINK_BIGINT:
                transToFlinkBigInt(starrocksType, beShowDataType, columnVector);
                break;
                case Const.DATA_TYPE_FLINK_FLOAT:
                transToFlinkFloat(starrocksType, beShowDataType, columnVector);
                break;
                case Const.DATA_TYPE_FLINK_DOUBLE:
                transToFlinkDouble(starrocksType, beShowDataType, columnVector);
                break;
                case Const.DATA_TYPE_FLINK_DECIMAL:
                transToFlinkDecimal(starrocksType, beShowDataType, columnVector);
                break;
            }
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
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex ++) {
            if (varCharVector.isNull(rowIndex)) {
                setValueToFlinkRows(rowIndex, null);
                continue;
            }
            String value = new String(varCharVector.get(rowIndex));
            LocalDate date = LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            int timestamp = (int)date.atStartOfDay(ZoneOffset.ofHours(8)).toLocalDate().toEpochDay();
            setValueToFlinkRows(rowIndex, timestamp);
        }
    }

    private void srDataTimeToFTimestamp(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.String => Flink Timestamp
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.VARCHAR), "");
        VarCharVector varCharVector = (VarCharVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex ++) {
            if (varCharVector.isNull(rowIndex)) {
                setValueToFlinkRows(rowIndex, null);
                continue;
            }
            String formatStringLong = "yyyy-MM-dd HH:mm:ss.SSSSSS";
            String formatStringShort = "yyyy-MM-dd HH:mm:ss";
            String value = new String(varCharVector.get(rowIndex));
            if (value.length() < formatStringShort.length()) {
                throw new RuntimeException("");
            }
            if (value.length() == formatStringShort.length()) {
                value = DataUtil.addZeroForNum(value + ".", formatStringLong.length());
            } 
            value = DataUtil.addZeroForNum(value, formatStringLong.length());
            DateTimeFormatter df = DateTimeFormatter.ofPattern(formatStringLong);
            LocalDateTime ldt = LocalDateTime.parse(value, df);
            setValueToFlinkRows(rowIndex, TimestampData.fromLocalDateTime(ldt));
        }
    }

    private void srCharToFChar(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.String => Flink CHAR/VARCHAR/STRING
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.VARCHAR), "");
        VarCharVector varCharVector = (VarCharVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex ++) {
            if (varCharVector.isNull(rowIndex)) {
                setValueToFlinkRows(rowIndex, null);
                continue;
            }
            String value = new String(varCharVector.get(rowIndex));
            setValueToFlinkRows(rowIndex, StringData.fromString(value));
        }
    }

    private void srBoolToFBool(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.Bit => Flink boolean
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.BIT), "");
        BitVector bitVector = (BitVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
            setValueToFlinkRows(rowIndex, fieldValue);
        }
    }

    private void srTinyIntToFTinyInt(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.TinyInt => Flink TinyInt
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.TINYINT), "");
        TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
            setValueToFlinkRows(rowIndex, fieldValue);
        }
    }

    private void srSmallToFSmalInt(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.SmalInt => Flink SmalInt
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.SMALLINT), "");
        SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
            setValueToFlinkRows(rowIndex, fieldValue);
        }
    }

    private void srIntToFInt(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.Int => Flink Int
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.INT), "");
        IntVector intVector = (IntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
            setValueToFlinkRows(rowIndex, fieldValue);
        }
    }

    private void srBigIntToFBigInt(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.BigInt => Flink BigInt
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.BIGINT), "");
        BigIntVector bigIntVector = (BigIntVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
            setValueToFlinkRows(rowIndex, fieldValue);
        }
    }

    private void srFloatToFloat(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.Float => Flink Float
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.FLOAT4), "");
        Float4Vector float4Vector = (Float4Vector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
            setValueToFlinkRows(rowIndex, fieldValue);
        }
    }

    private void srDoubleToFDouble(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.Double => Flink Double
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.FLOAT8), "");
        Float8Vector float8Vector = (Float8Vector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            Object fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
            setValueToFlinkRows(rowIndex, fieldValue);
        }
    }

    private void srDecimalToFDecimal(Types.MinorType beShowDataType, FieldVector curFieldVector) {
        // beShowDataType.Decimal => Flink Decimal
        Preconditions.checkArgument(beShowDataType.equals(Types.MinorType.DECIMAL), "");
        DecimalVector decimalVector = (DecimalVector) curFieldVector;
        for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
            if (decimalVector.isNull(rowIndex)) {
                setValueToFlinkRows(rowIndex, null);
                continue;
            }
            BigDecimal value = decimalVector.getObject(rowIndex);
            setValueToFlinkRows(rowIndex, DecimalData.fromBigDecimal(value, value.precision(), value.scale()));
        }
    }
}