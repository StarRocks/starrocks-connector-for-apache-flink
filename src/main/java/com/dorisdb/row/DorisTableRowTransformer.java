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

package com.dorisdb.row;

import java.io.Serializable;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

public class DorisTableRowTransformer implements DorisIRowTransformer<RowData>, Serializable {

    private static final long serialVersionUID = 1L;

    private TypeInformation<RowData> rowDataTypeInfo;
    private Function<RowData, RowData> valueTransform;
    private DataType[] dataTypes;
    private String[] fieldNames;
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public DorisTableRowTransformer(TypeInformation<RowData> rowDataTypeInfo) {
        this.rowDataTypeInfo = rowDataTypeInfo;
    }

    @Override
    public void setTableSchema(TableSchema ts) {
        dataTypes = ts.getFieldDataTypes();
        fieldNames = ts.getFieldNames();
    }

    @Override
    public void setRuntimeContext(RuntimeContext runtimeCtx) {
        final TypeSerializer<RowData> typeSerializer = rowDataTypeInfo.createSerializer(runtimeCtx.getExecutionConfig());
        valueTransform = runtimeCtx.getExecutionConfig().isObjectReuseEnabled() ? typeSerializer::copy : Function.identity();
    }

    @Override
    public String transform(RowData record) {
        return toJsonString(valueTransform.apply(record));
    }

    private String toJsonString(RowData record) {
        Map<String, Object> rowMap = new HashMap<>();
        int idx = 0;
        for (String fieldName : fieldNames) {
            DataType dataType = dataTypes[idx];
            Object value = typeConvertion(dataType.getLogicalType(), record, idx);
            rowMap.put(fieldName, value);
            idx++;
        }
        return JSON.toJSONString(rowMap);
    }

    private Object typeConvertion(LogicalType type, RowData record, int pos) {
        switch (type.getTypeRoot()) {
            case BOOLEAN: 
                return record.getBoolean(pos);
            case TINYINT:
                return record.getByte(pos);
            case SMALLINT:
                return record.getShort(pos);
            case INTEGER:
                return record.getInt(pos);
            case BIGINT:
                return record.getLong(pos);
            case FLOAT:
                return record.getFloat(pos);
            case DOUBLE:
                return record.getDouble(pos);
            case CHAR:
            case VARCHAR:
                return record.getString(pos).toString();
            case DATE:
                return dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay(record.getInt(pos))));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision =((TimestampType) type).getPrecision();
                return dateTimeFormatter.format(new Date(record.getTimestamp(pos, timestampPrecision).toTimestamp().getTime()));
            case DECIMAL: // for both largeint and decimal
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return record.getDecimal(pos, decimalPrecision, decimalScale).toBigDecimal();
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
