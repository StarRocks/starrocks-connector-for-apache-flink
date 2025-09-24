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

package com.starrocks.connector.flink.row.sink;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.tools.JsonWrapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StarRocksTableRowTransformer implements StarRocksIRowTransformer<RowData> {

    private static final long serialVersionUID = 1L;

    private TypeInformation<RowData> rowDataTypeInfo;
    private Function<RowData, RowData> valueTransform;
    private String[] columnNames;
    private DataType[] columnDataTypes;
    private Map<String, StarRocksDataType> columns;
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    private transient JsonWrapper jsonWrapper;

    public StarRocksTableRowTransformer(TypeInformation<RowData> rowDataTypeInfo) {
        this.rowDataTypeInfo = rowDataTypeInfo;
    }

    @Override
    public void setStarRocksColumns(Map<String, StarRocksDataType> columns) {
        this.columns = columns;
    }

    @Override
    public void setTableSchema(TableSchema ts) {
        this.columnNames = ts.getFieldNames();
        this.columnDataTypes = ts.getFieldDataTypes();
    }

    @Override
    public void setRuntimeContext(RuntimeContext runtimeCtx) {
        // No need to copy the value even if object reuse is enabled,
        // because the raw RowData value will not be buffered
        this.valueTransform = Function.identity();
    }

    @Override
    public void setFastJsonWrapper(JsonWrapper jsonWrapper) {
        this.jsonWrapper = jsonWrapper;
    }

    @Override
    public Object[] transform(RowData record, boolean supportUpsertDelete) {
        RowData transformRecord = valueTransform.apply(record);
        Object[] values = new Object[columnDataTypes.length + (supportUpsertDelete ? 1 : 0)];
        int idx = 0;
        for (DataType dataType : columnDataTypes) {
            values[idx] = typeConversion(dataType.getLogicalType(), transformRecord, idx);
            idx++;
        }
        if (supportUpsertDelete) {
            // set `__op` column
            values[idx] = StarRocksSinkOP.parse(record.getRowKind()).ordinal();
        }
        return values;
    }

    private Object typeConversion(LogicalType type, RowData record, int pos) {
        if (record.isNullAt(pos)) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN: 
                return record.getBoolean(pos) ? 1L : 0L;
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
                String sValue = record.getString(pos).toString();
                if (columns == null) {
                    return sValue;
                }
                StarRocksDataType starRocksDataType =
                        columns.getOrDefault(columnNames[pos], StarRocksDataType.UNKNOWN);
                if ((starRocksDataType == StarRocksDataType.JSON ||
                        starRocksDataType == StarRocksDataType.UNKNOWN)
                    && !sValue.isEmpty() && (sValue.charAt(0) == '{' || sValue.charAt(0) == '[')) {
                    // The json string need to be converted to a json object, and to the json string
                    // again via JSON.toJSONString in StarRocksJsonSerializer#serialize. Otherwise,
                    // the final json string in stream load will not be correct. For example, the received
                    // string is "{"a": 1, "b": 2}", and if input it to JSON.toJSONString directly, the
                    // result will be "{\"a\": 1, \"b\": 2}" which will not be recognized as a json in
                    // StarRocks
                    return jsonWrapper.parse(sValue);
                }
                return sValue;
            case DATE:
                return dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay(record.getInt(pos))));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision =((TimestampType) type).getPrecision();
                return record.getTimestamp(pos, timestampPrecision).toLocalDateTime().toString();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int localZonedTimestampPrecision =((LocalZonedTimestampType) type).getPrecision();
                return record.getTimestamp(pos, localZonedTimestampPrecision).toLocalDateTime().toString();
            case DECIMAL: // for both largeint and decimal
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return record.getDecimal(pos, decimalPrecision, decimalScale).toBigDecimal();
            case BINARY:
                final byte[] bts = record.getBinary(pos);
                long value = 0;
                for (int i = 0; i < bts.length; i++) {
                    value += (bts[bts.length - i - 1] & 0xffL) << (8 * i);
                }
                return value;
            case ARRAY:
                return convertNestedArray(record.getArray(pos), type);
            case MAP:
                return convertNestedMap(record.getMap(pos), type);
            case ROW:
                RowType rType = (RowType)type;
                Map<String, Object> m = new HashMap<>();
                RowData row = record.getRow(pos, rType.getFieldCount());
                rType.getFields().parallelStream().forEach(f -> m.put(f.getName(), typeConversion(f.getType(), row, rType.getFieldIndex(f.getName()))));
                if (columns == null) {
                    return m;
                }
                StarRocksDataType rStarRocksDataType =
                        columns.getOrDefault(columnNames[pos], StarRocksDataType.UNKNOWN);
                if (rStarRocksDataType == StarRocksDataType.STRING) {
                    return jsonWrapper.toJSONString(m);
                }
                return m;
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private List<Object> convertNestedArray(ArrayData arrData, LogicalType type) {
        if (arrData instanceof GenericArrayData) {
            return Lists.newArrayList(((GenericArrayData)arrData).toObjectArray());
        }
        if (arrData instanceof BinaryArrayData) {
            LogicalType lt = ((ArrayType)type).getElementType();
            List<Object> data = Lists.newArrayList(((BinaryArrayData)arrData).toObjectArray(lt));
            if (LogicalTypeRoot.ROW.equals(lt.getTypeRoot())) {
                RowType rType = (RowType)lt;
                // parse nested row data
                return data.parallelStream().map(row -> {
                    Map<String, Object> m = Maps.newHashMap();
                    rType.getFields().parallelStream().forEach(f -> m.put(f.getName(), typeConversion(f.getType(), (RowData)row, rType.getFieldIndex(f.getName()))));
                    return jsonWrapper.toJSONString(m);
                }).collect(Collectors.toList());
            }
            if (LogicalTypeRoot.MAP.equals(lt.getTypeRoot())) {
                // traversal of the nested map
                return data.parallelStream().map(m -> convertNestedMap((MapData)m, lt)).collect(Collectors.toList());
            }
            if (LogicalTypeRoot.DATE.equals(lt.getTypeRoot())) {
                return data.parallelStream().map(date -> dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay((Integer)date)))).collect(Collectors.toList());
            }
            if (LogicalTypeRoot.ARRAY.equals(lt.getTypeRoot())) {
                // traversal of the nested array
                return data.parallelStream().map(arr -> convertNestedArray((ArrayData)arr, lt)).collect(Collectors.toList());
            }
            return data;
        }
        throw new UnsupportedOperationException(String.format("Unsupported array data: %s", arrData.getClass()));
    }

    private Map<Object, Object> convertNestedMap(MapData mapData, LogicalType type) {
        if (mapData instanceof GenericMapData) {
            HashMap<Object, Object> m = Maps.newHashMap();
            for (Object k : ((GenericArrayData)((GenericMapData)mapData).keyArray()).toObjectArray()) {
                m.put(k, ((GenericMapData)mapData).get(k));
            }
            return m;
        }
        if (mapData instanceof BinaryMapData) {
            Map<Object, Object> result = Maps.newHashMap();
            LogicalType valType = ((MapType)type).getValueType();
            Map<?, ?> javaMap = ((BinaryMapData)mapData).toJavaMap(((MapType)type).getKeyType(), valType);
            for (Map.Entry<?,?> en : javaMap.entrySet()) {
                if (LogicalTypeRoot.MAP.equals(valType.getTypeRoot())) {
                    // traversal of the nested map
                    result.put(en.getKey().toString(), convertNestedMap((MapData)en.getValue(), valType));
                    continue;
                }
                if (LogicalTypeRoot.DATE.equals(valType.getTypeRoot())) {
                    result.put(en.getKey().toString(), dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay((Integer)en.getValue()))));
                    continue;
                }
                if (LogicalTypeRoot.ARRAY.equals(valType.getTypeRoot())) {
                    result.put(en.getKey().toString(), convertNestedArray((ArrayData)en.getValue(), valType));
                    continue;
                }
                result.put(en.getKey().toString(), en.getValue());
            }
            return result;
        }
        throw new UnsupportedOperationException(String.format("Unsupported map data: %s", mapData.getClass()));
    }
    
}
