package com.dorisdb.row;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;

public class DorisGenericRowTransformer<T> implements DorisIRowTransformer<T>, Serializable {

    public static interface RowConsumer<T> extends BiConsumer<Object[], T>, Serializable {}
    
    private static final long serialVersionUID = 1L;

    private RowConsumer<T> consumer;
    private String[] fieldNames;

    public DorisGenericRowTransformer(RowConsumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void setTableSchema(TableSchema ts) {
        fieldNames = ts.getFieldNames();
    }

    @Override
    public void setRuntimeContext(RuntimeContext ctx) {}

    @Override
    public String transform(T record) {
        Object[] rowData = new Object[fieldNames.length];
        consumer.accept(rowData, record);
        return toJsonString(rowData);
    }

    private String toJsonString(Object[] record) {
        // no type validation
        Map<String, Object> rowMap = new HashMap<>(record.length);
        int idx = 0;
        for (String fieldName : fieldNames) {
            rowMap.put(fieldName, record[idx++]);
        }
        return JSON.toJSONString(rowMap);
    }
    
}
