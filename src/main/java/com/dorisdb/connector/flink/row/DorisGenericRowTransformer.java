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

package com.dorisdb.connector.flink.row;

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
