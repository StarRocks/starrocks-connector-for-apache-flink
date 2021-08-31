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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

public class DorisGenericRowTransformer<T> implements DorisIRowTransformer<T> {
    
    private static final long serialVersionUID = 1L;

    private DorisSinkRowBuilder<T> consumer;
    private String[] fieldNames;

    public DorisGenericRowTransformer(DorisSinkRowBuilder<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void setTableSchema(TableSchema ts) {
        fieldNames = ts.getFieldNames();
    }

    @Override
    public void setRuntimeContext(RuntimeContext ctx) {}

    @Override
    public Object[] transform(T record, boolean supportUpsertDelete) {
        Object[] rowData = new Object[fieldNames.length + (supportUpsertDelete ? 1 : 0)];
        consumer.accept(rowData, record);
        if (supportUpsertDelete && (record instanceof RowData)) {
            // set `__op` column
            rowData[rowData.length - 1] = DorisSinkOP.parse(((RowData)record).getRowKind()).ordinal();
        }
        return rowData;
    }
    
}
