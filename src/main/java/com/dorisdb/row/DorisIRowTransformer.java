package com.dorisdb.row;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;

public interface DorisIRowTransformer<T> {

    void setTableSchema(TableSchema tableSchema);

    void setRuntimeContext(RuntimeContext ctx);

    String transform(T record);
    
}
