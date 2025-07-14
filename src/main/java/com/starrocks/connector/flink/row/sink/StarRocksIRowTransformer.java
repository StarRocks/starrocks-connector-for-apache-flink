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

import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.tools.JsonWrapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;

import java.io.Serializable;
import java.util.Map;

public interface StarRocksIRowTransformer<T> extends Serializable {

    void setStarRocksColumns(Map<String, StarRocksDataType> columns);

    void setTableSchema(TableSchema tableSchema);

    void setRuntimeContext(RuntimeContext ctx);

    default void setFastJsonWrapper(JsonWrapper jsonWrapper) {}

    Object[] transform(T record, boolean supportUpsertDelete, boolean ignoreDelete);
    
}
