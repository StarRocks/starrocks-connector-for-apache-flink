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

package com.starrocks.connector.flink.table.source.struct;

import com.starrocks.thrift.TScanColumnDesc;

import java.util.HashMap;
import java.util.List;

public class StarRocksSchema {

    private HashMap<String, Column> schemaMap;

    public StarRocksSchema() {
        schemaMap = new HashMap<>();
    }

    public HashMap<String, Column> getSchema() {
        return schemaMap;
    }

    public void setSchema(HashMap<String, Column> schema) {
        this.schemaMap = schema;
    }

    public void put(String name, String type, String comment, int scale, int precision) {
        schemaMap.put(name, new Column(name, type, comment, scale, precision));
    }

    public void put(Column column) {
        schemaMap.put(column.getName(), column);
    }

    public Column get(String colunmName) {
        return schemaMap.get(colunmName);
    }

    public int size() {
        return schemaMap.size();
    }

    public static StarRocksSchema genSchema(List<TScanColumnDesc> tscanColumnDescs) {
        StarRocksSchema schema = new StarRocksSchema();
        tscanColumnDescs.stream().forEach(desc -> schema.put(
            new Column(desc.getName(), desc.getType().name(), "", 0, 0))
        );
        return schema;
    }
}
