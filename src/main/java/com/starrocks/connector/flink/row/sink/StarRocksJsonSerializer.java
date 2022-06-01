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

import com.starrocks.connector.flink.table.DataType;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

public class StarRocksJsonSerializer implements StarRocksISerializer {

    private static final long serialVersionUID = 1L;
    
    private final String[] fieldNames;
    private final Map<String, DataType> mapping;

    public StarRocksJsonSerializer(String[] fieldNames) {
        this(fieldNames, null);
    }

    public StarRocksJsonSerializer(String[] fieldNames, Map<String, DataType> mapping) {
        this.fieldNames = fieldNames;
        this.mapping = mapping;
    }

    @Override
    public String serialize(Object[] values) {
        Map<String, Object> rowMap = new HashMap<>(values.length);
        int idx = 0;
        for (String fieldName : fieldNames) {
            Object value = values[idx];

            if (mapping != null && values[idx] instanceof String) {
                DataType dataType = mapping.getOrDefault(fieldName, DataType.UNKNOWN);
                if (dataType == DataType.JSON || dataType == DataType.UNKNOWN) {
                    String ori = (String) values[idx];
                    if (ori.charAt(0) == '{' || ori.charAt(0) == '[') {
                        value = JSON.parse(ori);
                    } else {
                        value = ori;
                    }
                }
            }
            rowMap.put(fieldName, value);
            idx++;
        }
        return JSON.toJSONString(rowMap);
    }
    
}
