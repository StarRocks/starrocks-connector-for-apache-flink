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
import com.starrocks.connector.flink.tools.ClassUtils;

import com.alibaba.fastjson.JSON;

import java.util.List;
import java.util.Map;

public class StarRocksCsvSerializer implements StarRocksISerializer {

    private static final long serialVersionUID = 1L;

    private final String columnSeparator;
    private DataType[] dataTypes;

    public StarRocksCsvSerializer(String sp) {
        this(sp, null, null);
    }

    public StarRocksCsvSerializer(String sp, String[] columns, Map<String, DataType> mapping) {
        this.columnSeparator = StarRocksDelimiterParser.parse(sp, "\t");
        if (mapping == null) {
            return;
        }

        if (columns == null) {
            this.dataTypes = mapping.values().toArray(new DataType[0]);
            return;
        }

        this.dataTypes = new DataType[columns.length];
        for (int i = 0; i < columns.length; i++) {
            dataTypes[i] = mapping.getOrDefault(columns[i], DataType.UNKNOWN);
        }

    }

    @Override
    public String serialize(Object[] values) {
        StringBuilder sb = new StringBuilder();
        if (dataTypes == null) {
            int idx = 0;
            for (Object val : values) {
                sb.append(null == val ? "\\N" : ((val instanceof Map || val instanceof List) ? JSON.toJSONString(val) : val));
                if (idx++ < values.length - 1) {
                    sb.append(columnSeparator);
                }
            }
            return sb.toString();
        }

        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(columnSeparator);
            }

            Object obj = values[i];
            if (obj == null) {
                sb.append("\\N");
                continue;
            }

            if (!(obj instanceof String)) {
                if (ClassUtils.isPrimitiveWrapper(obj.getClass())) {
                    sb.append(obj);
                    continue;
                }

                sb.append(JSON.toJSONString(obj));
                continue;
            }

            String value = (String) obj;
            if (value.isEmpty()) {
                sb.append(value);
                continue;
            }

            DataType dataType = i >= dataTypes.length ? DataType.UNKNOWN : dataTypes[i];
            if ((dataType == DataType.JSON || dataType == DataType.UNKNOWN)
                    && (value.charAt(0) == '{' || value.charAt(0) == '[')) {
                value = JSON.parse(value).toString();
            }

            sb.append(value);
        }


        return sb.toString();
    }
}
