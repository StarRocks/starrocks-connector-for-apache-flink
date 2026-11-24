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

import com.starrocks.connector.flink.tools.JsonWrapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Serializes a row of a flexible partial update, where the keys of the JSON object are the columns the
 * row updates. Unlike {@link StarRocksJsonSerializer}, a null is written as an explicit null, because
 * leaving the key out would keep the column's current value instead of setting it to NULL.
 */
public class StarRocksFlexibleJsonSerializer implements StarRocksISerializer {

    private static final long serialVersionUID = 1L;

    private final String[] fieldNames;

    private transient JsonWrapper jsonWrapper;

    public StarRocksFlexibleJsonSerializer(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public void open(SerializerContext context) {
        this.jsonWrapper = context.getFastJsonWrapper();
    }

    @Override
    public String serialize(Object[] values) {
        Map<String, Object> rowMap = new HashMap<>(values.length);
        for (int i = 0; i < fieldNames.length; i++) {
            Object value = values[i];
            if (value == FlexibleRowProjection.ABSENT) {
                continue;
            }
            // A user's row builder may leave __op unset. It is not a column, so it must not become a null.
            if (value == null && StarRocksSinkOP.COLUMN_KEY.equals(fieldNames[i])) {
                continue;
            }
            rowMap.put(fieldNames[i], value);
        }
        return jsonWrapper.toJSONStringWithNulls(rowMap);
    }
}
