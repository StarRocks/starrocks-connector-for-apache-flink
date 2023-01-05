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

package com.starrocks.connector.flink.table;

import java.util.HashMap;
import java.util.Map;

public enum StarRocksDataType {
    TINYINT,
    INT,
    LARGEINT,
    SMALLINT,
    BOOLEAN,
    DECIMAL,
    DOUBLE,
    FLOAT,
    BIGINT,
    VARCHAR,
    CHAR,
    STRING,
    JSON,
    DATE,
    DATETIME,
    UNKNOWN;

    private static final Map<String, StarRocksDataType> dataTypeMap = new HashMap<>();
    static {
        StarRocksDataType[] starRocksDataTypes = StarRocksDataType.values();

        for (StarRocksDataType starRocksDataType : starRocksDataTypes) {
            dataTypeMap.put(starRocksDataType.name(), starRocksDataType);
            dataTypeMap.put(starRocksDataType.name().toLowerCase(), starRocksDataType);
        }
    }


    public static StarRocksDataType fromString(String typeString) {
        if (typeString == null) {
            return UNKNOWN;
        }

        StarRocksDataType starRocksDataType = dataTypeMap.get(typeString);
        if (starRocksDataType == null) {
            starRocksDataType = dataTypeMap.getOrDefault(typeString.toUpperCase(), StarRocksDataType.UNKNOWN);
        }

        return starRocksDataType;
    }
}
