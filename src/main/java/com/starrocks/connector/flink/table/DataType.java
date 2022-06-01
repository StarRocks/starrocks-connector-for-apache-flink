package com.starrocks.connector.flink.table;

import java.util.HashMap;
import java.util.Map;

public enum DataType {
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

    private static final Map<String, DataType> dataTypeMap = new HashMap<>();
    static {
        DataType[] dataTypes = DataType.values();

        for (DataType dataType : dataTypes) {
            dataTypeMap.put(dataType.name(), dataType);
            dataTypeMap.put(dataType.name().toLowerCase(), dataType);
        }
    }


    public static DataType fromString(String typeString) {
        if (typeString == null) {
            return UNKNOWN;
        }

        DataType dataType = dataTypeMap.get(typeString);
        if (dataType == null) {
            dataType = dataTypeMap.getOrDefault(typeString.toUpperCase(), DataType.UNKNOWN);
        }

        return dataType;
    }
}
