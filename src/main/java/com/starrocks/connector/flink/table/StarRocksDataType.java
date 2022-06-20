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
