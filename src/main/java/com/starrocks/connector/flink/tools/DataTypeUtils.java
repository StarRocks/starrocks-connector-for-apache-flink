package com.starrocks.connector.flink.tools;

import org.apache.arrow.vector.types.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class DataTypeUtils {

    public static DataType map(Types.MinorType minorType) {
        switch (minorType) {
            case INT:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT4:
            case FLOAT8:
                return DataTypes.FLOAT();
            case NULL:
                return DataTypes.NULL();
            case TINYINT:
                return DataTypes.TINYINT();
            case VARCHAR:
                return DataTypes.STRING();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case DECIMAL:
                return DataTypes.DECIMAL(10,0);
            case DECIMAL256:
                return DataTypes.DECIMAL(38, 38);
            case DATEDAY:
                return DataTypes.DATE();
            case DATEMILLI:
                return DataTypes.TIME();
            case TIMEMILLI:
                return DataTypes.TIMESTAMP();
            default:
                return null;
        }
    }
}
