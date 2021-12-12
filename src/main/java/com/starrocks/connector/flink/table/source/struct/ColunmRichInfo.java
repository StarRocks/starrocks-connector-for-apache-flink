package com.starrocks.connector.flink.table.source.struct;

import java.io.Serializable;

import org.apache.flink.table.types.DataType;

public class ColunmRichInfo implements Serializable {

    private final String columnName;
    private final int colunmIndexInSchema;
    private final DataType dataType;


    public ColunmRichInfo(String columnName, int colunmIndexInSchema, DataType dataType) {
        this.columnName = columnName;
        this.colunmIndexInSchema = colunmIndexInSchema;
        this.dataType = dataType;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public int getColunmIndexInSchema() {
        return this.colunmIndexInSchema;
    }

    public DataType getDataType() {
        return this.dataType;
    }
}
