package com.starrocks.connector.flink.table.source.struct;

import java.io.Serializable;

public class SelectColumn implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private String columnName;
    private int columnIndexInFlinkTable;


    public SelectColumn(String columnName, int columnIndexInFlinkTable){
        this.columnName = columnName;
        this.columnIndexInFlinkTable = columnIndexInFlinkTable;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getColumnIndexInFlinkTable() {
        return columnIndexInFlinkTable;
    }

    public void setColumnIndexInFlinkTable(int columnIndexInFlinkTable) {
        this.columnIndexInFlinkTable = columnIndexInFlinkTable;
    }
}
