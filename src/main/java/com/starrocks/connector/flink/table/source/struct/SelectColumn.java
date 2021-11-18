package com.starrocks.connector.flink.table.source.struct;

import java.io.Serializable;

public class SelectColumn implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private String columnName;
    private int columnIndexInFlinkTable;
    private boolean needBack;


    public SelectColumn(String columnName, int columnIndexInFlinkTable, boolean needBack){
        this.columnName = columnName;
        this.columnIndexInFlinkTable = columnIndexInFlinkTable;
        this.needBack = needBack;
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

    public boolean getNeedBack() {
        return needBack;
    }

}
