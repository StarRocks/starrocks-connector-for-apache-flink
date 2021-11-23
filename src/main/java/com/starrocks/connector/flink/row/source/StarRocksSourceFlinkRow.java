package com.starrocks.connector.flink.row.source;

import java.util.ArrayList;
import java.util.List;

public class StarRocksSourceFlinkRow {

    private List<Object> columns;
    
    public StarRocksSourceFlinkRow(int count) {
        this.columns = new ArrayList<>(count);
    }
    public List<Object> getColumns() {
        return columns;
    }
    public void put(Object obj) {
        columns.add(obj);
    }
}