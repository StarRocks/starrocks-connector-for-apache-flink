package com.starrocks.connector.flink.table.data;

public interface StarRocksRowData {

    String getUniqueKey();
    String getDatabase();
    String getTable();
    String getRow();

}
