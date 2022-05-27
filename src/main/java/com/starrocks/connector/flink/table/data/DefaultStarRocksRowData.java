package com.starrocks.connector.flink.table.data;

public class DefaultStarRocksRowData implements StarRocksRowData {
    private String uniqueKey;
    private String database;
    private String table;
    private String row;

    public DefaultStarRocksRowData() {

    }

    public DefaultStarRocksRowData(String database, String table) {
        this.database = database;
        this.table = table;
    }

    public DefaultStarRocksRowData(String uniqueKey,
                                   String database,
                                   String table,
                                   String row) {
        this.uniqueKey = uniqueKey;
        this.database = database;
        this.table = table;
        this.row = row;
    }

    public void setUniqueKey(String uniqueKey) {
        this.uniqueKey = uniqueKey;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setRow(String row) {
        this.row = row;
    }

    @Override
    public String getUniqueKey() {
        return null;
    }

    @Override
    public String getDatabase() {
        return null;
    }

    @Override
    public String getTable() {
        return null;
    }

    @Override
    public String getRow() {
        return null;
    }
}
