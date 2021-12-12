package com.starrocks.connector.flink.table.source.struct;

import com.starrocks.connector.flink.table.source.StarRocksSourceQueryType;

public class PushDownHolder {

    private String filter = "";
    private long limit;
    private SelectColumn[] selectColumns; 
    private String columns;
    private StarRocksSourceQueryType queryType;

    public String getFilter() {
        return filter;
    }
    public void setFilter(String filter) {
        this.filter = filter;
    }
    public long getLimit() {
        return limit;
    }
    public void setLimit(long limit) {
        this.limit = limit;
    }
    public SelectColumn[] getSelectColumns() {
        return selectColumns;
    }
    public void setSelectColumns(SelectColumn[] selectColumns) {
        this.selectColumns = selectColumns;
    }
    public String getColumns() {
        return columns;
    }
    public void setColumns(String columns) {
        this.columns = columns;
    }
    public StarRocksSourceQueryType getQueryType() {
        return queryType;
    }
    public void setQueryType(StarRocksSourceQueryType queryType) {
        this.queryType = queryType;
    }
}
