package com.starrocks.connector.flink.table;

import org.apache.flink.core.io.InputSplit;

import com.starrocks.connector.flink.source.QueryBeXTablets;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.source.SelectColumn;

public class StarRocksTableInputSplit implements InputSplit {

    private final int splitNumber;
    private final QueryInfo queryInfo;
    private SelectColumn[] selectColumns;

    public StarRocksTableInputSplit(int splitNumber, QueryInfo queryInfo, SelectColumn[] selectColumns) {
        super();
        this.splitNumber = splitNumber;
        this.queryInfo = queryInfo;
        this.selectColumns = selectColumns;        
    }
    
    @Override
    public int getSplitNumber() {
        return splitNumber;
    }

    public QueryInfo getQueryInfo() {
        return queryInfo;
    }

    public QueryBeXTablets getBeXTablets() {
        return queryInfo.getBeXTablets().get(splitNumber);
    }

    public SelectColumn[] getSelectColumn() {
        return selectColumns;
    }
}
