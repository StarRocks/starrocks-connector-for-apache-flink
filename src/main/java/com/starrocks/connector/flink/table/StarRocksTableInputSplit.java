package com.starrocks.connector.flink.table;

import org.apache.flink.core.io.InputSplit;

import com.starrocks.connector.flink.source.QueryBeXTablets;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.source.SelectColumn;

public class StarRocksTableInputSplit implements InputSplit {

    private final int splitNumber;
    private final QueryInfo queryInfo;
    private SelectColumn[] selectColumns;
    private final boolean isQueryCount;
    private final int dataCount;

    public StarRocksTableInputSplit(int splitNumber, QueryInfo queryInfo, SelectColumn[] selectColumns, boolean isQueryCount, int dataCount) {
        super();
        this.splitNumber = splitNumber;
        this.queryInfo = queryInfo;
        this.selectColumns = selectColumns;        
        this.isQueryCount = isQueryCount;
        this.dataCount = dataCount;
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

    public boolean isQueryCount() {
        return isQueryCount;
    }

    public int getDataCount() {
        return dataCount;
    }
}
