package com.starrocks.connector.flink.table;

import org.apache.flink.core.io.InputSplit;

import com.starrocks.connector.flink.source.QueryBeXTablets;
import com.starrocks.connector.flink.source.QueryInfo;

public class StarRocksTableInputSplit implements InputSplit {

    private final int splitNumber;
    private final QueryInfo queryInfo;

    public StarRocksTableInputSplit(int splitNumber, QueryInfo queryInfo) {
        super();
        this.splitNumber = splitNumber;
        this.queryInfo = queryInfo;
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
}
