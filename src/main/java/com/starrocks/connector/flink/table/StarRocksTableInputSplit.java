package com.starrocks.connector.flink.table;

import org.apache.flink.core.io.InputSplit;

import java.io.Serializable;

import com.starrocks.connector.flink.source.QueryBeXTablets;

public class StarRocksTableInputSplit implements InputSplit, Serializable {

    private final int splitNumber;
    private final QueryBeXTablets queryBeXTablets;

    public StarRocksTableInputSplit(int splitNumber, QueryBeXTablets queryBeXTablets) {
        super();
        this.splitNumber = splitNumber;
        this.queryBeXTablets = queryBeXTablets;
    }
    
    @Override
    public int getSplitNumber() {
        return splitNumber;
    }

    public QueryBeXTablets getQueryBeXTablets() {
        return queryBeXTablets;
    }
}
