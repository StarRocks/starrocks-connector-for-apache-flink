package com.starrocks.connector.flink.table.source;

import java.io.Serializable;


import org.apache.flink.table.data.GenericRowData;

public class StarRocksSourceTrickReader implements StarRocksSourceDataReader, Serializable {

    private int dataCount;

    public StarRocksSourceTrickReader(int dataCount) {
        this.dataCount = dataCount;
    }

    @Override
    public GenericRowData getNext() {
        if (dataCount > 0) {
            dataCount--;
            return new GenericRowData(0);
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return dataCount > 0;
    }

    @Override
    public void close() { }
}
