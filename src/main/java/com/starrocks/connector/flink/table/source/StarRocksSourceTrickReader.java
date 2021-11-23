package com.starrocks.connector.flink.table.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class StarRocksSourceTrickReader implements StarRocksSourceDataReader, Serializable {

    private int dataCount;

    public StarRocksSourceTrickReader(int dataCount) {
        this.dataCount = dataCount;
    }

    @Override
    public List<Object> getNext() {
        if (dataCount > 0) {
            dataCount--;
            List<Object> c = new ArrayList<>();
            return c;
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return dataCount > 0;
    }

    @Override
    public void close() {
        
    }
}
