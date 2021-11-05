package com.starrocks.connector.flink.table;

import java.util.List;

public interface StarRocksSourceDataReader {

    List<Object> getNext();
    boolean hasNext();
    void close();
}
