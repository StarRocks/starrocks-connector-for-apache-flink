package com.starrocks.connector.flink.table.source;

import org.apache.flink.table.data.GenericRowData;

public interface StarRocksSourceDataReader {

    GenericRowData getNext();
    boolean hasNext();
    void close();
}
