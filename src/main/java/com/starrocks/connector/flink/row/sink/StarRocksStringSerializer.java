package com.starrocks.connector.flink.row.sink;

public class StarRocksStringSerializer implements  StarRocksISerializer{

    public StarRocksStringSerializer() {
    }

    @Override
    public String serialize(Object[] values) {
        if (values.length !=1){
            throw new RuntimeException(String.format("Columns number must be 1 When sink with no schema,current number is '%d'",values.length));
        }
        return String.valueOf(values[0]);
    }
}
