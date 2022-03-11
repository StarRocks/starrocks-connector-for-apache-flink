package com.starrocks.connector.flink.row.source;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;

public interface StarRocksToFlinkTrans {
    Object[] transToFlinkData(Types.MinorType beShowDataType, FieldVector curFieldVector, int rowCount, int colIndex, boolean nullable);
}

