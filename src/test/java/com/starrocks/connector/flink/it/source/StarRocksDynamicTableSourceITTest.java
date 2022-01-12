package com.starrocks.connector.flink.it.source;


import static org.junit.Assert.assertTrue;

import java.util.List;

import com.starrocks.connector.flink.StarRocksSource;
import com.starrocks.connector.flink.table.source.StarRocksSourceCommonFunc;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import mockit.Mock;
import mockit.MockUp;


public class StarRocksDynamicTableSourceITTest extends StarRocksSourceBaseTest {

    private Long dataCount = 30L;
    @Test
    public void testDataStream() throws Exception {

        new MockUp<StarRocksSourceCommonFunc>() {
            @Mock
            public Long getQueryCount(StarRocksSourceOptions sourceOptions, String SQL) {
                return dataCount;
            }
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<RowData> dList = env.addSource(StarRocksSource.source(OPTIONS_WITH_COLUMN_IS_COUNT, TABLE_SCHEMA)).setParallelism(5).executeAndCollect(50);
        assertTrue(dList.size() == dataCount);
    }
}
