package com.starrocks.connector.flink.table.source;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.starrocks.connector.flink.it.source.StarRocksSourceBaseTest;
import com.starrocks.connector.flink.table.source.struct.PushDownHolder;

import org.junit.Before;
import org.junit.Test;

public class StarRocksDynamicTableSourceTest extends StarRocksSourceBaseTest {


    StarRocksDynamicTableSource dynamicTableSource;
    PushDownHolder pushDownHolder;

    @Before
    public void init() {
        pushDownHolder = new PushDownHolder();
        dynamicTableSource = new StarRocksDynamicTableSource(OPTIONS, TABLE_SCHEMA, pushDownHolder);
    }

    @Test
    public void testApplyProjection() {

        dynamicTableSource.applyProjection(PROJECTION_ARRAY);
        assertEquals("char_1, int_1", pushDownHolder.getColumns());

        for (int i = 0; i < SELECT_COLUMNS.length; i ++) {
            assertEquals(SELECT_COLUMNS[i].getColumnIndexInFlinkTable(), pushDownHolder.getSelectColumns()[i].getColumnIndexInFlinkTable());
            assertEquals(SELECT_COLUMNS[i].getColumnName(), pushDownHolder.getSelectColumns()[i].getColumnName());
        } 
        assertEquals(StarRocksSourceQueryType.QuerySomeColumns, pushDownHolder.getQueryType());

        dynamicTableSource.applyProjection(PROJECTION_ARRAY_NULL);
        assertEquals(StarRocksSourceQueryType.QueryCount, pushDownHolder.getQueryType());
    }
}
