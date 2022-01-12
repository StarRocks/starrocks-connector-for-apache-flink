/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.table.source;

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
