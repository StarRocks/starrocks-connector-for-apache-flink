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

package com.starrocks.connector.flink.table.source.struct;

import com.starrocks.connector.flink.table.source.StarRocksSourceQueryType;
import org.junit.Assert;
import org.junit.Test;

public class PushDownHolderTest {
    private static final long LIMIT = 10;
    private static final StarRocksSourceQueryType QUERY_TYPE = StarRocksSourceQueryType.QuerySomeColumns;
    private static final String FILTER = "filter";
    private static final SelectColumn[] SELECT_COLUMNS = new SelectColumn[]{
            new SelectColumn("col1", 1),
            new SelectColumn("col2", 2)
    };

    @Test
    public void testPushDownHolderDeepCopy() {
        PushDownHolder holder = createPushDownHolder();
        PushDownHolder copied = holder.copy();

        holder.setLimit(20);
        holder.setQueryType(StarRocksSourceQueryType.QueryCount);
        holder.setFilter("newFilter");
        SelectColumn[] columns = new SelectColumn[]{new SelectColumn("col1", 1)};
        holder.setSelectColumns(columns);

        Assert.assertNotEquals(holder, copied);
        Assert.assertNotEquals(holder.getQueryType(), copied.getQueryType());
        Assert.assertNotEquals(holder.getFilter(), copied.getFilter());
        Assert.assertNotEquals(holder.getLimit(), copied.getLimit());
        Assert.assertNotEquals(holder.getSelectColumns(), copied.getSelectColumns());

        Assert.assertEquals(LIMIT, copied.getLimit());
        Assert.assertEquals(QUERY_TYPE, copied.getQueryType());
        Assert.assertEquals(FILTER, copied.getFilter());
        Assert.assertArrayEquals(SELECT_COLUMNS, copied.getSelectColumns());
    }

    private PushDownHolder createPushDownHolder() {
        PushDownHolder pushDownHolder = new PushDownHolder();
        pushDownHolder.setLimit(LIMIT);
        pushDownHolder.setQueryType(QUERY_TYPE);
        pushDownHolder.setFilter(FILTER);
        pushDownHolder.setSelectColumns(SELECT_COLUMNS);
        return pushDownHolder;
    }
}
