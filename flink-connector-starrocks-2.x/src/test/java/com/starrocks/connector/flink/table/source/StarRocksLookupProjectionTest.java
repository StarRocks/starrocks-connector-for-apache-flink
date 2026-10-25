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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.PushDownHolder;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.QueryPlan;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import mockit.Expectations;
import mockit.Mock;
import mockit.Mocked;
import mockit.MockUp;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class StarRocksLookupProjectionTest {
    private static final ResolvedSchema SCHEMA = ResolvedSchema.of(
            Column.physical("uin", DataTypes.BIGINT()),
            Column.physical("uid", DataTypes.INT()),
            Column.physical("name", DataTypes.STRING()));

    @Mocked
    private LookupContext context;

    @Test
    public void testPruneUnusedIntegerColumn() {
        List<RowData> rows = lookup(new int[][] {{0}, {2}}, new int[][] {{0}},
                new Object[] {7L}, "select `uin`,`name` from `test`.`users`", false);
        assertEquals(1, rows.size());
        // Read by the projected schema, just as Flink's generated collector does.
        assertEquals("Alice", rows.get(0).getString(1).toString());
        assertEquals(2, rows.get(0).getArity());
    }

    @Test
    public void testReorderedProjectionAndCompositeKey() {
        List<RowData> rows = lookup(new int[][] {{2}, {0}}, new int[][] {{1}, {0}},
                new Object[] {7L, StringData.fromString("Alice")},
                "select `name`,`uin` from `test`.`users`", false);
        assertEquals(1, rows.size());
        assertEquals("Alice", rows.get(0).getString(0).toString());
        assertEquals(7L, rows.get(0).getLong(1));
    }

    @Test
    public void testProjectedCopy() {
        List<RowData> rows = lookup(new int[][] {{0}, {2}}, new int[][] {{0}},
                new Object[] {7L}, "select `uin`,`name` from `test`.`users`", true);
        assertEquals(1, rows.size());
        assertEquals("Alice", rows.get(0).getString(1).toString());
        assertEquals(2, rows.get(0).getArity());
    }

    @Test
    public void testLookupWithoutProjection() {
        List<RowData> rows = lookup(null, new int[][] {{0}}, new Object[] {7L},
                "select `uin`,`uid`,`name` from `test`.`users`", false);
        assertEquals(1, rows.size());
        assertEquals(42, rows.get(0).getInt(1));
        assertEquals("Alice", rows.get(0).getString(2).toString());
    }

    private List<RowData> lookup(int[][] projection, int[][] keys, Object[] keyValues,
                                String expectedSql, boolean copy) {
        StarRocksSourceOptions options = new StarRocksSourceOptions.Builder()
                .withProperty("scan-url", "127.0.0.1:8030")
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", "test")
                .withProperty("table-name", "users")
                .build();
        StarRocksDynamicTableSource source = new StarRocksDynamicTableSource(options, SCHEMA, new PushDownHolder());
        if (projection != null) {
            source.applyProjection(projection, null);
        }
        if (copy) {
            source = (StarRocksDynamicTableSource) source.copy();
        }
        new Expectations() {{
            context.getKeys(); result = keys;
        }};
        AtomicReference<String> querySql = new AtomicReference<>();
        new MockUp<StarRocksSourceCommonFunc>() {
            @Mock
            public QueryInfo getQueryInfo(StarRocksSourceOptions ignored, String sql) {
                querySql.set(sql);
                return new QueryInfo(new QueryPlan(), Collections.singletonList(
                        new QueryBeXTablets("127.0.0.1:9060", Collections.singletonList(1L))));
            }
        };
        // Replace only BE I/O. The provider, query generation, cache key extraction and
        // lookup all run normally. Return columns in the order requested by the reader.
        new MockUp<StarRocksSourceBeReader>() {
            private GenericRowData row;
            private boolean consumed;

            @Mock
            public void $init(String node, List<ColumnRichInfo> columns, SelectColumn[] selected,
                              StarRocksSourceOptions ignored) {
                Object[] values = {7L, 42, StringData.fromString("Alice")};
                row = new GenericRowData(selected.length);
                for (int i = 0; i < selected.length; i++) {
                    int schemaIndex = selected[i].getColumnIndexInFlinkTable();
                    assertEquals(SCHEMA.getColumn(schemaIndex).get().getDataType(),
                            columns.get(schemaIndex).getDataType());
                    row.setField(i, values[schemaIndex]);
                }
            }

            @Mock
            public void openScanner(List<Long> tablets, String plan, StarRocksSourceOptions ignored) {}

            @Mock
            public void startToRead() {}

            @Mock
            public boolean hasNext() { return !consumed; }

            @Mock
            public GenericRowData getNext() {
                consumed = true;
                return row;
            }

            @Mock
            public void close() {}
        };
        LookupFunctionProvider provider = (LookupFunctionProvider) source.getLookupRuntimeProvider(context);
        StarRocksDynamicLookupFunction function = (StarRocksDynamicLookupFunction) provider.createLookupFunction();
        List<RowData> rows = new ArrayList<>(function.lookup(GenericRowData.of(keyValues)));
        assertEquals(expectedSql, querySql.get());
        // A key row always carries every key column, so only the first one misses.
        Object[] missingKey = keyValues.clone();
        missingKey[0] = -1L;
        assertEquals(0, function.lookup(GenericRowData.of(missingKey)).size());
        return rows;
    }
}
