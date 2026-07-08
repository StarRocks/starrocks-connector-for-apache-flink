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

package com.starrocks.connector.flink;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.starrocks.connector.flink.table.source.StarRocksFlinkSource;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StarRocksSourceTest {

    @Test
    public void testDataStreamScanOptionsPropagated() throws Exception {
        Configuration conf = baseConf();
        conf.set(StarRocksSourceOptions.SCAN_COLUMNS, "c1,c3");
        conf.set(StarRocksSourceOptions.SCAN_FILTER, "c2 > 10");
        StarRocksSourceOptions options = new StarRocksSourceOptions(conf, conf.toMap());

        Source<RowData, ?, ?> source = StarRocksSource.source(schema(), options);

        assertEquals("c2 > 10", field(source, "filter"));
        SelectColumn[] selectColumns = (SelectColumn[]) field(source, "selectColumns");
        assertArrayEquals(
                new String[] {"c1", "c3"},
                Arrays.stream(selectColumns).map(SelectColumn::getColumnName).toArray(String[]::new));
    }

    @Test
    public void testCountColumnsSkipColumnSelection() throws Exception {
        Configuration conf = baseConf();
        conf.set(StarRocksSourceOptions.SCAN_COLUMNS, "count(1)");
        StarRocksSourceOptions options = new StarRocksSourceOptions(conf, conf.toMap());

        Source<RowData, ?, ?> source = StarRocksSource.source(schema(), options);

        assertNull(field(source, "filter"));
        assertNull(field(source, "selectColumns"));
    }

    private static Configuration baseConf() {
        Configuration conf = new Configuration();
        conf.set(StarRocksSourceOptions.SCAN_URL, "127.0.0.1:8030");
        conf.set(StarRocksSourceOptions.JDBC_URL, "jdbc:mysql://127.0.0.1:9030");
        conf.set(StarRocksSourceOptions.USERNAME, "root");
        conf.set(StarRocksSourceOptions.PASSWORD, "");
        conf.set(StarRocksSourceOptions.DATABASE_NAME, "test_db");
        conf.set(StarRocksSourceOptions.TABLE_NAME, "test_table");
        return conf;
    }

    private static ResolvedSchema schema() {
        return ResolvedSchema.physical(
                new String[] {"c1", "c2", "c3"},
                new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()});
    }

    private static Object field(Object source, String name) throws Exception {
        Field field = StarRocksFlinkSource.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(source);
    }
}
