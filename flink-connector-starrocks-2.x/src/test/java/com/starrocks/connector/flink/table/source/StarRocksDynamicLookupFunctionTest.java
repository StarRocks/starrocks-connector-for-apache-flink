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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StarRocksDynamicLookupFunctionTest {

    @Test
    public void testLookupReturnsCopyOfCachedRows() throws Exception {
        Configuration conf = new Configuration();
        StarRocksDynamicLookupFunction function = new StarRocksDynamicLookupFunction(
                new StarRocksSourceOptions(conf, conf.toMap()),
                new ColumnRichInfo[0],
                new ArrayList<>(),
                new SelectColumn[0]);
        // keep reloadData a no-op so the injected cache stays in place
        Field timeField = StarRocksDynamicLookupFunction.class.getDeclaredField("nextLoadTime");
        timeField.setAccessible(true);
        timeField.setLong(function, Long.MAX_VALUE);

        List<RowData> cached = new ArrayList<>();
        cached.add(GenericRowData.of(1));
        Map<Row, List<RowData>> cacheMap = new HashMap<>();
        cacheMap.put(Row.of(), cached);
        Field field = StarRocksDynamicLookupFunction.class.getDeclaredField("cacheMap");
        field.setAccessible(true);
        field.set(function, cacheMap);

        Collection<RowData> first = function.lookup(GenericRowData.of());
        first.clear();

        assertEquals(1, function.lookup(GenericRowData.of()).size());
    }
}
