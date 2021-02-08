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

package com.dorisdb.connector.flink.row;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.junit.Test;

import mockit.Injectable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.dorisdb.connector.flink.DorisSinkBaseTest;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

public class DorisTableRowTransformerTest extends DorisSinkBaseTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testTransformer(@Injectable TypeInformation<RowData> rowDataTypeInfo, @Injectable RuntimeContext runtimeCtx) {
        DorisTableRowTransformer rowTransformer = new DorisTableRowTransformer(rowDataTypeInfo);
        rowTransformer.setRuntimeContext(runtimeCtx);
        rowTransformer.setTableSchema(TABLE_SCHEMA);
        GenericRowData rowData = createRowData();
        String result = DorisSerializerFactory.createSerializer(OPTIONS, TABLE_SCHEMA.getFieldNames()).serialize(rowTransformer.transform(rowData));

        Map<String, String> loadProsp = OPTIONS.getSinkStreamLoadProperties();
        String format = loadProsp.get("format");
        if (Strings.isNullOrEmpty(format) || "csv".equalsIgnoreCase(format)) {
            assertEquals(TABLE_SCHEMA.getFieldCount(), result.split("\t").length);
        }
        if ("json".equalsIgnoreCase(format)) {
            Map<String, Object> rMap = (Map<String, Object>)JSON.parse(result);
            assertNotNull(rMap);
            assertEquals(TABLE_SCHEMA.getFieldCount(), rMap.size());
            for (String name : TABLE_SCHEMA.getFieldNames()) {
                assertTrue(rMap.containsKey(name));
            }
        }
    }

    private GenericRowData createRowData() {
        GenericRowData genericRowData = new GenericRowData(TABLE_SCHEMA.getFieldCount());
        genericRowData.setField(0, (byte)20);
        genericRowData.setField(1, StringData.fromString("xxxssss"));
        genericRowData.setField(2, TimestampData.fromTimestamp(Timestamp.valueOf("2021-02-02 12:22:22.006")));
        genericRowData.setField(3, (int)LocalDate.now().toEpochDay());
        genericRowData.setField(4, DecimalData.fromBigDecimal(BigDecimal.valueOf(1000), 10, 2));
        genericRowData.setField(5, (short)30);
        genericRowData.setField(6, StringData.fromString("ch"));
        return genericRowData;
    }
}
