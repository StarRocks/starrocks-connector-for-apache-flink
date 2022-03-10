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

package com.starrocks.connector.flink.row.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.junit.Test;

import mockit.Injectable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.starrocks.connector.flink.StarRocksSinkBaseTest;
import com.starrocks.connector.flink.row.sink.StarRocksGenericRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;

public class StarRocksGenericRowTransformerTest extends StarRocksSinkBaseTest {

    class UserInfoForTest {
        public byte age;
        public String resume;
        public String birthDate;
        public String birthDateTime;
        public BigDecimal savings;
        public short todaySteps;
        public String name;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testTransformer(@Injectable RuntimeContext runtimeCtx) {
        StarRocksGenericRowTransformer<UserInfoForTest> rowTransformer = new StarRocksGenericRowTransformer<>((slots, m) -> {
            slots[0] = m.age;
            slots[1] = m.resume;
            slots[2] = m.birthDate;
            slots[3] = m.birthDateTime;
            slots[4] = m.savings;
            slots[5] = m.todaySteps;
            slots[6] = m.name;
        });
        rowTransformer.setRuntimeContext(runtimeCtx);
        rowTransformer.setTableSchema(TABLE_SCHEMA);
        UserInfoForTest rowData = createRowData();
        String result = StarRocksSerializerFactory.createSerializer(OPTIONS, TABLE_SCHEMA.getFieldNames()).serialize(rowTransformer.transform(rowData, OPTIONS.supportUpsertDelete()));

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

    private UserInfoForTest createRowData() {
        UserInfoForTest u = new UserInfoForTest();
        u.age = 88;
        u.resume = "Imposible is Nothing.";
        u.birthDate = "1979-01-01";
        u.birthDateTime = "1979-01-01 12:01:01";
        u.savings = BigDecimal.valueOf(1000000.25);
        u.todaySteps = 1024;
        u.name = "Stephen";
        return u;
    }
}
