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

package com.starrocks.connector.flink.row;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;
import com.starrocks.connector.flink.StarRocksSinkBaseTest;


public class StarRocksJsonSerializerTest extends StarRocksSinkBaseTest {

    @Test
    public void testCumstomizedSeparatorSerialize() throws IOException {
        OPTIONS.getSinkStreamLoadProperties().put("format", "json");
        StarRocksISerializer serializer = StarRocksSerializerFactory.createSerializer(OPTIONS, TABLE_SCHEMA.getFieldNames());
        List<Object[]> originRows = Arrays.asList(
            new Object[]{1,"222",333.1,true,"1dsasd","ppp","2020-01-01"},
            new Object[]{2,"333",444.2,false,"1dsasd","ppp","2020-01-01"}
        );
        List<byte[]> rows = originRows.stream()
            .map(vals -> serializer.serialize(vals).getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        String result = new String(joinRows(rows, rows.stream().collect(Collectors.summingInt(r -> r.length))));

        List<Map<String, Object>> rMapList = (List<Map<String, Object>>)JSON.parse(result);
        assertEquals(rMapList.size(), originRows.size());
        for (Map<String, Object> rMap : rMapList) {
            assertNotNull(rMap);
            assertEquals(TABLE_SCHEMA.getFieldCount(), rMap.size());
            for (String name : TABLE_SCHEMA.getFieldNames()) {
                assertTrue(rMap.containsKey(name));
            }
        }
    }
}
