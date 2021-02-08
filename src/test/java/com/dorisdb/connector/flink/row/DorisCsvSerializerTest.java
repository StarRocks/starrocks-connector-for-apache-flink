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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.dorisdb.connector.flink.DorisSinkBaseTest;


public class DorisCsvSerializerTest extends DorisSinkBaseTest {

    @Test
    public void testSerialize() {
        DorisISerializer serializer = DorisSerializerFactory.createSerializer(OPTIONS, TABLE_SCHEMA.getFieldNames());
        List<Object[]> originRows = Arrays.asList(
            new Object[]{1,"222",333.1,true},
            new Object[]{2,"333",444.2,false}
        );
        List<String> rows = originRows.stream()
            .map(vals -> serializer.serialize(vals))
            .collect(Collectors.toList());
        String data = new String(DorisSerializerFactory.joinRows(OPTIONS, rows));
        String[] parsedRows = data.split("\n");
        assertEquals(rows.size(), parsedRows.length);

        for (int i = 0; i < parsedRows.length; i++) {
            for (int j = 0; j < originRows.get(i).length; j++) {
                assertEquals(originRows.get(i)[j].toString(), parsedRows[i].split("\t")[j]);
            }
        }
    }

    @Test
    public void testCumstomizedSeparatorSerialize() {
        final String separator = "\\x01";
        OPTIONS.getSinkStreamLoadProperties().put("column_separator", separator);
        DorisISerializer serializer = DorisSerializerFactory.createSerializer(OPTIONS, TABLE_SCHEMA.getFieldNames());
        List<Object[]> originRows = Arrays.asList(
            new Object[]{1,"222",333.1,true},
            new Object[]{2,"333",444.2,false}
        );
        List<String> rows = originRows.stream()
            .map(vals -> serializer.serialize(vals))
            .collect(Collectors.toList());
        String data = new String(DorisSerializerFactory.joinRows(OPTIONS, rows));
        String[] parsedRows = data.split("\n");
        assertEquals(rows.size(), parsedRows.length);

        for (int i = 0; i < parsedRows.length; i++) {
            for (int j = 0; j < originRows.get(i).length; j++) {
                assertEquals(originRows.get(i)[j].toString(), parsedRows[i].split(separator)[j]);
            }
        }
    }
}
