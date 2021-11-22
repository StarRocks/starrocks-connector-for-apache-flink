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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.starrocks.connector.flink.it.sink.StarRocksSinkBaseTest;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;


public class StarRocksCsvSerializerTest extends StarRocksSinkBaseTest {

    @Test
    public void testSerialize() throws IOException {
        StarRocksISerializer serializer = StarRocksSerializerFactory.createSerializer(OPTIONS, TABLE_SCHEMA.getFieldNames());
        List<Object[]> originRows = Arrays.asList(
            new Object[]{1,"222",333.1,true},
            new Object[]{2,"333",444.2,false}
        );
        List<byte[]> rows = originRows.stream()
            .map(vals -> serializer.serialize(vals).getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        String data = new String(joinRows(rows, rows.stream().collect(Collectors.summingInt(r -> r.length))));
        String[] parsedRows = data.split("\n");
        assertEquals(rows.size(), parsedRows.length);

        for (int i = 0; i < parsedRows.length; i++) {
            for (int j = 0; j < originRows.get(i).length; j++) {
                assertEquals(originRows.get(i)[j].toString(), parsedRows[i].split("\t")[j]);
            }
        }
    }

    @Test
    public void testCumstomizedSeparatorSerialize() throws IOException {
        final String separator = "\\x01";
        OPTIONS.getSinkStreamLoadProperties().put("column_separator", separator);
        StarRocksISerializer serializer = StarRocksSerializerFactory.createSerializer(OPTIONS, TABLE_SCHEMA.getFieldNames());
        List<Object[]> originRows = Arrays.asList(
            new Object[]{1,"222",333.1,true},
            new Object[]{2,"333",444.2,false}
        );
        List<byte[]> rows = originRows.stream()
            .map(vals -> serializer.serialize(vals).getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        String data = new String(joinRows(rows, rows.stream().collect(Collectors.summingInt(r -> r.length))));
        String[] parsedRows = data.split("\n");
        assertEquals(rows.size(), parsedRows.length);

        for (int i = 0; i < parsedRows.length; i++) {
            for (int j = 0; j < originRows.get(i).length; j++) {
                assertEquals(originRows.get(i)[j].toString(), parsedRows[i].split(separator)[j]);
            }
        }
    }

    @Test
    public void testCumstomizedDelimiterSerialize() throws IOException {
        final String delimiter = "\\x02";
        OPTIONS.getSinkStreamLoadProperties().put("row_delimiter", delimiter);
        StarRocksISerializer serializer = StarRocksSerializerFactory.createSerializer(OPTIONS, TABLE_SCHEMA.getFieldNames());
        List<Object[]> originRows = Arrays.asList(
            new Object[]{1,"222",333.1,true},
            new Object[]{2,"333",444.2,false}
        );
        List<byte[]> rows = originRows.stream()
            .map(vals -> serializer.serialize(vals).getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
        String data = new String(joinRows(rows, rows.stream().collect(Collectors.summingInt(r -> r.length))));
        String[] parsedRows = data.split(delimiter);
        assertEquals(rows.size(), parsedRows.length);
    }
}
