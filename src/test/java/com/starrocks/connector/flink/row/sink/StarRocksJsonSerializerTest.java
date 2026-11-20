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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSinkBaseTest;
import com.starrocks.connector.flink.tools.JsonWrapper;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.MapDataSerializer;
import org.apache.flink.table.types.DataType;


public class StarRocksJsonSerializerTest extends StarRocksSinkBaseTest {

    @Test
    public void testCumstomizedSeparatorSerialize() throws IOException {
        OPTIONS.getSinkStreamLoadProperties().put("format", "json");
        StarRocksISerializer serializer = StarRocksSerializerFactory.createSerializer(OPTIONS, TABLE_SCHEMA.getFieldNames());
        serializer.open(new StarRocksISerializer.SerializerContext(new JsonWrapper()));
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

    /**
     * The keys and the values of the nested map should be serialized as the plain java types,
     * so that the map keys are written as the field names of a json object, and the null value
     * of a map/array must be serialized as the json null.
     */
    @Test
    public void testNestedMapSerialize() {
        OPTIONS.getSinkStreamLoadProperties().put("format", "json");

        TableSchema schema = TableSchema.builder()
            .field("m1", DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())))
            .field("m3", DataTypes.MAP(DataTypes.STRING(), DataTypes.DATE()))
            .field("m4", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
            .build();

        GenericRowData rowData = new GenericRowData(schema.getFieldCount());
        Map<Object, Object> m1 = new HashMap<>();
        m1.put(StringData.fromString("a"), new GenericArrayData(new Object[]{StringData.fromString("x"), null}));
        rowData.setField(0, new GenericMapData(m1));
        Map<Object, Object> m3 = new HashMap<>();
        m3.put(StringData.fromString("d"), (int) LocalDate.of(2021, 1, 2).toEpochDay());
        rowData.setField(1, new GenericMapData(m3));
        Map<Object, Object> m4 = new HashMap<>();
        m4.put(StringData.fromString("s"), null);
        rowData.setField(2, new GenericMapData(m4));

        StarRocksTableRowTransformer rowTransformer = new StarRocksTableRowTransformer(null);
        rowTransformer.setRuntimeContext(null);
        rowTransformer.setTableSchema(schema);
        StarRocksISerializer serializer = StarRocksSerializerFactory.createSerializer(OPTIONS, schema.getFieldNames());
        serializer.open(new StarRocksISerializer.SerializerContext(new JsonWrapper()));

        String result = serializer.serialize(rowTransformer.transform(rowData, false));
        Map<String, Object> rMap = (Map<String, Object>)JSON.parse(result);
        assertNotNull(rMap);
        assertEquals("unexpected json: " + result, schema.getFieldCount(), rMap.size());

        // the keys of a nested map must be serialized as the plain field names of a json object
        JSONObject m1Json = (JSONObject) rMap.get("m1");
        assertNotNull("unexpected json: " + result, m1Json);
        JSONArray a = m1Json.getJSONArray("a");
        assertNotNull("unexpected json: " + result, a);
        assertEquals("x", a.getString(0));
        // the null element of the nested array must be kept as a json null
        assertEquals("unexpected json: " + result, 2, a.size());
        assertNull("unexpected json: " + result, a.get(1));

        // the date value of a nested map must be formatted as a string
        JSONObject m3Json = (JSONObject) rMap.get("m3");
        assertNotNull("unexpected json: " + result, m3Json);
        assertEquals("unexpected json: " + result, "2021-01-02", m3Json.getString("d"));

        // a null value of a nested map is omitted by the default fastjson features of JsonWrapper
        JSONObject m4Json = (JSONObject) rMap.get("m4");
        assertNotNull("unexpected json: " + result, m4Json);
        assertEquals("unexpected json: " + result, 0, m4Json.size());
    }

    /**
     * The binary representation of the nested map must be serialized to the same json as the
     * generic representation: the keys of the map must be the plain field names of a json object,
     * and the null values/elements must not break the serialization of the row.
     */
    @Test
    public void testNestedBinaryMapSerialize() {
        TableSchema schema = TableSchema.builder()
            .field("m1", DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())))
            .field("m3", DataTypes.MAP(DataTypes.STRING(), DataTypes.DATE()))
            .field("m4", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
            .build();

        GenericRowData rowData = new GenericRowData(schema.getFieldCount());
        Map<Object, Object> m1 = new HashMap<>();
        m1.put(StringData.fromString("a"), new GenericArrayData(new Object[]{StringData.fromString("x"), null}));
        // the value of a map is allowed to be null even if its type is an array
        m1.put(StringData.fromString("an"), null);
        rowData.setField(0, toBinaryMap(m1, DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())));
        Map<Object, Object> m3 = new HashMap<>();
        m3.put(StringData.fromString("d"), (int) LocalDate.of(2021, 1, 2).toEpochDay());
        rowData.setField(1, toBinaryMap(m3, DataTypes.STRING(), DataTypes.DATE()));
        Map<Object, Object> m4 = new HashMap<>();
        m4.put(StringData.fromString("s"), null);
        rowData.setField(2, toBinaryMap(m4, DataTypes.STRING(), DataTypes.STRING()));

        String result = serializeRow(schema, rowData);
        Map<String, Object> rMap = (Map<String, Object>)JSON.parse(result);
        assertNotNull(rMap);
        assertEquals("unexpected json: " + result, schema.getFieldCount(), rMap.size());

        // the keys of a binary nested map must be serialized as the plain field names of a json object
        JSONObject m1Json = (JSONObject) rMap.get("m1");
        assertNotNull("unexpected json: " + result, m1Json);
        JSONArray a = m1Json.getJSONArray("a");
        assertNotNull("unexpected json: " + result, a);
        assertEquals("unexpected json: " + result, "x", a.getString(0));
        // the null element of the nested array must be kept as a json null
        assertEquals("unexpected json: " + result, 2, a.size());
        assertNull("unexpected json: " + result, a.get(1));
        // a null value whose type is an array is omitted by the default fastjson features of JsonWrapper
        assertEquals("unexpected json: " + result, 1, m1Json.size());
        assertFalse("unexpected json: " + result, m1Json.containsKey("an"));

        // the date value of a binary nested map must be formatted as a string
        JSONObject m3Json = (JSONObject) rMap.get("m3");
        assertNotNull("unexpected json: " + result, m3Json);
        assertEquals("unexpected json: " + result, "2021-01-02", m3Json.getString("d"));

        // a null value of a binary nested map is omitted by the default fastjson features of JsonWrapper
        JSONObject m4Json = (JSONObject) rMap.get("m4");
        assertNotNull("unexpected json: " + result, m4Json);
        assertEquals("unexpected json: " + result, 0, m4Json.size());
    }

    /**
     * A map key is the field name of a json object, so a key which is not a string must be written
     * as a quoted string: fastjson writes the raw value of a non-string key, and the parser of
     * fastjson is lenient, so the raw json must be asserted here.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testNestedNonStringMapKeySerialize() {
        TableSchema schema = TableSchema.builder()
            .field("mi", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
            .field("md", DataTypes.MAP(DataTypes.DECIMAL(10, 2), DataTypes.STRING()))
            .field("mdt", DataTypes.MAP(DataTypes.DATE(), DataTypes.STRING()))
            .field("mts", DataTypes.MAP(DataTypes.TIMESTAMP(3), DataTypes.STRING()))
            .field("mb", DataTypes.MAP(DataTypes.BINARY(3), DataTypes.STRING()))
            .build();
        assertNonStringMapKeyJson(schema, createNonStringMapKeyRowData(false));
        assertNonStringMapKeyJson(schema, createNonStringMapKeyRowData(true));
    }

    private GenericRowData createNonStringMapKeyRowData(boolean binary) {
        GenericRowData rowData = new GenericRowData(5);

        Map<Object, Object> mi = new HashMap<>();
        mi.put(1, StringData.fromString("v"));
        rowData.setField(0, binary
            ? toBinaryMap(mi, DataTypes.INT(), DataTypes.STRING()) : new GenericMapData(mi));

        Map<Object, Object> md = new HashMap<>();
        md.put(DecimalData.fromBigDecimal(new BigDecimal("1.50"), 10, 2), StringData.fromString("v"));
        rowData.setField(1, binary
            ? toBinaryMap(md, DataTypes.DECIMAL(10, 2), DataTypes.STRING()) : new GenericMapData(md));

        Map<Object, Object> mdt = new HashMap<>();
        mdt.put((int) LocalDate.of(2021, 1, 2).toEpochDay(), StringData.fromString("v"));
        rowData.setField(2, binary
            ? toBinaryMap(mdt, DataTypes.DATE(), DataTypes.STRING()) : new GenericMapData(mdt));

        Map<Object, Object> mts = new HashMap<>();
        mts.put(TimestampData.fromTimestamp(Timestamp.valueOf("2021-01-02 03:04:05.006")),
            StringData.fromString("v"));
        rowData.setField(3, binary
            ? toBinaryMap(mts, DataTypes.TIMESTAMP(3), DataTypes.STRING()) : new GenericMapData(mts));

        Map<Object, Object> mb = new HashMap<>();
        mb.put(new byte[]{0x01, 0x02, 0x03}, StringData.fromString("v"));
        rowData.setField(4, binary
            ? toBinaryMap(mb, DataTypes.BINARY(3), DataTypes.STRING()) : new GenericMapData(mb));

        return rowData;
    }

    @SuppressWarnings("unchecked")
    private void assertNonStringMapKeyJson(TableSchema schema, GenericRowData rowData) {
        String result = serializeRow(schema, rowData);
        // a non-string key which is not converted to a string is written without quotes
        assertFalse("unexpected json: " + result, result.contains("{1:"));
        assertTrue("unexpected json: " + result, result.contains("\"mi\":{\"1\":\"v\"}"));
        // a binary key which is not converted to a string is written as an object id
        assertFalse("unexpected json: " + result, result.contains("[B@"));
        assertTrue("unexpected json: " + result, result.contains("\"mb\":{\"66051\":\"v\"}"));

        Map<String, Object> rMap = (Map<String, Object>)JSON.parse(result);
        assertEquals("unexpected json: " + result, "v", mapValue(rMap.get("mi"), "1"));
        assertEquals("unexpected json: " + result, "v", mapValue(rMap.get("md"), "1.50"));
        assertEquals("unexpected json: " + result, "v", mapValue(rMap.get("mdt"), "2021-01-02"));
        assertEquals("unexpected json: " + result, "v", mapValue(rMap.get("mts"), "2021-01-02T03:04:05.006"));
        assertEquals("unexpected json: " + result, "v", mapValue(rMap.get("mb"), "66051"));
    }

    private String mapValue(Object map, String key) {
        JSONObject json = (JSONObject) map;
        assertNotNull("unexpected value: " + map, json);
        assertEquals(1, json.size());
        assertTrue("unexpected key of " + json, json.containsKey(key));
        return json.getString(key);
    }

    private static MapData toBinaryMap(Map<Object, Object> map, DataType keyType, DataType valueType) {
        return new MapDataSerializer(keyType.getLogicalType(), valueType.getLogicalType())
            .toBinaryMap(new GenericMapData(map));
    }

    private String serializeRow(TableSchema schema, GenericRowData rowData) {
        OPTIONS.getSinkStreamLoadProperties().put("format", "json");
        StarRocksTableRowTransformer rowTransformer = new StarRocksTableRowTransformer(null);
        rowTransformer.setRuntimeContext(null);
        rowTransformer.setTableSchema(schema);
        StarRocksISerializer serializer = StarRocksSerializerFactory.createSerializer(OPTIONS, schema.getFieldNames());
        serializer.open(new StarRocksISerializer.SerializerContext(new JsonWrapper()));
        return serializer.serialize(rowTransformer.transform(rowData, false));
    }
}
