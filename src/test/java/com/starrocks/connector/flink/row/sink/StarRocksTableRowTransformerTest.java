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

import com.google.common.base.Strings;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.Test;

import mockit.Injectable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSinkBaseTest;
import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.tools.JsonWrapper;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.runtime.typeutils.MapDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;

public class StarRocksTableRowTransformerTest extends StarRocksSinkBaseTest {

    private static final LocalDate NESTED_MAP_DATE = LocalDate.of(2021, 1, 2);

    private static final TableSchema NESTED_MAP_SCHEMA = TableSchema.builder()
        .field("m1", DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())))
        .field("m2", DataTypes.MAP(DataTypes.STRING(), DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())))
        .field("m3", DataTypes.MAP(DataTypes.STRING(), DataTypes.DATE()))
        .field("m4", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
        .field("a1", DataTypes.ARRAY(DataTypes.STRING()))
        .build();

    private static final Timestamp NESTED_MAP_TIMESTAMP = Timestamp.valueOf("2021-01-02 03:04:05.006");

    private static final DataType NESTED_MAP_ROW_TYPE = DataTypes.ROW(
        DataTypes.FIELD("a", DataTypes.INT()), DataTypes.FIELD("b", DataTypes.STRING()));

    private static final TableSchema NON_STRING_MAP_KEY_SCHEMA = TableSchema.builder()
        .field("mi", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
        .field("md", DataTypes.MAP(DataTypes.DECIMAL(10, 2), DataTypes.STRING()))
        .field("mdt", DataTypes.MAP(DataTypes.DATE(), DataTypes.STRING()))
        .field("mts", DataTypes.MAP(DataTypes.TIMESTAMP(3), DataTypes.STRING()))
        .build();

    private static final byte[] BINARY_MAP_KEY = new byte[]{0x01, 0x02, 0x03};

    /**
     * The contents of {@link #BINARY_MAP_KEY} read as an unsigned big-endian number: the numeric
     * representation which the top level BINARY column uses.
     */
    private static final long BINARY_MAP_KEY_NUMBER = 66051L;

    private static final TableSchema BINARY_KEY_SCHEMA = TableSchema.builder()
        .field("b", DataTypes.BINARY(BINARY_MAP_KEY.length))
        .field("mb", DataTypes.MAP(DataTypes.BINARY(BINARY_MAP_KEY.length), DataTypes.STRING()))
        .build();

    private static final DataType DATE_MAP_TYPE = DataTypes.MAP(DataTypes.DATE(), DataTypes.DATE());

    private static final TableSchema DATE_MAP_ARRAY_SCHEMA = TableSchema.builder()
        .field("am", DataTypes.ARRAY(DATE_MAP_TYPE))
        .build();

    /** Enough elements/dates to convert the elements of the array concurrently. */
    private static final int PARALLEL_DATE_ELEMENTS = 240;

    private static final int PARALLEL_DATES_PER_ELEMENT = 4;

    private static final TableSchema NESTED_ROW_VALUE_SCHEMA = TableSchema.builder()
        .field("m", DataTypes.MAP(DataTypes.STRING(), NESTED_MAP_ROW_TYPE))
        .build();

    private static final TableSchema NESTED_ROW_ARRAY_SCHEMA = TableSchema.builder()
        .field("a", DataTypes.ARRAY(NESTED_MAP_ROW_TYPE))
        .build();

    private static final TableSchema TOP_LEVEL_ROW_SCHEMA = TableSchema.builder()
        .field("r", NESTED_MAP_ROW_TYPE)
        .field("j", DataTypes.STRING())
        .field("s", DataTypes.STRING())
        .build();

    @SuppressWarnings("unchecked")
    @Test
    public void testTransformer(@Injectable TypeInformation<RowData> rowDataTypeInfo, @Injectable RuntimeContext runtimeCtx) {
        StarRocksTableRowTransformer rowTransformer = new StarRocksTableRowTransformer(rowDataTypeInfo);
        rowTransformer.setRuntimeContext(runtimeCtx);
        rowTransformer.setTableSchema(TABLE_SCHEMA);
        GenericRowData rowData = createRowData();
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

    @Test
    public void testNestedMapTransformer() {
        StarRocksTableRowTransformer rowTransformer = createNestedMapTransformer();
        assertNestedMapValues(rowTransformer.transform(createNestedMapRowData(), false));
    }

    /**
     * The nested map data of the binary representation should be converted in the same way as
     * the generic representation, and the null value/element should be kept as null.
     */
    @Test
    public void testNestedBinaryMapTransformer() {
        StarRocksTableRowTransformer rowTransformer = createNestedMapTransformer();
        GenericRowData genericRowData = createNestedMapRowData();
        GenericRowData binaryRowData = new GenericRowData(NESTED_MAP_SCHEMA.getFieldCount());
        for (int i = 0; i < NESTED_MAP_SCHEMA.getFieldCount(); i++) {
            LogicalType logicalType = NESTED_MAP_SCHEMA.getFieldDataTypes()[i].getLogicalType();
            if (!LogicalTypeRoot.MAP.equals(logicalType.getTypeRoot())) {
                binaryRowData.setField(i, genericRowData.getField(i));
                continue;
            }
            MapType mapType = (MapType) logicalType;
            binaryRowData.setField(i, new MapDataSerializer(mapType.getKeyType(), mapType.getValueType())
                .toBinaryMap((MapData) genericRowData.getField(i)));
        }
        assertNestedMapValues(rowTransformer.transform(binaryRowData, false));
    }

    private StarRocksTableRowTransformer createNestedMapTransformer() {
        StarRocksTableRowTransformer rowTransformer = new StarRocksTableRowTransformer(null);
        rowTransformer.setRuntimeContext(null);
        rowTransformer.setTableSchema(NESTED_MAP_SCHEMA);
        return rowTransformer;
    }

    private GenericRowData createNestedMapRowData() {
        GenericRowData rowData = new GenericRowData(NESTED_MAP_SCHEMA.getFieldCount());

        Map<Object, Object> m1 = new HashMap<>();
        m1.put(StringData.fromString("a"), new GenericArrayData(new Object[]{StringData.fromString("x"), null}));
        m1.put(StringData.fromString("b"), new GenericArrayData(new Object[]{StringData.fromString("y")}));
        m1.put(StringData.fromString("an"), null);
        rowData.setField(0, new GenericMapData(m1));

        Map<Object, Object> nested = new HashMap<>();
        nested.put(StringData.fromString("nk"), null);
        Map<Object, Object> m2 = new HashMap<>();
        m2.put(StringData.fromString("n"), new GenericMapData(nested));
        m2.put(StringData.fromString("nn"), null);
        rowData.setField(1, new GenericMapData(m2));

        Map<Object, Object> m3 = new HashMap<>();
        m3.put(StringData.fromString("d"), (int) NESTED_MAP_DATE.toEpochDay());
        m3.put(StringData.fromString("dn"), null);
        rowData.setField(2, new GenericMapData(m3));

        Map<Object, Object> m4 = new HashMap<>();
        m4.put(StringData.fromString("s"), null);
        rowData.setField(3, new GenericMapData(m4));

        rowData.setField(4, new GenericArrayData(new Object[]{StringData.fromString("p"), null}));
        return rowData;
    }

    @SuppressWarnings("unchecked")
    private void assertNestedMapValues(Object[] values) {
        // MAP<STRING, ARRAY<STRING>>, the null element of the nested array must be kept as null
        Map<Object, Object> m1 = (Map<Object, Object>) values[0];
        assertEquals(3, m1.size());
        List<Object> a = (List<Object>) m1.get("a");
        assertNotNull(a);
        assertEquals(2, a.size());
        assertEquals(StringData.fromString("x"), a.get(0));
        assertNull(a.get(1));
        List<Object> b = (List<Object>) m1.get("b");
        assertNotNull(b);
        assertEquals(1, b.size());
        assertEquals(StringData.fromString("y"), b.get(0));
        // a null value whose type is an array must be kept as null instead of being traversed
        assertTrue(m1.containsKey("an"));
        assertNull(m1.get("an"));

        // MAP<STRING, MAP<STRING, STRING>>, the null value of the nested map must be kept as null
        Map<Object, Object> m2 = (Map<Object, Object>) values[1];
        assertEquals(2, m2.size());
        Map<Object, Object> n = (Map<Object, Object>) m2.get("n");
        assertNotNull(n);
        assertEquals(1, n.size());
        assertTrue(n.containsKey("nk"));
        assertNull(n.get("nk"));
        // a null value whose type is a map must be kept as null instead of being traversed
        assertTrue(m2.containsKey("nn"));
        assertNull(m2.get("nn"));

        // MAP<STRING, DATE>, the date value should be formatted as a string
        Map<Object, Object> m3 = (Map<Object, Object>) values[2];
        assertEquals(2, m3.size());
        assertEquals(NESTED_MAP_DATE.toString(), m3.get("d"));
        // a null value whose type is a date must be kept as null instead of being formatted
        assertTrue(m3.containsKey("dn"));
        assertNull(m3.get("dn"));

        // MAP<STRING, STRING>, the null value of the map must be kept as null
        Map<Object, Object> m4 = (Map<Object, Object>) values[3];
        assertEquals(1, m4.size());
        assertTrue(m4.containsKey("s"));
        assertNull(m4.get("s"));

        // the null element of the top level array must be kept as null
        List<Object> a1 = (List<Object>) values[4];
        assertEquals(2, a1.size());
        assertEquals(StringData.fromString("p"), a1.get(0));
        assertNull(a1.get(1));
    }

    /**
     * The key of a map is the field name of a json object, so it must be a string even if the key
     * type is not a string type: fastjson writes a non-string key without quotes.
     */
    @Test
    public void testNestedMapNonStringKeysTransformer() {
        assertNestedMapKeyStrings(createTransformer(NON_STRING_MAP_KEY_SCHEMA, StarRocksDataType.JSON)
            .transform(createNonStringMapKeyRowData(false), false));
        assertNestedMapKeyStrings(createTransformer(NON_STRING_MAP_KEY_SCHEMA, StarRocksDataType.JSON)
            .transform(createNonStringMapKeyRowData(true), false));
    }

    private GenericRowData createNonStringMapKeyRowData(boolean binary) {
        DataType[] mapTypes = NON_STRING_MAP_KEY_SCHEMA.getFieldDataTypes();
        GenericRowData rowData = new GenericRowData(NON_STRING_MAP_KEY_SCHEMA.getFieldCount());

        Map<Object, Object> mi = new HashMap<>();
        mi.put(1, StringData.fromString("v"));
        rowData.setField(0, binary ? toBinaryMap(mi, mapTypes[0]) : new GenericMapData(mi));

        Map<Object, Object> md = new HashMap<>();
        md.put(DecimalData.fromBigDecimal(new BigDecimal("1.50"), 10, 2), StringData.fromString("v"));
        rowData.setField(1, binary ? toBinaryMap(md, mapTypes[1]) : new GenericMapData(md));

        Map<Object, Object> mdt = new HashMap<>();
        mdt.put((int) NESTED_MAP_DATE.toEpochDay(), StringData.fromString("v"));
        rowData.setField(2, binary ? toBinaryMap(mdt, mapTypes[2]) : new GenericMapData(mdt));

        Map<Object, Object> mts = new HashMap<>();
        mts.put(TimestampData.fromTimestamp(NESTED_MAP_TIMESTAMP), StringData.fromString("v"));
        rowData.setField(3, binary ? toBinaryMap(mts, mapTypes[3]) : new GenericMapData(mts));

        return rowData;
    }

    @SuppressWarnings("unchecked")
    private void assertNestedMapKeyStrings(Object[] values) {
        assertEquals("1", assertSingleStringKey(values[0]));
        assertEquals("1.50", assertSingleStringKey(values[1]));
        assertEquals(NESTED_MAP_DATE.toString(), assertSingleStringKey(values[2]));
        assertEquals("2021-01-02T03:04:05.006", assertSingleStringKey(values[3]));
    }

    @SuppressWarnings("unchecked")
    private String assertSingleStringKey(Object value) {
        Map<Object, Object> map = (Map<Object, Object>) value;
        assertEquals(1, map.size());
        Object key = map.keySet().iterator().next();
        // a key which is not a string is written by fastjson without quotes
        assertTrue("unexpected key: " + key, key instanceof String);
        assertEquals("v", map.get(key));
        return (String) key;
    }

    /**
     * The key of a map is the field name of a json object, so a binary key must be written as a
     * quoted string: the numeric representation of the binary value is used, like the top level
     * binary column.
     */
    @Test
    public void testBinaryMapKeyTransformer() {
        assertBinaryMapKeys(createTransformer(BINARY_KEY_SCHEMA, StarRocksDataType.JSON)
            .transform(createBinaryMapKeyRowData(false), false));
        assertBinaryMapKeys(createTransformer(BINARY_KEY_SCHEMA, StarRocksDataType.JSON)
            .transform(createBinaryMapKeyRowData(true), false));
    }

    private GenericRowData createBinaryMapKeyRowData(boolean binary) {
        GenericRowData rowData = new GenericRowData(BINARY_KEY_SCHEMA.getFieldCount());
        rowData.setField(0, BINARY_MAP_KEY.clone());

        Map<Object, Object> mb = new HashMap<>();
        mb.put(BINARY_MAP_KEY, StringData.fromString("v"));
        mb.put(new byte[]{0x01, 0x02, 0x04}, StringData.fromString("v"));
        mb.put(new byte[]{0x01, 0x02, (byte) 0xFF}, StringData.fromString("v"));
        rowData.setField(1, binary
            ? toBinaryMap(mb, BINARY_KEY_SCHEMA.getFieldDataTypes()[1])
            : new GenericMapData(mb));

        return rowData;
    }

    @SuppressWarnings("unchecked")
    private void assertBinaryMapKeys(Object[] values) {
        // the top level binary column is written as its numeric representation
        Object b = values[0];
        assertTrue("unexpected value: " + b, b instanceof Number);
        assertEquals(BINARY_MAP_KEY_NUMBER, ((Number) b).longValue());

        // the keys of a map are the field names of a json object, so they must be strings even if
        // the key type is a binary type
        Map<Object, Object> mb = (Map<Object, Object>) values[1];
        assertEquals(3, mb.size());
        assertTrue("unexpected keys: " + mb.keySet(), mb.containsKey("66051"));
        assertTrue("unexpected keys: " + mb.keySet(), mb.containsKey("66052"));
        assertTrue("unexpected keys: " + mb.keySet(), mb.containsKey("66303"));
        for (Object key : mb.keySet()) {
            assertTrue("unexpected key: " + key, key instanceof String);
            assertEquals("v", String.valueOf(mb.get(key)));
        }
    }

    /**
     * The date values of the maps of a binary array must be formatted as strings: the elements of
     * the array are converted concurrently, so every date must be formatted independently, and a
     * null date value must be kept as null instead of being formatted.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testArrayDateMapTransformer() {
        StarRocksTableRowTransformer rowTransformer =
            createTransformer(DATE_MAP_ARRAY_SCHEMA, StarRocksDataType.JSON);

        Object[] mapElements = new Object[PARALLEL_DATE_ELEMENTS];
        List<List<String>> expectedDates = new ArrayList<>(PARALLEL_DATE_ELEMENTS);
        for (int i = 0; i < PARALLEL_DATE_ELEMENTS; i++) {
            Map<Object, Object> map = new HashMap<>();
            List<String> dates = new ArrayList<>(PARALLEL_DATES_PER_ELEMENT + 1);
            for (int j = 0; j < PARALLEL_DATES_PER_ELEMENT; j++) {
                // an independent date for every element and position, used as the key and the
                // value of the map
                LocalDate date = LocalDate.ofEpochDay(i * (PARALLEL_DATES_PER_ELEMENT + 1) + j + 1);
                dates.add(date.toString());
                map.put((int) date.toEpochDay(), (int) date.toEpochDay());
            }
            // a null date value must be kept as null instead of being formatted
            LocalDate nullDate = LocalDate.ofEpochDay(0);
            dates.add(nullDate.toString());
            map.put((int) nullDate.toEpochDay(), null);
            expectedDates.add(dates);
            mapElements[i] = toBinaryMap(map, DATE_MAP_TYPE);
        }

        ArrayData arrayData = new ArrayDataSerializer(DATE_MAP_TYPE.getLogicalType())
            .toBinaryArray(new GenericArrayData(mapElements));
        GenericRowData rowData = new GenericRowData(DATE_MAP_ARRAY_SCHEMA.getFieldCount());
        rowData.setField(0, arrayData);

        List<Object> elements = (List<Object>) rowTransformer.transform(rowData, false)[0];
        assertEquals(PARALLEL_DATE_ELEMENTS, elements.size());
        for (int i = 0; i < PARALLEL_DATE_ELEMENTS; i++) {
            Map<Object, Object> map = (Map<Object, Object>) elements.get(i);
            assertNotNull("unexpected element: " + i, map);
            List<String> dates = expectedDates.get(i);
            assertEquals("unexpected element: " + i, dates.size(), map.size());
            for (int j = 0; j < dates.size(); j++) {
                String date = dates.get(j);
                assertTrue("unexpected keys: " + map.keySet(), map.containsKey(date));
                if (j == PARALLEL_DATES_PER_ELEMENT) {
                    // the null date value must be kept as null
                    assertNull("unexpected value of " + date, map.get(date));
                } else {
                    // the date value must be formatted independently as LocalDate.toString()
                    assertEquals("unexpected value of " + date, date, String.valueOf(map.get(date)));
                }
            }
        }
    }

    /**
     * The metadata of the starrocks columns is only known for the top level columns, so the fields
     * of a nested row must be converted without looking up that metadata.
     */
    @Test
    public void testNestedRowValueWithColumnMetadataTransformer() {
        assertNestedRowValues(createTransformer(NESTED_ROW_VALUE_SCHEMA, StarRocksDataType.JSON)
            .transform(createNestedRowValueRowData(false), false));
        assertNestedRowValues(createTransformer(NESTED_ROW_VALUE_SCHEMA, StarRocksDataType.JSON)
            .transform(createNestedRowValueRowData(true), false));
    }

    private GenericRowData createNestedRowValueRowData(boolean binary) {
        GenericRowData rowData = new GenericRowData(NESTED_ROW_VALUE_SCHEMA.getFieldCount());
        Map<Object, Object> m = new HashMap<>();
        m.put(StringData.fromString("r"), createNestedRow());
        rowData.setField(0, binary
            ? toBinaryMap(m, NESTED_ROW_VALUE_SCHEMA.getFieldDataTypes()[0])
            : new GenericMapData(m));
        return rowData;
    }

    @SuppressWarnings("unchecked")
    private void assertNestedRowValues(Object[] values) {
        Map<Object, Object> m = (Map<Object, Object>) values[0];
        assertEquals(1, m.size());
        Map<String, Object> row = (Map<String, Object>) m.get("r");
        assertNotNull(row);
        assertEquals(7, ((Number) row.get("a")).intValue());
        // the field of the nested row must not be converted with the metadata of a top level column
        assertTrue("unexpected value: " + row.get("b"), row.get("b") instanceof String);
        assertEquals("{\"x\":1}", row.get("b"));
    }

    /**
     * The elements of a binary array of rows are serialized as json strings, and the metadata of
     * the top level column must not be applied to the fields of the nested rows.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testArrayRowValueWithColumnMetadataTransformer() {
        StarRocksTableRowTransformer rowTransformer =
            createTransformer(NESTED_ROW_ARRAY_SCHEMA, StarRocksDataType.JSON);
        ArrayData arrayData = new ArrayDataSerializer(NESTED_MAP_ROW_TYPE.getLogicalType())
            .toBinaryArray(new GenericArrayData(new Object[]{createNestedRow(), null}));
        GenericRowData rowData = new GenericRowData(NESTED_ROW_ARRAY_SCHEMA.getFieldCount());
        rowData.setField(0, arrayData);

        List<Object> elements = (List<Object>) rowTransformer.transform(rowData, false)[0];
        assertEquals(2, elements.size());
        // the row element is serialized as a json string
        assertTrue("unexpected element: " + elements.get(0), elements.get(0) instanceof String);
        JSONObject rowJson = JSON.parseObject((String) elements.get(0));
        assertEquals(7, rowJson.getIntValue("a"));
        // the field of the nested row must not be converted with the metadata of the top level column
        assertTrue("unexpected value: " + rowJson.get("b"), rowJson.get("b") instanceof String);
        assertEquals("{\"x\":1}", rowJson.get("b"));
        // the null element of the nested array must be kept as null
        assertNull(elements.get(1));
    }

    /**
     * The metadata of a top level column must keep its behaviour: a row column whose metadata is a
     * string is serialized to a json string, a json string column is parsed, and a string column
     * whose metadata is not a json is kept as a string.
     */
    @Test
    public void testTopLevelRowColumnWithMetadataTransformer() {
        StarRocksTableRowTransformer rowTransformer =
            createTransformer(TOP_LEVEL_ROW_SCHEMA, null);
        Map<String, StarRocksDataType> columns = new HashMap<>();
        columns.put("r", StarRocksDataType.STRING);
        columns.put("j", StarRocksDataType.JSON);
        columns.put("s", StarRocksDataType.VARCHAR);
        rowTransformer.setStarRocksColumns(columns);

        GenericRowData rowData = new GenericRowData(TOP_LEVEL_ROW_SCHEMA.getFieldCount());
        rowData.setField(0, createNestedRow());
        rowData.setField(1, StringData.fromString("{\"x\":1}"));
        rowData.setField(2, StringData.fromString("{\"x\":1}"));

        Object[] values = rowTransformer.transform(rowData, false);
        // a row column whose metadata is `string` is serialized to a json string
        assertTrue("unexpected value: " + values[0], values[0] instanceof String);
        JSONObject rJson = JSON.parseObject((String) values[0]);
        assertEquals(7, rJson.getIntValue("a"));
        // the field of the top level row must not be converted with the metadata of another column
        assertTrue("unexpected value: " + rJson.get("b"), rJson.get("b") instanceof String);
        assertEquals("{\"x\":1}", rJson.get("b"));
        // a string column whose metadata is `json` is parsed to a json object
        assertTrue("unexpected value: " + values[1], values[1] instanceof JSONObject);
        assertEquals(1, ((JSONObject) values[1]).getIntValue("x"));
        // a string column whose metadata is not a json is kept as a string
        assertEquals("{\"x\":1}", values[2]);
    }

    private static final LocalDate GENERIC_ARRAY_DATE_1 = LocalDate.of(2021, 5, 6);

    private static final LocalDate GENERIC_ARRAY_DATE_2 = LocalDate.of(2021, 5, 7);

    /**
     * Two keys longer than 8 bytes which collide in the numeric representation of the top level
     * binary column: the first byte of the first key and the last byte of the second key are
     * shifted into the same low byte, so both would be read as the number 1. The full contents of
     * the bytes must be part of the key.
     */
    private static final byte[] LONG_BINARY_MAP_KEY_1 =
        new byte[]{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

    private static final byte[] LONG_BINARY_MAP_KEY_2 =
        new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};

    private static final TableSchema LONG_BINARY_KEY_SCHEMA = TableSchema.builder()
        .field("mb", DataTypes.MAP(DataTypes.BINARY(LONG_BINARY_MAP_KEY_1.length), DataTypes.STRING()))
        .build();

    private static final DataType STRING_MAP_TYPE = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());

    private static final TableSchema GENERIC_ARRAY_SCHEMA = TableSchema.builder()
        .field("ad", DataTypes.ARRAY(DataTypes.DATE()))
        .field("am", DataTypes.ARRAY(STRING_MAP_TYPE))
        .field("ar", DataTypes.ARRAY(NESTED_MAP_ROW_TYPE))
        .build();

    /**
     * The keys of two different binary values longer than 8 bytes must not collide: the full
     * contents of the bytes are encoded, and the serialized json object keeps both entries.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testLongBinaryMapKeyTransformer() {
        StarRocksTableRowTransformer rowTransformer =
            createTransformer(LONG_BINARY_KEY_SCHEMA, StarRocksDataType.JSON);

        Map<Object, Object> mb = new HashMap<>();
        mb.put(LONG_BINARY_MAP_KEY_1, StringData.fromString("v1"));
        mb.put(LONG_BINARY_MAP_KEY_2, StringData.fromString("v2"));
        GenericRowData rowData = new GenericRowData(LONG_BINARY_KEY_SCHEMA.getFieldCount());
        rowData.setField(0, new GenericMapData(mb));

        Map<Object, Object> keys = (Map<Object, Object>) rowTransformer.transform(rowData, false)[0];
        assertEquals(2, keys.size());
        // the two keys collide in the numeric representation, so both must be encoded with their
        // full contents and kept apart
        String key1 = "0x010000000000000000";
        String key2 = "0x000000000000000001";
        assertTrue("unexpected keys: " + keys.keySet(), keys.containsKey(key1));
        assertTrue("unexpected keys: " + keys.keySet(), keys.containsKey(key2));
        assertEquals("v1", String.valueOf(keys.get(key1)));
        assertEquals("v2", String.valueOf(keys.get(key2)));

        // the two keys survive a real serialization of the payload
        JSONObject payload = JSON.parseObject(JSON.toJSONString(keys));
        assertEquals(2, payload.size());
        assertEquals("v1", payload.getString(key1));
        assertEquals("v2", payload.getString(key2));
    }

    /**
     * The elements of a generic array must be converted recursively according to the logical
     * type of the element, like the elements of a binary array, and a null element must be kept
     * as null.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testGenericArrayLogicalElementTransformer() {
        StarRocksTableRowTransformer rowTransformer =
            createTransformer(GENERIC_ARRAY_SCHEMA, StarRocksDataType.JSON);

        Map<Object, Object> nestedMap = new HashMap<>();
        nestedMap.put(StringData.fromString("k"), StringData.fromString("v"));

        GenericRowData rowData = new GenericRowData(GENERIC_ARRAY_SCHEMA.getFieldCount());
        rowData.setField(0, new GenericArrayData(new Object[]{
            (int) GENERIC_ARRAY_DATE_1.toEpochDay(), null, (int) GENERIC_ARRAY_DATE_2.toEpochDay()}));
        rowData.setField(1, new GenericArrayData(new Object[]{new GenericMapData(nestedMap), null}));
        rowData.setField(2, new GenericArrayData(new Object[]{createNestedRow(), null}));

        Object[] values = rowTransformer.transform(rowData, false);

        List<Object> dates = (List<Object>) values[0];
        assertEquals(3, dates.size());
        assertEquals(GENERIC_ARRAY_DATE_1.toString(), dates.get(0));
        assertNull(dates.get(1));
        assertEquals(GENERIC_ARRAY_DATE_2.toString(), dates.get(2));

        List<Object> maps = (List<Object>) values[1];
        assertEquals(2, maps.size());
        Map<Object, Object> map = (Map<Object, Object>) maps.get(0);
        assertNotNull(map);
        assertEquals("v", map.get("k"));
        assertNull(maps.get(1));

        List<Object> rows = (List<Object>) values[2];
        assertEquals(2, rows.size());
        // the row element is serialized as a json string, like the element of a binary array
        assertTrue("unexpected element: " + rows.get(0), rows.get(0) instanceof String);
        JSONObject rowJson = JSON.parseObject((String) rows.get(0));
        assertEquals(7, rowJson.getIntValue("a"));
        assertEquals("{\"x\":1}", rowJson.get("b"));
        assertNull(rows.get(1));
    }

    /**
     * The generic and the binary representation of the same array of dates must be converted in
     * the same way.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testGenericAndBinaryArrayEquivalenceTransformer() {
        StarRocksTableRowTransformer rowTransformer =
            createTransformer(GENERIC_ARRAY_SCHEMA, StarRocksDataType.JSON);

        Object[] elements = new Object[]{
            (int) GENERIC_ARRAY_DATE_1.toEpochDay(), null, (int) GENERIC_ARRAY_DATE_2.toEpochDay()};

        GenericRowData genericRowData = new GenericRowData(GENERIC_ARRAY_SCHEMA.getFieldCount());
        genericRowData.setField(0, new GenericArrayData(elements));

        GenericRowData binaryRowData = new GenericRowData(GENERIC_ARRAY_SCHEMA.getFieldCount());
        binaryRowData.setField(0, new ArrayDataSerializer(DataTypes.DATE().getLogicalType())
            .toBinaryArray(new GenericArrayData(elements)));

        assertEquals(
            (List<Object>) rowTransformer.transform(genericRowData, false)[0],
            (List<Object>) rowTransformer.transform(binaryRowData, false)[0]);
    }

    /**
     * The generic and the binary representation of the same array of maps and rows must be
     * converted in the same way: the payload of both representations must be identical, the key
     * of a generic map must be normalized to a string like the key of a binary map, and the row
     * element of both must be serialized as a json string.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testGenericAndBinaryArrayRowEquivalenceTransformer() {
        StarRocksTableRowTransformer rowTransformer =
            createTransformer(GENERIC_ARRAY_SCHEMA, StarRocksDataType.JSON);

        Map<Object, Object> nestedMap = new HashMap<>();
        nestedMap.put(StringData.fromString("k"), StringData.fromString("v"));
        Object[] mapElements = new Object[]{new GenericMapData(nestedMap), null};
        Object[] rowElements = new Object[]{createNestedRow(), null};

        GenericRowData genericRowData = new GenericRowData(GENERIC_ARRAY_SCHEMA.getFieldCount());
        genericRowData.setField(1, new GenericArrayData(mapElements));
        genericRowData.setField(2, new GenericArrayData(rowElements));

        GenericRowData binaryRowData = new GenericRowData(GENERIC_ARRAY_SCHEMA.getFieldCount());
        binaryRowData.setField(1, new ArrayDataSerializer(STRING_MAP_TYPE.getLogicalType())
            .toBinaryArray(new GenericArrayData(new Object[]{toBinaryMap(nestedMap, STRING_MAP_TYPE), null})));
        binaryRowData.setField(2, new ArrayDataSerializer(NESTED_MAP_ROW_TYPE.getLogicalType())
            .toBinaryArray(new GenericArrayData(rowElements)));

        Object[] genericValues = rowTransformer.transform(genericRowData, false);
        Object[] binaryValues = rowTransformer.transform(binaryRowData, false);
        assertEquals(binaryValues[1], genericValues[1]);
        assertEquals(binaryValues[2], genericValues[2]);
        Map<Object, Object> map = (Map<Object, Object>) ((List<Object>) genericValues[1]).get(0);
        assertTrue("unexpected key: " + map.keySet(), map.containsKey("k"));
        assertEquals("v", String.valueOf(map.get("k")));
        List<Object> rows = (List<Object>) genericValues[2];
        assertTrue("unexpected element: " + rows.get(0), rows.get(0) instanceof String);
        assertNull(rows.get(1));
    }

    private StarRocksTableRowTransformer createTransformer(TableSchema schema, StarRocksDataType columnType) {
        StarRocksTableRowTransformer rowTransformer = new StarRocksTableRowTransformer(null);
        rowTransformer.setRuntimeContext(null);
        rowTransformer.setTableSchema(schema);
        rowTransformer.setFastJsonWrapper(new JsonWrapper());
        if (columnType != null) {
            // the sink always provides the metadata of the starrocks columns
            Map<String, StarRocksDataType> columns = new HashMap<>();
            for (String name : schema.getFieldNames()) {
                columns.put(name, columnType);
            }
            rowTransformer.setStarRocksColumns(columns);
        }
        return rowTransformer;
    }

    private GenericRowData createNestedRow() {
        GenericRowData row = new GenericRowData(NESTED_MAP_ROW_TYPE.getLogicalType().getChildren().size());
        row.setField(0, 7);
        row.setField(1, StringData.fromString("{\"x\":1}"));
        return row;
    }

    private static MapData toBinaryMap(Map<Object, Object> map, DataType mapDataType) {
        MapType mapType = (MapType) mapDataType.getLogicalType();
        return new MapDataSerializer(mapType.getKeyType(), mapType.getValueType())
            .toBinaryMap(new GenericMapData(map));
    }

    private GenericRowData createRowData() {
        GenericRowData genericRowData = new GenericRowData(TABLE_SCHEMA.getFieldCount());
        genericRowData.setField(0, (byte)20);
        genericRowData.setField(1, StringData.fromString("xxxssss"));
        genericRowData.setField(2, TimestampData.fromTimestamp(Timestamp.valueOf("2021-02-02 12:22:22.01")));
        genericRowData.setField(3, (int)LocalDate.now().toEpochDay());
        genericRowData.setField(4, DecimalData.fromBigDecimal(BigDecimal.valueOf(1000), 10, 2));
        genericRowData.setField(5, (short)30);
        genericRowData.setField(6, StringData.fromString("ch"));
        return genericRowData;
    }
}
