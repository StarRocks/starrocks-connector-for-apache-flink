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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.tools.JsonWrapper;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StarRocksFlexibleRowTest {

    private static final String UNAVAILABLE = "__debezium_unavailable_value";
    private static final byte[] UNAVAILABLE_BYTES = UNAVAILABLE.getBytes(StandardCharsets.UTF_8);

    private static final TableSchema SCHEMA = TableSchema.builder()
            .field("id", DataTypes.BIGINT().notNull())
            .field("k2", DataTypes.STRING().notNull())
            .field("name", DataTypes.STRING())
            .field("doc", DataTypes.STRING())
            .field("bin", DataTypes.BINARY(UNAVAILABLE_BYTES.length))
            .field("v", DataTypes.INT())
            .primaryKey("id", "k2")
            .build();

    private static StarRocksSinkOptions.Builder plainOptions() {
        return StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", "db")
                .withProperty("table-name", "t")
                .withProperty("sink.properties.format", "json");
    }

    private static StarRocksSinkOptions.Builder flexibleOptions() {
        return plainOptions()
                .withProperty("sink.properties.partial_update", "true")
                .withProperty("sink.properties.partial_update_mode", "flexible_row");
    }

    private static StarRocksSinkOptions.Builder cdcOptions() {
        return flexibleOptions().withProperty("sink.flexible-partial-update.unavailable-value", UNAVAILABLE);
    }

    private static StarRocksTableRowTransformer transformer(StarRocksSinkOptions options) {
        StarRocksTableRowTransformer transformer = new StarRocksTableRowTransformer(null);
        transformer.setRuntimeContext(null);
        transformer.setTableSchema(SCHEMA);
        Map<String, StarRocksDataType> columns = new HashMap<>();
        columns.put("name", StarRocksDataType.STRING);
        columns.put("doc", StarRocksDataType.JSON);
        transformer.setStarRocksColumns(columns);
        transformer.setFastJsonWrapper(new JsonWrapper());
        if (options.isFlexiblePartialUpdate()) {
            transformer.setFlexibleRowProjection(FlexibleRowProjection.create(options, SCHEMA));
        }
        return transformer;
    }

    private static StarRocksISerializer serializer(StarRocksSinkOptions options) {
        options.enableUpsertDelete();
        StarRocksISerializer serializer = StarRocksSerializerFactory.createSerializer(options, SCHEMA.getFieldNames());
        serializer.open(new StarRocksISerializer.SerializerContext(new JsonWrapper()));
        return serializer;
    }

    private static GenericRowData row(RowKind kind, long id, String k2, String name, String doc, byte[] bin, Integer v) {
        return GenericRowData.ofKind(kind, id, StringData.fromString(k2), string(name), string(doc), bin, v);
    }

    private static StringData string(String value) {
        return value == null ? null : StringData.fromString(value);
    }

    private static JSONObject write(StarRocksSinkOptions options, RowData row) {
        String json = serializer(options).serialize(transformer(options).transform(row, true));
        return JSON.parseObject(json);
    }

    private static void assertKeys(JSONObject row, String... keys) {
        assertEquals(new HashSet<>(Arrays.asList(keys)), row.keySet());
    }

    @Test
    public void testNullIsWrittenExplicitly() {
        JSONObject written = write(cdcOptions().build(),
                row(RowKind.UPDATE_AFTER, 1L, "a", null, "{\"x\":1}", null, 5));

        assertKeys(written, "id", "k2", "name", "doc", "bin", "v", "__op");
        assertNull(written.get("name"));
        assertNull(written.get("bin"));
        assertEquals(1, written.getJSONObject("doc").getIntValue("x"));
        assertEquals(0, written.getIntValue("__op"));
    }

    @Test
    public void testNullIsWrittenWithoutAnUnavailableValueConfigured() {
        JSONObject written = write(flexibleOptions().build(),
                row(RowKind.UPDATE_AFTER, 1L, "a", null, UNAVAILABLE, null, 5));

        assertKeys(written, "id", "k2", "name", "doc", "bin", "v", "__op");
        // Without the option the value is ordinary data.
        assertEquals(UNAVAILABLE, written.getString("doc"));
    }

    @Test
    public void testNullInsideAJsonColumnIsKept() {
        JSONObject written = write(cdcOptions().build(),
                row(RowKind.UPDATE_AFTER, 1L, "a", "n", "{\"x\":null,\"y\":1}", null, 5));

        assertTrue(written.getJSONObject("doc").containsKey("x"));
    }

    @Test
    public void testUnavailableColumnsAreLeftOut() {
        JSONObject written = write(cdcOptions().build(),
                row(RowKind.UPDATE_AFTER, 1L, "a", UNAVAILABLE, UNAVAILABLE, UNAVAILABLE_BYTES, 2));

        assertKeys(written, "id", "k2", "v", "__op");
        assertEquals(2, written.getIntValue("v"));
    }

    @Test
    public void testOnlyAnExactMatchIsUnavailable() {
        JSONObject written = write(cdcOptions().build(),
                row(RowKind.UPDATE_AFTER, 1L, "a", UNAVAILABLE + " ", "", UNAVAILABLE_BYTES.clone(), 2));

        assertKeys(written, "id", "k2", "name", "doc", "v", "__op");
        assertEquals(UNAVAILABLE + " ", written.getString("name"));
        assertEquals("", written.getString("doc"));
    }

    @Test
    public void testUnavailableValueIsCheckedBeforeJsonParsing() {
        String jsonLikePlaceholder = "{\"unavailable\":true}";
        StarRocksSinkOptions options = flexibleOptions()
                .withProperty("sink.flexible-partial-update.unavailable-value", jsonLikePlaceholder)
                .build();

        JSONObject written = write(options,
                row(RowKind.UPDATE_AFTER, 1L, "a", "n", jsonLikePlaceholder, null, 2));

        assertFalse(written.containsKey("doc"));
    }

    @Test
    public void testKeyHoldingTheUnavailableValueIsKept() {
        JSONObject written = write(cdcOptions().build(),
                row(RowKind.UPDATE_AFTER, 1L, UNAVAILABLE, UNAVAILABLE, "{}", null, 2));

        assertEquals(UNAVAILABLE, written.getString("k2"));
        assertFalse(written.containsKey("name"));
    }

    @Test
    public void testDeleteWritesOnlyTheKeyAndOp() {
        // REPLICA IDENTITY DEFAULT: a delete has null in every non key column. A null would get the row
        // filtered for a NOT NULL column.
        JSONObject written = write(cdcOptions().build(),
                row(RowKind.DELETE, 1L, "a", null, null, null, null));
        assertKeys(written, "id", "k2", "__op");
        assertEquals(1, written.getIntValue("__op"));

        // ChangelogNormalize sends the last row it stored instead, which can hold the placeholder.
        written = write(cdcOptions().build(),
                row(RowKind.DELETE, 1L, "a", UNAVAILABLE, "{\"x\":1}", UNAVAILABLE_BYTES, 3));
        assertKeys(written, "id", "k2", "__op");

        written = write(flexibleOptions().build(), row(RowKind.DELETE, 1L, "a", "n", null, null, 3));
        assertKeys(written, "id", "k2", "__op");
    }

    @Test
    public void testBinaryRowData() {
        LogicalType[] types = Arrays.stream(SCHEMA.getFieldDataTypes())
                .map(DataType::getLogicalType)
                .toArray(LogicalType[]::new);
        RowData binaryRow = new RowDataSerializer(types)
                .toBinaryRow(row(RowKind.UPDATE_AFTER, 1L, "a", UNAVAILABLE, "{\"x\":1}", UNAVAILABLE_BYTES, 2));

        JSONObject written = write(cdcOptions().build(), binaryRow);

        assertKeys(written, "id", "k2", "doc", "v", "__op");
        assertEquals(1, written.getJSONObject("doc").getIntValue("x"));
    }

    @Test
    public void testInsertWithUnavailableValueIsWrittenAndCounted() {
        StarRocksSinkOptions options = cdcOptions().build();
        StarRocksTableRowTransformer transformer = transformer(options);
        SimpleCounter counter = new SimpleCounter();
        transformer.setMetricGroup(new UnregisteredMetricsGroup() {
            @Override
            public Counter counter(String name) {
                assertEquals(FlexibleRowProjection.COUNTER_UNAVAILABLE_VALUE_INSERT_ROWS, name);
                return counter;
            }
        });
        StarRocksISerializer serializer = serializer(options);

        JSONObject written = JSON.parseObject(serializer.serialize(transformer.transform(
                row(RowKind.INSERT, 1L, "a", "n", UNAVAILABLE, null, 2), true)));
        assertKeys(written, "id", "k2", "name", "bin", "v", "__op");
        assertEquals(1, counter.getCount());

        // No placeholder, or not an INSERT: nothing to report.
        transformer.transform(row(RowKind.INSERT, 2L, "a", "n", "{}", null, 2), true);
        transformer.transform(row(RowKind.UPDATE_AFTER, 3L, "a", "n", UNAVAILABLE, null, 2), true);
        transformer.transform(row(RowKind.DELETE, 4L, "a", UNAVAILABLE, UNAVAILABLE, null, 2), true);
        assertEquals(1, counter.getCount());
    }

    @Test
    public void testInsertWithUnavailableValueFailsWhenConfigured() {
        StarRocksSinkOptions options = cdcOptions()
                .withProperty("sink.flexible-partial-update.unavailable-value-on-insert", "fail")
                .build();
        StarRocksTableRowTransformer transformer = transformer(options);

        try {
            transformer.transform(row(RowKind.INSERT, 7L, "key-value", "n", UNAVAILABLE, null, 2), true);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("db.t"));
            assertTrue(expected.getMessage(), expected.getMessage().contains("[doc]"));
            assertTrue(expected.getMessage(), expected.getMessage().contains("key-value"));
            assertTrue(expected.getMessage(), expected.getMessage().contains("REPLICA IDENTITY FULL"));
        }

        // Rows that are not an INSERT with the placeholder still go through.
        transformer.transform(row(RowKind.INSERT, 2L, "a", "n", "{}", null, 2), true);
        transformer.transform(row(RowKind.UPDATE_AFTER, 3L, "a", UNAVAILABLE, UNAVAILABLE, null, 2), true);
    }

    @Test
    public void testFailureMessageHidesTheKeyWhenSanitizing() {
        StarRocksSinkOptions options = cdcOptions()
                .withProperty("sink.flexible-partial-update.unavailable-value-on-insert", "FAIL")
                .withProperty("sink.sanitize-error-log", "true")
                .build();

        try {
            transformer(options).transform(row(RowKind.INSERT, 7L, "key-value", "n", UNAVAILABLE, null, 2), true);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            assertFalse(expected.getMessage(), expected.getMessage().contains("key-value"));
            assertTrue(expected.getMessage(), expected.getMessage().contains("[doc]"));
        }
    }

    @Test
    public void testNonFlexibleOutputIsUnchanged() {
        StarRocksSinkOptions options = plainOptions().build();
        StarRocksISerializer serializer = serializer(options);
        assertTrue(serializer instanceof StarRocksJsonSerializer);

        Object[] values = transformer(options).transform(
                row(RowKind.UPDATE_AFTER, 1L, "a", null, UNAVAILABLE, null, 2), true);

        // Built the way the serializer has always built it: a null is dropped, the placeholder is data.
        Map<String, Object> expected = new HashMap<>(values.length);
        expected.put("id", 1L);
        expected.put("k2", "a");
        expected.put("name", null);
        expected.put("doc", UNAVAILABLE);
        expected.put("bin", null);
        expected.put("v", 2);
        expected.put("__op", 0);
        assertEquals(new JsonWrapper().toJSONString(expected), serializer.serialize(values));
        assertFalse(JSON.parseObject(serializer.serialize(values)).containsKey("name"));
    }

    @Test
    public void testGenericTransformerWithRowData() {
        StarRocksSinkOptions options = cdcOptions().build();
        StarRocksGenericRowTransformer<RowData> transformer = new StarRocksGenericRowTransformer<>((slots, row) -> {
            slots[0] = row.getLong(0);
            slots[1] = row.getString(1).toString();
            slots[2] = row.isNullAt(2) ? null : row.getString(2);
            slots[3] = row.isNullAt(3) ? null : row.getString(3).toString();
            slots[4] = row.isNullAt(4) ? null : row.getBinary(4);
            slots[5] = row.isNullAt(5) ? null : row.getInt(5);
        });
        transformer.setTableSchema(SCHEMA);
        transformer.setFlexibleRowProjection(FlexibleRowProjection.create(options, SCHEMA));
        StarRocksISerializer serializer = serializer(options);

        JSONObject update = JSON.parseObject(serializer.serialize(transformer.transform(
                row(RowKind.UPDATE_AFTER, 1L, "a", UNAVAILABLE, UNAVAILABLE, UNAVAILABLE_BYTES, null), true)));
        assertKeys(update, "id", "k2", "v", "__op");
        assertNull(update.get("v"));

        JSONObject delete = JSON.parseObject(serializer.serialize(transformer.transform(
                row(RowKind.DELETE, 1L, "a", "n", "d", null, 2), true)));
        assertKeys(delete, "id", "k2", "__op");

        StarRocksSinkOptions failing = cdcOptions()
                .withProperty("sink.flexible-partial-update.unavailable-value-on-insert", "fail")
                .build();
        transformer.setFlexibleRowProjection(FlexibleRowProjection.create(failing, SCHEMA));
        try {
            transformer.transform(row(RowKind.INSERT, 1L, "a", UNAVAILABLE, "d", null, 2), true);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("[name]"));
        }
    }

    @Test
    public void testGenericTransformerWithAUserRecord() {
        StarRocksSinkOptions options = cdcOptions().build();
        StarRocksGenericRowTransformer<Object[]> transformer = new StarRocksGenericRowTransformer<>(
                (slots, record) -> System.arraycopy(record, 0, slots, 0, record.length));
        transformer.setTableSchema(SCHEMA);
        transformer.setFlexibleRowProjection(FlexibleRowProjection.create(options, SCHEMA));
        StarRocksISerializer serializer = serializer(options);

        // The builder left __op unset: it must not be written as a null column.
        JSONObject upsert = JSON.parseObject(serializer.serialize(transformer.transform(
                new Object[] {1L, "a", UNAVAILABLE, null, null, 2}, true)));
        assertKeys(upsert, "id", "k2", "doc", "bin", "v");

        JSONObject delete = JSON.parseObject(serializer.serialize(transformer.transform(
                new Object[] {1L, "a", "n", null, null, 2, StarRocksSinkOP.DELETE.ordinal()}, true)));
        assertKeys(delete, "id", "k2", "__op");
    }

    @Test
    public void testProjectionSurvivesJobSerialization() throws Exception {
        StarRocksSinkOptions options = cdcOptions().build();
        // As built on the client. The runtime context is only set after deployment.
        StarRocksTableRowTransformer client = new StarRocksTableRowTransformer(null);
        client.setTableSchema(SCHEMA);
        client.setStarRocksColumns(new HashMap<>());
        client.setFlexibleRowProjection(FlexibleRowProjection.create(options, SCHEMA));
        StarRocksTableRowTransformer deployed = InstantiationUtil.clone(client);
        deployed.setRuntimeContext(null);
        deployed.setFastJsonWrapper(new JsonWrapper());
        deployed.setMetricGroup(new UnregisteredMetricsGroup());

        JSONObject written = JSON.parseObject(serializer(options).serialize(deployed.transform(
                row(RowKind.UPDATE_AFTER, 1L, "a", UNAVAILABLE, "{\"x\":1}", UNAVAILABLE_BYTES, 2), true)));

        assertKeys(written, "id", "k2", "doc", "v", "__op");
    }

    @Test
    public void testCustomTransformerMustOptIn() {
        StarRocksIRowTransformer<RowData> custom = new StarRocksIRowTransformer<RowData>() {
            @Override
            public void setStarRocksColumns(Map<String, StarRocksDataType> columns) {
            }

            @Override
            public void setTableSchema(TableSchema tableSchema) {
            }

            @Override
            public void setRuntimeContext(org.apache.flink.api.common.functions.RuntimeContext ctx) {
            }

            @Override
            public Object[] transform(RowData record, boolean supportUpsertDelete) {
                return new Object[0];
            }
        };

        try {
            custom.setFlexibleRowProjection(FlexibleRowProjection.create(cdcOptions().build(), SCHEMA));
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
            assertTrue(expected.getMessage().contains("flexible partial update"));
        }
    }
}
