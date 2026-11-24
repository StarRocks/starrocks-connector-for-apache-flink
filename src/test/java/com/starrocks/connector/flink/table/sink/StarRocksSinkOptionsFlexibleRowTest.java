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

package com.starrocks.connector.flink.table.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.UserCodeClassLoader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.manager.StarRocksQueryVisitor;
import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.connector.flink.row.sink.FlexibleRowProjection;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;
import com.starrocks.connector.flink.row.sink.StarRocksTableRowTransformer;
import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.v2.RowDataSerializationSchema;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.mergecommit.MetricListener;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadListener;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StarRocksSinkOptionsFlexibleRowTest {

    private static final String UNAVAILABLE = "__debezium_unavailable_value";
    private static final String UNAVAILABLE_OPTION = "sink.flexible-partial-update.unavailable-value";
    private static final String ON_INSERT_OPTION = "sink.flexible-partial-update.unavailable-value-on-insert";

    private static StarRocksSinkOptions.Builder base() {
        return StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", "db")
                .withProperty("table-name", "t");
    }

    private static StarRocksSinkOptions.Builder flexible() {
        return base()
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.partial_update", "true")
                .withProperty("sink.properties.partial_update_mode", "flexible_row");
    }

    private static void assertRefused(StarRocksSinkOptions.Builder builder, String... fragments) {
        try {
            builder.build();
            fail("expected ValidationException");
        } catch (ValidationException expected) {
            for (String fragment : fragments) {
                assertTrue(expected.getMessage(), expected.getMessage().contains(fragment));
            }
        }
    }

    private static void mockVersion() {
        new MockUp<StarRocksSinkTable>() {
            @Mock
            public String getVersion() {
                return "3.3.0";
            }
        };
    }

    @Test
    public void testValidConfiguration() {
        StarRocksSinkOptions options = flexible()
                .withProperty(UNAVAILABLE_OPTION, UNAVAILABLE)
                .withProperty("sink.at-least-once.use-transaction-stream-load", "false")
                .withProperty("sink.ignore.update-before", "true")
                .withProperty("sink.version", "V2")
                .build();

        assertTrue(options.isFlexiblePartialUpdate());
        assertEquals(UNAVAILABLE, options.getFlexiblePartialUpdateUnavailableValue());
        assertEquals(StarRocksSinkOptions.UnavailableValueOnInsert.LOG,
                options.getFlexiblePartialUpdateUnavailableValueOnInsert());
        assertFalse(options.getSinkAtLeastOnceUseTransactionStreamLoad());
    }

    @Test
    public void testOtherModesAreNotFlexible() {
        assertFalse(base().build().isFlexiblePartialUpdate());
        StarRocksSinkOptions row = base()
                .withProperty("sink.properties.partial_update", "true")
                .withProperty("sink.properties.partial_update_mode", "row")
                .build();
        assertFalse(row.isFlexiblePartialUpdate());
        assertTrue(row.getSinkAtLeastOnceUseTransactionStreamLoad());
    }

    @Test
    public void testMisspelledModeIsRefused() {
        for (String mode : new String[] {"FLEXIBLE_ROW", "Flexible_Row", " flexible_row "}) {
            assertRefused(base().withProperty("sink.properties.partial_update_mode", mode),
                    "case sensitive", "flexible_row");
        }
    }

    @Test
    public void testFormatMustBeJson() {
        assertRefused(base().withProperty("sink.properties.partial_update", "true")
                .withProperty("sink.properties.partial_update_mode", "flexible_row"), "format=json");
        assertRefused(flexible().withProperty("sink.properties.format", "csv"), "format=json");
    }

    @Test
    public void testPartialUpdateMustBeTrue() {
        assertRefused(base().withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.partial_update_mode", "flexible_row"), "partial_update=true");
        assertRefused(flexible().withProperty("sink.properties.partial_update", "false"), "partial_update=true");
    }

    @Test
    public void testExactlyOnceIsRefused() {
        assertRefused(flexible().withProperty("sink.semantic", "exactly-once"), "exactly-once");
    }

    @Test
    public void testMultiTableTransactionIsRefused() {
        assertRefused(flexible().withProperty("sink.transaction.multi-table.enabled", "true"),
                "sink.transaction.multi-table.enabled");
    }

    @Test
    public void testMergeCommitIsRefused() {
        assertRefused(flexible().withProperty("sink.properties.enable_merge_commit", "true")
                .withProperty("sink.properties.merge_commit_interval_ms", "1000"), "merge commit");
    }

    @Test
    public void testSinkVersionV1IsRefused() {
        assertRefused(flexible().withProperty("sink.version", "V1"), "V1");
        assertRefused(flexible().withProperty("sink.version", "v1"), "V1");
    }

    @Test
    public void testTransactionStreamLoadIsRefused() {
        assertRefused(flexible().withProperty("sink.at-least-once.use-transaction-stream-load", "true"),
                "sink.at-least-once.use-transaction-stream-load");
    }

    @Test
    public void testHeadersThatConflictAreRefused() {
        for (String header : new String[] {"columns", "jsonpaths", "where", "merge_condition"}) {
            assertRefused(flexible().withProperty("sink.properties." + header, "x"), "sink.properties." + header);
        }
        // sink.properties keys are case insensitive.
        assertRefused(flexible().withProperty("sink.properties.COLUMNS", "x"), "sink.properties.columns");
    }

    @Test
    public void testColumnsFromFlinkSchemaIsRefused() {
        assertRefused(flexible().withProperty("sink.json.columns-from-flink-schema", "true"),
                "sink.json.columns-from-flink-schema");
    }

    @Test
    public void testKeepingUpdateBeforeIsRefused() {
        assertRefused(flexible().withProperty("sink.ignore.update-before", "false"), "sink.ignore.update-before");
    }

    @Test
    public void testUnavailableValueNeedsFlexibleMode() {
        assertRefused(base().withProperty("sink.properties.format", "json")
                .withProperty(UNAVAILABLE_OPTION, UNAVAILABLE), UNAVAILABLE_OPTION, "flexible_row");
    }

    @Test
    public void testEmptyUnavailableValueIsRefused() {
        assertRefused(flexible().withProperty(UNAVAILABLE_OPTION, ""), UNAVAILABLE_OPTION);
    }

    @Test
    public void testOnInsertNeedsAnUnavailableValue() {
        assertRefused(flexible().withProperty(ON_INSERT_OPTION, "fail"), ON_INSERT_OPTION, UNAVAILABLE_OPTION);
    }

    @Test
    public void testOnInsertValues() {
        assertEquals(StarRocksSinkOptions.UnavailableValueOnInsert.FAIL,
                flexible().withProperty(UNAVAILABLE_OPTION, UNAVAILABLE).withProperty(ON_INSERT_OPTION, "fail")
                        .build().getFlexiblePartialUpdateUnavailableValueOnInsert());
        assertEquals(StarRocksSinkOptions.UnavailableValueOnInsert.LOG,
                flexible().withProperty(UNAVAILABLE_OPTION, UNAVAILABLE).withProperty(ON_INSERT_OPTION, "LOG")
                        .build().getFlexiblePartialUpdateUnavailableValueOnInsert());
        try {
            flexible().withProperty(UNAVAILABLE_OPTION, UNAVAILABLE).withProperty(ON_INSERT_OPTION, "skip")
                    .build().getFlexiblePartialUpdateUnavailableValueOnInsert();
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // Flink's option parsing lists the valid values.
        }
    }

    @Test
    public void testNoColumnsHeaderAndNoTransaction() {
        mockVersion();
        StarRocksSinkOptions options = flexible().build();
        options.setTableSchemaFieldNames(new String[] {"a", "b"});
        options.enableUpsertDelete();
        // The server supports transaction stream load, and the option defaults to using it.
        options.setSupportTransactionStreamLoad(true);

        StreamLoadProperties properties =
                options.getProperties(StarRocksSinkTable.builder().sinkOptions(options).build());

        assertFalse(properties.isEnableTransaction());
        assertNull(properties.getTableProperties("db-t", "db", "t").getColumns());
        assertNull(properties.getHeaders().get("columns"));
        assertEquals("flexible_row", properties.getHeaders().get("partial_update_mode"));
        assertEquals("true", properties.getHeaders().get("partial_update"));
        assertEquals("true", properties.getHeaders().get("strip_outer_array"));
    }

    @Test
    public void testPlainJsonSinkStillUsesTransaction() {
        mockVersion();
        StarRocksSinkOptions options = base().withProperty("sink.properties.format", "json").build();
        options.setSupportTransactionStreamLoad(true);

        StreamLoadProperties properties =
                options.getProperties(StarRocksSinkTable.builder().sinkOptions(options).build());

        assertTrue(properties.isEnableTransaction());
    }

    private static void assertOverrideRefused(StarRocksSinkOptions options,
                                              StreamLoadTableProperties.Builder override, String fragment) {
        options.addTableProperties(override.database("db").table("other").build());
        try {
            options.getProperties(StarRocksSinkTable.builder().sinkOptions(options).build());
            fail("expected ValidationException");
        } catch (ValidationException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(fragment));
            assertTrue(expected.getMessage(), expected.getMessage().contains("db.other"));
        }
    }

    private static StreamLoadTableProperties.Builder jsonOverride() {
        return StreamLoadTableProperties.builder().streamLoadDataFormat(StreamLoadDataFormat.JSON);
    }

    @Test
    public void testTableOverridesAreChecked() {
        mockVersion();
        assertOverrideRefused(flexible().build(), jsonOverride().columns("`a`"), "columns");
        assertOverrideRefused(flexible().build(), jsonOverride().addProperty("where", "a > 1"), "where");
        assertOverrideRefused(flexible().build(), jsonOverride().addProperty("JSONPATHS", "[\"$.a\"]"), "jsonpaths");
        Map<String, String> common = new HashMap<>();
        common.put("merge_condition", "a");
        assertOverrideRefused(flexible().build(), jsonOverride().addCommonProperties(common), "merge_condition");
        assertOverrideRefused(flexible().build(), jsonOverride().addProperty("partial_update_mode", "row"), "turn it off");
        assertOverrideRefused(flexible().build(), jsonOverride().addProperty("partial_update", "false"), "turn it off");
        assertOverrideRefused(flexible().build(),
                StreamLoadTableProperties.builder().streamLoadDataFormat(new StreamLoadDataFormat.CSVFormat()), "json");
        assertOverrideRefused(flexible().build(), jsonOverride().addProperty("format", "csv"), "json");
    }

    @Test
    public void testOverrideCannotEnableFlexibleMode() {
        mockVersion();
        assertOverrideRefused(base().withProperty("sink.properties.format", "json").build(),
                jsonOverride().addProperty("partial_update_mode", "flexible_row"), "flexible_row");
    }

    @Test
    public void testPlainOverrideIsAccepted() {
        mockVersion();
        StarRocksSinkOptions options = flexible().build();
        options.addTableProperties(jsonOverride().database("db").table("other")
                .addProperty("partial_update_mode", "flexible_row")
                .addProperty("timeout", "60")
                .build());

        assertNotNull(options.getProperties(StarRocksSinkTable.builder().sinkOptions(options).build()));
    }

    @Test
    public void testMiniBatchIsRefusedWithAnUnavailableValue() {
        Configuration session = new Configuration();
        session.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        StarRocksSinkOptions options = flexible().withProperty(UNAVAILABLE_OPTION, UNAVAILABLE).build();

        try {
            StarRocksDynamicTableSinkFactory.checkNoMiniBatchWithUnavailableValue(options, session);
            fail("expected ValidationException");
        } catch (ValidationException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("table.exec.mini-batch.enabled"));
        }

        // Without a placeholder every row carries its full value, so keeping the last one is correct.
        StarRocksDynamicTableSinkFactory.checkNoMiniBatchWithUnavailableValue(flexible().build(), session);
        StarRocksDynamicTableSinkFactory.checkNoMiniBatchWithUnavailableValue(options, new Configuration());
    }

    @Test
    public void testOptionsAreRegisteredWithTheTableFactory() {
        assertTrue(new StarRocksDynamicTableSinkFactory().optionalOptions()
                .contains(StarRocksSinkOptions.SINK_FLEXIBLE_PARTIAL_UPDATE_UNAVAILABLE_VALUE));
        assertTrue(new StarRocksDynamicTableSinkFactory().optionalOptions()
                .contains(StarRocksSinkOptions.SINK_FLEXIBLE_PARTIAL_UPDATE_UNAVAILABLE_VALUE_ON_INSERT));
    }

    private static Map<String, Object> column(String name, String keyType, String type, long position) {
        Map<String, Object> column = new HashMap<>();
        column.put("COLUMN_NAME", name);
        column.put("COLUMN_KEY", keyType);
        column.put("DATA_TYPE", type);
        column.put("ORDINAL_POSITION", position);
        return column;
    }

    private static void mockColumns(List<Map<String, Object>> columns) {
        new MockUp<StarRocksQueryVisitor>() {
            @Mock
            public List<Map<String, Object>> getTableColumnsMetaData() {
                return columns;
            }

            @Mock
            public Map<String, StarRocksDataType> getFieldMapping() {
                Map<String, StarRocksDataType> mapping = new LinkedHashMap<>();
                for (Map<String, Object> column : columns) {
                    mapping.put(column.get("COLUMN_NAME").toString(),
                            StarRocksDataType.fromString(column.get("DATA_TYPE").toString()));
                }
                return mapping;
            }
        };
    }

    private static void validate(StarRocksSinkOptions options, TableSchema schema) {
        StarRocksSinkTable.builder().sinkOptions(options).build().validateTableStructure(options, schema);
    }

    @Test
    public void testOnlyPrimaryKeyTablesAreAccepted() {
        mockColumns(Arrays.asList(column("id", "DUP", "bigint", 1), column("v", "", "int", 2)));
        TableSchema schema = TableSchema.builder()
                .field("id", DataTypes.BIGINT()).field("v", DataTypes.INT()).build();

        try {
            validate(flexible().build(), schema);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("primary key"));
        }
    }

    @Test
    public void testColumnNamesMustMatchExactly() {
        mockColumns(Arrays.asList(column("id", "PRI", "bigint", 1), column("name", "", "varchar", 2)));
        TableSchema schema = TableSchema.builder()
                .field("id", DataTypes.BIGINT().notNull()).field("Name", DataTypes.STRING())
                .primaryKey("id").build();

        try {
            validate(flexible().build(), schema);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("'Name'"));
            assertTrue(expected.getMessage(), expected.getMessage().contains("'name'"));
        }

        // A non flexible load keeps matching names case insensitively.
        validate(base().withProperty("sink.properties.format", "json").build(), schema);
    }

    @Test
    public void testColumnTypesThatCannotCarryThePlaceholderAreRefused() {
        mockColumns(Arrays.asList(column("id", "PRI", "bigint", 1), column("tags", "", "string", 2),
                column("m", "", "string", 3), column("r", "", "string", 4)));
        TableSchema[] schemas = {
            TableSchema.builder().field("id", DataTypes.BIGINT().notNull())
                    .field("tags", DataTypes.ARRAY(DataTypes.STRING())).primaryKey("id").build(),
            TableSchema.builder().field("id", DataTypes.BIGINT().notNull())
                    .field("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())).primaryKey("id").build(),
            TableSchema.builder().field("id", DataTypes.BIGINT().notNull())
                    .field("r", DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT()))).primaryKey("id").build()
        };
        for (TableSchema schema : schemas) {
            try {
                validate(flexible().withProperty(UNAVAILABLE_OPTION, UNAVAILABLE).build(), schema);
                fail("expected IllegalArgumentException for " + schema);
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains(schema.getFieldNames()[1]));
                assertTrue(expected.getMessage(), expected.getMessage().contains("REPLICA IDENTITY FULL"));
            }
            // With no placeholder there is no unchanged TOAST value to protect.
            validate(flexible().build(), schema);
        }
    }

    @Test
    public void testStringAndScalarColumnsAreAcceptedWithThePlaceholder() {
        mockColumns(Arrays.asList(column("id", "PRI", "bigint", 1), column("created_at", "PRI", "datetime", 2),
                column("doc", "", "json", 3), column("v", "", "int", 4), column("d", "", "decimal", 5)));
        TableSchema schema = TableSchema.builder()
                .field("id", DataTypes.BIGINT().notNull())
                .field("created_at", DataTypes.TIMESTAMP(3).notNull())
                .field("doc", DataTypes.STRING())
                .field("v", DataTypes.INT())
                .field("d", DataTypes.DECIMAL(10, 2))
                .primaryKey("id", "created_at")
                .build();

        validate(flexible().withProperty(UNAVAILABLE_OPTION, UNAVAILABLE).build(), schema);
    }

    private static final TableSchema CDC_SCHEMA = TableSchema.builder()
            .field("id", DataTypes.BIGINT().notNull())
            .field("doc", DataTypes.STRING())
            .field("v", DataTypes.INT())
            .primaryKey("id")
            .build();

    private static GenericRowData cdcRow(RowKind kind, long id, String doc, Integer v) {
        return GenericRowData.ofKind(kind, id, doc == null ? null : StringData.fromString(doc), v);
    }

    private static void assertCdcRows(List<String> written) {
        assertEquals(written.toString(), 3, written.size());

        JSONObject unchangedToast = JSON.parseObject(written.get(0));
        assertEquals(new HashSet<>(Arrays.asList("id", "v", "__op")), unchangedToast.keySet());
        assertEquals(0, unchangedToast.getIntValue("__op"));

        JSONObject setToNull = JSON.parseObject(written.get(1));
        assertEquals(new HashSet<>(Arrays.asList("id", "doc", "v", "__op")), setToNull.keySet());
        assertNull(setToNull.get("doc"));

        JSONObject delete = JSON.parseObject(written.get(2));
        assertEquals(new HashSet<>(Arrays.asList("id", "__op")), delete.keySet());
        assertEquals(1, delete.getIntValue("__op"));
    }

    @Test
    public void testSinkFunctionV2WritesFlexibleRows() throws Exception {
        mockVersion();
        mockColumns(Arrays.asList(column("id", "PRI", "bigint", 1), column("doc", "", "json", 2),
                column("v", "", "int", 3)));
        List<String> written = new ArrayList<>();
        new MockUp<StreamLoadManagerV2>() {
            @Mock
            public void $init(StreamLoadProperties properties, boolean enableAutoCommit) {
                assertFalse(properties.isEnableTransaction());
                assertNull(properties.getTableProperties("db-t", "db", "t").getColumns());
            }

            @Mock
            public void setStreamLoadListener(StreamLoadListener streamLoadListener) {
            }

            @Mock
            public void setMetricListener(MetricListener metricListener) {
            }

            @Mock
            public void setLabelGeneratorFactory(LabelGeneratorFactory labelGeneratorFactory) {
            }

            @Mock
            public void init() {
            }

            @Mock
            public void write(String uniqueKey, String database, String table, String... rows) {
                written.addAll(Arrays.asList(rows));
            }

            @Mock
            public void flush() {
            }

            @Mock
            public StreamLoadSnapshot snapshot() {
                return null;
            }

            @Mock
            public boolean abort(StreamLoadSnapshot snapshot) {
                return true;
            }

            @Mock
            public void close() {
            }
        };
        StarRocksSinkOptions options = flexible().withProperty(UNAVAILABLE_OPTION, UNAVAILABLE).build();
        options.setSupportTransactionStreamLoad(true);
        StarRocksDynamicSinkFunctionV2<RowData> sink = new StarRocksDynamicSinkFunctionV2<>(
                options, CDC_SCHEMA, new StarRocksTableRowTransformer(null));

        try (OneInputStreamOperatorTestHarness<RowData, Object> harness =
                     new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink))) {
            harness.open();
            // An UPDATE_BEFORE must not reach StarRocks as a delete.
            harness.processElement(cdcRow(RowKind.UPDATE_BEFORE, 1L, "{\"old\":1}", 1), 0);
            harness.processElement(cdcRow(RowKind.UPDATE_AFTER, 1L, UNAVAILABLE, 2), 0);
            harness.processElement(cdcRow(RowKind.UPDATE_AFTER, 1L, null, 3), 0);
            harness.processElement(cdcRow(RowKind.DELETE, 1L, UNAVAILABLE, 3), 0);
        }

        assertCdcRows(written);
    }

    @Test
    public void testNewSinkApiWritesFlexibleRows() {
        mockColumns(Arrays.asList(column("id", "PRI", "bigint", 1), column("doc", "", "json", 2),
                column("v", "", "int", 3)));
        StarRocksSinkOptions options = flexible().withProperty(UNAVAILABLE_OPTION, UNAVAILABLE).build();
        validate(options, CDC_SCHEMA);
        StarRocksISerializer serializer =
                StarRocksSerializerFactory.createSerializer(options, CDC_SCHEMA.getFieldNames());
        StarRocksTableRowTransformer transformer = new StarRocksTableRowTransformer(null);
        transformer.setTableSchema(CDC_SCHEMA);
        transformer.setFlexibleRowProjection(FlexibleRowProjection.create(options, CDC_SCHEMA));
        RowDataSerializationSchema schema = new RowDataSerializationSchema("db", "t",
                options.supportUpsertDelete(), options.getIgnoreUpdateBefore(), serializer, transformer);
        schema.open(new SerializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return new UnregisteredMetricsGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return null;
            }
        }, null);

        assertNull(schema.serialize(cdcRow(RowKind.UPDATE_BEFORE, 1L, "{\"old\":1}", 1)));
        List<String> written = new ArrayList<>();
        for (RowData row : Arrays.<RowData>asList(
                cdcRow(RowKind.UPDATE_AFTER, 1L, UNAVAILABLE, 2),
                cdcRow(RowKind.UPDATE_AFTER, 1L, null, 3),
                cdcRow(RowKind.DELETE, 1L, UNAVAILABLE, 3))) {
            StarRocksRowData data = schema.serialize(row);
            written.add(data.getRow());
        }

        assertCdcRows(written);
    }

    @Test
    public void testRawJsonSinkOnlyNeedsTheHeaders() {
        mockVersion();
        StarRocksSinkOptions options = flexible().build();
        options.setSupportTransactionStreamLoad(true);

        StreamLoadProperties properties = options.getProperties(null);

        assertFalse(properties.isEnableTransaction());
        assertNull(properties.getTableProperties("db-t", "db", "t").getColumns());
        assertEquals("flexible_row", properties.getHeaders().get("partial_update_mode"));
    }
}
