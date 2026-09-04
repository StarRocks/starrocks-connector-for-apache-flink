/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StarRocksSinkOptionsColumnsFromFlinkSchemaTest {

    private StarRocksSinkOptions.Builder base() {
        return StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", "db")
                .withProperty("table-name", "t")
                .withProperty("sink.properties.format", "json");
    }

    @Test
    public void testDefaultIsOff() {
        assertFalse(base().build().isColumnsFromFlinkSchema());
        assertFalse(base().withProperty("sink.json.columns-from-flink-schema", "false")
                .build().isColumnsFromFlinkSchema());
    }

    @Test
    public void testOptionIsReadAndNotForwardedAsAHeader() {
        StarRocksSinkOptions options = base()
                .withProperty("sink.json.columns-from-flink-schema", "true")
                .build();

        assertTrue(options.isColumnsFromFlinkSchema());
        // A real option lives outside sink.properties, so nothing about it reaches the header map.
        assertNull(options.getSinkStreamLoadProperties().get("json.columns-from-flink-schema"));
    }

    @Test
    public void testNonBooleanValueIsRejected() {
        try {
            base().withProperty("sink.json.columns-from-flink-schema", "table_schema").build();
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // Flink's option parsing rejects it before the connector sees it.
        }
    }

    @Test
    public void testExplicitColumnsConflictIsRejected() {
        try {
            base().withProperty("sink.json.columns-from-flink-schema", "true")
                    .withProperty("sink.properties.columns", "`a`,`b`")
                    .build();
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("columns"));
        }
    }

    @Test
    public void testCsvIsRejected() {
        try {
            StarRocksSinkOptions.builder()
                    .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                    .withProperty("load-url", "127.0.0.1:8030")
                    .withProperty("username", "root")
                    .withProperty("password", "")
                    .withProperty("database-name", "db")
                    .withProperty("table-name", "t")
                    .withProperty("sink.json.columns-from-flink-schema", "true")
                    .build();
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("json"));
        }
    }

    @Test
    public void testJsonpathsIsRejected() {
        try {
            base().withProperty("sink.json.columns-from-flink-schema", "true")
                    .withProperty("sink.properties.jsonpaths", "[\"a\"]")
                    .build();
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("jsonpaths"));
        }
    }

    @Test
    public void testRawSinkWithoutAFlinkSchemaIsRefused() {
        StarRocksSinkOptions options = base()
                .withProperty("sink.json.columns-from-flink-schema", "true")
                .build();
        try {
            // The raw String sink never sets Flink schema field names.
            options.getProperties(null);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("needs a Flink schema"));
        }
    }

    @Test
    public void testHeaderCarriesTheFlinkFieldsWhenEnabled() {
        new MockUp<StarRocksSinkTable>() {
            @Mock
            public String getVersion() {
                return "3.3.0";
            }
        };

        StarRocksSinkOptions on = base()
                .withProperty("sink.json.columns-from-flink-schema", "true")
                .build();
        on.setTableSchemaFieldNames(new String[] {"a", "b"});
        on.enableUpsertDelete();
        StreamLoadTableProperties enabled = on.getProperties(StarRocksSinkTable.builder().sinkOptions(on).build())
                .getTableProperties("db-t", "db", "t");
        assertEquals("`a`,`b`,`__op`", enabled.getColumns());

        // Same job with the option off: json on a modern server sends no header at all.
        StarRocksSinkOptions off = base().build();
        off.setTableSchemaFieldNames(new String[] {"a", "b"});
        off.enableUpsertDelete();
        StreamLoadTableProperties disabled = off.getProperties(StarRocksSinkTable.builder().sinkOptions(off).build())
                .getTableProperties("db-t", "db", "t");
        assertNull(disabled.getColumns());
    }

    private Map<String, Object> column(String name, String keyType) {
        Map<String, Object> row = new HashMap<>();
        row.put("COLUMN_NAME", name);
        row.put("COLUMN_KEY", keyType);
        return row;
    }

    @Test
    public void testOmittingAKeyRowsMergeOnIsRejected() {
        // Unique and aggregate keys identify a row just as a primary key does, so filling one from
        // its default would give every row the same key and they would merge into one another.
        for (String keyType : new String[] {"PRI", "UNI", "AGG"}) {
            List<Map<String, Object>> starRocksColumns = Arrays.asList(
                    column("id", keyType), column("v", ""));
            try {
                StarRocksSinkOptions.checkMergingKeysDeclared(
                        starRocksColumns, new String[] {"v"}, "t");
                fail("expected IllegalArgumentException for " + keyType);
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().contains("id"));
            }
        }
    }

    @Test
    public void testOmittingADuplicateKeySortColumnIsAllowed() {
        // Nothing merges on a duplicate key table's sort key, so its default may fire like any
        // other column's.
        List<Map<String, Object>> starRocksColumns = Arrays.asList(
                column("event_day", "DUP"), column("payload", ""));

        StarRocksSinkOptions.checkMergingKeysDeclared(starRocksColumns, new String[] {"payload"}, "t");
    }

    @Test
    public void testDeclaringTheKeyIsAccepted() {
        List<Map<String, Object>> starRocksColumns = Arrays.asList(
                column("id", "UNI"), column("v", ""));

        StarRocksSinkOptions.checkMergingKeysDeclared(starRocksColumns, new String[] {"ID", "v"}, "t");
    }

    @Test
    public void testSameTableOverrideWithJsonpathsIsRejected() throws Exception {
        new MockUp<StarRocksSinkTable>() {
            @Mock
            public String getVersion() {
                return "3.3.0";
            }
        };

        StarRocksSinkOptions options = base()
                .withProperty("sink.json.columns-from-flink-schema", "true")
                .build();
        options.setTableSchemaFieldNames(new String[] {"a", "b"});
        // The global validation ran at construction, before this override existed, so it never saw
        // these jsonpaths.
        options.addTableProperties(StreamLoadTableProperties.builder()
                .database("db").table("t")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addProperty("jsonpaths", "[\"a\"]")
                .build());

        try {
            options.getProperties(StarRocksSinkTable.builder().sinkOptions(options).build());
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("jsonpaths"));
            assertTrue(expected.getMessage().contains("db.t"));
        }
    }

    @Test
    public void testQuotingEscapesRatherThanNormalises() {
        // Stripping a backtick or trimming whitespace renames the column, and the serializer still
        // emits the original Flink field name, so the header would point at a column that is not there.
        assertEquals("`c1`", StarRocksSinkOptions.quoteColumnName("c1"));
        assertEquals("`we``ird`", StarRocksSinkOptions.quoteColumnName("we`ird"));
        assertEquals("` padded `", StarRocksSinkOptions.quoteColumnName(" padded "));
    }

    @Test
    public void testOverrideForTheSameTableInheritsTheDerivedHeader() throws Exception {
        new MockUp<StarRocksSinkTable>() {
            @Mock
            public String getVersion() {
                return "3.3.0";
            }
        };

        StarRocksSinkOptions options = base()
                .withProperty("sink.json.columns-from-flink-schema", "true")
                .build();
        options.setTableSchemaFieldNames(new String[] {"a", "b"});
        // An override registered for this sink's own table would otherwise be selected by the sdk
        // ahead of the default properties, and it carries no columns, so the option would do nothing.
        options.addTableProperties(StreamLoadTableProperties.builder()
                .database("db").table("t")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .addProperty("max_filter_ratio", "0.1")
                .build());

        StreamLoadProperties properties =
                options.getProperties(StarRocksSinkTable.builder().sinkOptions(options).build());
        StreamLoadTableProperties resolved = properties.getTableProperties("db-t", "db", "t");

        assertEquals("`a`,`b`", resolved.getColumns());
        // The override's own headers must survive the rebuild.
        assertEquals("0.1", resolved.getProperties().get("max_filter_ratio"));
    }

    @Test
    public void testOverrideForAnotherTableDoesNotInheritTheHeader() throws Exception {
        new MockUp<StarRocksSinkTable>() {
            @Mock
            public String getVersion() {
                return "3.3.0";
            }
        };

        StarRocksSinkOptions options = base()
                .withProperty("sink.json.columns-from-flink-schema", "true")
                .build();
        options.setTableSchemaFieldNames(new String[] {"a", "b"});
        // A different table's columns are not this sink's Flink schema, so handing it this header
        // would name that table's columns wrongly.
        options.addTableProperties(StreamLoadTableProperties.builder()
                .database("db").table("other")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build());

        StreamLoadProperties properties =
                options.getProperties(StarRocksSinkTable.builder().sinkOptions(options).build());

        assertNull(properties.getTableProperties("db-other", "db", "other").getColumns());
    }

    @Test
    public void testFlinkSchemaForcesTheHeaderForJson() {
        // Today: json on a modern server sends no header even for a primary key table.
        assertFalse(StarRocksSinkOptions.shouldSendColumnsHeader(false, false, true, true, false));
        assertFalse(StarRocksSinkOptions.shouldSendColumnsHeader(false, false, false, true, false));
        // With the option: the header is always sent, so the Flink schema decides the columns.
        assertTrue(StarRocksSinkOptions.shouldSendColumnsHeader(false, false, true, true, true));
        assertTrue(StarRocksSinkOptions.shouldSendColumnsHeader(false, false, false, true, true));
    }

    @Test
    public void testExistingDecisionsAreUnchangedWithoutTheOption() {
        // json, primary key table, StarRocks 1.x: header sent, as before
        assertTrue(StarRocksSinkOptions.shouldSendColumnsHeader(false, false, true, false, false));
        // csv aligned: no header; csv unaligned: header
        assertFalse(StarRocksSinkOptions.shouldSendColumnsHeader(true, true, false, false, false));
        assertTrue(StarRocksSinkOptions.shouldSendColumnsHeader(true, false, false, false, false));
    }
}
