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
