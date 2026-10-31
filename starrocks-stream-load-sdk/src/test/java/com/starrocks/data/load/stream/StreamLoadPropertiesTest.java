/*
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

package com.starrocks.data.load.stream;

import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class StreamLoadPropertiesTest {

    private StreamLoadProperties.Builder createBaseBuilder() {
        return StreamLoadProperties.builder()
                .jdbcUrl("jdbc:mysql://localhost:9030")
                .loadUrls("http://localhost:8030")
                .username("root")
                .password("")
                .defaultTableProperties(StreamLoadTableProperties.builder()
                        .database("db").table("tbl").build());
    }

    @Test
    public void testCheckLabelStateDefaultValues() {
        StreamLoadProperties props = createBaseBuilder().build();

        // Verify default values for check label state options
        assertEquals(500, props.getCheckLabelStateInitDelayMs());
        assertEquals(500, props.getCheckLabelStateIntervalMs());
        assertEquals(-1, props.getCheckLabelStateTimeoutMs());
    }

    @Test
    public void testSetCheckLabelStateInitDelayMs() {
        StreamLoadProperties props = createBaseBuilder()
                .setCheckLabelStateInitDelayMs(1000)
                .build();

        assertEquals(1000, props.getCheckLabelStateInitDelayMs());
    }

    @Test
    public void testSetCheckLabelStateIntervalMs() {
        StreamLoadProperties props = createBaseBuilder()
                .setCheckLabelStateIntervalMs(2000)
                .build();

        assertEquals(2000, props.getCheckLabelStateIntervalMs());
    }

    @Test
    public void testSetCheckLabelStateTimeoutMs() {
        StreamLoadProperties props = createBaseBuilder()
                .setCheckLabelStateTimeoutMs(60000)
                .build();

        assertEquals(60000, props.getCheckLabelStateTimeoutMs());
    }

    @Test
    public void testSetAllCheckLabelStateOptions() {
        StreamLoadProperties props = createBaseBuilder()
                .setCheckLabelStateInitDelayMs(100)
                .setCheckLabelStateIntervalMs(200)
                .setCheckLabelStateTimeoutMs(30000)
                .build();

        assertEquals(100, props.getCheckLabelStateInitDelayMs());
        assertEquals(200, props.getCheckLabelStateIntervalMs());
        assertEquals(30000, props.getCheckLabelStateTimeoutMs());
    }

    @Test
    public void testMergeCommitHttpOptions() {
        StreamLoadProperties props = createBaseBuilder()
                .setHttpThreadNum(5)
                .setHttpMaxConnectionsPerRoute(10)
                .setHttpTotalMaxConnections(50)
                .setHttpIdleConnectionTimeoutMs(30000)
                .setNodeMetaUpdateIntervalMs(3000)
                .setMaxConcurrentRequests(100)
                .setBackendDirectConnection(true)
                .build();

        assertEquals(5, props.getHttpThreadNum());
        assertEquals(10, props.getHttpMaxConnectionsPerRoute());
        assertEquals(50, props.getHttpTotalMaxConnections());
        assertEquals(30000, props.getHttpIdleConnectionTimeoutMs());
        assertEquals(3000, props.getNodeMetaUpdateIntervalMs());
        assertEquals(100, props.getMaxConcurrentRequests());
        assertEquals(true, props.isBackendDirectConnection());
    }

    @Test
    public void testMergeCommitHttpOptionsDefaultValues() {
        StreamLoadProperties props = createBaseBuilder().build();

        assertEquals(3, props.getHttpThreadNum());
        assertEquals(3, props.getHttpMaxConnectionsPerRoute());
        assertEquals(30, props.getHttpTotalMaxConnections());
        assertEquals(60000, props.getHttpIdleConnectionTimeoutMs());
        assertEquals(2000, props.getNodeMetaUpdateIntervalMs());
        assertEquals(-1, props.getMaxConcurrentRequests());
        assertFalse(props.isBackendDirectConnection());
    }

    @Test
    public void testBlackholeOption() {
        StreamLoadProperties props = createBaseBuilder()
                .setBlackhole(true)
                .build();

        assertEquals(true, props.isBlackhole());
    }

    @Test
    public void testBlackholeDefaultValue() {
        StreamLoadProperties props = createBaseBuilder().build();

        assertFalse(props.isBlackhole());
    }

    @Test
    public void testPublishTimeoutMsDefaultValue() {
        StreamLoadProperties props = createBaseBuilder().build();

        assertEquals(-1, props.getPublishTimeoutMs());
    }

    @Test
    public void testSetPublishTimeoutMs() {
        StreamLoadProperties props = createBaseBuilder()
                .setPublishTimeoutMs(10000)
                .build();

        assertEquals(10000, props.getPublishTimeoutMs());
    }

    // Expanding a wildcard default onto a concrete table must keep the headers added to the default.
    @Test
    public void testExpandingWildcardDefaultKeepsAddedProperties() {
        StreamLoadTableProperties wildcardDefault = StreamLoadTableProperties.builder()
                .database("*")
                .table("*")
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("partial_update", "true")
                .addCommonProperties(Collections.singletonMap("timeout", "60"))
                .build();
        StreamLoadProperties props = createBaseBuilder().defaultTableProperties(wildcardDefault).build();

        StreamLoadTableProperties expanded = props.getTableProperties("db-t", "db", "t");

        assertEquals("json", expanded.getProperty("format").orElse(null));
        assertEquals("true", expanded.getProperty("strip_outer_array").orElse(null));
        assertEquals("true", expanded.getProperty("partial_update").orElse(null));
        assertEquals("60", expanded.getProperty("timeout").orElse(null));
        assertEquals("60", expanded.getCommonProperties().get("timeout"));
    }

    @Test
    public void testExpandedDefaultBelongsToTheDestinationTable() {
        StreamLoadTableProperties wildcardDefault = StreamLoadTableProperties.builder()
                .database("*")
                .table("*")
                .addProperty("format", "json")
                .build();
        StreamLoadProperties props = createBaseBuilder().defaultTableProperties(wildcardDefault).build();

        StreamLoadTableProperties expanded = props.getTableProperties("db-t", "db", "t");

        assertEquals("db", expanded.getDatabase());
        assertEquals("t", expanded.getTable());
        // build() writes this builder's own db/table over the copied ones.
        assertEquals("db", expanded.getProperty("db").orElse(null));
        assertEquals("t", expanded.getProperty("table").orElse(null));
        assertEquals(StreamLoadUtils.getTableUniqueKey("db", "t"), expanded.getUniqueKey());
        assertFalse(wildcardDefault.getUniqueKey().equals(expanded.getUniqueKey()));
    }

    @Test
    public void testCopiedColumnsPropertyIsOverriddenByBuilderColumns() {
        StreamLoadTableProperties wildcardDefault = StreamLoadTableProperties.builder()
                .database("*")
                .table("*")
                .addProperty("columns", "`a`,`b`")
                .build();
        StreamLoadProperties props = createBaseBuilder().defaultTableProperties(wildcardDefault).build();

        StreamLoadTableProperties expanded = props.getTableProperties("db-t", "db", "t");
        assertEquals("`a`,`b`", expanded.getProperty("columns").orElse(null));
        assertNull(expanded.getColumns());

        StreamLoadTableProperties overridden = StreamLoadTableProperties.builder()
                .copyFrom(wildcardDefault)
                .database("db")
                .table("t")
                .columns("`c`")
                .build();
        assertEquals("`c`", overridden.getColumns());
        assertEquals("`c`", overridden.getProperty("columns").orElse(null));
        assertEquals("db", overridden.getProperty("db").orElse(null));
        assertEquals("t", overridden.getProperty("table").orElse(null));
    }

    @Test
    public void testCopyFromDoesNotShareMapsWithTheSource() {
        StreamLoadTableProperties source = StreamLoadTableProperties.builder()
                .database("db")
                .table("src")
                .addProperty("format", "json")
                .build();
        StreamLoadTableProperties.Builder builder = StreamLoadTableProperties.builder().copyFrom(source);

        source.getProperties().put("format", "csv");
        source.getProperties().put("added-later", "true");
        source.getTableProperties().put("added-later", "true");

        StreamLoadTableProperties copy = builder.database("db").table("dst").build();
        assertEquals("json", copy.getProperty("format").orElse(null));
        assertNull(copy.getProperty("added-later").orElse(null));
        assertFalse(copy.getTableProperties().containsKey("added-later"));
        assertEquals("csv", source.getProperty("format").orElse(null));
    }

    // A clone that resets columns must not keep sending the source table's columns header, or the
    // destination would be loaded with the source's column mapping instead of the server default.
    @Test
    public void testCopyFromResettingColumnsDropsTheGeneratedColumnsHeader() {
        StreamLoadTableProperties source = StreamLoadTableProperties.builder()
                .database("db")
                .table("src")
                .columns("`a`,`b`")
                .addProperty("max_filter_ratio", "0.1")
                .build();
        assertEquals("`a`,`b`", source.getColumns());
        assertEquals("`a`,`b`", source.getProperty("columns").orElse(null));

        StreamLoadTableProperties copy = StreamLoadTableProperties.builder()
                .copyFrom(source)
                .database("db")
                .table("dst")
                .columns(null)
                .build();

        assertNull(copy.getColumns());
        assertFalse(copy.getProperties().containsKey("columns"));
        assertNull(copy.getProperty("columns").orElse(null));
        // The other headers of the copied table, and the destination names, still survive.
        assertEquals("0.1", copy.getProperty("max_filter_ratio").orElse(null));
        assertEquals("db", copy.getProperty("db").orElse(null));
        assertEquals("dst", copy.getProperty("table").orElse(null));
        assertEquals("`a`,`b`", source.getProperty("columns").orElse(null));
    }

    @Test
    public void testCopyFromKeepsTheColumnsHeaderWhenTheFieldIsNotReset() {
        StreamLoadTableProperties source = StreamLoadTableProperties.builder()
                .database("db")
                .table("src")
                .columns("`a`,`b`")
                .build();

        StreamLoadTableProperties copy = StreamLoadTableProperties.builder()
                .copyFrom(source)
                .database("db")
                .table("dst")
                .build();

        assertEquals("`a`,`b`", copy.getColumns());
        assertEquals("`a`,`b`", copy.getProperty("columns").orElse(null));
    }

    @Test
    public void testCopyFromExplicitColumnsOverrideTheCopiedHeader() {
        StreamLoadTableProperties source = StreamLoadTableProperties.builder()
                .database("db")
                .table("src")
                .columns("`a`,`b`")
                .build();

        StreamLoadTableProperties copy = StreamLoadTableProperties.builder()
                .copyFrom(source)
                .database("db")
                .table("dst")
                .columns("`c`")
                .build();

        assertEquals("`c`", copy.getColumns());
        assertEquals("`c`", copy.getProperty("columns").orElse(null));
    }

    // A `columns` header added as a raw property is not generated by build(), so a copy keeps it
    // even when the columns field is reset. This is the behaviour the wildcard default relies on.
    @Test
    public void testCopyFromKeepsRawColumnsPropertyWhenTheFieldIsReset() {
        StreamLoadTableProperties source = StreamLoadTableProperties.builder()
                .database("db")
                .table("src")
                .addProperty("columns", "`a`,`b`")
                .build();
        assertNull(source.getColumns());

        StreamLoadTableProperties copy = StreamLoadTableProperties.builder()
                .copyFrom(source)
                .database("db")
                .table("dst")
                .columns(null)
                .build();

        assertNull(copy.getColumns());
        assertEquals("`a`,`b`", copy.getProperty("columns").orElse(null));
    }
    // HTTP header names are case insensitive, so a `Columns` header copied from the source and the
    // `columns` header build() derives from the destination's columns field are one header, sent
    // twice. The explicit destination field is the one that must survive.
    @Test
    public void testCopyFromDropsCopiedMixedCaseColumnsHeaderWhenTheFieldIsSet() {
        StreamLoadTableProperties source = StreamLoadTableProperties.builder()
                .database("db")
                .table("src")
                .addProperty("Columns", "`a`,`b`")
                .build();
        assertNull(source.getColumns());
        assertEquals("`a`,`b`", source.getProperties().get("Columns"));

        StreamLoadTableProperties copy = StreamLoadTableProperties.builder()
                .copyFrom(source)
                .database("db2")
                .table("t2")
                .columns("`c`")
                .build();

        assertEquals("`c`", copy.getColumns());
        assertEquals("`c`", copy.getProperty("columns").orElse(null));
        assertFalse(copy.getProperties().containsKey("Columns"));
        assertEquals(1, countIgnoreCase(copy.getProperties(), "columns"));
    }

    // The same holds for db and table, whose fields build() always derives from: a copy sent to
    // another table must not carry the source's spelling of them.
    @Test
    public void testCopyFromDropsCopiedMixedCaseDbTableHeadersWhenTheFieldsChange() {
        StreamLoadTableProperties source = StreamLoadTableProperties.builder()
                .database("db")
                .table("src")
                .addProperty("DB", "stale_db")
                .addProperty("Table", "stale_table")
                .build();

        StreamLoadTableProperties copy = StreamLoadTableProperties.builder()
                .copyFrom(source)
                .database("db2")
                .table("t2")
                .build();

        assertEquals("db2", copy.getDatabase());
        assertEquals("t2", copy.getTable());
        assertEquals("db2", copy.getProperty("db").orElse(null));
        assertEquals("t2", copy.getProperty("table").orElse(null));
        assertFalse(copy.getProperties().containsKey("DB"));
        assertFalse(copy.getProperties().containsKey("Table"));
        assertEquals(1, countIgnoreCase(copy.getProperties(), "db"));
        assertEquals(1, countIgnoreCase(copy.getProperties(), "table"));
    }

    // LoadParameters puts the common map on the wire before the per table map, so a reserved header
    // left in the common map would also reach the server next to the derived per table one.
    @Test
    public void testCopyFromDropsCopiedMixedCaseReservedHeadersFromTheCommonMap() {
        Map<String, String> common = new HashMap<>();
        common.put("COLUMNS", "`a`,`b`");
        common.put("DB", "stale_db");
        common.put("TABLE", "stale_table");
        common.put("Warehouse", "wh1");
        StreamLoadTableProperties source = StreamLoadTableProperties.builder()
                .database("db")
                .table("src")
                .addCommonProperties(common)
                .build();
        assertEquals("`a`,`b`", source.getCommonProperties().get("COLUMNS"));

        StreamLoadTableProperties copy = StreamLoadTableProperties.builder()
                .copyFrom(source)
                .database("db2")
                .table("t2")
                .columns("`c`")
                .build();

        assertFalse(copy.getCommonProperties().containsKey("COLUMNS"));
        assertFalse(copy.getCommonProperties().containsKey("DB"));
        assertFalse(copy.getCommonProperties().containsKey("TABLE"));
        // A header the destination does not derive keeps the caller's spelling.
        assertEquals("wh1", copy.getCommonProperties().get("Warehouse"));

        Map<String, String> headers = mergedHeaders(copy);
        assertEquals("`c`", headers.get("columns"));
        assertEquals("db2", headers.get("db"));
        assertEquals("t2", headers.get("table"));
        assertEquals(1, countIgnoreCase(headers, "columns"));
        assertEquals(1, countIgnoreCase(headers, "db"));
        assertEquals(1, countIgnoreCase(headers, "table"));
    }

    // Nothing is derived when the destination leaves the columns field null, so a raw `Columns`
    // header survives the copy whatever its spelling.
    @Test
    public void testCopyFromKeepsRawMixedCaseColumnsPropertyWhenTheFieldIsNull() {
        StreamLoadTableProperties source = StreamLoadTableProperties.builder()
                .database("db")
                .table("src")
                .addProperty("Columns", "`x`,`y`")
                .addProperty("max_filter_ratio", "0.1")
                .build();
        assertNull(source.getColumns());

        StreamLoadTableProperties copy = StreamLoadTableProperties.builder()
                .copyFrom(source)
                .database("db2")
                .table("t2")
                .columns(null)
                .build();

        assertNull(copy.getColumns());
        assertEquals("`x`,`y`", copy.getProperties().get("Columns"));
        assertEquals(1, countIgnoreCase(copy.getProperties(), "columns"));
        assertEquals("0.1", copy.getProperty("max_filter_ratio").orElse(null));
        assertEquals("db2", copy.getProperty("db").orElse(null));
        assertEquals("t2", copy.getProperty("table").orElse(null));
    }

    @Test
    public void testCopyFromKeepsRawMixedCaseColumnsCommonPropertyWhenTheFieldIsNull() {
        StreamLoadTableProperties source = StreamLoadTableProperties.builder()
                .database("db")
                .table("src")
                .addCommonProperties(Collections.singletonMap("COLUMNS", "`x`,`y`"))
                .build();

        StreamLoadTableProperties copy = StreamLoadTableProperties.builder()
                .copyFrom(source)
                .database("db2")
                .table("t2")
                .columns(null)
                .build();

        assertNull(copy.getColumns());
        assertEquals("`x`,`y`", copy.getCommonProperties().get("COLUMNS"));
        assertFalse(copy.getProperties().containsKey("columns"));
        assertEquals(1, countIgnoreCase(copy.getCommonProperties(), "columns"));
    }

    private static int countIgnoreCase(Map<String, String> properties, String name) {
        int count = 0;
        for (String key : properties.keySet()) {
            if (name.equalsIgnoreCase(key)) {
                count++;
            }
        }
        return count;
    }

    // Mirrors what LoadParameters does before the headers reach the wire: the common map first, then
    // the per table map, so the per table entry wins a name both maps carry.
    private static Map<String, String> mergedHeaders(StreamLoadTableProperties properties) {
        Map<String, String> headers = new HashMap<>(properties.getCommonProperties());
        headers.putAll(properties.getProperties());
        return headers;
    }
}
