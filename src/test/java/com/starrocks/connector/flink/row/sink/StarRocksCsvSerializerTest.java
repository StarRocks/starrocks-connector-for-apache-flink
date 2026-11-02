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

import com.starrocks.connector.flink.StarRocksSinkBaseTest;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.tools.JsonWrapper;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


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

    @Test
    public void testEncloseWrapsEveryNonNullFieldAndDoublesTheEncloseChar() {
        StarRocksISerializer serializer = csvSerializer(props("enclose", "'"));
        String row = serializer.serialize(new Object[]{"a'b", "c\td", "e\nf", null, "", "\\N"});
        // The reader also interprets an enclosed literal backslash-N as NULL; that ambiguity is unchanged.
        assertEquals("'a''b'\t'c\td'\t'e\nf'\t\\N\t''\t'\\N'", row);
    }

    @Test
    public void testEscapeIsUsedForTheEncloseCharWhenBothAreConfigured() {
        StarRocksISerializer serializer = csvSerializer(props("enclose", "'", "escape", "\\"));
        String row = serializer.serialize(new Object[]{"a'b", "c\\d", "e\tf", "g\nh", null});
        assertEquals("'a\\'b'\t'c\\\\d'\t'e\tf'\t'g\nh'\t\\\\N", row);
    }

    @Test
    public void testNullMarkersEscapePrefixForABackslashEscape() {
        StarRocksISerializer escapeOnly = csvSerializer(props("escape", "\\"));
        String row = escapeOnly.serialize(new Object[]{null});
        assertEquals("\\\\N", row);
        assertArrayEquals(new byte[] {'\\', '\\', 'N'}, row.getBytes(StandardCharsets.UTF_8));

        StarRocksISerializer enclosed = csvSerializer(props("enclose", "'", "escape", "\\"));
        assertEquals("\\\\N\t'a'", enclosed.serialize(new Object[]{null, "a"}));
    }

    @Test
    public void testNullMarkerIsUntouchedByADifferentEscapeChar() {
        StarRocksISerializer serializer = csvSerializer(props("escape", "|"));
        assertEquals("\\N\ta||b", serializer.serialize(new Object[]{null, "a|b"}));
        assertEquals("\\N", serializer.serialize(new Object[]{null}));
    }

    @Test
    public void testEscapePrefixesTheColumnSeparatorRowDelimiterAndItself() {
        StarRocksISerializer serializer = csvSerializer(props(
                "column_separator", "\\x0102",
                "row_delimiter", "\\x0304",
                "escape", "\\"));
        assertEquals("a\\\u0001\u0002b\u0001\u0002c\u0003d\u0001\u0002e\\\\f\u0001\u0002p\\\u0003\u0004q",
                serializer.serialize(new Object[]{"a\u0001\u0002b", "c\u0003d", "e\\f", "p\u0003\u0004q"}));
    }

    @Test
    public void testMapAndListValuesKeepGoingThroughJsonBeforeEscaping() {
        StarRocksISerializer serializer = csvSerializer(props("enclose", "'"));
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("k", "a'b");
        String json = new JsonWrapper().toJSONString(map);
        assertEquals("'" + json.replace("'", "''") + "'\t'1'\t\\N",
                serializer.serialize(new Object[]{map, 1, null}));
    }

    @Test
    public void testLegacyBytesAreUnchangedWithoutEncloseAndEscape() {
        assertEquals("a\tb\t1\t\\N\tc\nd",
                csvSerializer(props()).serialize(new Object[]{"a\tb", 1, null, "c\nd"}));
        assertEquals("a\u0001b\u00011\u0001\\N",
                new StarRocksCsvSerializer("\\x01").serialize(new Object[]{"a\u0001b", "1", null}));
    }

    @Test
    public void testEncloseAndEscapeMustBeALiteralSingleByteCharacter() {
        assertOptionRejected("enclose", "ab");
        assertOptionRejected("enclose", "\\x22");
        assertOptionRejected("enclose", "\\x01\\x02");
        assertOptionRejected("escape", "\\x5c");
        assertOptionRejected("escape", "''");
        assertOptionRejected("escape", "\u00e9");
    }

    private void assertOptionRejected(String option, String value) {
        try {
            csvSerializer(props(option, value));
            fail(option + "=" + value + " should have been rejected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(option));
        }
    }

    @Test
    public void testDifferingSameTableOverrideOfTheDialectIsRejected() {
        // The bytes carry the sink level dialect while a V2 sink asks the server to parse the table
        // level one, so the two cannot be reconciled by the serializer.
        assertDialectOverrideRejected("enclose", "\"", props("enclose", "'"));
        assertDialectOverrideRejected("escape", "!", props("escape", "\\"));
        assertDialectOverrideRejected("column_separator", "\\x02", props("column_separator", "\\x01"));
        assertDialectOverrideRejected("row_delimiter", "\\x03", props("row_delimiter", "\\x02"));
        // A sink without its own setting still writes the default dialect, which an override changes.
        assertDialectOverrideRejected("enclose", "'", props());
        assertDialectOverrideRejected("column_separator", ",", props());
    }

    @Test
    public void testEquivalentSameTableOverrideIsAccepted() {
        Map<String, String> globals = props("column_separator", "\\x01", "enclose", "'");
        String expected = csvSerializer(globals).serialize(new Object[]{"a'b", 1});
        // A hex separator, a separator spelled the way it is defaulted, a defaulted row delimiter and
        // an empty escape all spell the dialect the serializer already writes.
        StarRocksISerializer equivalent = csvSerializer(globals, Arrays.asList(
                tableOverride(DATABASE, TABLE, "column_separator", "\\x01"),
                tableOverride(DATABASE, TABLE, "row_delimiter", "\n"),
                tableOverride(DATABASE, TABLE, "enclose", "'"),
                tableOverride(DATABASE, TABLE, "escape", "")));
        assertEquals(expected, equivalent.serialize(new Object[]{"a'b", 1}));
    }

    @Test
    public void testOverrideForAnotherTableIsIgnored() {
        StarRocksISerializer serializer = csvSerializer(props("enclose", "'"), Arrays.asList(
                tableOverride(DATABASE, "other_tbl", "enclose", "\""),
                tableOverride("other_db", TABLE, "enclose", "\"")));
        assertEquals("'a''b'\t'1'", serializer.serialize(new Object[]{"a'b", 1}));
    }

    @Test
    public void testLastRegisteredSameTableOverrideDecidesTheDialect() {
        // The sdk keeps one entry per unique key, so only the last registration reaches the table.
        StarRocksISerializer accepted = csvSerializer(props("enclose", "'"), Arrays.asList(
                tableOverride(DATABASE, TABLE, "enclose", "\""),
                tableOverride(DATABASE, TABLE, "enclose", "'")));
        assertEquals("'a'", accepted.serialize(new Object[]{"a"}));

        assertDialectOverrideRejected("enclose", "\"", props("enclose", "'"),
                tableOverride(DATABASE, TABLE, "enclose", "'"));
    }

    @Test
    public void testOverrideWithAnotherUniqueKeyIsIgnored() {
        // The sdk looks an override up by the unique key of the table, so a foreign one never applies.
        StarRocksISerializer serializer = csvSerializer(props("enclose", "'"), Arrays.asList(
                StreamLoadTableProperties.builder()
                        .uniqueKey("another-key")
                        .database(DATABASE)
                        .table(TABLE)
                        .addProperty("enclose", "\"")
                        .build()));
        assertEquals("'a'", serializer.serialize(new Object[]{"a"}));
    }

    @Test
    public void testCollidingUniqueKeyRemapDropsTheEntryDialectUnderMergeCommit() {
        // The sdk unique key is `database + "-" + table`, so `a-b`.`c` and `a`.`b-c` both key to
        // `a-b-c`: the entry registered for `a-b`.`c` is what the sdk looks up for this sink's table
        // `a`.`b-c`. Because the names differ the sdk remaps it with copyFrom, which drops the per
        // table headers, so its `enclose` never reaches the load. Merge commit then sends no `enclose`
        // and the server parses the bytes with its own default, while the serializer writes the sink
        // level one: the registration must not be read as if it applied to this table unchanged.
        Map<String, String> globals = props(
                "enable_merge_commit", "true",
                "merge_commit_interval_ms", "1000",
                "enclose", "'");
        try {
            csvSerializer("a", "b-c", globals, Arrays.asList(tableEntry("a-b", "c", "enclose", "'")));
            fail("the remapped entry loses `enclose`, which the merge commit load should have rejected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("`enclose`"));
            assertTrue(e.getMessage(), e.getMessage().contains("a.b-c"));
        }
    }

    @Test
    public void testCollidingUniqueKeyRemapIsIgnoredWithoutMergeCommit() {
        // Without merge commit the ordinary V2 loader starts from the sink level headers, which the
        // remap left alone, so this table keeps the sink dialect even though the colliding entry sets
        // another `enclose`: that header was dropped by the remap and never applies here.
        StarRocksISerializer serializer = csvSerializer("a", "b-c", props("enclose", "'"),
                Arrays.asList(tableEntry("a-b", "c", "enclose", "\"")));
        assertEquals("'a'", serializer.serialize(new Object[]{"a"}));
    }

    @Test
    public void testCollidingUniqueKeyRemapOfANonDialectEntryIsAccepted() {
        // The same collision with an entry that sets no dialect header: dropping it leaves the default
        // dialect the serializer already writes, so the ordinary default case still goes through.
        Map<String, String> globals = props("column_separator", "\\x01", "enclose", "'");
        StarRocksISerializer serializer = csvSerializer("a", "b-c", globals,
                Arrays.asList(tableEntry("a-b", "c", "timeout", "600")));
        assertEquals("'a''b'\u0001'1'", serializer.serialize(new Object[]{"a'b", 1}));
    }

    private void assertDialectOverrideRejected(String option, String overrideValue, Map<String, String> globals,
                                               StreamLoadTableProperties... earlierOverrides) {
        List<StreamLoadTableProperties> overrides = new ArrayList<>(Arrays.asList(earlierOverrides));
        overrides.add(tableOverride(DATABASE, TABLE, option, overrideValue));
        try {
            csvSerializer(globals, overrides);
            fail("the `" + option + "` override of this table should have been rejected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("`" + option + "`"));
            assertTrue(e.getMessage(), e.getMessage().contains(DATABASE + "." + TABLE));
        }
    }

    private static StreamLoadTableProperties tableOverride(String database, String table, String option, String value) {
        return StreamLoadTableProperties.builder()
                .database(database)
                .table(table)
                .addProperty(option, value)
                .build();
    }

    /** Builds a serializer from its own options, so the shared OPTIONS of the base test is untouched. */
    private StarRocksISerializer csvSerializer(Map<String, String> streamLoadProperties) {
        return csvSerializer(streamLoadProperties, Collections.emptyList());
    }

    private StarRocksISerializer csvSerializer(Map<String, String> streamLoadProperties,
                                               List<StreamLoadTableProperties> tableProperties) {
        return csvSerializer(DATABASE, TABLE, streamLoadProperties, tableProperties);
    }

    private StarRocksISerializer csvSerializer(String database, String table,
                                               Map<String, String> streamLoadProperties,
                                               List<StreamLoadTableProperties> tableProperties) {
        StarRocksSinkOptions.Builder builder = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", JDBC_URL)
                .withProperty("load-url", LOAD_URL)
                .withProperty("database-name", database)
                .withProperty("table-name", table)
                .withProperty("username", USERNAME)
                .withProperty("password", PASSWORD);
        streamLoadProperties.forEach((key, value) -> builder.withProperty("sink.properties." + key, value));
        StarRocksSinkOptions options = builder.build();
        tableProperties.forEach(options::addTableProperties);
        StarRocksISerializer serializer =
                StarRocksSerializerFactory.createSerializer(options, TABLE_SCHEMA.getFieldNames());
        serializer.open(new StarRocksISerializer.SerializerContext(new JsonWrapper()));
        return serializer;
    }

    private static Map<String, String> props(String... keysAndValues) {
        Map<String, String> properties = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            properties.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return properties;
    }

    @Test
    public void testMergeCommitEntryOfThisTableThatOmitsAGlobalDialectHeaderIsRejected() {
        // Merge commit builds the headers of one table from the entry of that table alone, so a
        // header the entry leaves out is not sent and the server parses the bytes with its own
        // default, while the serializer wrote the sink level value.
        assertMergeCommitMissingHeaderRejected("enclose", "'");
        assertMergeCommitMissingHeaderRejected("escape", "\\");
        assertMergeCommitMissingHeaderRejected("column_separator", "\\x01");
        assertMergeCommitMissingHeaderRejected("row_delimiter", "\\x02");
    }

    @Test
    public void testMergeCommitEntryOfThisTableThatSetsTheSameDialectIsAccepted() {
        Map<String, String> globals = props(
                "enable_merge_commit", "true",
                "merge_commit_interval_ms", "1000",
                "column_separator", "\\x01",
                "row_delimiter", "\\x02",
                "enclose", "'",
                "escape", "\\");
        StarRocksISerializer serializer = csvSerializer(globals, Arrays.asList(
                tableEntry(DATABASE, TABLE,
                        "column_separator", "\\x01",
                        "row_delimiter", "\\x02",
                        "enclose", "'",
                        "escape", "\\")));
        assertEquals("'a\\'b'\u0001'c\\\\d'", serializer.serialize(new Object[]{"a'b", "c\\d"}));
    }

    @Test
    public void testEntryOfThisTableThatOmitsAHeaderIsAcceptedWithoutMergeCommit() {
        // Without merge commit the ordinary V2 loader starts from the sink level headers, so the
        // entry keeps them and the bytes stay consistent with the load of this table.
        Map<String, String> globals = props("column_separator", "\\x01", "enclose", "'", "escape", "\\");
        StarRocksISerializer serializer = csvSerializer(globals, Arrays.asList(
                tableEntry(DATABASE, TABLE, "timeout", "600")));
        assertEquals("'a\\'b'\u0001'c\\\\d'", serializer.serialize(new Object[]{"a'b", "c\\d"}));
    }

    @Test
    public void testMergeCommitWithoutAnEntryOfThisTableKeepsTheGlobalDialect() {
        Map<String, String> globals = props(
                "enable_merge_commit", "true",
                "merge_commit_interval_ms", "1000",
                "column_separator", "\\x01",
                "enclose", "'");
        // No entry at all: the sdk loads the default table properties, whose headers are the sink
        // level ones, so the merge commit load parses with the dialect the serializer wrote.
        assertEquals("'a''b'\u0001'1'",
                csvSerializer(globals).serialize(new Object[]{"a'b", 1}));
        // An entry registered for another table is not the entry of this table, so it is ignored.
        assertEquals("'a''b'\u0001'1'", csvSerializer(globals, Arrays.asList(
                tableEntry(DATABASE, "other_tbl", "enclose", "\""),
                tableEntry("other_db", TABLE, "row_delimiter", "\\x03")))
                .serialize(new Object[]{"a'b", 1}));
    }

    @Test
    public void testMergeCommitEntryOfThisTableWithTheDefaultDialectIsAccepted() {
        Map<String, String> globals = props("enable_merge_commit", "true", "merge_commit_interval_ms", "1000");
        // With no dialect header set on the sink, an entry that leaves them out sends the server
        // defaults, which are the defaults the serializer writes.
        assertEquals("a\tb", csvSerializer(globals, Arrays.asList(tableEntry(DATABASE, TABLE)))
                .serialize(new Object[]{"a", "b"}));
        // Spelling a header as the value it defaults to agrees with the sink just the same.
        assertEquals("a\tb", csvSerializer(globals, Arrays.asList(
                tableEntry(DATABASE, TABLE, "row_delimiter", "\n", "escape", "")))
                .serialize(new Object[]{"a", "b"}));
    }

    @Test
    public void testOrdinaryV2IgnoresTheCommonMapOfTheEntryOfThisTable() {
        // The ordinary V2 loader starts from the sink level headers and reads the per table map only,
        // so a dialect header the entry sets through its common map never reaches the load: the sink
        // level value the serializer wrote is what the server parses with, so a common value that
        // disagrees with the sink level one is accepted here even though merge commit would reject it.
        Map<String, String> globals = props("enclose", "'", "escape", "\\");
        StarRocksISerializer serializer = csvSerializer(globals, Arrays.asList(
                tableCommonEntry(DATABASE, TABLE, "enclose", "\"", "escape", "!")));
        assertEquals("'a\\'b'", serializer.serialize(new Object[]{"a'b"}));
    }

    @Test
    public void testMergeCommitEntryOfThisTableThatMismatchesAGlobalDialectHeaderInItsCommonMapIsRejected() {
        // Merge commit builds the headers of a table from that table's own maps alone, so a dialect
        // header the entry sets through its common map counts exactly like one set through the per
        // table map: a value that disagrees with the sink level one is a dialect the server would
        // parse differently from the serializer, so it must be rejected.
        Map<String, String> globals = props(
                "enable_merge_commit", "true",
                "merge_commit_interval_ms", "1000",
                "enclose", "'");
        try {
            csvSerializer(globals, Arrays.asList(
                    tableCommonEntry(DATABASE, TABLE, "enclose", "\"")));
            fail("the `enclose` the common map of the entry of this table sets should have been rejected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("`enclose`"));
            assertTrue(e.getMessage(), e.getMessage().contains(DATABASE + "." + TABLE));
        }
    }

    private void assertMergeCommitMissingHeaderRejected(String option, String sinkValue) {
        Map<String, String> globals = props(
                "enable_merge_commit", "true",
                "merge_commit_interval_ms", "1000",
                option, sinkValue);
        try {
            csvSerializer(globals, Arrays.asList(tableEntry(DATABASE, TABLE)));
            fail("the `" + option + "` the entry of this table leaves out should have been rejected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("`" + option + "`"));
            assertTrue(e.getMessage(), e.getMessage().contains(DATABASE + "." + TABLE));
        }
    }

    private static StreamLoadTableProperties tableEntry(String database, String table, String... keysAndValues) {
        StreamLoadTableProperties.Builder builder = StreamLoadTableProperties.builder()
                .database(database)
                .table(table);
        for (int i = 0; i < keysAndValues.length; i += 2) {
            builder.addProperty(keysAndValues[i], keysAndValues[i + 1]);
        }
        return builder.build();
    }

    private static StreamLoadTableProperties tableCommonEntry(String database, String table,
                                                              String... keysAndValues) {
        return StreamLoadTableProperties.builder()
                .database(database)
                .table(table)
                .addCommonProperties(props(keysAndValues))
                .build();
    }
}
