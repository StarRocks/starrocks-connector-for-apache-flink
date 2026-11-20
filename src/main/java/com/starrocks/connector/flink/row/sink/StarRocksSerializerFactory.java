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

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StarRocksSerializerFactory {

    private static final String COLUMN_SEPARATOR = "column_separator";
    private static final String ROW_DELIMITER = "row_delimiter";
    private static final String ENCLOSE = "enclose";
    private static final String ESCAPE = "escape";
    private static final List<String> DIALECT_HEADERS = Collections.unmodifiableList(
            Arrays.asList(COLUMN_SEPARATOR, ROW_DELIMITER, ENCLOSE, ESCAPE));

    private StarRocksSerializerFactory() {}

    public static StarRocksISerializer createSerializer(StarRocksSinkOptions sinkOptions, String[] fieldNames) {
        if (StarRocksSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat())) {
            Map<String, String> streamLoadProperties = sinkOptions.getSinkStreamLoadProperties();
            rejectTableDialectMismatch(sinkOptions, streamLoadProperties);
            return new StarRocksCsvSerializer(
                    streamLoadProperties.get(COLUMN_SEPARATOR),
                    streamLoadProperties.get(ROW_DELIMITER),
                    streamLoadProperties.get(ENCLOSE),
                    streamLoadProperties.get(ESCAPE));
        }
        if (StarRocksSinkOptions.StreamLoadFormat.JSON.equals(sinkOptions.getStreamLoadFormat())) {
            if (sinkOptions.supportUpsertDelete()) {
                String[] tmp = new String[fieldNames.length + 1];
                System.arraycopy(fieldNames, 0, tmp, 0, fieldNames.length);
                tmp[fieldNames.length] = StarRocksSinkOP.COLUMN_KEY;
                fieldNames = tmp;
            }
            return new StarRocksJsonSerializer(fieldNames);
        }
        throw new RuntimeException("Failed to create row serializer, unsupported `format` from stream load properties.");
    }

    /**
     * The csv serializer writes every row with the sink level dialect, while a load of the table the
     * sink writes to can parse it with another one, which corrupts the load: quote bytes are loaded
     * literally and an embedded separator splits a row. The serializer cannot follow the table either,
     * because a V1 sink sends the sink level headers only and resolving a per table dialect goes
     * through the frontend. Reject the combinations that would disagree instead.
     *
     * <p>Two of them exist. A registered entry that sets a dialect header to something else than the
     * sink level value disagrees on every path, since a V2 sink puts that header on the load. An entry
     * that leaves a header out disagrees on the merge commit path alone: it builds the headers of a
     * table from the entry's own maps, so the header is not sent and the server parses the bytes with
     * its default, whereas the ordinary V2 loader starts from the sink level headers.
     */
    private static void rejectTableDialectMismatch(StarRocksSinkOptions sinkOptions,
                                                   Map<String, String> streamLoadProperties) {
        StarRocksSinkOptions.TableCsvDialect tableDialect = sinkOptions.getTableCsvDialect();
        for (String header : DIALECT_HEADERS) {
            String sinkValue = streamLoadProperties.get(header);
            String registeredValue = tableDialect.getRegisteredHeader(header);
            // Both sides are resolved the way StarRocksCsvSerializer resolves them, so an equivalent
            // setting, such as the separator spelled as the real character it defaults to, still passes.
            if (registeredValue != null) {
                if (dialectsDiffer(header, registeredValue, sinkValue)) {
                    throw new IllegalArgumentException(String.format(
                            "The properties registered for table %s.%s set `%s` to %s, but the csv serializer "
                                    + "writes every row with the sink level `%s`, %s. A per table csv dialect is not "
                                    + "supported, the bytes and the load headers would disagree. Set %s%s on the sink "
                                    + "instead.",
                            sinkOptions.getDatabaseName(), sinkOptions.getTableName(),
                            header, display(registeredValue),
                            header, display(sinkValue),
                            StarRocksSinkOptions.SINK_PROPERTIES_PREFIX, header));
                }
                continue;
            }
            if (!tableDialect.isMergeCommit()) {
                continue;
            }
            // The entry of this table leaves the header out. The merge commit load sends no header for it
            // and the server then parses the bytes with its own default for that header.
            if (dialectsDiffer(header, tableDialect.getLoadHeader(header), sinkValue)) {
                throw new IllegalArgumentException(String.format(
                        "The properties registered for table %s.%s do not set `%s`, but the sink sets it to %s. "
                                + "Merge commit sends the headers of a table alone, so the server would parse every "
                                + "row with its own default for `%s` instead, while the csv serializer writes the "
                                + "sink level value. Set %s%s on the sink, or register the same `%s` for the table.",
                        sinkOptions.getDatabaseName(), sinkOptions.getTableName(),
                        header, display(sinkValue),
                        header,
                        StarRocksSinkOptions.SINK_PROPERTIES_PREFIX, header, header));
            }
        }
    }

    private static boolean dialectsDiffer(String header, String left, String right) {
        return !Objects.equals(normalizeDialect(header, left), normalizeDialect(header, right));
    }

    private static String normalizeDialect(String header, String value) {
        if (COLUMN_SEPARATOR.equals(header)) {
            return StarRocksDelimiterParser.parse(value, "\t");
        }
        if (ROW_DELIMITER.equals(header)) {
            return StarRocksDelimiterParser.parse(value, "\n");
        }
        return StarRocksCsvSerializer.singleByteOption(value, header);
    }

    private static String display(String value) {
        return value == null || value.isEmpty() ? "unset" : "`" + value + "`";
    }
}
