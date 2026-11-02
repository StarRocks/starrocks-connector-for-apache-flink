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

import com.starrocks.connector.flink.tools.JsonWrapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class StarRocksCsvSerializer implements StarRocksISerializer {

    private static final long serialVersionUID = 1L;

    private static final String NULL_VALUE = "\\N";

    private final String columnSeparator;
    private final String rowDelimiter;
    private final String enclose;
    private final String escape;

    private transient JsonWrapper jsonWrapper;

    public StarRocksCsvSerializer(String sp) {
        this(sp, null, null, null);
    }

    StarRocksCsvSerializer(String columnSeparator, String rowDelimiter, String enclose, String escape) {
        this.columnSeparator = StarRocksDelimiterParser.parse(columnSeparator, "\t");
        this.rowDelimiter = StarRocksDelimiterParser.parse(rowDelimiter, "\n");
        this.enclose = singleByteOption(enclose, "enclose");
        this.escape = singleByteOption(escape, "escape");
    }

    @Override
    public void open(SerializerContext context) {
        this.jsonWrapper = context.getFastJsonWrapper();
    }

    @Override
    public String serialize(Object[] values) {
        StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < values.length; idx++) {
            if (idx > 0) {
                sb.append(columnSeparator);
            }
            Object val = values[idx];
            if (null == val) {
                appendNull(sb);
            } else {
                appendField(sb, fieldValue(val));
            }
        }
        return sb.toString();
    }

    // The CSV reader strips escapes before checking the NULL marker.
    private void appendNull(StringBuilder sb) {
        if (null == escape) {
            sb.append(NULL_VALUE);
        } else {
            appendEscaped(sb, NULL_VALUE, false);
        }
    }

    private String fieldValue(Object val) {
        return ((val instanceof Map || val instanceof List) ? jsonWrapper.toJSONString(val) : val).toString();
    }

    private void appendField(StringBuilder sb, String field) {
        if (null == enclose) {
            if (null == escape) {
                sb.append(field);
            } else {
                appendEscaped(sb, field, false);
            }
            return;
        }
        sb.append(enclose);
        if (null == escape) {
            sb.append(field.replace(enclose, enclose + enclose));
        } else {
            appendEscaped(sb, field, true);
        }
        sb.append(enclose);
    }

    // The reader consumes an escaped multi-character separator as a whole.
    private void appendEscaped(StringBuilder sb, String field, boolean insideEnclose) {
        for (int i = 0; i < field.length(); i++) {
            char c = field.charAt(i);
            if (c == escape.charAt(0)
                    || (insideEnclose ? c == enclose.charAt(0) : startsSeparator(field, i))) {
                sb.append(escape);
            }
            sb.append(c);
        }
    }

    private boolean startsSeparator(String field, int index) {
        return field.startsWith(columnSeparator, index) || field.startsWith(rowDelimiter, index);
    }

    // Unlike separators, the server reads enclose/escape as raw single-byte headers. Package
    // private because the serializer factory resolves an override's value the same way.
    static String singleByteOption(String value, String optionName) {
        if (null == value || value.isEmpty()) {
            return null;
        }
        if (value.getBytes(StandardCharsets.UTF_8).length != 1) {
            throw new IllegalArgumentException("Failed to parse `" + optionName + "`: `" + value
                    + "` is not a single-byte character");
        }
        return value;
    }
}
