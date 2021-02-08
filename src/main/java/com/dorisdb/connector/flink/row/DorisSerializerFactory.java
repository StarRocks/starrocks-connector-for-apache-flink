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

package com.dorisdb.connector.flink.row;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.dorisdb.connector.flink.table.DorisSinkOptions;

import org.apache.flink.calcite.shaded.com.google.common.base.Strings;

public class DorisSerializerFactory {

    enum StreamLoadFormat {
        CSV, JSON;
    }

    private DorisSerializerFactory() {}

    private static final String FORMAT_KEY = "format";

    public static DorisISerializer createSerializer(DorisSinkOptions sinkOptions, String[] fieldNames) {
        Map<String, String> loadProsp = sinkOptions.getSinkStreamLoadProperties();
        String format = loadProsp.get(FORMAT_KEY);
        if (Strings.isNullOrEmpty(format) || StreamLoadFormat.CSV.name().equalsIgnoreCase(format)) {
            return new DorisCsvSerializer(loadProsp.get("column_separator"));
        }
        if (StreamLoadFormat.JSON.name().equalsIgnoreCase(format)) {
            return new DorisJsonSerializer(fieldNames);
        }
        throw new RuntimeException("Failed to create row serializer, wrong `format` from stream load properties:" + format);
    }

    public static byte[] joinRows(DorisSinkOptions sinkOptions, List<String> rows) {
        Map<String, String> loadProsp = sinkOptions.getSinkStreamLoadProperties();
        String format = loadProsp.get(FORMAT_KEY);
        if (Strings.isNullOrEmpty(format) || StreamLoadFormat.CSV.name().equalsIgnoreCase(format)) {
            return String.join("\n", rows).getBytes(StandardCharsets.UTF_8);
        }
        if (StreamLoadFormat.JSON.name().equalsIgnoreCase(format)) {
            return new StringBuilder("[").append(String.join(",", rows)).append("]").toString().getBytes(StandardCharsets.UTF_8);
        }
        throw new RuntimeException("Failed to join rows data, wrong `format` from stream load properties:" + format);
    }
    
}
