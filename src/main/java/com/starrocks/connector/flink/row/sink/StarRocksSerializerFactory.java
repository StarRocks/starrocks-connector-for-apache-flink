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

import com.starrocks.connector.flink.table.DataType;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;

import java.util.Map;

public class StarRocksSerializerFactory {

    private StarRocksSerializerFactory() {}

    public static StarRocksISerializer createSerializer(StarRocksSinkOptions sinkOptions, String[] fieldNames) {
        return createSerializer(sinkOptions, fieldNames, null);
    }


    public static StarRocksISerializer createSerializer(StarRocksSinkOptions sinkOptions,
                                                        String[] fieldNames,
                                                        Map<String, DataType> mapping) {

        if (StarRocksSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat())) {
            return new StarRocksCsvSerializer(sinkOptions.getSinkStreamLoadProperties().get("column_separator"), fieldNames, mapping);
        }
        if (StarRocksSinkOptions.StreamLoadFormat.JSON.equals(sinkOptions.getStreamLoadFormat())) {
            if (sinkOptions.supportUpsertDelete()) {
                String[] tmp = new String[fieldNames.length + 1];
                System.arraycopy(fieldNames, 0, tmp, 0, fieldNames.length);
                tmp[fieldNames.length] = StarRocksSinkOP.COLUMN_KEY;
                fieldNames = tmp;
            }
            return new StarRocksJsonSerializer(fieldNames, mapping);
        }
        throw new RuntimeException("Failed to create row serializer, unsupported `format` from stream load properties.");
    }



}
