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

package com.starrocks.connector.flink.table.sink;

import org.apache.flink.table.catalog.ResolvedSchema;

import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;
import com.starrocks.connector.flink.table.sink.v2.RecordSerializationSchema;
import com.starrocks.connector.flink.table.sink.v2.RowDataSerializationSchema;
import com.starrocks.connector.flink.table.sink.v2.StarRocksSink;
import com.starrocks.connector.flink.table.sink.v2.StringSerializationSchema;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.starrocks.data.load.stream.StreamLoadUtils.isStarRocksSupportTransactionLoad;

public class SinkFunctionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SinkFunctionFactory.class);

    enum SinkVersion {
        V2,
        AUTO
    }

    public static void detectStarRocksFeature(StarRocksSinkOptions sinkOptions) {
        try {
            boolean supportTransactionLoad = isStarRocksSupportTransactionLoad(
                    sinkOptions.getLoadUrlList(), sinkOptions.getConnectTimeout(), sinkOptions.getUsername(), sinkOptions.getPassword());
            sinkOptions.setSupportTransactionStreamLoad(supportTransactionLoad);
            if (supportTransactionLoad) {
                LOG.info("StarRocks supports transaction load");
            } else {
                LOG.info("StarRocks does not support transaction load");
            }
        } catch (Exception e) {
            LOG.warn("Can't decide whether StarRocks supports transaction load, and enable it by default.");
            sinkOptions.setSupportTransactionStreamLoad(true);
        }
    }

    public static SinkVersion chooseSinkVersionAutomatically(StarRocksSinkOptions sinkOptions) {
        return SinkVersion.V2;
    }

    public static SinkVersion getSinkVersion(StarRocksSinkOptions sinkOptions) {
        String sinkTypeOption = sinkOptions.getSinkVersion().trim().toUpperCase();
        SinkVersion sinkVersion;
        if ("V1".equals(sinkTypeOption)) {
            throw new UnsupportedOperationException(
                    "Sink V1 is not supported in Flink 2.x. Use V2 or AUTO.");
        } else if (SinkVersion.V2.name().equals(sinkTypeOption)) {
            sinkVersion = SinkVersion.V2;
        } else if (SinkVersion.AUTO.name().equals(sinkTypeOption)) {
            sinkVersion = chooseSinkVersionAutomatically(sinkOptions);
        } else {
            throw new UnsupportedOperationException("Unsupported sink type " + sinkTypeOption);
        }
        LOG.info("Choose sink version {}", sinkVersion.name());
        return sinkVersion;
    }

    public static <T> StarRocksSink<T> createSink(
            StarRocksSinkOptions sinkOptions, ResolvedSchema schema, StarRocksIRowTransformer<T> rowTransformer) {
        detectStarRocksFeature(sinkOptions);
        StarRocksSinkTable sinkTable = StarRocksSinkTable.builder()
                .sinkOptions(sinkOptions)
                .build();
        sinkTable.validateTableStructure(sinkOptions, schema);
        // StarRocksJsonSerializer depends on SinkOptions#supportUpsertDelete which is decided in
        // StarRocksSinkTable#validateTableStructure, so create serializer after validating table structure
        StarRocksISerializer serializer = StarRocksSerializerFactory.createSerializer(sinkOptions, schema.getColumnNames().toArray(new String[0]));
        rowTransformer.setStarRocksColumns(sinkTable.getFieldMapping());
        rowTransformer.setTableSchema(schema);
        RowDataSerializationSchema<T> serializationSchema = new RowDataSerializationSchema<>(
                sinkOptions.getDatabaseName(),
                sinkOptions.getTableName(),
                sinkOptions.supportUpsertDelete(),
                sinkOptions.getIgnoreUpdateBefore(),
                serializer,
                rowTransformer);
        StreamLoadProperties streamLoadProperties = sinkOptions.getProperties(sinkTable);
        return new StarRocksSink<>(sinkOptions, serializationSchema, streamLoadProperties);
    }

    public static <T> StarRocksSink<T> createSink(
            StarRocksSinkOptions sinkOptions, RecordSerializationSchema<T> serializationSchema) {
        detectStarRocksFeature(sinkOptions);
        StreamLoadProperties streamLoadProperties = sinkOptions.getProperties(null);
        return new StarRocksSink<>(sinkOptions, serializationSchema, streamLoadProperties);
    }

    public static StarRocksSink<String> createSink(StarRocksSinkOptions sinkOptions) {
        detectStarRocksFeature(sinkOptions);
        StringSerializationSchema serializationSchema = new StringSerializationSchema(
                sinkOptions.getDatabaseName(), sinkOptions.getTableName());
        StreamLoadProperties streamLoadProperties = sinkOptions.getProperties(null);
        return new StarRocksSink<>(sinkOptions, serializationSchema, streamLoadProperties);
    }
}
