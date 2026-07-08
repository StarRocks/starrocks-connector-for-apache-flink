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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.MEGA_BYTES_SCALE;
import static com.starrocks.data.load.stream.mergecommit.LoadParameters.ENABLE_MERGE_COMMIT;
import static com.starrocks.data.load.stream.mergecommit.LoadParameters.MERGE_COMMIT_ASYNC;
import static com.starrocks.data.load.stream.mergecommit.LoadParameters.MERGE_COMMIT_PARALLEL;

public class MergeCommitOptions {

    public static final String MERGE_COMMIT_PREFIX = "sink.merge-commit.";

    private static final int DEFAULT_PUBLISH_TIMEOUT_MS = 10000;
    private static final long DEFAULT_CHUNK_SIZE_FOR_IN_ORDER = 500 * MEGA_BYTES_SCALE;
    private static final long DEFAULT_CHUNK_SIZE_FOR_OUT_OF_ORDER = 20 * MEGA_BYTES_SCALE;
    public static final ConfigOption<Long> CHUNK_SIZE =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "chunk.size").longType().noDefaultValue();
    public static final ConfigOption<Integer> MAX_CONCURRENT_REQUESTS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "max-concurrent-requests").intType().defaultValue(Integer.MAX_VALUE)
                    .withDeprecatedKeys(MERGE_COMMIT_PREFIX + "max-inflight-requests")
                    .withDescription("non-positive value ensures the order which is useful for primary key table");
    public static final ConfigOption<Integer> HTTP_THREAD_NUM =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "http.thread.num").intType().defaultValue(3);
    public static final ConfigOption<Integer> HTTP_MAX_CONNECTIONS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "http.max-connections").intType().defaultValue(20);
    public static final ConfigOption<Integer> HTTP_TOTAL_MAX_CONNECTIONS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "http.total-max-connections").intType().defaultValue(1000);
    public static final ConfigOption<Integer> HTTP_IDLE_CONNECTION_TIMEOUT_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "http.idle-connection-timeout-ms").intType().defaultValue(60000);
    public static final ConfigOption<Integer> NODE_META_UPDATE_INTERVAL_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "node-meta.update-interval-ms").intType().defaultValue(5000);
    public static final ConfigOption<Integer> CHECK_LABEL_STATE_INIT_DELAY_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "check-label-state.init-delay-ms").intType().defaultValue(500)
                    .withDeprecatedKeys(MERGE_COMMIT_PREFIX + "check-state.init-delay-ms");
    public static final ConfigOption<Integer> CHECK_LABEL_STATE_INTERVAL_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "check-label-state.interval-ms").intType().defaultValue(500)
                    .withDeprecatedKeys(MERGE_COMMIT_PREFIX + "check-state.interval-ms");
    public static final ConfigOption<Integer> CHECK_LABEL_STATE_TIMEOUT_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "check-label-state.timeout-ms").intType().noDefaultValue()
                    .withDeprecatedKeys(MERGE_COMMIT_PREFIX + "check-state.timeout-ms")
                    .withDescription("If not set, will use stream load timeout instead");
    public static final ConfigOption<Boolean> BACKEND_DIRECT_CONNECTION =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "backend-direct-connection").booleanType().defaultValue(false);

    public static List<ConfigOption<?>> getAllConfigOptions() {
        List<ConfigOption<?>> configOptions = new ArrayList<>();
        Field[] fields = MergeCommitOptions.class.getDeclaredFields();
        for (Field field : fields) {
            if (!field.getType().equals(ConfigOption.class)) {
                continue;
            }
            try {
                field.setAccessible(true);
                ConfigOption<?> option = (ConfigOption<?>) field.get(null);
                configOptions.add(option);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("can't get ConfigOption " + field.getName(), e);
            }
        }
        return configOptions;
    }

    public static void buildMergeCommitOptions(
            ReadableConfig options, Map<String, String> streamLoadProperties,
            StreamLoadTableProperties.Builder defaultTablePropertiesBuilder,
            StreamLoadProperties.Builder streamLoadPropertiesBuilder) {
        if ("true".equalsIgnoreCase(streamLoadProperties.get(ENABLE_MERGE_COMMIT))) {
            if (!streamLoadProperties.containsKey(MERGE_COMMIT_PARALLEL)) {
                streamLoadProperties.put(MERGE_COMMIT_PARALLEL, "3");
            }
            if (!streamLoadProperties.containsKey(MERGE_COMMIT_ASYNC)) {
                streamLoadProperties.put(MERGE_COMMIT_ASYNC, "true");
            }
            int maxInflightRequests = options.get(MergeCommitOptions.MAX_CONCURRENT_REQUESTS);
            Optional<Long> optionalChunkSize = options.getOptional(MergeCommitOptions.CHUNK_SIZE);
            if (optionalChunkSize.isPresent()) {
                defaultTablePropertiesBuilder.chunkLimit(optionalChunkSize.get());
            } else {
                long chunkSize = maxInflightRequests <= 0 ? DEFAULT_CHUNK_SIZE_FOR_IN_ORDER : DEFAULT_CHUNK_SIZE_FOR_OUT_OF_ORDER;
                defaultTablePropertiesBuilder.chunkLimit(chunkSize);
            }

            // set reties to 0 by default to release memory as soon as possible and improve performance
            int maxRetries = options.getOptional(StarRocksSinkOptions.SINK_MAX_RETRIES).orElse(0);
            streamLoadPropertiesBuilder.setCheckLabelStateInitDelayMs(options.get(CHECK_LABEL_STATE_INIT_DELAY_MS))
                    .setCheckLabelStateIntervalMs(options.get(CHECK_LABEL_STATE_INTERVAL_MS))
                    .setHttpThreadNum(options.get(HTTP_THREAD_NUM))
                    .setHttpMaxConnectionsPerRoute(options.get(HTTP_MAX_CONNECTIONS))
                    .setHttpTotalMaxConnections(options.get(HTTP_TOTAL_MAX_CONNECTIONS))
                    .setHttpIdleConnectionTimeoutMs(options.get(HTTP_IDLE_CONNECTION_TIMEOUT_MS))
                    .setNodeMetaUpdateIntervalMs(options.get(NODE_META_UPDATE_INTERVAL_MS))
                    .setMaxConcurrentRequests(options.get(MAX_CONCURRENT_REQUESTS))
                    .setBackendDirectConnection(options.get(BACKEND_DIRECT_CONNECTION))
                    .maxRetries(maxRetries);
            options.getOptional(CHECK_LABEL_STATE_TIMEOUT_MS).ifPresent(streamLoadPropertiesBuilder::setCheckLabelStateTimeoutMs);
            int publishTimeoutMs = options.getOptional(StarRocksSinkOptions.SINK_PUBLISH_TIMEOUT).orElse(DEFAULT_PUBLISH_TIMEOUT_MS);
            streamLoadPropertiesBuilder.setPublishTimeoutMs(publishTimeoutMs);
        }
    }
}
