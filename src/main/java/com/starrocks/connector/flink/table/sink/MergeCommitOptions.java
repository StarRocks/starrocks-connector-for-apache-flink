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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MergeCommitOptions {

    public static final String MERGE_COMMIT_PREFIX = "sink.merge-commit.";

    public static final ConfigOption<Long> CHUNK_SIZE =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "chunk.size").longType().defaultValue(20971520L);
    public static final ConfigOption<Integer> MAX_INFLIGHT_REQUESTS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "max-inflight-requests").intType().defaultValue(5);
    public static final ConfigOption<Integer> BRPC_MAX_CONNECTIONS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "brpc.max-connections").intType().defaultValue(3);
    public static final ConfigOption<Integer> BRPC_MIN_CONNECTIONS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "brpc.min-connections").intType().defaultValue(1);
    public static final ConfigOption<Integer> BRPC_CONNECTION_TIMEOUT_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "brpc.connection-timeout-ms").intType().defaultValue(60000);
    public static final ConfigOption<Integer> BRPC_READ_TIMEOUT_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "brpc.read-timeout-ms").intType().defaultValue(60000);
    public static final ConfigOption<Integer> BRPC_WRITE_TIMEOUT_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "brpc.write-timeout-ms").intType().defaultValue(60000);
    public static final ConfigOption<Integer> BRPC_MAX_RETRIES =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "brpc.max-tries").intType().defaultValue(1);
    public static final ConfigOption<Integer> BRPC_IO_THREAD_NUM =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "brpc.io-thread-num").intType().defaultValue(-1);
    public static final ConfigOption<Integer> BRPC_WORKER_THREAD_NUM =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "brpc.worker-thread-num").intType().defaultValue(-1);
    public static final ConfigOption<Integer> HTTP_THREAD_NUM =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "http.thread.num").intType().defaultValue(3);
    public static final ConfigOption<Integer> HTTP_MAX_CONNECTIONS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "http.max-connections").intType().defaultValue(3);
    public static final ConfigOption<Integer> HTTP_TOTAL_MAX_CONNECTIONS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "http.total-max-connections").intType().defaultValue(30);
    public static final ConfigOption<Integer> HTTP_IDLE_CONNECTION_TIMEOUT_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "http.idle-connection-timeout-ms").intType().defaultValue(60000);
    public static final ConfigOption<Integer> NODE_META_UPDATE_INTERVAL_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "node-meta.update-interval-ms").intType().defaultValue(2000);
    public static final ConfigOption<Integer> CHECK_STATE_INIT_DELAY_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "check-state.init-delay-ms").intType().defaultValue(500);
    public static final ConfigOption<Integer> CHECK_STATE_INTERVAL_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "check-state.interval-ms").intType().defaultValue(500);
    public static final ConfigOption<Integer> CHECK_STATE_TIMEOUT_MS =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "check-state.timeout-ms").intType().defaultValue(60000);
    public static final ConfigOption<String> PROTOCOL =
            ConfigOptions.key(MERGE_COMMIT_PREFIX + "protocol").stringType().defaultValue("http");

    public static final String ENABLE_MERGE_COMMIT = "enable_merge_commit";
    public static final String MERGE_COMMIT_INTERVAL_MS = "merge_commit_interval_ms";
    public static final String MERGE_COMMIT_PARALLEL = "merge_commit_parallel";
    public static final String MERGE_COMMIT_ASYNC = "merge_commit_async";

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
            ReadableConfig options, Map<String, String> streamLoadProperties, StreamLoadProperties.Builder builder) {
        String protocol = options.get(PROTOCOL);
        if ("true".equalsIgnoreCase(streamLoadProperties.get(ENABLE_MERGE_COMMIT))) {
            if (!streamLoadProperties.containsKey(MERGE_COMMIT_PARALLEL)) {
                streamLoadProperties.put(MERGE_COMMIT_PARALLEL, "3");
            }
            if (!streamLoadProperties.containsKey(MERGE_COMMIT_ASYNC)) {
                if ("brpc".equalsIgnoreCase(protocol)) {
                    streamLoadProperties.put(MERGE_COMMIT_ASYNC, "false");
                } else {
                    streamLoadProperties.put(MERGE_COMMIT_ASYNC, "true");
                }
            }
        }
        builder.setCheckLabelInitDelayMs(options.get(CHECK_STATE_INIT_DELAY_MS))
                .setCheckLabelIntervalMs(options.get(CHECK_STATE_INTERVAL_MS))
                .setCheckLabelTimeoutMs(options.get(CHECK_STATE_TIMEOUT_MS))
                .setBrpcConnectTimeoutMs(options.get(BRPC_CONNECTION_TIMEOUT_MS))
                .setBrpcReadTimeoutMs(options.get(BRPC_READ_TIMEOUT_MS))
                .setBrpcWriteTimeoutMs(options.get(BRPC_WRITE_TIMEOUT_MS))
                .setBrpcMaxRetryTimes(options.get(BRPC_MAX_RETRIES))
                .setBrpcMaxConnections(options.get(BRPC_MAX_CONNECTIONS))
                .setBrpcMinConnections(options.get(BRPC_MIN_CONNECTIONS))
                .setBrpcIoThreadNum(options.get(BRPC_IO_THREAD_NUM))
                .setBrpcWorkerThreadNum(options.get(BRPC_WORKER_THREAD_NUM))
                .setHttpThreadNum(options.get(HTTP_THREAD_NUM))
                .setHttpMaxConnectionsPerRoute(options.get(HTTP_MAX_CONNECTIONS))
                .setHttpTotalMaxConnections(options.get(HTTP_TOTAL_MAX_CONNECTIONS))
                .setHttpIdleConnectionTimeoutMs(options.get(HTTP_IDLE_CONNECTION_TIMEOUT_MS))
                .setNodeMetaUpdateIntervalMs(options.get(NODE_META_UPDATE_INTERVAL_MS))
                .setMaxInflightRequests(options.get(MAX_INFLIGHT_REQUESTS))
                .setProtocol(protocol);
    }
}
