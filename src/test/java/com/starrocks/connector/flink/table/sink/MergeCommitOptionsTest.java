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

import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MergeCommitOptions}.
 */
public class MergeCommitOptionsTest {

    @Test
    public void testPublishTimeoutDefaultValueWhenMergeCommitEnabled() {
        Configuration options = new Configuration();
        Map<String, String> streamLoadProperties = new HashMap<>();
        streamLoadProperties.put("enable_merge_commit", "true");

        StreamLoadTableProperties.Builder tablePropsBuilder = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_table");
        StreamLoadProperties.Builder propsBuilder = StreamLoadProperties.builder()
                .loadUrls("http://localhost:8030")
                .username("root")
                .password("");

        MergeCommitOptions.buildMergeCommitOptions(options, streamLoadProperties, tablePropsBuilder, propsBuilder);

        propsBuilder.defaultTableProperties(tablePropsBuilder.build());
        StreamLoadProperties props = propsBuilder.build();

        // Default publish timeout should be 10000ms when merge commit is enabled
        assertEquals(10000, props.getPublishTimeoutMs());
    }

    @Test
    public void testPublishTimeoutCustomValueWhenMergeCommitEnabled() {
        Configuration options = new Configuration();
        options.setInteger(StarRocksSinkOptions.SINK_PUBLISH_TIMEOUT, 30000);

        Map<String, String> streamLoadProperties = new HashMap<>();
        streamLoadProperties.put("enable_merge_commit", "true");

        StreamLoadTableProperties.Builder tablePropsBuilder = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_table");
        StreamLoadProperties.Builder propsBuilder = StreamLoadProperties.builder()
                .loadUrls("http://localhost:8030")
                .username("root")
                .password("");

        MergeCommitOptions.buildMergeCommitOptions(options, streamLoadProperties, tablePropsBuilder, propsBuilder);

        propsBuilder.defaultTableProperties(tablePropsBuilder.build());
        StreamLoadProperties props = propsBuilder.build();

        // Custom publish timeout should override default
        assertEquals(30000, props.getPublishTimeoutMs());
    }

    @Test
    public void testPublishTimeoutNotSetWhenMergeCommitDisabled() {
        Configuration options = new Configuration();
        Map<String, String> streamLoadProperties = new HashMap<>();
        // merge commit is not enabled

        StreamLoadTableProperties.Builder tablePropsBuilder = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_table");
        StreamLoadProperties.Builder propsBuilder = StreamLoadProperties.builder()
                .loadUrls("http://localhost:8030")
                .username("root")
                .password("");

        MergeCommitOptions.buildMergeCommitOptions(options, streamLoadProperties, tablePropsBuilder, propsBuilder);

        propsBuilder.defaultTableProperties(tablePropsBuilder.build());
        StreamLoadProperties props = propsBuilder.build();

        // When merge commit is disabled, publish timeout should be default (-1)
        assertEquals(-1, props.getPublishTimeoutMs());
    }

    @Test
    public void testPublishTimeoutZeroValueWhenMergeCommitEnabled() {
        Configuration options = new Configuration();
        options.setInteger(StarRocksSinkOptions.SINK_PUBLISH_TIMEOUT, 0);

        Map<String, String> streamLoadProperties = new HashMap<>();
        streamLoadProperties.put("enable_merge_commit", "true");

        StreamLoadTableProperties.Builder tablePropsBuilder = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_table");
        StreamLoadProperties.Builder propsBuilder = StreamLoadProperties.builder()
                .loadUrls("http://localhost:8030")
                .username("root")
                .password("");

        MergeCommitOptions.buildMergeCommitOptions(options, streamLoadProperties, tablePropsBuilder, propsBuilder);

        propsBuilder.defaultTableProperties(tablePropsBuilder.build());
        StreamLoadProperties props = propsBuilder.build();

        // Zero value should be set as-is
        assertEquals(0, props.getPublishTimeoutMs());
    }

    @Test
    public void testPublishTimeoutNegativeValueOverridesDefault() {
        Configuration options = new Configuration();
        options.setInteger(StarRocksSinkOptions.SINK_PUBLISH_TIMEOUT, -1);

        Map<String, String> streamLoadProperties = new HashMap<>();
        streamLoadProperties.put("enable_merge_commit", "true");

        StreamLoadTableProperties.Builder tablePropsBuilder = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_table");
        StreamLoadProperties.Builder propsBuilder = StreamLoadProperties.builder()
                .loadUrls("http://localhost:8030")
                .username("root")
                .password("");

        MergeCommitOptions.buildMergeCommitOptions(options, streamLoadProperties, tablePropsBuilder, propsBuilder);

        propsBuilder.defaultTableProperties(tablePropsBuilder.build());
        StreamLoadProperties props = propsBuilder.build();

        // -1 should override the default 10000
        assertEquals(-1, props.getPublishTimeoutMs());
    }
}
