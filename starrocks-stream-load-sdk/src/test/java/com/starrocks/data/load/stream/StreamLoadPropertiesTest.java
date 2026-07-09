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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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

    @Test
    public void testArrowFormatTableProperties() {
        StreamLoadTableProperties tableProperties = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_tbl")
                .streamLoadDataFormat(StreamLoadDataFormat.ARROW)
                .build();

        assertEquals(StreamLoadDataFormat.ARROW, tableProperties.getDataFormat());
        assertEquals("arrow", tableProperties.getDataFormat().name());
        assertEquals(0, tableProperties.getDataFormat().first().length);
        assertEquals(0, tableProperties.getDataFormat().delimiter().length);
        assertEquals(0, tableProperties.getDataFormat().end().length);
        assertEquals("arrow", tableProperties.getProperties().get("format"));
    }

    @Test
    public void testArrowFormatPropertyNotDuplicated() {
        StreamLoadTableProperties tableProperties = StreamLoadTableProperties.builder()
                .database("test_db")
                .table("test_tbl")
                .streamLoadDataFormat(StreamLoadDataFormat.ARROW)
                .addProperty("format", "arrow")
                .build();

        assertEquals(StreamLoadDataFormat.ARROW, tableProperties.getDataFormat());
        assertEquals("arrow", tableProperties.getProperties().get("format"));
    }
}
