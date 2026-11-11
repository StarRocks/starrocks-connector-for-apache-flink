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

import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import mockit.Mock;
import mockit.MockUp;
import org.apache.flink.configuration.ConfigOption;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@code sink.http.keep-alive.enabled} and {@code sink.http.keep-alive.idle-timeout-ms}.
 */
public class StarRocksSinkOptionsHttpKeepAliveTest {

    private StarRocksSinkOptions.Builder base() {
        return StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", "db")
                .withProperty("table-name", "t");
    }

    /** The SDK properties the sink is started with, without a live FE. */
    private StreamLoadProperties streamLoadProperties(StarRocksSinkOptions options) {
        new MockUp<StarRocksSinkTable>() {
            @Mock
            public String getVersion() {
                return "3.3.0";
            }
        };
        return options.getProperties(StarRocksSinkTable.builder().sinkOptions(options).build());
    }

    @Test
    public void testKeepAliveIsOnByDefault() {
        StarRocksSinkOptions options = base().build();
        assertTrue(options.isHttpKeepAliveEnabled());
        assertEquals(60000, options.getHttpKeepAliveIdleTimeoutMs());

        StreamLoadProperties properties = streamLoadProperties(options);
        assertTrue(properties.isEnableHttpKeepAlive());
        assertEquals(60000, properties.getHttpKeepAliveIdleTimeoutMs());
    }

    @Test
    public void testOptionsReachTheStreamLoadProperties() {
        StarRocksSinkOptions options = base()
                .withProperty("sink.http.keep-alive.enabled", "false")
                .withProperty("sink.http.keep-alive.idle-timeout-ms", "15000")
                .build();
        assertFalse(options.isHttpKeepAliveEnabled());

        StreamLoadProperties properties = streamLoadProperties(options);
        assertFalse(properties.isEnableHttpKeepAlive());
        assertEquals(15000, properties.getHttpKeepAliveIdleTimeoutMs());
    }

    @Test
    public void testNonPositiveIdleTimeoutIsRejected() {
        for (String value : new String[] {"0", "-1"}) {
            try {
                base().withProperty("sink.http.keep-alive.idle-timeout-ms", value).build();
                fail("Expected a validation failure for an idle timeout of " + value);
            } catch (RuntimeException e) {
                assertEquals("ValidationException", e.getClass().getSimpleName());
                assertTrue("The message should name the option: " + e.getMessage(),
                        e.getMessage().contains(StarRocksSinkOptions.SINK_HTTP_KEEP_ALIVE_IDLE_TIMEOUT.key()));
            }
        }
    }

    @Test
    public void testOptionsAreRegisteredWithTableFactory() {
        // Without the registration the SQL/Table API would reject the options as unknown.
        Set<ConfigOption<?>> optionalOptions = new StarRocksDynamicTableSinkFactory().optionalOptions();
        assertTrue(optionalOptions.contains(StarRocksSinkOptions.SINK_HTTP_KEEP_ALIVE_ENABLED));
        assertTrue(optionalOptions.contains(StarRocksSinkOptions.SINK_HTTP_KEEP_ALIVE_IDLE_TIMEOUT));
    }
}
