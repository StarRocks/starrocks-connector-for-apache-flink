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

import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link StarRocksSinkOptions}.
 */
public class StarRocksSinkOptionsTest {

    private StarRocksSinkOptions createSinkOptions(Configuration conf) {
        return new StarRocksSinkOptions(conf, conf.toMap());
    }

    private Configuration createBaseConfiguration() {
        Configuration conf = new Configuration();
        conf.set(StarRocksSinkOptions.TABLE_NAME, "test_table");
        conf.set(StarRocksSinkOptions.DATABASE_NAME, "test_db");
        conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:8030");
        conf.set(StarRocksSinkOptions.JDBC_URL, "jdbc:mysql://127.0.0.1:9030");
        conf.set(StarRocksSinkOptions.USERNAME, "root");
        conf.set(StarRocksSinkOptions.PASSWORD, "");
        return conf;
    }

    @Test
    public void testPublishTimeoutMsDefaultValue() {
        Configuration conf = createBaseConfiguration();
        StarRocksSinkOptions sinkOptions = createSinkOptions(conf);

        // Default value should be -1
        assertEquals(-1, sinkOptions.getPublishTimeoutMs());
    }

    @Test
    public void testPublishTimeoutMsCustomValue() {
        Configuration conf = createBaseConfiguration();
        conf.set(StarRocksSinkOptions.SINK_PUBLISH_TIMEOUT, 10000);
        StarRocksSinkOptions sinkOptions = createSinkOptions(conf);

        assertEquals(10000, sinkOptions.getPublishTimeoutMs());
    }

    @Test
    public void testPublishTimeoutMsZeroValue() {
        Configuration conf = createBaseConfiguration();
        conf.set(StarRocksSinkOptions.SINK_PUBLISH_TIMEOUT, 0);
        StarRocksSinkOptions sinkOptions = createSinkOptions(conf);

        assertEquals(0, sinkOptions.getPublishTimeoutMs());
    }

    @Test
    public void testPublishTimeoutMsLargeValue() {
        Configuration conf = createBaseConfiguration();
        conf.set(StarRocksSinkOptions.SINK_PUBLISH_TIMEOUT, 300000);
        StarRocksSinkOptions sinkOptions = createSinkOptions(conf);

        assertEquals(300000, sinkOptions.getPublishTimeoutMs());
    }
}
