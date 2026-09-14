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
        conf.setString(StarRocksSinkOptions.TABLE_NAME, "test_table");
        conf.setString(StarRocksSinkOptions.DATABASE_NAME, "test_db");
        conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:8030");
        conf.setString(StarRocksSinkOptions.JDBC_URL, "jdbc:mysql://127.0.0.1:9030");
        conf.setString(StarRocksSinkOptions.USERNAME, "root");
        conf.setString(StarRocksSinkOptions.PASSWORD, "");
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
        conf.setInteger(StarRocksSinkOptions.SINK_PUBLISH_TIMEOUT, 10000);
        StarRocksSinkOptions sinkOptions = createSinkOptions(conf);

        assertEquals(10000, sinkOptions.getPublishTimeoutMs());
    }

    @Test
    public void testPublishTimeoutMsZeroValue() {
        Configuration conf = createBaseConfiguration();
        conf.setInteger(StarRocksSinkOptions.SINK_PUBLISH_TIMEOUT, 0);
        StarRocksSinkOptions sinkOptions = createSinkOptions(conf);

        assertEquals(0, sinkOptions.getPublishTimeoutMs());
    }

    @Test
    public void testPublishTimeoutMsLargeValue() {
        Configuration conf = createBaseConfiguration();
        conf.setInteger(StarRocksSinkOptions.SINK_PUBLISH_TIMEOUT, 300000);
        StarRocksSinkOptions sinkOptions = createSinkOptions(conf);

        assertEquals(300000, sinkOptions.getPublishTimeoutMs());
    }

    // -------------------------------------------------------------------------
    // sink.transaction.multi-table.max-txn-bytes
    // -------------------------------------------------------------------------

    private Configuration createMultiTableConfiguration() {
        Configuration conf = createBaseConfiguration();
        conf.setBoolean(StarRocksSinkOptions.SINK_MULTI_TABLE_TXN_ENABLED, true);
        conf.setString(StarRocksSinkOptions.SINK_SEMANTIC, StarRocksSinkSemantic.AT_LEAST_ONCE.getName());
        return conf;
    }

    @Test
    public void testMultiTableMaxTxnBytesDefaultsToUnlimited() {
        StarRocksSinkOptions sinkOptions = createSinkOptions(createMultiTableConfiguration());
        // 0 = no hard cap: the writer buffers an in-progress source transaction
        // until txnEnd and only the JVM heap bounds it.
        assertEquals(0L, sinkOptions.getMultiTableMaxTxnBytes());
    }

    @Test
    public void testMultiTableMaxTxnBytesCustomValue() {
        Configuration conf = createMultiTableConfiguration();
        conf.setLong(StarRocksSinkOptions.SINK_MULTI_TABLE_TXN_MAX_TXN_BYTES, 512L * 1024 * 1024);
        StarRocksSinkOptions sinkOptions = createSinkOptions(conf);
        assertEquals(512L * 1024 * 1024, sinkOptions.getMultiTableMaxTxnBytes());
    }

    @Test
    public void testMultiTableMaxTxnBytesRejectsNegative() {
        Configuration conf = createMultiTableConfiguration();
        conf.setLong(StarRocksSinkOptions.SINK_MULTI_TABLE_TXN_MAX_TXN_BYTES, -1L);
        try {
            createSinkOptions(conf);
            org.junit.Assert.fail("Expected a validation failure for a negative max-txn-bytes");
        } catch (RuntimeException e) {
            assertEquals("ValidationException", e.getClass().getSimpleName());
            org.junit.Assert.assertTrue("Message should name the option: " + e.getMessage(),
                    e.getMessage().contains(StarRocksSinkOptions.SINK_MULTI_TABLE_TXN_MAX_TXN_BYTES.key()));
        }
    }

    @Test
    public void testMultiTableMaxTxnBytesNotValidatedWhenMultiTableDisabled() {
        // The option is meaningless outside multi-table mode; a stray negative
        // value must not break an ordinary sink.
        Configuration conf = createBaseConfiguration();
        conf.setLong(StarRocksSinkOptions.SINK_MULTI_TABLE_TXN_MAX_TXN_BYTES, -1L);
        StarRocksSinkOptions sinkOptions = createSinkOptions(conf);
        assertEquals(-1L, sinkOptions.getMultiTableMaxTxnBytes());
    }

    @Test
    public void testMultiTableMaxTxnBytesRegisteredWithTableFactory() {
        // Without this registration the SQL/Table API would reject the option as unknown.
        org.junit.Assert.assertTrue(new StarRocksDynamicTableSinkFactory().optionalOptions()
                .contains(StarRocksSinkOptions.SINK_MULTI_TABLE_TXN_MAX_TXN_BYTES));
    }
}
