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

package com.starrocks.connector.flink.manager;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class StarRocksStreamLoadListenerTest {

    private static final String SHARED_TXN_LATENCY_MS = "sharedTxnLatencyMs";

    @Test
    public void testSharedTxnLatencyHistogramInMultiTableMode() {
        RecordingMetricGroup metricGroup = new RecordingMetricGroup();
        StarRocksStreamLoadListener listener = new StarRocksStreamLoadListener(metricGroup, buildSinkOptions(true));

        Histogram histogram = metricGroup.histograms.get(SHARED_TXN_LATENCY_MS);
        assertNotNull(histogram);
        listener.onSharedTransactionCommitted("label-1", 1000L, 120L);
        listener.onSharedTransactionCommitted("label-2", 2000L, 80L);
        assertEquals(2, histogram.getCount());
        assertEquals(120L, histogram.getStatistics().getMax());
        assertEquals(80L, histogram.getStatistics().getMin());
    }

    @Test
    public void testNoSharedTxnLatencyHistogramWithoutMultiTableMode() {
        RecordingMetricGroup metricGroup = new RecordingMetricGroup();
        StarRocksStreamLoadListener listener = new StarRocksStreamLoadListener(metricGroup, buildSinkOptions(false));

        assertFalse(metricGroup.histograms.containsKey(SHARED_TXN_LATENCY_MS));
        // must be a no-op, not an NPE
        listener.onSharedTransactionCommitted("label-1", 1000L, 120L);
    }

    private static StarRocksSinkOptions buildSinkOptions(boolean multiTable) {
        Configuration conf = new Configuration();
        conf.setString(StarRocksSinkOptions.TABLE_NAME, "test_table");
        conf.setString(StarRocksSinkOptions.DATABASE_NAME, "test_db");
        conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:8030");
        conf.setString(StarRocksSinkOptions.JDBC_URL, "jdbc:mysql://127.0.0.1:9030");
        conf.setString(StarRocksSinkOptions.USERNAME, "root");
        conf.setString(StarRocksSinkOptions.PASSWORD, "");
        if (multiTable) {
            conf.setBoolean(StarRocksSinkOptions.SINK_MULTI_TABLE_TXN_ENABLED, true);
            conf.setString(StarRocksSinkOptions.SINK_SEMANTIC, StarRocksSinkSemantic.AT_LEAST_ONCE.getName());
        }
        return new StarRocksSinkOptions(conf, conf.toMap());
    }

    /** Keeps the registered histograms so a test can read them back. */
    private static class RecordingMetricGroup extends UnregisteredMetricsGroup {
        final Map<String, Histogram> histograms = new HashMap<>();

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            histograms.put(name, histogram);
            return histogram;
        }
    }
}
