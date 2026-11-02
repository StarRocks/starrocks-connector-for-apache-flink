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

package com.starrocks.data.load.stream.v2;

import com.starrocks.data.load.stream.LoadMetrics;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TransportFailureCallbackTest {

    private static final String DB = "db";
    private static final String TBL = "tbl";

    private static final class RecordingListener implements StreamLoadListener {
        private final List<StreamLoadResponse> responses = new ArrayList<>();

        @Override
        public void onResponse(StreamLoadResponse response) {
            responses.add(response);
        }
    }

    private static StreamLoadProperties buildProperties() {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(DB)
                .table(TBL)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(10)
                .build();
        return StreamLoadProperties.builder()
                .loadUrls("http://127.0.0.1:1")
                .username("root")
                .password("")
                .version("3.5.6")
                .labelPrefix("sr508-")
                .defaultTableProperties(tableProps)
                .scanningFrequency(50)
                .ioThreadCount(1)
                .build();
    }

    private static LoadMetrics loadMetricsOf(DefaultStreamLoadManager manager) throws Exception {
        Field field = DefaultStreamLoadManager.class.getDeclaredField("loadMetrics");
        field.setAccessible(true);
        return (LoadMetrics) field.get(manager);
    }

    @Test
    public void testTransportFailureNotifiesListenerAndCountsFailure() throws Exception {
        DefaultStreamLoadManager manager = new DefaultStreamLoadManager(buildProperties(), true);
        manager.init();
        try {
            RecordingListener listener = new RecordingListener();
            manager.setStreamLoadListener(listener);
            LoadMetrics metrics = loadMetricsOf(manager);

            RuntimeException transportFailure = new RuntimeException("connection reset by peer");
            manager.callback(transportFailure);

            Assert.assertSame("manager must keep the original Throwable",
                    transportFailure, manager.getException());
            Assert.assertEquals("listener must be notified exactly once", 1, listener.responses.size());
            Assert.assertSame("listener must observe the original Throwable",
                    transportFailure, listener.responses.get(0).getException());
            Assert.assertNull("a transport failure carries no flush rows",
                    listener.responses.get(0).getFlushRows());
            Assert.assertEquals("transport failure must be counted as failed",
                    1L, failedLoads(metrics));
            Assert.assertEquals("transport failure must not be counted as success",
                    0L, successLoads(metrics));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testTransportFailureWithoutListenerIsSafe() throws Exception {
        DefaultStreamLoadManager manager = new DefaultStreamLoadManager(buildProperties(), true);
        manager.init();
        try {
            RuntimeException transportFailure = new RuntimeException("broken pipe");
            manager.callback(transportFailure);

            Assert.assertSame("manager must keep the original Throwable",
                    transportFailure, manager.getException());
            LoadMetrics metrics = loadMetricsOf(manager);
            Assert.assertEquals(1L, failedLoads(metrics));
            Assert.assertEquals(0L, successLoads(metrics));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testResponseCallbacksBehaveUnchanged() throws Exception {
        DefaultStreamLoadManager manager = new DefaultStreamLoadManager(buildProperties(), true);
        manager.init();
        try {
            RecordingListener listener = new RecordingListener();
            manager.setStreamLoadListener(listener);
            LoadMetrics metrics = loadMetricsOf(manager);

            Exception responseFailure = new RuntimeException("server rejected the load");
            StreamLoadResponse failed = new StreamLoadResponse();
            failed.setException(responseFailure);
            manager.callback(failed);

            Assert.assertEquals("response failure still notifies the listener",
                    1, listener.responses.size());
            Assert.assertSame("response failure keeps the original Throwable",
                    responseFailure, manager.getException());
            Assert.assertEquals("response failure still counts as failed",
                    1L, failedLoads(metrics));
            Assert.assertEquals("response failure still counts no success",
                    0L, successLoads(metrics));

            StreamLoadResponse succeeded = new StreamLoadResponse();
            succeeded.setFlushRows(5L);
            succeeded.setFlushBytes(10L);
            succeeded.setCostNanoTime(1000L);
            manager.callback(succeeded);

            Assert.assertEquals("success still notifies the listener", 2, listener.responses.size());
            Assert.assertEquals("success still counts as success",
                    1L, successLoads(metrics));
            Assert.assertEquals("success must not add a failure",
                    1L, failedLoads(metrics));
            Assert.assertSame("manager error is not overwritten by a success",
                    responseFailure, manager.getException());
        } finally {
            manager.close();
        }
    }

    private static long failedLoads(LoadMetrics metrics) throws Exception {
        return counterOf(metrics, "numberOfFailedLoad");
    }

    private static long successLoads(LoadMetrics metrics) throws Exception {
        return counterOf(metrics, "numberOfSuccessLoad");
    }

    /**
     * LoadMetrics exposes no accessor for these two counters, so read the private
     * fields directly, like the other tests in this module do.
     */
    private static long counterOf(LoadMetrics metrics, String fieldName) throws Exception {
        Field field = LoadMetrics.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return ((AtomicLong) field.get(metrics)).get();
    }
}
