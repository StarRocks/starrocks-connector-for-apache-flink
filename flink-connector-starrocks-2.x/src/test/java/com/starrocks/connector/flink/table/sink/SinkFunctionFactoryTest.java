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

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.mock.MockFeHttpServer;
import com.starrocks.data.load.stream.StreamLoadUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.starrocks.data.load.stream.StreamLoadUtils.isStarRocksSupportTransactionLoad;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link SinkFunctionFactory}.
 */
public class SinkFunctionFactoryTest {

    @Test
    public void testIsStarRocksSupportTransactionLoad() throws Exception {
        try (MockFeHttpServer httpServer = new MockFeHttpServer()) {
            httpServer.start();
            Configuration conf = new Configuration();
            conf.set(StarRocksSinkOptions.TABLE_NAME, "test");
            conf.set(StarRocksSinkOptions.DATABASE_NAME, "test");
            conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:" + httpServer.getListenPort());
            conf.set(StarRocksSinkOptions.JDBC_URL, "jdbc://127.0.0.1:1234");
            conf.set(StarRocksSinkOptions.USERNAME, "root");
            conf.set(StarRocksSinkOptions.PASSWORD, "");
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());

            {
                httpServer.addJsonResponse("");
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("status", "FAILED");
                jsonObject.put("msg", "Not implemented");
                httpServer.addJsonResponse(jsonObject.toJSONString());
                boolean support = probeTransactionStreamLoad(sinkOptions);
                assertFalse(support);
            }

            {
                httpServer.addJsonResponse("");
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("Status", "INVALID_ARGUMENT");
                jsonObject.put("Message", "empty label");
                httpServer.addJsonResponse(jsonObject.toJSONString());
                boolean support = probeTransactionStreamLoad(sinkOptions);
                assertTrue(support);
            }

            {
                httpServer.addJsonResponse("");
                httpServer.addJsonResponse(MockFeHttpServer.NULL_RESPONSE);
                try {
                    probeTransactionStreamLoad(sinkOptions);
                    fail();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    private boolean probeTransactionStreamLoad(StarRocksSinkOptions sinkOptions) {
        return isStarRocksSupportTransactionLoad(
                sinkOptions.getLoadUrlList(), sinkOptions.getConnectTimeout(), sinkOptions.getUsername(), sinkOptions.getPassword());
    }

    @Test
    public void testDetectStarRocksFeature() {
        AtomicReference<Boolean> supportTransactionLoad = new AtomicReference<>();
        new MockUp<StreamLoadUtils>() {
            @Mock
            public boolean isStarRocksSupportTransactionLoad(List<String> httpUrls, int connectTimeout, String userName, String password) {
                if (supportTransactionLoad.get() == null) {
                    throw new NullPointerException();
                }
                return supportTransactionLoad.get();
            }
        };

        {
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(new Configuration(), new HashMap<>());
            supportTransactionLoad.set(null);
            assertFalse(sinkOptions.isSupportTransactionStreamLoad());
            SinkFunctionFactory.detectStarRocksFeature(sinkOptions);
            assertTrue(sinkOptions.isSupportTransactionStreamLoad());
        }

        {
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(new Configuration(), new HashMap<>());
            supportTransactionLoad.set(true);
            assertFalse(sinkOptions.isSupportTransactionStreamLoad());
            SinkFunctionFactory.detectStarRocksFeature(sinkOptions);
            assertTrue(sinkOptions.isSupportTransactionStreamLoad());
        }

        {
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(new Configuration(), new HashMap<>());
            supportTransactionLoad.set(false);
            assertFalse(sinkOptions.isSupportTransactionStreamLoad());
            SinkFunctionFactory.detectStarRocksFeature(sinkOptions);
            assertFalse(sinkOptions.isSupportTransactionStreamLoad());
        }
    }

    @Test
    public void testChooseSinkVersionAlwaysReturnsV2() {
        // In Flink 2.x, chooseSinkVersionAutomatically always returns V2
        for (String semantic : Arrays.asList(
                StarRocksSinkSemantic.EXACTLY_ONCE.getName(),
                StarRocksSinkSemantic.AT_LEAST_ONCE.getName())) {
            Configuration conf = new Configuration();
            conf.setString(StarRocksSinkOptions.SINK_SEMANTIC.key(), semantic);
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());

            sinkOptions.setSupportTransactionStreamLoad(true);
            assertEquals(SinkFunctionFactory.SinkVersion.V2,
                    SinkFunctionFactory.chooseSinkVersionAutomatically(sinkOptions));

            sinkOptions.setSupportTransactionStreamLoad(false);
            assertEquals(SinkFunctionFactory.SinkVersion.V2,
                    SinkFunctionFactory.chooseSinkVersionAutomatically(sinkOptions));
        }
    }

    @Test
    public void testGetSinkVersion() {
        Configuration conf = new Configuration();

        // V1 should throw in Flink 2.x
        {
            conf.set(StarRocksSinkOptions.SINK_VERSION, "V1");
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());
            try {
                SinkFunctionFactory.getSinkVersion(sinkOptions);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof UnsupportedOperationException);
                assertTrue(e.getMessage().contains("not supported in Flink 2.x"));
            }
        }

        // V2 should work
        {
            conf.set(StarRocksSinkOptions.SINK_VERSION, "V2");
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());
            assertEquals(SinkFunctionFactory.SinkVersion.V2, SinkFunctionFactory.getSinkVersion(sinkOptions));
        }

        // AUTO should resolve to V2
        {
            conf.set(StarRocksSinkOptions.SINK_VERSION, "AUTO");
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());
            assertEquals(SinkFunctionFactory.SinkVersion.V2, SinkFunctionFactory.getSinkVersion(sinkOptions));
        }

        // Unknown should throw
        {
            conf.set(StarRocksSinkOptions.SINK_VERSION, "UNKNOWN");
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());
            try {
                SinkFunctionFactory.getSinkVersion(sinkOptions);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof UnsupportedOperationException);
            }
        }
    }

    @Test
    public void testCreateSinkRejectsExplicitV1() {
        Configuration conf = new Configuration();
        conf.set(StarRocksSinkOptions.TABLE_NAME, "test");
        conf.set(StarRocksSinkOptions.DATABASE_NAME, "test");
        conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:8030");
        conf.set(StarRocksSinkOptions.JDBC_URL, "jdbc://127.0.0.1:1234");
        conf.set(StarRocksSinkOptions.USERNAME, "root");
        conf.set(StarRocksSinkOptions.PASSWORD, "");
        conf.set(StarRocksSinkOptions.SINK_VERSION, "V1");
        StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());
        try {
            SinkFunctionFactory.createSink(sinkOptions);
            fail();
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("not supported in Flink 2.x"));
        }
    }
}
