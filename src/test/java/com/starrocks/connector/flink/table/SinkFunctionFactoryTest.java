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

package com.starrocks.connector.flink.table;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.mock.MockFeHttpServer;
import mockit.Mock;
import mockit.MockUp;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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
            conf.setString(StarRocksSinkOptions.TABLE_NAME, "test");
            conf.setString(StarRocksSinkOptions.DATABASE_NAME, "test");
            conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:" + httpServer.getListenPort());
            conf.setString(StarRocksSinkOptions.JDBC_URL, "jdbc://127.0.0.1:1234");
            conf.setString(StarRocksSinkOptions.USERNAME, "root");
            conf.setString(StarRocksSinkOptions.PASSWORD, "");
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());

            {
                httpServer.addJsonResponse("");
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("status", "FAILED");
                jsonObject.put("msg", "Not implemented");
                httpServer.addJsonResponse(jsonObject.toJSONString());
                boolean support = SinkFunctionFactory.isStarRocksSupportTransactionLoad(sinkOptions);
                assertFalse(support);
            }

            {
                httpServer.addJsonResponse("");
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("Status", "INVALID_ARGUMENT");
                jsonObject.put("Message", "empty label");
                httpServer.addJsonResponse(jsonObject.toJSONString());
                boolean support = SinkFunctionFactory.isStarRocksSupportTransactionLoad(sinkOptions);
                assertTrue(support);
            }

            {
                httpServer.addJsonResponse("");
                httpServer.addJsonResponse(MockFeHttpServer.NULL_RESPONSE);
                try {
                    SinkFunctionFactory.isStarRocksSupportTransactionLoad(sinkOptions);
                    fail();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    @Test
    public void testChooseSinkVersionAutomaticallyForExactlyOnce() {
        testChooseSinkVersionAutomaticallyBase(true,
                Arrays.asList(
                        SinkFunctionFactory.SinkVersion.V2,
                        SinkFunctionFactory.SinkVersion.V2,
                        SinkFunctionFactory.SinkVersion.V1)
        );
    }

    @Test
    public void testChooseSinkVersionAutomaticallyForAtLeastOnce() {
        testChooseSinkVersionAutomaticallyBase(false,
                Arrays.asList(
                        SinkFunctionFactory.SinkVersion.V2,
                        SinkFunctionFactory.SinkVersion.V2,
                        SinkFunctionFactory.SinkVersion.V2)
        );
    }

    private void testChooseSinkVersionAutomaticallyBase(
            boolean isExactlyOnce, List<SinkFunctionFactory.SinkVersion> expectedVersions) {
        Configuration conf = new Configuration();
        conf.setString(StarRocksSinkOptions.SINK_SEMANTIC.key(),
                isExactlyOnce ? StarRocksSinkSemantic.EXACTLY_ONCE.getName()
                        : StarRocksSinkSemantic.AT_LEAST_ONCE.getName());
        StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());
        AtomicReference<Boolean> supportTransactionLoad = new AtomicReference<>();
        new MockUp<SinkFunctionFactory>() {
            @Mock
            public boolean isStarRocksSupportTransactionLoad(StarRocksSinkOptions sinkOptions) {
                if (supportTransactionLoad.get() == null) {
                    throw new NullPointerException();
                }
                return supportTransactionLoad.get();
            }
        };

        supportTransactionLoad.set(null);
        assertEquals(expectedVersions.get(0), SinkFunctionFactory.chooseSinkVersionAutomatically(sinkOptions));

        supportTransactionLoad.set(true);
        assertEquals(expectedVersions.get(1), SinkFunctionFactory.chooseSinkVersionAutomatically(sinkOptions));

        supportTransactionLoad.set(false);
        assertEquals(expectedVersions.get(2), SinkFunctionFactory.chooseSinkVersionAutomatically(sinkOptions));
    }

    @Test
    public void testGetSinkVersion() {
        Configuration conf = new Configuration();
        {
            conf.setString(StarRocksSinkOptions.SINK_VERSION, "V1");
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());
            assertEquals(SinkFunctionFactory.SinkVersion.V1, SinkFunctionFactory.getSinkVersion(sinkOptions));
        }

        {
            conf.setString(StarRocksSinkOptions.SINK_VERSION, "V2");
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());
            assertEquals(SinkFunctionFactory.SinkVersion.V2, SinkFunctionFactory.getSinkVersion(sinkOptions));
        }

        {
            AtomicReference<SinkFunctionFactory.SinkVersion> autoVersion = new AtomicReference<>(null);
            new MockUp<SinkFunctionFactory>() {
                @Mock
                public SinkFunctionFactory.SinkVersion chooseSinkVersionAutomatically(StarRocksSinkOptions sinkOptions) {
                    return autoVersion.get();
                }
            };
            conf.setString(StarRocksSinkOptions.SINK_VERSION, "AUTO");
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());

            autoVersion.set(SinkFunctionFactory.SinkVersion.V1);
            assertEquals(SinkFunctionFactory.SinkVersion.V1, SinkFunctionFactory.getSinkVersion(sinkOptions));

            autoVersion.set(SinkFunctionFactory.SinkVersion.V2);
            assertEquals(SinkFunctionFactory.SinkVersion.V2, SinkFunctionFactory.getSinkVersion(sinkOptions));
        }

        {
            conf.setString(StarRocksSinkOptions.SINK_VERSION, "UNKNOWN");
            StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(conf, new HashMap<>());
            try {
                SinkFunctionFactory.getSinkVersion(sinkOptions);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof UnsupportedOperationException);
            }
        }
    }
}
