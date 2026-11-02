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

package com.starrocks.data.load.stream.mergecommit.fe;

import com.starrocks.data.load.stream.MockedStarRocksHttpServer;
import com.starrocks.data.load.stream.auth.Credentials;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class DefaultFeHttpServiceCredentialsTest {

    private MockedStarRocksHttpServer mockedServer;

    @Before
    public void setUp() throws Exception {
        mockedServer = MockedStarRocksHttpServer.builder()
                .port(0)
                .enforceAuth("user_a", "password_a")
                .build();
        mockedServer.start();
    }

    @After
    public void tearDown() {
        if (mockedServer != null) {
            mockedServer.stop();
        }
    }

    private DefaultFeHttpService.Config baseConfig() {
        DefaultFeHttpService.Config config = new DefaultFeHttpService.Config();
        config.candidateHosts = Collections.singletonList(mockedServer.getBaseUrl());
        return config;
    }

    @Test
    public void testProviderIsAskedForEveryRequest() throws Exception {
        AtomicReference<Credentials> current = new AtomicReference<>(new Credentials("user_a", "password_a"));
        DefaultFeHttpService.Config config = baseConfig();
        config.credentialsProvider = current::get;

        try (DefaultFeHttpService service = new DefaultFeHttpService(config)) {
            service.start();
            assertEquals(200, (int) service.getLabelState("db", "label").getFirst());

            mockedServer.setCredentials("user_b", "password_b");
            current.set(new Credentials("user_b", "password_b"));

            assertEquals(200, (int) service.getLabelState("db", "label").getFirst());
        }
    }

    @Test
    public void testUsernameAndPasswordAreUsedWithoutProvider() throws Exception {
        DefaultFeHttpService.Config config = baseConfig();
        config.username = "user_a";
        config.password = "password_a";

        try (DefaultFeHttpService service = new DefaultFeHttpService(config)) {
            service.start();
            assertEquals(200, (int) service.getLabelState("db", "label").getFirst());

            mockedServer.setCredentials("user_b", "password_b");

            assertEquals(401, (int) service.getLabelState("db", "label").getFirst());
        }
    }
}
