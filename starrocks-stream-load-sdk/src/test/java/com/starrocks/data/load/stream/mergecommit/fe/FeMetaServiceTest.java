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

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class FeMetaServiceTest {

    private static FeMetaService.Config config(String username, String... hosts) {
        DefaultFeHttpService.Config httpConfig = new DefaultFeHttpService.Config();
        httpConfig.candidateHosts = Arrays.asList(hosts);
        httpConfig.username = username;
        httpConfig.password = "";
        FeMetaService.Config config = new FeMetaService.Config();
        config.httpServiceConfig = httpConfig;
        config.nodesStateServiceConfig = new NodesStateService.Config();
        config.numThreads = 1;
        return config;
    }

    @Test
    public void testDifferentConfigsGetDifferentServices() {
        FeMetaService serviceA = FeMetaService.getInstance(config("userA", "http://fe-a:8030"));
        FeMetaService serviceB = FeMetaService.getInstance(config("userB", "http://fe-b:8030"));
        assertNotSame(serviceA, serviceB);
        assertEquals("http://fe-a:8030", serviceA.getFeUrl());
        assertEquals("http://fe-b:8030", serviceB.getFeUrl());
    }

    @Test
    public void testNewConfigTakesEffectAfterLastRelease() throws Exception {
        FeMetaService oldService = FeMetaService.getInstance(config("user", "http://fe-a:8030"));
        oldService.takeRef();
        oldService.releaseRef();

        FeMetaService newService = FeMetaService.getInstance(config("user", "http://fe-b:8030"));
        assertEquals("http://fe-b:8030", newService.getFeUrl());
    }
}
