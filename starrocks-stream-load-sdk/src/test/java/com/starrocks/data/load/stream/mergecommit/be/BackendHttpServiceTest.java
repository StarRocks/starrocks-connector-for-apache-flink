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

package com.starrocks.data.load.stream.mergecommit.be;

import org.junit.Test;

import static org.junit.Assert.assertNotSame;

public class BackendHttpServiceTest {

    private static BackendHttpService.Config config(int maxConnectionsPerRoute, String username) {
        BackendHttpService.Config config = new BackendHttpService.Config();
        config.maxConnectionsPerRoute = maxConnectionsPerRoute;
        config.username = username;
        config.password = "";
        return config;
    }

    @Test
    public void testDifferentConnectionSettingsGetDifferentServices() {
        BackendHttpService service1 = BackendHttpService.getInstance(config(3, "user"));
        BackendHttpService service2 = BackendHttpService.getInstance(config(5, "user"));
        assertNotSame(service1, service2);
    }
}
