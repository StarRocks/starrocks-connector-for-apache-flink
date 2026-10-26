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
import static org.junit.Assert.assertSame;

public class BackendHttpServiceTest {

    private static BackendHttpService.Config config(int maxConnectionsPerRoute, String username) {
        BackendHttpService.Config config = new BackendHttpService.Config();
        config.maxConnectionsPerRoute = maxConnectionsPerRoute;
        config.username = username;
        config.password = "";
        return config;
    }

    @Test
    public void testDifferentConnectionSettingsGetDifferentServices() throws Exception {
        BackendHttpService service1 = BackendHttpService.acquire(config(3, "user"));
        BackendHttpService service2 = BackendHttpService.acquire(config(5, "user"));
        try {
            assertNotSame(service1, service2);
        } finally {
            service1.release();
            service2.release();
        }
    }

    @Test
    public void testEqualConnectionSettingsShareService() throws Exception {
        BackendHttpService service1 = BackendHttpService.acquire(config(3, "userA"));
        // each request carries its own credentials, so other users share the connections
        BackendHttpService service2 = BackendHttpService.acquire(config(3, "userB"));
        try {
            assertSame(service1, service2);
        } finally {
            service1.release();
            service2.release();
        }
    }

    @Test
    public void testLastReleaseDropsService() throws Exception {
        BackendHttpService oldService = BackendHttpService.acquire(config(3, "user"));
        oldService.release();

        BackendHttpService newService = BackendHttpService.acquire(config(3, "user"));
        try {
            assertNotSame(oldService, newService);
        } finally {
            newService.release();
        }
    }
}
