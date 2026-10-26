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

package com.starrocks.data.load.stream.mergecommit;

import org.junit.Test;
import org.slf4j.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class SharedServiceRegistryTest {

    private static class TestService extends SharedService {

        private final boolean failInit;
        private int numInits;
        private int numResets;

        TestService(boolean failInit) {
            this.failInit = failInit;
        }

        @Override
        protected Logger getLogger() {
            return null;
        }

        @Override
        protected void init() throws Exception {
            if (failInit) {
                throw new Exception("init failed");
            }
            numInits++;
        }

        @Override
        protected void reset() {
            numResets++;
        }
    }

    @Test
    public void testShareServiceAmongEqualKeys() throws Exception {
        SharedServiceRegistry<String, TestService> registry = new SharedServiceRegistry<>();
        TestService service1 = registry.acquire("a", () -> new TestService(false));
        TestService service2 = registry.acquire("a", () -> new TestService(false));
        TestService service3 = registry.acquire("b", () -> new TestService(false));
        assertSame(service1, service2);
        assertNotSame(service1, service3);
        assertEquals(1, service1.numInits);
        assertEquals(2, registry.size());

        registry.release("a", service1);
        assertEquals(0, service1.numResets);
        registry.release("a", service2);
        assertEquals(1, service1.numResets);
        assertEquals(1, registry.size());

        // the last release dropped the service, so the next acquire creates a new one
        TestService service4 = registry.acquire("a", () -> new TestService(false));
        assertNotSame(service1, service4);
        assertEquals(1, service4.numInits);

        registry.release("a", service4);
        registry.release("b", service3);
        assertEquals(0, registry.size());
    }

    @Test
    public void testFailedInitKeepsNoService() throws Exception {
        SharedServiceRegistry<String, TestService> registry = new SharedServiceRegistry<>();
        try {
            registry.acquire("a", () -> new TestService(true));
            fail("acquire should fail when the service fails to init");
        } catch (Exception e) {
            assertEquals("init failed", e.getMessage());
        }
        assertEquals(0, registry.size());

        TestService service = registry.acquire("a", () -> new TestService(false));
        assertEquals(1, service.numInits);
        registry.release("a", service);
        assertEquals(0, registry.size());
    }
}
