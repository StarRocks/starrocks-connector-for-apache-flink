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

import com.starrocks.data.load.stream.MockedStarRocksHttpServer;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.auth.Credentials;
import com.starrocks.data.load.stream.auth.CredentialsProvider;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Rotates the credentials the server accepts between two loads and checks that the loader
 * picks up the new credentials from its {@link CredentialsProvider} without being restarted.
 */
public class CredentialsRotationTest {

    private static final String DB = "db";
    private static final String TABLE = "tbl";
    private static final Credentials USER_A = new Credentials("user_a", "password_a");
    private static final Credentials USER_B = new Credentials("user_b", "password_b");

    /** Stands in for a provider that reads a secret which is rotated outside the job. */
    static class MutableCredentialsProvider implements CredentialsProvider {
        private static final long serialVersionUID = 1L;
        private volatile Credentials credentials;

        MutableCredentialsProvider(Credentials credentials) {
            this.credentials = credentials;
        }

        void set(Credentials credentials) {
            this.credentials = credentials;
        }

        @Override
        public Credentials getCredentials() {
            return credentials;
        }
    }

    private MockedStarRocksHttpServer mockedServer;

    @Before
    public void setUp() throws Exception {
        mockedServer = MockedStarRocksHttpServer.builder()
                .port(0)
                .enforceAuth(USER_A.getUsername(), USER_A.getPassword())
                .build();
        mockedServer.start();
    }

    @After
    public void tearDown() {
        if (mockedServer != null) {
            mockedServer.stop();
        }
    }

    private StreamLoadProperties.Builder propertiesBuilder() {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(DB)
                .table(TABLE)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(10)
                .build();
        return StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .version("3.5.6")
                .labelPrefix("test-rotation-")
                .defaultTableProperties(tableProps)
                .scanningFrequency(50)
                .ioThreadCount(1);
    }

    private void rotateServerTo(Credentials credentials) {
        mockedServer.setCredentials(credentials.getUsername(), credentials.getPassword());
    }

    private static void writeAndFlush(StreamLoadManagerV2 manager, int id) {
        manager.write(null, DB, TABLE, "{\"id\":" + id + "}");
        manager.flush();
    }

    @Test
    public void testTransactionLoadUsesRotatedCredentials() {
        MutableCredentialsProvider provider = new MutableCredentialsProvider(USER_A);
        StreamLoadProperties properties = propertiesBuilder()
                .credentialsProvider(provider)
                .enableTransaction()
                .build();
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();
        try {
            writeAndFlush(manager, 1);
            rotateServerTo(USER_B);
            provider.set(USER_B);
            writeAndFlush(manager, 2);

            assertNull(manager.getException());
            assertEquals(2, mockedServer.getBeginCount());
            assertEquals(2, mockedServer.getLoadCount());
            assertEquals(2, mockedServer.getPrepareCount());
            assertEquals(2, mockedServer.getCommitCount());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testStreamLoadUsesRotatedCredentials() {
        MutableCredentialsProvider provider = new MutableCredentialsProvider(USER_A);
        StreamLoadProperties properties = propertiesBuilder()
                .credentialsProvider(provider)
                .build();
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();
        try {
            writeAndFlush(manager, 1);
            rotateServerTo(USER_B);
            provider.set(USER_B);
            writeAndFlush(manager, 2);

            assertNull(manager.getException());
            assertEquals(2, mockedServer.getStreamLoadCount());
            assertEquals(0, mockedServer.getBeginCount());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testStaticCredentialsFailAfterRotation() {
        StreamLoadProperties properties = propertiesBuilder()
                .username(USER_A.getUsername())
                .password(USER_A.getPassword())
                .enableTransaction()
                .build();
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();
        try {
            writeAndFlush(manager, 1);
            rotateServerTo(USER_B);
            try {
                writeAndFlush(manager, 2);
                fail("a load with the credentials from before the rotation should be rejected");
            } catch (RuntimeException e) {
                assertNotNull(manager.getException());
            }
            assertEquals(1, mockedServer.getCommitCount());
        } finally {
            manager.close();
        }
    }
}
