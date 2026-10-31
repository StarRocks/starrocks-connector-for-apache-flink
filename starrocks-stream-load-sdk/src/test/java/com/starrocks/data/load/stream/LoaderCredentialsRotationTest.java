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

package com.starrocks.data.load.stream;

import com.starrocks.data.load.stream.auth.Credentials;
import com.starrocks.data.load.stream.auth.CredentialsProvider;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Checks the loader requests outside the load path (label state, rollback, cancel) use the
 * credentials the provider returns at request time, and that a broken provider fails the loader
 * at start.
 */
public class LoaderCredentialsRotationTest {

    private static final String DB = "db";
    private static final String TABLE = "tbl";

    private MockedStarRocksHttpServer mockedServer;
    private final AtomicReference<Credentials> current =
            new AtomicReference<>(new Credentials("user_a", "password_a"));

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

    private StreamLoadProperties properties(CredentialsProvider provider) {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database(DB)
                .table(TABLE)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();
        return StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .credentialsProvider(provider)
                .version("3.5.6")
                .enableTransaction()
                .defaultTableProperties(tableProps)
                .ioThreadCount(1)
                .build();
    }

    private static StreamLoadManager noopManager() {
        return new StreamLoadManager() {
            @Override public void init() {}
            @Override public void write(String uniqueKey, String database, String table, String... rows) {}
            @Override public void callback(StreamLoadResponse response) {}
            @Override public void callback(Throwable e) {}
            @Override public void flush() {}
            @Override public StreamLoadSnapshot snapshot() { return null; }
            @Override public boolean prepare(StreamLoadSnapshot snapshot) { return true; }
            @Override public boolean commit(StreamLoadSnapshot snapshot) { return true; }
            @Override public boolean abort(StreamLoadSnapshot snapshot) { return true; }
            @Override public void close() {}
        };
    }

    private void rotateToUserB() {
        mockedServer.setCredentials("user_b", "password_b");
        current.set(new Credentials("user_b", "password_b"));
    }

    @Test
    public void testLoadStatusUsesRotatedCredentials() throws Exception {
        TransactionStreamLoader loader = new TransactionStreamLoader(true);
        loader.start(properties(current::get), noopManager());
        try {
            rotateToUserB();
            mockedServer.setLabelState(DB, TABLE, "label-1", TransactionStatus.VISIBLE);

            assertEquals(TransactionStatus.VISIBLE, loader.getLoadStatus(DB, TABLE, "label-1"));
        } finally {
            loader.close();
        }
    }

    @Test
    public void testRollbackUsesRotatedCredentials() {
        TransactionStreamLoader loader = new TransactionStreamLoader(true);
        loader.start(properties(current::get), noopManager());
        try {
            assertTrue(loader.doBegin("label-2", DB, TABLE));
            rotateToUserB();

            assertTrue(loader.rollback(new StreamLoadSnapshot.Transaction(DB, TABLE, "label-2")));
        } finally {
            loader.close();
        }
    }

    @Test
    public void testCancelUsesRotatedCredentials() throws Exception {
        TransactionStreamLoader loader = new TransactionStreamLoader(true);
        loader.start(properties(current::get), noopManager());
        try {
            rotateToUserB();

            assertTrue(loader.cancelLoad(DB, TABLE, "label-3"));
        } finally {
            loader.close();
        }
    }

    @Test
    public void testStartFailsWhenProviderReturnsNull() {
        TransactionStreamLoader loader = new TransactionStreamLoader(true);
        try {
            loader.start(properties(() -> null), noopManager());
            fail("a provider returning null should fail the loader at start");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("returned null credentials"));
        } finally {
            loader.close();
        }
    }
}
