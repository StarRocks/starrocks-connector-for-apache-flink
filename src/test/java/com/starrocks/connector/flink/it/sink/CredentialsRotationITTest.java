/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
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

package com.starrocks.connector.flink.it.sink;

import com.starrocks.connector.flink.it.StarRocksITTestBase;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.auth.Credentials;
import com.starrocks.data.load.stream.auth.CredentialsProvider;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Rotates the StarRocks password of the user a running sink loads as, and checks that loads keep
 * succeeding once the credentials provider returns the new credentials, without restarting the sink.
 *
 * <p>The rotation alternates between two users, so the credentials for the next load are valid
 * before the previous user's password is changed.
 */
public class CredentialsRotationITTest extends StarRocksITTestBase {

    /** Stands in for a provider that reads a secret which is rotated outside the job. */
    static class SwitchableCredentialsProvider implements CredentialsProvider {
        private static final long serialVersionUID = 1L;
        private volatile Credentials credentials;

        SwitchableCredentialsProvider(Credentials credentials) {
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

    private final List<String> usersToDrop = new ArrayList<>();
    private String tableName;

    @Before
    public void createTable() throws Exception {
        tableName = "rotation_" + genRandomUuid();
        executeSrSQL(String.format(
                "CREATE TABLE `%s`.`%s` (id INT, name STRING) ENGINE = OLAP DUPLICATE KEY(id) " +
                        "DISTRIBUTED BY HASH (id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")",
                DB_NAME, tableName));
    }

    @After
    public void dropUsers() throws Exception {
        for (String user : usersToDrop) {
            executeSrSQL(String.format("DROP USER IF EXISTS '%s'@'%%'", user));
        }
    }

    private Credentials createLoadUser(String prefix) throws Exception {
        String user = prefix + "_" + genRandomUuid().substring(0, 8);
        String password = "pw_" + genRandomUuid();
        usersToDrop.add(user);
        executeSrSQL(String.format("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user, password));
        executeSrSQL(String.format("GRANT SELECT, INSERT ON ALL TABLES IN DATABASE %s TO USER '%s'@'%%'", DB_NAME, user));
        return new Credentials(user, password);
    }

    /** Gives the user a new password and returns the new credentials; the old ones stop working. */
    private static Credentials changePassword(Credentials credentials) throws Exception {
        String newPassword = "changed_" + genRandomUuid();
        executeSrSQL(String.format("ALTER USER '%s'@'%%' IDENTIFIED BY '%s'",
                credentials.getUsername(), newPassword));
        return new Credentials(credentials.getUsername(), newPassword);
    }

    private StarRocksSinkOptions.Builder sinkOptions() {
        return StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", getHttpUrls())
                .withProperty("database-name", DB_NAME)
                .withProperty("table-name", tableName)
                // read the table metadata when the job is built; loads use the provider
                .withProperty("username", USERNAME)
                .withProperty("password", PASSWORD)
                .withProperty("sink.semantic", "at-least-once")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.label-prefix", "rotation_");
    }

    private StreamLoadManagerV2 startManager(StarRocksSinkOptions options, boolean transaction) {
        return startManager(options, transaction, true);
    }

    private StreamLoadManagerV2 startManager(StarRocksSinkOptions options, boolean transaction,
                                             boolean enableAutoCommit) {
        options.setSupportTransactionStreamLoad(transaction);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(options.getProperties(null), enableAutoCommit);
        manager.init();
        return manager;
    }

    private void writeAndFlush(StreamLoadManagerV2 manager, int id) {
        manager.write(null, DB_NAME, tableName, String.format("{\"id\":%d,\"name\":\"row_%d\"}", id, id));
        manager.flush();
    }

    private int countRows() throws Exception {
        return StarRocksTableUtils.scanTable(DB_CONNECTION, DB_NAME, tableName).size();
    }

    private void verifyLoadsSurviveRotation(boolean transaction) throws Exception {
        Credentials userA = createLoadUser("rot_a");
        Credentials userB = createLoadUser("rot_b");
        SwitchableCredentialsProvider provider = new SwitchableCredentialsProvider(userA);
        StreamLoadManagerV2 manager = startManager(sinkOptions().withCredentialsProvider(provider).build(), transaction);
        try {
            writeAndFlush(manager, 1);

            // rows buffered before the rotation are sent with the credentials valid at send time
            manager.write(null, DB_NAME, tableName, "{\"id\":2,\"name\":\"row_2\"}");
            provider.set(userB);
            changePassword(userA);
            manager.flush();

            writeAndFlush(manager, 3);

            // rotate back: userA gets a new password and the provider returns it before userB's changes
            Credentials newUserA = changePassword(userA);
            provider.set(newUserA);
            changePassword(userB);
            writeAndFlush(manager, 4);

            assertEquals(4, countRows());
            assertNull(manager.getException());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testTransactionStreamLoadSurvivesRotation() throws Exception {
        verifyLoadsSurviveRotation(true);
    }

    @Test
    public void testStreamLoadSurvivesRotation() throws Exception {
        verifyLoadsSurviveRotation(false);
    }

    /**
     * Exactly-once prepares a transaction at a checkpoint and commits it when the checkpoint
     * completes, so a rotation can fall between the two. The transaction is begun, loaded and
     * prepared as one user and committed as another, whose password is the only valid one left.
     */
    @Test
    public void testTransactionPreparedByOneUserIsCommittedByAnother() throws Exception {
        Credentials userA = createLoadUser("rot_a");
        Credentials userB = createLoadUser("rot_b");
        SwitchableCredentialsProvider provider = new SwitchableCredentialsProvider(userA);
        StarRocksSinkOptions options = sinkOptions()
                .withProperty("sink.semantic", "exactly-once")
                .withCredentialsProvider(provider)
                .build();
        StreamLoadManagerV2 manager = startManager(options, true, false);
        try {
            manager.write(null, DB_NAME, tableName, "{\"id\":1,\"name\":\"row_1\"}");
            manager.flush();
            StreamLoadSnapshot snapshot = manager.snapshot();
            assertTrue(manager.prepare(snapshot));

            provider.set(userB);
            changePassword(userA);

            assertTrue(manager.commit(snapshot));
            assertEquals(1, countRows());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testStaticCredentialsFailAfterRotation() throws Exception {
        Credentials user = createLoadUser("rot_static");
        StarRocksSinkOptions options = sinkOptions()
                .withProperty("username", user.getUsername())
                .withProperty("password", user.getPassword())
                .build();
        StreamLoadManagerV2 manager = startManager(options, true);
        try {
            writeAndFlush(manager, 1);
            changePassword(user);
            try {
                writeAndFlush(manager, 2);
                fail("a load with the password from before the rotation should be rejected");
            } catch (RuntimeException expected) {
                // the server rejects the stale password
            }
            assertEquals(1, countRows());
        } finally {
            manager.close();
        }
    }
}
