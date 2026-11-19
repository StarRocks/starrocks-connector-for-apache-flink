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

package com.starrocks.connector.flink.table.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StarRocksCommitterTest {

    @Mocked
    private Committer.CommitRequest<StarRocksCommittable> commitRequest;

    private StarRocksSinkOptions sinkOptions;

    @Before
    public void setUp() {
        sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("database-name", "db")
                .withProperty("table-name", "table")
                .withProperty("sink.semantic", "exactly-once")
                .withProperty("sink.label-prefix", "prefix")
                // Try to commit 3 times at most
                .withProperty("sink.max-retries", "2")
                .build();
        new Expectations() {{
            commitRequest.getCommittable();
            result = new StarRocksCommittable(new StreamLoadSnapshot());
        }};
    }

    @Test
    public void testCommitFailsAfterAllTries() throws Exception {
        // Replace the sink manager methods that would connect to StarRocks. Every commit fails.
        AtomicInteger numCommits = new AtomicInteger();
        new MockUp<StreamLoadManagerV2>() {
            @Mock
            public void $init(StreamLoadProperties properties, boolean enableAutoCommit) {
            }

            @Mock
            public void init() {
            }

            @Mock
            public boolean commit(StreamLoadSnapshot snapshot) {
                numCommits.incrementAndGet();
                return false;
            }
        };
        StarRocksCommitter committer = new StarRocksCommitter(sinkOptions, null);

        // Flink must not take the transactions as committed, or their data is lost.
        try {
            committer.commit(Collections.singletonList(commitRequest));
            fail("The commit should fail");
        } catch (RuntimeException e) {
            assertEquals("Failed to commit", e.getMessage());
        }
        assertEquals(3, numCommits.get());
    }

    @Test
    public void testCommitSucceedsAfterFailedTry() throws Exception {
        // Replace the sink manager methods that would connect to StarRocks. The first commit
        // fails, and the second one succeeds.
        AtomicInteger numCommits = new AtomicInteger();
        new MockUp<StreamLoadManagerV2>() {
            @Mock
            public void $init(StreamLoadProperties properties, boolean enableAutoCommit) {
            }

            @Mock
            public void init() {
            }

            @Mock
            public boolean commit(StreamLoadSnapshot snapshot) {
                if (numCommits.incrementAndGet() == 1) {
                    throw new RuntimeException("Connection refused");
                }
                return true;
            }
        };
        StarRocksCommitter committer = new StarRocksCommitter(sinkOptions, null);

        // The transactions are committed, so the job goes on.
        committer.commit(Collections.singletonList(commitRequest));
        assertEquals(2, numCommits.get());
    }
}
