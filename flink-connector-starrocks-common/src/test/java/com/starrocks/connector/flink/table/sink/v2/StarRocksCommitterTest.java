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

import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Test;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StarRocksCommitterTest {

    @Test
    public void testCommitFailureAfterRetriesThrows(@Injectable StreamLoadManagerV2 sinkManager) throws Exception {
        new Expectations() {{
            sinkManager.commit((StreamLoadSnapshot) any);
            result = false;
        }};
        StarRocksCommitter committer = createCommitter(sinkManager, 2);

        RuntimeException caught = null;
        try {
            committer.commit(Collections.singletonList(commitRequest()));
            fail("commit must throw after all retries fail, otherwise Flink marks the data as committed");
        } catch (RuntimeException e) {
            caught = e;
        }
        assertNotNull(caught);
        assertTrue(caught.getMessage().contains("Failed to commit"));
    }

    @Test
    public void testCommitSuccess(@Injectable StreamLoadManagerV2 sinkManager) throws Exception {
        new Expectations() {{
            sinkManager.commit((StreamLoadSnapshot) any);
            result = true;
        }};
        StarRocksCommitter committer = createCommitter(sinkManager, 2);
        committer.commit(Collections.singletonList(commitRequest()));
    }

    private static StarRocksCommitter createCommitter(StreamLoadManagerV2 sinkManager, int maxRetries) throws Exception {
        Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Unsafe unsafe = (Unsafe) unsafeField.get(null);
        StarRocksCommitter committer = (StarRocksCommitter) unsafe.allocateInstance(StarRocksCommitter.class);
        setField(committer, "sinkManager", sinkManager);
        setField(committer, "maxRetries", maxRetries);
        return committer;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = StarRocksCommitter.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Committer.CommitRequest<StarRocksCommittable> commitRequest() {
        StarRocksCommittable committable = new StarRocksCommittable(new StreamLoadSnapshot());
        return new Committer.CommitRequest<StarRocksCommittable>() {
            @Override
            public StarRocksCommittable getCommittable() {
                return committable;
            }

            @Override
            public int getNumberOfRetries() {
                return 0;
            }

            @Override
            public void signalFailedWithKnownReason(Throwable t) {
            }

            @Override
            public void signalFailedWithUnknownReason(Throwable t) {
            }

            @Override
            public void retryLater() {
            }

            @Override
            public void updateAndRetryLater(StarRocksCommittable committable) {
            }

            @Override
            public void signalAlreadyCommitted() {
            }
        };
    }
}
