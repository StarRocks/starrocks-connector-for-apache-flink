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

import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class MergeCommitManagerTest {

    @Test
    public void testFlushFailsFastAfterTableFailure() {
        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls("http://localhost:8030")
                .username("root")
                .password("")
                .defaultTableProperties(StreamLoadTableProperties.builder()
                        .database("db").table("tbl").build())
                .build();
        MergeCommitManager manager = new MergeCommitManager(properties);
        RuntimeException failure = new RuntimeException("table failed");
        manager.onTableFailure(mock(Table.class), failure);

        // flush() must report the failure before it flushes any table
        try {
            manager.flush();
            fail("flush should report the table failure");
        } catch (RuntimeException e) {
            assertSame(failure, e.getCause());
        }
    }
}
