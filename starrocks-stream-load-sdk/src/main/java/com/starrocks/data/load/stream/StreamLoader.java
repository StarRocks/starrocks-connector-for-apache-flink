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

package com.starrocks.data.load.stream;

import com.starrocks.data.load.stream.properties.StreamLoadProperties;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public interface StreamLoader {

    void start(StreamLoadProperties properties, StreamLoadManager manager);
    void close();

    boolean begin(TableRegion region);
    Future<StreamLoadResponse> send(TableRegion region);

    Future<StreamLoadResponse> send(TableRegion region, int delayMs);

    TransactionStatus getLoadStatus(String db, String table, String label) throws Exception;

    boolean prepare(StreamLoadSnapshot.Transaction transaction);
    boolean commit(StreamLoadSnapshot.Transaction transaction);
    boolean rollback(StreamLoadSnapshot.Transaction transaction);

    boolean prepare(StreamLoadSnapshot snapshot);
    boolean commit(StreamLoadSnapshot snapshot);
    boolean rollback(StreamLoadSnapshot snapshot);

    default ExecutorService getExecutorService() {
        throw new UnsupportedOperationException();
    }
}
