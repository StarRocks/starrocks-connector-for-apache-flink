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

package com.starrocks.data.load.stream.mergecommit;

import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.TransactionStatus;

import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

public abstract class MergeCommitLoader implements StreamLoader, Serializable {

    public abstract ScheduledFuture<?> scheduleFlush(Table table, long chunkId, int delayMs);

    public abstract void sendLoad(LoadRequest.RequestRun requestRun, int delayMs);

    @Override
    public boolean begin(TableRegion region) {
        return false;
    }

    @Override
    public Future<StreamLoadResponse> send(TableRegion region) {
        return null;
    }

    @Override
    public Future<StreamLoadResponse> send(TableRegion region, int delayMs) {
        return null;
    }

    @Override
    public boolean prepare(StreamLoadSnapshot.Transaction transaction) {
        return false;
    }

    @Override
    public boolean commit(StreamLoadSnapshot.Transaction transaction) {
        return false;
    }

    @Override
    public boolean rollback(StreamLoadSnapshot.Transaction transaction) {
        return false;
    }

    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        return false;
    }

    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        return false;
    }

    @Override
    public boolean rollback(StreamLoadSnapshot snapshot) {
        return false;
    }

    @Override
    public TransactionStatus getLoadStatus(String db, String table, String label) throws Exception {
        return null;
    }
}
