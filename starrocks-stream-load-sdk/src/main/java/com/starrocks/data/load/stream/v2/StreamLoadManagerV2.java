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

package com.starrocks.data.load.stream.v2;

import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.mergecommit.MergeCommitManager;
import com.starrocks.data.load.stream.mergecommit.MetricListener;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;

import java.io.Serializable;

public class StreamLoadManagerV2 implements StreamLoadManager, Serializable {

    private final StreamLoadManager delegateManager;

    public StreamLoadManagerV2(StreamLoadProperties properties, boolean enableAutoCommit) {
        boolean enableMergeCommit = properties.getHeaders()
                .getOrDefault("enable_merge_commit", "false").equals("true");
        this.delegateManager = enableMergeCommit ? new MergeCommitManager(properties)
                : new NormalStreamLoadManager(properties, enableAutoCommit);
    }

    @Override
    public void init() {
        delegateManager.init();
    }

    @Override
    public void write(String uniqueKey, String database, String table, String... rows) {
        delegateManager.write(uniqueKey, database, table, rows);
    }

    @Override
    public void callback(StreamLoadResponse response) {
        delegateManager.callback(response);
    }

    @Override
    public void callback(Throwable e) {
        delegateManager.callback(e);
    }

    @Override
    public void flush() {
        delegateManager.flush();
    }

    @Override
    public StreamLoadSnapshot snapshot() {
        return delegateManager.snapshot();
    }

    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        return delegateManager.prepare(snapshot);
    }

    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        return delegateManager.commit(snapshot);
    }

    @Override
    public boolean abort(StreamLoadSnapshot snapshot) {
        return delegateManager.abort(snapshot);
    }

    @Override
    public void close() {
        delegateManager.close();
    }

    @Override
    public void setStreamLoadListener(StreamLoadListener streamLoadListener) {
        delegateManager.setStreamLoadListener(streamLoadListener);
    }

    @Override
    public void setLabelGeneratorFactory(LabelGeneratorFactory labelGeneratorFactory) {
        delegateManager.setLabelGeneratorFactory(labelGeneratorFactory);
    }

    @Override
    public StreamLoader getStreamLoader() {
        return delegateManager.getStreamLoader();
    }

    @Override
    public void setMetricListener(MetricListener metricListener) {
        delegateManager.setMetricListener(metricListener);
    }
}
