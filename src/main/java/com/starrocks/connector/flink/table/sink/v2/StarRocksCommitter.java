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

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import com.starrocks.connector.flink.tools.EnvUtils;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.apache.flink.api.connector.sink2.Committer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class StarRocksCommitter implements Committer<StarRocksCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCommitter.class);

    private final int maxRetries;
    private final StreamLoadManagerV2 sinkManager;

    public StarRocksCommitter(
            StarRocksSinkOptions sinkOptions,
            StreamLoadProperties streamLoadProperties) {
        this.maxRetries = sinkOptions.getSinkMaxRetries();
        this.sinkManager = new StreamLoadManagerV2(streamLoadProperties,
                sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE);
        try {
            // TODO no need to start flush thread in sinkManager
            sinkManager.init();
        } catch (Exception e) {
            LOG.error("Failed to init sink manager.", e);
            try {
                sinkManager.close();
            } catch (Exception ie) {
                LOG.error("Failed to close sink manager after init failure.", ie);
            }
            throw new RuntimeException("Failed to init sink manager", e);
        }
        LOG.info("Create StarRocksCommitter, maxRetries: {}. {}", maxRetries, EnvUtils.getGitInformation());
    }

    @Override
    public void commit(Collection<CommitRequest<StarRocksCommittable>> committables)
            throws IOException, InterruptedException {
        for (CommitRequest<StarRocksCommittable> commitRequest : committables) {
            StarRocksCommittable committable = commitRequest.getCommittable();
            RuntimeException firstException = null;
            for (int i = 0; i <= maxRetries; i++) {
                try {
                    boolean success = sinkManager.commit(committable.getLabelSnapshot());
                    if (success) {
                        break;
                    }
                    throw new RuntimeException("Please see the taskmanager log for the failure reason");
                } catch (Exception e) {
                    LOG.error("Fail to commit after {} retries, max retries: {}", i, maxRetries, e);
                    if (firstException != null) {
                        firstException = new RuntimeException("Failed to commit", e);
                    }
                }
            }
            if (firstException != null) {
                throw firstException;
            }
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close StarRocksCommitter.");
        if(sinkManager != null) {
            sinkManager.close();
        }
    }
}
