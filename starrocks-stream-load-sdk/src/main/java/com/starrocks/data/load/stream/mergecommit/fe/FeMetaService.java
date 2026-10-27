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

package com.starrocks.data.load.stream.mergecommit.fe;

import com.starrocks.data.load.stream.mergecommit.SharedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public class FeMetaService extends SharedService {

    private static final Logger LOG = LoggerFactory.getLogger(FeMetaService.class);

    private static volatile FeMetaService INSTANCE;

    private final Config config;
    private ScheduledExecutorService executorService;
    private DefaultFeHttpService httpService;
    private volatile LabelStateService labelStateService;
    private volatile NodesStateService nodesStateService;
    private volatile String currentUuid;

    public FeMetaService(Config config) {
        this.config = config;
    }

    public static FeMetaService getInstance(Config config) {
        if (INSTANCE == null) {
            synchronized (FeMetaService.class) {
                if (INSTANCE == null) {
                    INSTANCE = new FeMetaService(config);
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected void init() throws Exception {
        currentUuid = UUID.randomUUID().toString();
        this.executorService = Executors.newScheduledThreadPool(
                config.numThreads,
                r -> {
                    Thread thread = new Thread(null, r, "FeMetaService-" + currentUuid);
                    thread.setDaemon(true);
                    return thread;
                }
        );
        httpService = new DefaultFeHttpService(config.httpServiceConfig);
        labelStateService = new LabelStateService(httpService, executorService);
        nodesStateService = new NodesStateService(httpService, executorService, config.nodesStateServiceConfig);

        boolean success = false;
        try {
            httpService.start();
            labelStateService.start();
            nodesStateService.start();
            success = true;
        } finally {
            if (!success) {
                closeService();
            }
        }
        LOG.info("Init fe meta service, uuid: {}, numExecutors: {}", currentUuid, config.numThreads);
    }

    @Override
    protected void reset() {
        closeService();
        LOG.info("Reset fe meta service, uuid: {}", currentUuid);
    }

    private void closeService() {
        if (nodesStateService != null) {
            nodesStateService.close();
            nodesStateService = null;
        }
        if (labelStateService != null) {
            labelStateService.close();
            labelStateService = null;
        }
        if (httpService != null) {
            httpService.close();
            httpService = null;
        }

        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    public Optional<NodesStateService> getNodesStateService() {
        return Optional.of(nodesStateService);
    }

    public Optional<LabelStateService> getLabelStateService() {
        return Optional.of(labelStateService);
    }

    public String getFeUrl() {
        int i = ThreadLocalRandom.current().nextInt(config.httpServiceConfig.candidateHosts.size());
        return config.httpServiceConfig.candidateHosts.get(i);
    }

    public static class Config {
        public DefaultFeHttpService.Config httpServiceConfig;
        public NodesStateService.Config nodesStateServiceConfig;
        public int numThreads = 3;
    }
}
