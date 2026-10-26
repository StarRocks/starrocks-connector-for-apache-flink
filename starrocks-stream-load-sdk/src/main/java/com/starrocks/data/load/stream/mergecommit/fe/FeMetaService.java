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
import com.starrocks.data.load.stream.mergecommit.SharedServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public class FeMetaService extends SharedService {

    private static final Logger LOG = LoggerFactory.getLogger(FeMetaService.class);

    // Loaders with equal configs share a service, so that they share the label polling and connections
    private static final SharedServiceRegistry<Key, FeMetaService> REGISTRY = new SharedServiceRegistry<>();

    private final Config config;
    private final Key key;
    private ScheduledExecutorService executorService;
    private DefaultFeHttpService httpService;
    private volatile LabelStateService labelStateService;
    private volatile NodesStateService nodesStateService;
    private volatile String currentUuid;

    private FeMetaService(Config config, Key key) {
        this.config = config;
        this.key = key;
    }

    // Takes a reference to the service of the config, which is shared with equal configs.
    // Call release() to give it back.
    public static FeMetaService acquire(Config config) throws Exception {
        Key key = new Key(config);
        return REGISTRY.acquire(key, () -> new FeMetaService(config, key));
    }

    public void release() {
        REGISTRY.release(key, this);
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

    // The config values that the service uses. Configs with equal values share a service.
    private static final class Key {

        private final List<Object> values;

        Key(Config config) {
            DefaultFeHttpService.Config httpConfig = config.httpServiceConfig;
            // the service picks a random host, so the order of the hosts does not matter
            List<String> hosts = new ArrayList<>(httpConfig.candidateHosts);
            Collections.sort(hosts);
            this.values = Arrays.<Object>asList(hosts, httpConfig.username, httpConfig.password,
                    httpConfig.maxConnectionsPerRoute, httpConfig.totalMaxConnections,
                    httpConfig.idleConnectionTimeoutMs, httpConfig.validateAfterInactivityMs,
                    config.nodesStateServiceConfig.updateIntervalMs, config.numThreads);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Key && values.equals(((Key) o).values);
        }

        @Override
        public int hashCode() {
            return values.hashCode();
        }
    }
}
