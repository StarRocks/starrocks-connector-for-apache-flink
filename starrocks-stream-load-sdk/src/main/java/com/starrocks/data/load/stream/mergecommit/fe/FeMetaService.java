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
import com.starrocks.data.load.stream.mergecommit.TableId;
import com.starrocks.data.load.stream.mergecommit.WorkerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

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

    public static class Config {
        public DefaultFeHttpService.Config httpServiceConfig;
        public NodesStateService.Config nodesStateServiceConfig;
        public int numThreads = 3;
    }

    public static void main(String[] args) throws Exception {
        DefaultFeHttpService.Config httpConfig = new DefaultFeHttpService.Config();
        httpConfig.username = "root";
        httpConfig.password = "";
        httpConfig.candidateHosts = Arrays.asList("http://127.0.0.1:11901", "http://127.0.0.1:11901");

        NodesStateService.Config nodesConfig = new NodesStateService.Config();
        nodesConfig.updateIntervalMs = 2000;

        Config config = new Config();
        config.httpServiceConfig = httpConfig;
        config.nodesStateServiceConfig = nodesConfig;
        config.numThreads = 3;
        FeMetaService service = FeMetaService.getInstance(config);
        service.takeRef();

        TableId tableId = TableId.of("test", "tbl");
        CompletableFuture<LabelStateService.LabelMeta> future1 =
                service.getLabelStateService().get().getFinalStatus(tableId, "insert_1bd3005b-a559-11ef-b5ea-5e0024ae5de7",
                        1000, 1000, 10000);
        CompletableFuture<LabelStateService.LabelMeta> future2 =
                service.getLabelStateService().get().getFinalStatus(tableId, "insert_test_label",
                        1000, 1000, 10000);
        System.out.println(future1.get().transactionStatus);
        System.out.println(future2.get().transactionStatus);

        Map<String, String> loadParams = new HashMap<>();
        loadParams.put("format", "csv");
        loadParams.put("enable_merge_commit", "true");
        loadParams.put("merge_commit_interval_ms", "1000");
        loadParams.put("merge_commit_async", "true");
        loadParams.put("merge_commit_parallel", "2");
        Future<WorkerAddress> future3 = service.getNodesStateService().get().getHttpAddress(tableId, loadParams);
        Future<WorkerAddress> future4 = service.getNodesStateService().get().getBrpcAddress(tableId, loadParams);
        System.out.printf("http: %s, brpc: %s%n", future3.get(), future4.get());
        service.releaseRef();
    }
}
