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

package com.starrocks.data.load.stream.mergecommit.fe;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.data.load.stream.mergecommit.TableId;
import com.starrocks.data.load.stream.mergecommit.WorkerAddress;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NodesStateService implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(NodesStateService.class);

    private final FeHttpService httpService;
    private final ScheduledExecutorService executorService;
    private final Config config;
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<TableId, NodesInfo> nodesInfoMap;
    private final RequestStat requestStat;

    public NodesStateService(FeHttpService httpService, ScheduledExecutorService executorService, Config config) {
        this.httpService = httpService;
        this.executorService = executorService;
        this.config = config;
        this.objectMapper = new ObjectMapper();
        // StreamLoadResponseBody does not contain all fields returned by StarRocks
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // filed names in StreamLoadResponseBody are case-insensitive
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        this.nodesInfoMap = new ConcurrentHashMap<>();
        this.requestStat = new RequestStat(60000, LOG);
    }

    public void start() {
    }

    @Override
    public void close() {
    }

    public CompletableFuture<WorkerAddress> getHttpAddress(TableId tableId, Map<String, String> loadParams) {
        NodesInfo info = nodesInfoMap.computeIfAbsent(tableId, k -> new NodesInfo(tableId, loadParams, config.updateIntervalMs));
        return info.getHttpAddress();
    }

    public CompletableFuture<WorkerAddress> getBrpcAddress(TableId tableId, Map<String, String> loadParams) {
        NodesInfo info = nodesInfoMap.computeIfAbsent(tableId, k -> new NodesInfo(tableId, loadParams, config.updateIntervalMs));
        return info.getBrpcAddress();
    }

    public void forceUpdateNodesInfo(TableId tableId, Map<String, String> loadParams) {
       nodesInfoMap.computeIfAbsent(tableId, k -> new NodesInfo(tableId, loadParams, config.updateIntervalMs)).forceSchedule();
    }

    private void requestNodesInfo(NodesInfo nodesInfo, boolean force) {
        boolean success = false;
        try {
            long startNs = System.nanoTime();
            Pair<Integer, String> pair = httpService.getNodes(nodesInfo.tableId.db, nodesInfo.tableId.table, nodesInfo.loadParams);
            requestStat.addRequest(System.nanoTime() - startNs);
            if (pair.getKey() != 200) {
                throw new Exception(String.format("Response code not 200, code: %s, response: %s", pair.getKey(), pair.getValue()));
            }
            String content = pair.getValue();
            if (content == null) {
                throw new Exception(String.format("Content is null, code: %s", pair.getKey()));
            }
            StreamLoadMetaResponse response = objectMapper.readValue(content, StreamLoadMetaResponse.class);
            if (response.status == null || !response.status.equalsIgnoreCase("OK")) {
                throw new Exception("Wrong status: " + content);
            }
            if (response.nodes == null) {
                throw new Exception("Wrong nodes: " + content);
            }
            NodesResponse nodesResponse = response.nodes;
            if (nodesResponse.http == null || nodesResponse.brpc == null) {
                throw new Exception("Wrong nodes: " + content);
            }
            String[] httpAddresses = nodesResponse.http.split(",");
            String[] brpcAddresses = nodesResponse.brpc.split(",");
            if (httpAddresses.length == 0 || brpcAddresses.length == 0) {
                throw new Exception("Wrong nodes: " + content);
            }
            List<WorkerAddress> httpWorkerAddresses = new ArrayList<>();
            for (String address : httpAddresses) {
                String[] parts = address.split(":");
                httpWorkerAddresses.add(new WorkerAddress(parts[0], parts[1]));
            }
            List<WorkerAddress> brpcWorkerAddresses = new ArrayList<>();
            for (String address : brpcAddresses) {
                String[] parts = address.split(":");
                brpcWorkerAddresses.add(new WorkerAddress(parts[0], parts[1]));
            }
            nodesInfo.updateNodesInfo(httpWorkerAddresses, brpcWorkerAddresses);
            success = true;
            LOG.info("Update nodes info, db: {}, table: {}, force: {}, http: {}, brpc: {}",
                    nodesInfo.tableId.db, nodesInfo.tableId.table, force, httpAddresses, brpcAddresses);
        } catch (Exception e) {
            LOG.error("Failed to get nodes info, db: {}, table: {}, force: {}",
                    nodesInfo.tableId.db, nodesInfo.tableId.table, force, e);
        } finally {
            if (!success && force) {
                executorService.schedule(
                        () -> requestNodesInfo(nodesInfo, true),
                        Math.min(500, nodesInfo.scheduleIntervalMs), TimeUnit.MILLISECONDS);
            } else {
                executorService.schedule(
                        () -> requestNodesInfo(nodesInfo, false), nodesInfo.scheduleIntervalMs, TimeUnit.MILLISECONDS);
            }
        }
    }

    private static class NodesResponse {
        public String brpc;
        public String http;
    }

    private static class StreamLoadMetaResponse {
        public String status;
        public String code;
        public String message;
        public String msg;
        public NodesResponse nodes;
    }

    private class NodesInfo {
        final TableId tableId;
        final Map<String, String> loadParams;
        int scheduleIntervalMs;
        ReentrantReadWriteLock lock;
        List<WorkerAddress> httpAddresses;
        List<WorkerAddress> brpcAddresses;
        AtomicLong nextHttpIndex;
        AtomicLong nextBrpcIndex;
        ConcurrentLinkedQueue<CompletableFuture<WorkerAddress>> pendingHttpRequests;
        ConcurrentLinkedQueue<CompletableFuture<WorkerAddress>> pendingBrpcRequests;
        long lastForceScheduleTimeMs;

        public NodesInfo(TableId tableId, Map<String, String> loadParams, int scheduleIntervalMs) {
            this.tableId = tableId;
            this.loadParams = new HashMap<>(loadParams);
            this.scheduleIntervalMs = scheduleIntervalMs;
            this.lock = new ReentrantReadWriteLock();
            this.brpcAddresses = new ArrayList<>();
            this.httpAddresses = new ArrayList<>();
            this.nextHttpIndex = new AtomicLong();
            this.nextBrpcIndex = new AtomicLong();
            this.pendingHttpRequests = new ConcurrentLinkedQueue<>();
            this.pendingBrpcRequests = new ConcurrentLinkedQueue<>();
            this.lastForceScheduleTimeMs = 0;
        }

        public void updateNodesInfo(List<WorkerAddress> httpAddresses, List<WorkerAddress> brpcAddresses) {
            lock.writeLock().lock();
            try {
                this.httpAddresses = httpAddresses;
                this.brpcAddresses = brpcAddresses;
            } finally {
                lock.writeLock().unlock();
            }
            lock.readLock().lock();
            try {
                if (!httpAddresses.isEmpty()) {
                    while (!pendingHttpRequests.isEmpty()) {
                        CompletableFuture<WorkerAddress> future = pendingHttpRequests.poll();
                        future.complete(httpAddresses.get((int) (nextHttpIndex.getAndIncrement() % httpAddresses.size())));
                    }
                }
                if (!brpcAddresses.isEmpty()) {
                    while (!pendingBrpcRequests.isEmpty()) {
                        CompletableFuture<WorkerAddress> future = pendingBrpcRequests.poll();
                        future.complete(brpcAddresses.get((int) (nextBrpcIndex.getAndIncrement() % brpcAddresses.size())));
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
        }

        public CompletableFuture<WorkerAddress> getHttpAddress() {
            CompletableFuture<WorkerAddress> future = new CompletableFuture<>();
            boolean forceSchedule = false;
            lock.readLock().lock();
            try {
                if (httpAddresses.isEmpty()) {
                    pendingHttpRequests.add(future);
                    forceSchedule = true;
                } else {
                    future.complete(httpAddresses.get((int) (nextHttpIndex.getAndIncrement() % httpAddresses.size())));
                }
            } finally {
                lock.readLock().unlock();
            }
            if (forceSchedule) {
                forceSchedule();
            }
            return future;
        }

        public CompletableFuture<WorkerAddress> getBrpcAddress() {
            CompletableFuture<WorkerAddress> future = new CompletableFuture<>();
            boolean forceSchedule = false;
            lock.readLock().lock();
            try {
                if (brpcAddresses.isEmpty()) {
                    pendingBrpcRequests.add(future);
                    forceSchedule = true;
                } else {
                    future.complete(brpcAddresses.get((int) (nextBrpcIndex.getAndIncrement() % brpcAddresses.size())));
                }
            } finally {
                lock.readLock().unlock();
            }
            if (forceSchedule) {
                forceSchedule();
            }
            return future;
        }

        public void forceSchedule() {
            lock.writeLock().lock();
            try {
                long now = System.currentTimeMillis();
                if (now - lastForceScheduleTimeMs < 1000) {
                    return;
                }
                brpcAddresses.clear();
                httpAddresses.clear();
                lastForceScheduleTimeMs = System.currentTimeMillis();
                executorService.schedule(
                            () -> NodesStateService.this.requestNodesInfo(this, true), 0, TimeUnit.MILLISECONDS);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    public static class Config {
        public int updateIntervalMs = 5000;
    }

    public static void main(String[] args) throws Exception {
        DefaultFeHttpService.Config config = new DefaultFeHttpService.Config();
        config.username = "root";
        config.password = "";
        config.candidateHosts = Arrays.asList("http://127.0.0.1:11901", "http://127.0.0.1:11901");

        DefaultFeHttpService httpService = new DefaultFeHttpService(config);
        httpService.start();
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(
                100,
                r -> {
                    Thread thread = new Thread(null, r, "FeMetaService-" + UUID.randomUUID());
                    thread.setDaemon(true);
                    return thread;
                }
        );

        NodesStateService.Config config1 = new NodesStateService.Config();
        config1.updateIntervalMs = 2000;
        try (NodesStateService metaService = new NodesStateService(httpService, executorService, config1)) {
            metaService.start();
            Map<String, String> loadParams = new HashMap<>();
            loadParams.put("format", "csv");
            loadParams.put("enable_merge_commit", "true");
            loadParams.put("merge_commit_interval_ms", "1000");
            loadParams.put("merge_commit_async", "true");
            loadParams.put("merge_commit_parallel", "2");
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                TableId tableId = TableId.of("test", "tbl");
                threads.add(new Thread(() -> {
                    for (int j = 0; j < 10000; j++) {
                        try {
                            Future<WorkerAddress> future1 = metaService.getHttpAddress(tableId, loadParams);
                            Future<WorkerAddress> future2 = metaService.getBrpcAddress(tableId, loadParams);
                            System.out.printf("http: %s, brpc: %s%n", future1.get(), future2.get());
                            Thread.sleep(100);
                            if (j % 40 == 0) {
                                metaService.forceUpdateNodesInfo(tableId, loadParams);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));
            }
            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
        }
    }
}
