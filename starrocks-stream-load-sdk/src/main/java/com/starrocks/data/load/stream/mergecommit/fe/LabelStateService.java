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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.mergecommit.TableId;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LabelStateService implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(LabelStateService.class);

    private final FeHttpService httpService;
    private final ScheduledExecutorService executorService;
    private final ConcurrentHashMap<LabelId, LabelMeta> labelMetas;
    private final ObjectMapper objectMapper;
    private final ReentrantReadWriteLock lock;
    private final RequestStat requestStat;
    private boolean closed;

    public LabelStateService(FeHttpService httpService, ScheduledExecutorService executorService) {
        this.httpService = httpService;
        this.executorService = executorService;
        this.objectMapper = new ObjectMapper();
        // StreamLoadResponseBody does not contain all fields returned by StarRocks
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // filed names in StreamLoadResponseBody are case-insensitive
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        this.labelMetas = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.requestStat = new RequestStat(60000, LOG);
        this.closed = false;
    }

    public void start() {
        executorService.schedule(this::cleanUselessLabels, 30, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }

        for (LabelMeta labelMeta : labelMetas.values()) {
            labelMeta.future.cancel(true);
        }
        labelMetas.clear();
        LOG.info("Close label meta service");
    }

    public CompletableFuture<LabelMeta> getFinalStatus(
            TableId tableId, String label, int scheduleDelayMs, int scheduleIntervalMs, int timeoutMs) {
        lock.readLock().lock();
        try {
         if (closed) {
             CompletableFuture<LabelMeta> future = new CompletableFuture<>();
             future.completeExceptionally(new RuntimeException("LabelStateService is closed"));
             return future;
         }
         LabelId labelId = new LabelId(tableId.db, label);
         LabelMeta labelMeta = labelMetas
              .computeIfAbsent(labelId, key -> new LabelMeta(tableId, label, scheduleDelayMs, scheduleIntervalMs, timeoutMs));
          if (labelMeta.isScheduled.compareAndSet(false, true)) {
              labelMeta.lastScheduleTimeNs = System.nanoTime();
              executorService.schedule(()
                                                  -> checkLabelState(labelMeta),
                                              Math.max(0, labelMeta.scheduleDelayMs),
                                              TimeUnit.MILLISECONDS);
            LOG.debug("Get label final status, db: {}, table: {}, label: {}, delay: {} ms",
                    tableId.db, tableId.table, label, scheduleDelayMs);
          }
          return labelMeta.future;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void checkLabelState(LabelMeta labelMeta) {
        labelMeta.requestCount += 1;
        long startNs = System.nanoTime();
        labelMeta.pendingTimeNs += startNs - labelMeta.lastScheduleTimeNs;
        TransactionStatus status = null;
      try {
            Pair<Integer, String> pair = httpService.getLabelState(labelMeta.tableId.db, labelMeta.label);
            requestStat.addRequest(System.nanoTime() - startNs);
            if (pair.getKey() != 200) {
                throw new Exception("Http response code is not 200, code: " + pair.getKey() + ", body: " + pair.getValue());
            }
            if (pair.getValue() == null) {
                throw new Exception("Http response body is null");
            }

            LabelResponse response = objectMapper.readValue(pair.getValue(), LabelResponse.class);
            if (response.status == null || !response.status.equalsIgnoreCase("OK")) {
                throw new Exception("Wrong status: " + pair.getValue());
            }

            status = TransactionStatus.valueOf(response.state.toUpperCase());
            if (TransactionStatus.isFinalStatus(status)) {
                labelMeta.finishTimeMs = System.currentTimeMillis();
              labelMeta.transactionStatus = status;
              labelMeta.reason = response.reason;
              labelMeta.future.complete(labelMeta);
              long costMs = labelMeta.finishTimeMs - labelMeta.createTimeMs;
              LOG.info(
                  "Get final label state, db: {}, table: {}, label: {}, cost: {}ms, count: {}, status: {}, reason: {}",
                  labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, costMs,
                  labelMeta.requestCount, status, response.reason);
              return;
            }
            LOG.debug(
                "Label is not in final status, db: {}, table: {}, label: {}, status: {}, reason: {}",
                labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, status, response.reason);
        } catch (Exception e) {
            LOG.error("Failed to get label state, db: {}, table: {}, label: {}",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, e);
        } finally {
            labelMeta.executeTimeNs += System.nanoTime() - startNs;
        }

        LabelMeta newMeta = labelMetas.get(new LabelId(labelMeta.tableId.db, labelMeta.label));
        if (newMeta == null || newMeta != labelMeta) {
            labelMeta.future.completeExceptionally(new RuntimeException("Label is discarded"));
            LOG.error("Failed to retry to get label state because label is discarded, db: {}, table: {}, label: {}",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label);
            return;
        }

        if (System.currentTimeMillis() - labelMeta.createTimeMs >= labelMeta.timeoutMs) {
            labelMeta.future.completeExceptionally(new RuntimeException("Get label state timeout"));
            LOG.error("Failed to retry to get label state because of timeout, db: {}, table: {}, label: {}, timeout: {}ms",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, labelMeta.timeoutMs);
            return;
        }

        if (labelMeta.future.isCancelled()) {
            LOG.error("Failed to retry to get label state because future is cancelled, db: {}, table: {}, label: {}",
                    labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label);
            return;
        }

        labelMeta.lastScheduleTimeNs = System.nanoTime();
        executorService.schedule(() -> checkLabelState(labelMeta), labelMeta.scheduleIntervalMs, TimeUnit.MILLISECONDS);
        LOG.debug("Retry to get label state, db: {}, table: {}, label: {}, status: {}. count: {}",
                labelMeta.tableId.db, labelMeta.tableId.table, labelMeta.label, status, labelMeta.requestCount);
    }

    private void cleanUselessLabels() {
        List<LabelMeta> uselessLabels = new ArrayList<>();
        for (LabelMeta labelMeta : labelMetas.values()) {
            if (labelMeta.future.isDone() && System.currentTimeMillis() - labelMeta.finishTimeMs > 300 * 1000) {
                uselessLabels.add(labelMeta);
            }
        }
        for (LabelMeta labelMeta : uselessLabels) {
            labelMetas.remove(new LabelId(labelMeta.tableId.db, labelMeta.label));
            LOG.debug("Remove useless label: {}, status: {}, reason: {}", labelMeta.label, labelMeta.transactionStatus, labelMeta.reason);
        }
        executorService.schedule(this::cleanUselessLabels, 30, TimeUnit.SECONDS);
    }

    public static class LabelResponse {
        public String status;
        public String code;
        public String message;
        public String msg;
        public String state;
        public String reason;
    }

    public static class LabelId {
        final String db;
        final String label;

        public LabelId(String db, String label) {
            this.db = db;
            this.label = label;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LabelId labelId = (LabelId) o;
            return Objects.equals(db, labelId.db) && Objects.equals(label, labelId.label);
        }

        @Override
        public int hashCode() {
            return Objects.hash(db, label);
        }

        @Override
        public String toString() {
            return "LabelId{" +
                    "db='" + db + '\'' +
                    ", label='" + label + '\'' +
                    '}';
        }
    }

    public static class LabelMeta {
      TableId tableId;
      String label;
      int scheduleDelayMs;
      int scheduleIntervalMs;
      int timeoutMs;
      CompletableFuture<LabelMeta> future;
      AtomicBoolean isScheduled;
      final long createTimeMs;
      volatile int requestCount = 0;
      volatile long executeTimeNs = 0;
      volatile long pendingTimeNs = 0;
      volatile long finishTimeMs;
      volatile long lastScheduleTimeNs;

      public volatile TransactionStatus transactionStatus;
      public volatile String reason;

      LabelMeta(TableId tableId, String label, int scheduleDelayMs,
                int scheduleIntervalMs, int timeoutMs) {
        this.tableId = tableId;
        this.label = label;
        this.scheduleDelayMs = scheduleDelayMs;
        this.scheduleIntervalMs = scheduleIntervalMs;
        this.timeoutMs = timeoutMs;
        this.future = new CompletableFuture<>();
        this.isScheduled = new AtomicBoolean(false);
        this.createTimeMs = System.currentTimeMillis();
      }

        public int getRequestCount() {
            return requestCount;
        }

        public long getLatencyMs() {
          return finishTimeMs - createTimeMs;
      }

      public long getHttpCostMs() {
          return executeTimeNs / 1000000;
      }

      public long getPendingCostMs() {
          return pendingTimeNs / 1000000;
      }
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
        try (LabelStateService metaService = new LabelStateService(httpService, executorService)) {
            metaService.start();
            TableId tableId = TableId.of("test", "tbl");
            CompletableFuture<LabelMeta> future1 =
                    metaService.getFinalStatus(tableId, "insert_1bd3005b-a559-11ef-b5ea-5e0024ae5de7",
                            1000, 1000, 10000);
            CompletableFuture<LabelMeta> future2 =
                    metaService.getFinalStatus(tableId, "insert_test_label",
                            1000, 1000, 10000);
            LabelMeta meta1 = future1.get();
            LabelMeta meta2 = future2.get();
            System.out.println(meta1.transactionStatus);
            System.out.println(meta2.transactionStatus);
        }
    }
}
