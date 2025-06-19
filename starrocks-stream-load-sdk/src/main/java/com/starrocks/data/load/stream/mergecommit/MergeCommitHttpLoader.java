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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.data.load.stream.StreamLoadConstants;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadUtils;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.compress.NoCompressCodec;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.mergecommit.be.BackendHttpService;
import com.starrocks.data.load.stream.mergecommit.fe.DefaultFeHttpService;
import com.starrocks.data.load.stream.mergecommit.fe.FeMetaService;
import com.starrocks.data.load.stream.mergecommit.fe.LabelStateService;
import com.starrocks.data.load.stream.mergecommit.fe.NodesStateService;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.starrocks.data.load.stream.mergecommit.LoadRequest.State.RECEIVED_RESPONSE;

public class MergeCommitHttpLoader extends MergeCommitLoader {

    private static final Logger LOG = LoggerFactory.getLogger(MergeCommitHttpLoader.class);

    private StreamLoadProperties properties;
    private ObjectMapper objectMapper;
    private FeMetaService feMetaService = null;
    private BackendHttpService backendService = null;
    private ScheduledExecutorService executorService;

    public MergeCommitHttpLoader() {
    }

    @Override
    public void start(StreamLoadProperties properties, StreamLoadManager manager) {
        try {
            this.properties = properties;
            this.objectMapper = new ObjectMapper();
            // StreamLoadResponseBody does not contain all fields returned by StarRocks
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            // filed names in StreamLoadResponseBody are case-insensitive
            objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

            DefaultFeHttpService.Config httpConfig = new DefaultFeHttpService.Config();
            httpConfig.username = properties.getUsername();
            httpConfig.password = properties.getPassword();
            httpConfig.candidateHosts = Arrays.asList(properties.getLoadUrls());
            httpConfig.maxConnectionsPerRoute = properties.getHttpMaxConnectionsPerRoute();
            httpConfig.totalMaxConnections = properties.getHttpTotalMaxConnections();
            httpConfig.idleConnectionTimeoutMs = properties.getHttpIdleConnectionTimeoutMs();
            NodesStateService.Config nodesConfig = new NodesStateService.Config();
            nodesConfig.updateIntervalMs = properties.getNodeMetaUpdateIntervalMs();
            FeMetaService.Config config = new FeMetaService.Config();
            config.httpServiceConfig = httpConfig;
            config.nodesStateServiceConfig = nodesConfig;
            config.numThreads = properties.getHttpThreadNum();
            FeMetaService service1 = FeMetaService.getInstance(config);
            service1.takeRef();
            feMetaService = service1;

            BackendHttpService.Config backendConfig = new BackendHttpService.Config();
            backendConfig.username = properties.getUsername();
            backendConfig.password = properties.getPassword();
            backendConfig.maxConnectionsPerRoute = properties.getHttpMaxConnectionsPerRoute();
            backendConfig.totalMaxConnections = properties.getHttpTotalMaxConnections();
            backendConfig.idleConnectionTimeoutMs = properties.getHttpIdleConnectionTimeoutMs();
            BackendHttpService service2 = BackendHttpService.getInstance(backendConfig);
            service2.takeRef();
            backendService = service2;

            this.executorService = new ScheduledThreadPoolExecutor(
                    properties.getIoThreadCount(),
                    r -> {
                        Thread thread = new Thread(null, r, "merge-commit-load-" + UUID.randomUUID());
                        thread.setDaemon(true);
                        thread.setUncaughtExceptionHandler((t, e) -> {
                            LOG.error("Stream loader " + Thread.currentThread().getName() + " error", e);
                            manager.callback(e);
                        });
                        return thread;
                    });
            LOG.info("Start merge commit loader");
        } catch (Throwable e) {
            if (feMetaService != null) {
                feMetaService.releaseRef();
                feMetaService = null;
            }
            if (backendService != null) {
                backendService.releaseRef();
                backendService = null;
            }
            throw new RuntimeException("Failed to start merge commit loader", e);
        }
    }

    @Override
    public void close() {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        if (feMetaService != null) {
            feMetaService.releaseRef();
            feMetaService = null;
        }
        LOG.info("Close merge commit loader");
    }

    @Override
    public ScheduledFuture<?> scheduleFlush(Table table, long chunkId, int delayMs) {
        return executorService.schedule(() -> table.checkFlushInterval(chunkId), delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sendLoad(LoadRequest.RequestRun requestRun, int delayMs) {
        executorService.schedule(() -> sendHttp(requestRun), delayMs, TimeUnit.MILLISECONDS);
    }

    private void sendHttp(LoadRequest.RequestRun requestRun) {
        requestRun.executeTimeMs = System.currentTimeMillis();
        requestRun.state = LoadRequest.State.SENDING_REQUEST;
        LoadRequest loadRequest = requestRun.loadRequest;
        Table table = loadRequest.getTable();
        String database = table.getDatabase();
        String tableName = table.getTable();
        TableId tableId = TableId.of(database, tableName);
        try {
            byte[] data = ChunkCompressUtil.compress(loadRequest.getChunk(),
                    table.getCompressionCodec().orElseGet(NoCompressCodec::new));
            requestRun.rawSize = loadRequest.getChunk().chunkBytes();
            requestRun.compressSize = data.length;
            requestRun.compressTimeMs = System.currentTimeMillis();
            Future<WorkerAddress> httpAddressFuture = feMetaService.getNodesStateService()
                    .get().getHttpAddress(tableId, table.getLoadParameters());
            requestRun.workerAddress = httpAddressFuture.get();
            requestRun.getWorkerAddrTimeMs = System.currentTimeMillis();
            String loadUrl = backendService.getLoadUrl(requestRun.workerAddress, database, tableName);
            HttpPut httpPut = new HttpPut(loadUrl);
            httpPut.setConfig(RequestConfig.custom()
                                .setSocketTimeout(properties.getSocketTimeout())
                                .build());
            table.getLoadParameters().forEach(httpPut::addHeader);
            httpPut.addHeader("label", requestRun.userLabel);
            httpPut.addHeader(HttpHeaders.AUTHORIZATION,
                    StreamLoadUtils.getBasicAuthHeader(properties.getUsername(), properties.getPassword()));
            httpPut.setEntity(new ByteArrayEntity(data));
            requestRun.callRpcTimeMs = System.currentTimeMillis();
            requestRun.state = LoadRequest.State.WAITING_RESPONSE;
            LOG.debug(
                    "Send load request, db: {}, table: {}, user label: {}, chunkId: {}, worker: {}",
                    database, tableName, requestRun.userLabel, loadRequest.getChunk().getChunkId(),
                    requestRun.workerAddress.getHost());
            sendAndWaitResponse(requestRun, httpPut);
        } catch (Throwable e) {
            LOG.error("Failed to send load http, db: {}, table: {}, chunkId: {}",
                    database, tableName, loadRequest.getChunk().getChunkId(), e);
            table.loadFinish(requestRun, e);
        }
    }

    private void sendAndWaitResponse(LoadRequest.RequestRun requestRun, HttpPut httpPut) {
        try {
            Pair<Integer, String> response = backendService.streamLoad(httpPut);
            requestRun.state = RECEIVED_RESPONSE;
            requestRun.receiveResponseTimeMs = System.currentTimeMillis();
            LoadRequest request = requestRun.loadRequest;
            String db = request.getTable().getDatabase();
            String table = request.getTable().getTable();
            String jsonResult = response.getRight();
            StreamLoadResponse.StreamLoadResponseBody streamLoadBody;
            try {
                streamLoadBody = objectMapper.readValue(jsonResult, StreamLoadResponse.StreamLoadResponseBody.class);
            } catch (Exception e) {
                String errorMsg = "Fail to parse response, json response: " + jsonResult;
                throw new StreamLoadFailException(errorMsg, e);
            }

            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            streamLoadResponse.setBody(streamLoadBody);
            String status = streamLoadBody.getStatus();
            if (StreamLoadConstants.RESULT_STATUS_SUCCESS.equals(status)) {
                requestRun.loadResult = streamLoadResponse;
                if (streamLoadBody.getLabel() == null || streamLoadBody.getLabel().isEmpty()) {
                    throw new StreamLoadFailException(String.format("Load rpc response success with an empty label, " +
                                    "db: %s, table: %s, user label: %s, worker: %s, response: %s",
                            db, table, requestRun.userLabel, requestRun.workerAddress.getHost(), response));
                }
                requestRun.state = LoadRequest.State.WAITING_LABEL;
                LOG.debug(
                        "Load rpc success, db: {}, table: {}, user label: {}, chunkId: {}, txn label: {}",
                        db, table, requestRun.userLabel, request.getChunk().getChunkId(), streamLoadBody.getLabel());
                if (request.getTable().isMergeCommitAsync()) {
                    request.getTable().loadSyncFinish(requestRun);
                    waitLabelAsync(requestRun);
                } else {
                    completeSyncMode(requestRun);
                }
            } else {
                // TODO check label again
                String errorMsg = String.format(
                        "Load rpc failed, db: %s, table: %s, user label: %s, worker: %s, "
                                + "response: %s",
                        db, table, requestRun.userLabel, requestRun.workerAddress.getHost(), response);
                throw new StreamLoadFailException(errorMsg, streamLoadBody);
            }
        } catch (Throwable throwable) {
            LoadRequest request = requestRun.loadRequest;
            String db = request.getTable().getDatabase();
            String table = request.getTable().getTable();
            LOG.error("Load request fail, db: {}, table: {}, user label: {}, chunkId: {}, worker: {}",
                    db, table, requestRun.userLabel, request.getChunk().getChunkId(), requestRun.workerAddress.getHost(), throwable);
            String errorMsg = String.format("Load request fail, db: %s, table: %s, user label: %s, worker: %s",
                    db, table, requestRun.userLabel, requestRun.workerAddress.getHost());
            Throwable exception = new StreamLoadFailException(errorMsg, throwable);
            request.getTable().loadFinish(requestRun, exception);
        }
    }

    private void waitLabelAsync(LoadRequest.RequestRun requestRun) {
        LoadRequest request = requestRun.loadRequest;
        Table table = request.getTable();
        StreamLoadResponse loadResponse = requestRun.loadResult;
        long leftTimeMs = loadResponse.getBody().getLeftTimeMs() == null ? -1 :
                loadResponse.getBody().getLeftTimeMs() + properties.getCheckLabelInitDelayMs();
        CompletableFuture<LabelStateService.LabelMeta> future =
                feMetaService.getLabelStateService()
                        .get().getFinalStatus(
                                TableId.of(table.getDatabase(), table.getTable()),
                                loadResponse.getBody().getLabel(),
                                (int) leftTimeMs,
                                properties.getCheckLabelIntervalMs(),
                                properties.getCheckLabelTimeoutMs())
                        .whenCompleteAsync(
                                (labelMeta, throwable)
                                        -> completeAsyncMode(requestRun, labelMeta, throwable),
                                executorService);
        requestRun.labelFuture = future;
    }

    private void completeAsyncMode(LoadRequest.RequestRun requestRun,
                                   LabelStateService.LabelMeta labelMeta,
                                   Throwable throwable) {
        TransactionStatus status = labelMeta.transactionStatus;
        requestRun.labelFinalTimeMs = System.currentTimeMillis();
        requestRun.labelRequestCount = labelMeta.getRequestCount();
        requestRun.labelLatencyMs = labelMeta.getLatencyMs();
        requestRun.labelHttpCostMs = labelMeta.getHttpCostMs();
        requestRun.labelPendingCostMs = labelMeta.getPendingCostMs();
        LoadRequest loadRequest = requestRun.loadRequest;
        if (throwable != null) {
            loadRequest.getTable().loadFinish(requestRun, throwable);
            return;
        }

        if (status != TransactionStatus.VISIBLE) {
            loadRequest.getTable().loadFinish(
                    requestRun,
                    new RuntimeException(String.format(
                            "Label %s does not in final status, current status: %s, reason: %s",
                            requestRun.loadResult.getBody().getLabel(), status, labelMeta.reason)));
        } else {
            loadRequest.getTable().loadFinish(requestRun, null);
        }
    }

    private void completeSyncMode(LoadRequest.RequestRun requestRun) {
        requestRun.labelFinalTimeMs = System.currentTimeMillis();
        requestRun.labelRequestCount = 0;
        requestRun.labelLatencyMs = 0;
        requestRun.labelHttpCostMs = 0;
        requestRun.labelPendingCostMs = 0;
        LoadRequest loadRequest = requestRun.loadRequest;
        loadRequest.getTable().loadFinish(requestRun, null);
    }
}
