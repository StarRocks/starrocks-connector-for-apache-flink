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
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
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
    private boolean backendDirectConnection = false;

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
            this.backendDirectConnection = properties.isBackendDirectConnection();

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
        if (backendService != null) {
            backendService.releaseRef();
            backendService = null;
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
            String loadUrl;
            if (backendDirectConnection) {
                Future<WorkerAddress> httpAddressFuture = feMetaService.getNodesStateService()
                        .get().getHttpAddress(tableId, table.getLoadParameters());
                requestRun.workerAddress = httpAddressFuture.get();
                loadUrl = getLoadUrlFromBackend(requestRun.workerAddress, database, tableName);
            } else {
                String feUrl = feMetaService.getFeUrl();
                requestRun.workerAddress = new WorkerAddress(feUrl, "");
                loadUrl = getLoadUrlFromFrontend(feUrl, database, tableName);
            }
            requestRun.getWorkerAddrTimeMs = System.currentTimeMillis();
            HttpPut httpPut = new HttpPut(loadUrl);
            httpPut.setConfig(RequestConfig.custom()
                                .setSocketTimeout(properties.getSocketTimeout())
                                .build());
            table.getLoadParameters().forEach(httpPut::addHeader);
            httpPut.addHeader("label", requestRun.userLabel);
            httpPut.addHeader(HttpHeaders.AUTHORIZATION,
                    StreamLoadUtils.getBasicAuthHeader(properties.getUsername(), properties.getPassword()));
            if (!backendDirectConnection) {
                httpPut.addHeader(HttpHeaders.EXPECT, "100-continue");
            }
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

    private static final String BACKEND_LOAD_URL_PATTERN = "http://%s:%s/api/%s/%s/_stream_load";
    private static final String FRONTEND_LOAD_URL_PATTERN = "%s/api/%s/%s/_stream_load";

    public String getLoadUrlFromBackend(WorkerAddress worker, String database, String table) {
        return String.format(BACKEND_LOAD_URL_PATTERN, worker.getHost(), worker.getPort(), database, table);
    }

    public String getLoadUrlFromFrontend(String feUrl, String database, String table) {
        return String.format(FRONTEND_LOAD_URL_PATTERN, feUrl, database, table);
    }

    private void sendAndWaitResponse(LoadRequest.RequestRun requestRun, HttpPut httpPut) {
        try {
            Pair<Integer, String> response = backendService.streamLoad(httpPut);
            requestRun.state = RECEIVED_RESPONSE;
            requestRun.receiveResponseTimeMs = System.currentTimeMillis();
            LoadRequest request = requestRun.loadRequest;
            String db = request.getTable().getDatabase();
            String table = request.getTable().getTable();
            checkResponseCode(db, table, requestRun.userLabel, response.getFirst(), response.getSecond());
            String jsonResult = response.getSecond();
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
            if (StreamLoadConstants.RESULT_STATUS_SUCCESS.equals(status)
                    || StreamLoadConstants.RESULT_STATUS_TRANSACTION_PUBLISH_TIMEOUT.equals(status)) {
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

    private void checkResponseCode(String db, String table, String requestId, int code, String response) throws StreamLoadFailException {
        String errorMsg = null;
        if (307 == code) {
            errorMsg = String.format("Request failed because http response code is 307 which means 'Temporary Redirect'. " +
                            "This can happen when FE responds the request slowly , you should find the reason first. The reason may be " +
                            "StarRocks FE/Flink/Spark GC, network delay, or others. db: %s, table: %s, request id: %s, response: %s",
                            db, table, requestId, response);

        } else if (401 == code) {
            errorMsg = String.format("Request failed because of access denied. You need to grant at least SELECT and INSERT " +
                    "privilege on %s.%s. request id: %s, response: %s", db, table, requestId, response);
        } else if (200 != code) {
            errorMsg = String.format("Request failed because http response code is not 200. db: %s, table: %s," +
                    "request id: %s, response: %s", db, table, requestId, response);
        }
        if (errorMsg != null) {
            LOG.error("{}", errorMsg);
            throw new StreamLoadFailException(errorMsg);
        }
    }

    private void waitLabelAsync(LoadRequest.RequestRun requestRun) {
        LoadRequest request = requestRun.loadRequest;
        Table table = request.getTable();
        StreamLoadResponse loadResponse = requestRun.loadResult;
        Optional<Long> mergeLeftTimeMs = Optional.ofNullable(loadResponse.getBody().getLeftTimeMs());
        int scheduleDelayMs = mergeLeftTimeMs.map(
                time -> time.intValue() + properties.getCheckLabelStateInitDelayMs()).orElse(-1);
        int scheduleTimeoutMs = properties.getCheckLabelStateTimeoutMs() <= 0 ?
                table.getLoadTimeoutMs() :
                mergeLeftTimeMs.map(Long::intValue).orElse(0) + properties.getCheckLabelStateTimeoutMs();
        requestRun.labelFuture = feMetaService.getLabelStateService()
                .get().waitForLabelFinalState(
                        TableId.of(table.getDatabase(), table.getTable()),
                        loadResponse.getBody().getLabel(),
                        scheduleDelayMs,
                        properties.getCheckLabelStateIntervalMs(),
                        scheduleTimeoutMs,
                        properties.getPublishTimeoutMs())
                .whenCompleteAsync(
                        (labelMeta, throwable)
                                -> completeAsyncMode(requestRun, labelMeta, throwable),
                        executorService);
    }

    private void completeAsyncMode(LoadRequest.RequestRun requestRun,
                                   LabelStateService.LabelMeta labelMeta,
                                   Throwable throwable) {
        LoadRequest loadRequest = requestRun.loadRequest;
        if (throwable != null) {
            loadRequest.getTable().loadFinish(requestRun, throwable);
            return;
        }
        TransactionStatus status = labelMeta.transactionStatus;
        requestRun.labelFinalTimeMs = System.currentTimeMillis();
        requestRun.labelRequestCount = labelMeta.getRequestCount();
        requestRun.labelLatencyMs = labelMeta.getLatencyMs();
        requestRun.labelHttpCostMs = labelMeta.getHttpCostMs();
        requestRun.labelPendingCostMs = labelMeta.getPendingCostMs();

        if (status != TransactionStatus.VISIBLE && status != TransactionStatus.COMMITTED) {
            Optional<String> errorLog =
                    StreamLoadUtils.tryGetErrorLogForMergeCommit(labelMeta.reason, properties.isSanitizeErrorLog());
            String errorMsg = String.format(
                    "Label %s is not in final status, current status: %s, reason: %s",
                    requestRun.loadResult.getBody().getLabel(), status, labelMeta.reason);
            if (errorLog.isPresent()) {
                errorMsg = errorMsg + ", errorLog: " + errorLog.get();
            }
            loadRequest.getTable().loadFinish(requestRun, new RuntimeException(errorMsg));
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
