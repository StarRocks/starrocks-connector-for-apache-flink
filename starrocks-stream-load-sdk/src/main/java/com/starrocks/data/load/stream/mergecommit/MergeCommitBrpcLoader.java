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

import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.RpcCallback;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.channel.ChannelType;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.data.load.stream.StreamLoadConstants;
import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.compress.NoCompressCodec;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.mergecommit.be.BackendBrpcService;
import com.starrocks.data.load.stream.mergecommit.be.PStreamLoadRequest;
import com.starrocks.data.load.stream.mergecommit.be.PStreamLoadResponse;
import com.starrocks.data.load.stream.mergecommit.be.PStringPair;
import com.starrocks.data.load.stream.mergecommit.fe.DefaultFeHttpService;
import com.starrocks.data.load.stream.mergecommit.fe.FeMetaService;
import com.starrocks.data.load.stream.mergecommit.fe.LabelStateService;
import com.starrocks.data.load.stream.mergecommit.fe.NodesStateService;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.starrocks.data.load.stream.mergecommit.LoadRequest.State.RECEIVED_RESPONSE;

public class MergeCommitBrpcLoader extends MergeCommitLoader {

    private static final Logger LOG = LoggerFactory.getLogger(MergeCommitBrpcLoader.class);

    private StreamLoadProperties properties;
    private ObjectMapper objectMapper;
    private BackendBrpcService brpcService = null;
    private FeMetaService feMetaService = null;
    private ScheduledExecutorService executorService;

    public MergeCommitBrpcLoader() {
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

            RpcClientOptions clientOptions = new RpcClientOptions();
            clientOptions.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
            clientOptions.setConnectTimeoutMillis(properties.getBrpcConnectTimeoutMs());
            clientOptions.setReadTimeoutMillis(properties.getBrpcReadTimeoutMs());
            clientOptions.setWriteTimeoutMillis(properties.getBrpcWriteTimeoutMs());
            clientOptions.setChannelType(ChannelType.POOLED_CONNECTION);
            clientOptions.setMaxTotalConnections(properties.getBrpcMaxConnections());
            clientOptions.setMinIdleConnections(properties.getBrpcMinConnections());
            clientOptions.setMaxTryTimes(properties.getBrpcMaxRetryTimes());
            clientOptions.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
            clientOptions.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
            int nproc = Runtime.getRuntime().availableProcessors();
            clientOptions.setIoThreadNum(properties.getBrpcIoThreadNum() > 0 ? properties.getBrpcIoThreadNum() : nproc);
            clientOptions.setWorkThreadNum(properties.getBrpcWorkerThreadNum() > 0 ? properties.getBrpcWorkerThreadNum() : nproc);
            BackendBrpcService.BrpcConfig brpcConfig = new BackendBrpcService.BrpcConfig(clientOptions);
            BackendBrpcService service = BackendBrpcService.getInstance(brpcConfig);
            service.takeRef();
            brpcService = service;

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
            if (brpcService != null) {
                brpcService.releaseRef();
                brpcService = null;
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
        if (brpcService != null) {
            brpcService.releaseRef();
            brpcService = null;
        }
        LOG.info("Close merge commit loader");
    }

    @Override
    public ScheduledFuture<?> scheduleFlush(Table table, long chunkId, int delayMs) {
        return executorService.schedule(() -> table.checkFlushInterval(chunkId), delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sendLoad(LoadRequest.RequestRun requestRun, int delayMs) {
        executorService.schedule(() -> sendBrpc(requestRun), delayMs, TimeUnit.MILLISECONDS);
    }

    private void sendBrpc(LoadRequest.RequestRun requestRun) {
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
            Future<WorkerAddress> brpcAddressFuture = feMetaService.getNodesStateService()
                    .get().getBrpcAddress(tableId, table.getLoadParameters());
            WorkerAddress workerAddress = brpcAddressFuture.get();
            requestRun.getWorkerAddrTimeMs = System.currentTimeMillis();
            PStreamLoadRequest request = new PStreamLoadRequest();
            request.setDb(database);
            request.setTable(tableName);
            request.setUser(properties.getUsername());
            request.setPasswd(properties.getPassword());
            List<PStringPair> parameters = new ArrayList<>();
            table.getLoadParameters().forEach((k, v) -> parameters.add(PStringPair.of(k, v)));
            parameters.add(PStringPair.of("label", requestRun.userLabel));
            request.setParameters(parameters);
            RpcContext.getContext().setRequestBinaryAttachment(data);
            LoadRpcCallback callback = new LoadRpcCallback(requestRun);
            requestRun.workerAddress = workerAddress;
            brpcService.streamLoad(workerAddress, request, callback);
            requestRun.callRpcTimeMs = System.currentTimeMillis();
            requestRun.state = LoadRequest.State.WAITING_RESPONSE;
            LOG.debug(
                    "Send load request, db: {}, table: {}, user label: {}, chunkId: {}, worker: {}",
                    database, tableName, requestRun.userLabel, loadRequest.getChunk().getChunkId(), workerAddress.getHost());
        } catch (Throwable e) {
            LOG.error("Failed to send load brpc, db: {}, table: {}, chunkId: {}",
                    database, tableName, loadRequest.getChunk().getChunkId(), e);
            table.loadFinish(requestRun, e);
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

    private class LoadRpcCallback implements RpcCallback<PStreamLoadResponse> {

        private final LoadRequest.RequestRun requestRun;
        private PStreamLoadResponse response;

        public LoadRpcCallback(LoadRequest.RequestRun requestRun) { this.requestRun = requestRun; }

        @Override
        public void success(PStreamLoadResponse response) {
            this.response = response;
            requestRun.state = RECEIVED_RESPONSE;
            requestRun.receiveResponseTimeMs = System.currentTimeMillis();
            try {
                LoadRequest request = requestRun.loadRequest;
                String db = request.getTable().getDatabase();
                String table = request.getTable().getTable();
                requestRun.rpcResponse = response;
                String jsonResult = response.getJson_result();
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
                fail(throwable);
            }
        }

        @Override
        public void fail(Throwable throwable) {
            if (response != null) {
                requestRun.state = RECEIVED_RESPONSE;
                requestRun.receiveResponseTimeMs = System.currentTimeMillis();
            }
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
}
