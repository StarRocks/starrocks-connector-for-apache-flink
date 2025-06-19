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

import com.starrocks.data.load.stream.Chunk;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.mergecommit.be.PStreamLoadResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadRequest {

    private static final Logger LOG = LoggerFactory.getLogger(LoadRequest.class);

    private final Table table;
    private final Chunk chunk;
    private final int maxRetries;
    private final int retryIntervalMs;
    private final AtomicInteger numRuns;
    private final ConcurrentLinkedDeque<RequestRun> requestRuns = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean bufferReleased;
    private final long startTimeMs;

    public LoadRequest(Table table, Chunk chunk, int maxRetries, int retryIntervalMs) {
        this.table = table;
        this.chunk = chunk;
        this.maxRetries = maxRetries;
        this.retryIntervalMs = retryIntervalMs;
        this.numRuns = new AtomicInteger(0);
        this.bufferReleased = new AtomicBoolean(false);
        this.startTimeMs = System.currentTimeMillis();
    }

    public int nextRetryInterval() {
        if (numRuns.get() > maxRetries + 1 || retryIntervalMs <= 0) {
            return -1;
        }
        return retryIntervalMs + ThreadLocalRandom.current().nextInt(2000);
    }

    public Table getTable() {
        return table;
    }

    public Chunk getChunk() {
        return chunk;
    }

    public void setBufferReleased() {
        bufferReleased.set(true);
    }

    public boolean isBufferReleased() {
        return bufferReleased.get();
    }

    public RequestRun newRun() {
        int id = numRuns.getAndIncrement();
        String userLabel = UUID.randomUUID().toString();
        RequestRun requestRun = new RequestRun(this, id, userLabel);
        requestRuns.add(requestRun);
        return requestRun;
    }

    public int getNumRuns() {
        return numRuns.get();
    }

    public RequestRun getLastRun() {
        return requestRuns.getLast();
    }

    public long getTotalTimeMs() {
        return System.currentTimeMillis() - startTimeMs;
    }

    public void stateSummary(StringBuilder builder) {
        builder.append("chunkId: ").append(chunk.getChunkId())
                .append(", num runs: ").append(numRuns.get());
        int count = 0;
        for (RequestRun requestRun : requestRuns) {
            builder.append(", run ").append(count)
                    .append(": {");
            requestRun.stateSummary(builder);
            builder.append("}");
            count += 1;
        }
    }

    public void logRequestTrace() {
        StringBuilder builder = new StringBuilder();
        stateSummary(builder);
        LOG.info("Load request trace, db: {}, table: {}, {}",
                getTable().getDatabase(), getTable().getTable(), builder);
    }

    enum State {
        INIT,
        SENDING_REQUEST,
        WAITING_RESPONSE,
        RECEIVED_RESPONSE,
        WAITING_LABEL,
        SUCCESS,
        FAILED
    }

    public static class RequestRun {
        public final LoadRequest loadRequest;
        public final int id;
        public final String userLabel;
        public volatile State state = State.INIT;
        public volatile long rawSize = -1;
        public volatile long compressSize = -1;
        public volatile WorkerAddress workerAddress = null;
        public volatile PStreamLoadResponse rpcResponse;
        public volatile StreamLoadResponse loadResult;
        public volatile CompletableFuture<?> labelFuture = null;
        public volatile Throwable throwable = null;

        // Time trace
        public volatile long createTimeMs;
        public volatile long executeTimeMs = -1;
        public volatile long compressTimeMs = -1;
        public volatile long getWorkerAddrTimeMs = -1;
        public volatile long callRpcTimeMs = -1;
        public volatile long receiveResponseTimeMs = -1;
        public volatile long labelFinalTimeMs = -1;
        public volatile long labelRequestCount = -1;
        public volatile long labelLatencyMs = -1;
        public volatile long labelHttpCostMs = -1;
        public volatile long labelPendingCostMs = -1;
        public volatile long finishTime = -1;

        public RequestRun(LoadRequest loadRequest, int id, String userLabel) {
            this.loadRequest = loadRequest;
            this.id = id;
            this.userLabel = userLabel;
            this.createTimeMs = System.currentTimeMillis();
        }

        public long getServerTimeMs() {
            return receiveResponseTimeMs > 0 ? receiveResponseTimeMs - callRpcTimeMs : -1;
        }

        public void stateSummary(StringBuilder builder) {
            builder.append("id: ").append(id)
                    .append(", state: ").append(state)
                    .append(", userLabel: ").append(userLabel);
            String txnLabel = loadResult != null && loadResult.getBody() != null ? loadResult.getBody().getLabel() : "null";
            builder.append(", txnLabel: ").append(txnLabel);
            String worker = workerAddress != null ? workerAddress.getHost() : "null";
            builder.append(", worker: ").append(worker);
            builder.append(", raw/compress: ").append(rawSize).append("/").append(compressSize);
            builder.append(", total: ").append(finishTime - createTimeMs).append(" ms");
            builder.append(", pending: ").append(executeTimeMs > 0 ? executeTimeMs - createTimeMs : -1).append(" ms");
            builder.append(", compress: ").append(compressTimeMs > 0 ? compressTimeMs - executeTimeMs : -1).append(" ms");
            builder.append(", getWorkerAddr: ").append(getWorkerAddrTimeMs > 0 ? getWorkerAddrTimeMs - compressTimeMs : -1).append(" ms");
            builder.append(", callRpc: ").append(callRpcTimeMs > 0 ? callRpcTimeMs - getWorkerAddrTimeMs : -1).append(" ms");
            builder.append(", server: ").append(getServerTimeMs()).append(" ms");
            if (loadResult != null && loadResult.getBody() != null) {
                StreamLoadResponse.StreamLoadResponseBody body = loadResult.getBody();
                builder.append(", readData: ").append(body.getReadDataTimeMs() != null ? body.getReadDataTimeMs() : -1).append(" ms");
                builder.append(", serverPending: ").append(body.getPendingTimeMs() != null ? body.getPendingTimeMs() : -1).append(" ms");
                builder.append(", waitPlan: ").append(body.getWaitPlanTimeMs() != null ? body.getWaitPlanTimeMs() : -1).append(" ms");
                builder.append(", writeData: ").append(body.getWriteDataTimeMs() != null ? body.getWriteDataTimeMs() : -1).append(" ms");
                builder.append(", waitFinish: ").append(body.getWaitFinishTimeMs() != null ? body.getWaitFinishTimeMs() : -1).append(" ms");
                builder.append(", leftMerge: ").append(body.getLeftMergeTimeMs() != null ? body.getLeftMergeTimeMs() : -1).append(" ms");
            }
            builder.append(", waitLabel: ").append(labelFinalTimeMs > 0 ? labelFinalTimeMs - receiveResponseTimeMs : -1).append(" ms");
            if (labelFinalTimeMs > 0) {
                builder.append(", labelRequestCount: ").append(labelRequestCount);
                builder.append(", labelLatency: ").append(labelLatencyMs).append(" ms");
                builder.append(", labelHttpCost: ").append(labelHttpCostMs).append(" ms");
                builder.append(", labelPendingCost: ").append(labelPendingCostMs).append(" ms");
            }
            builder.append(", exception: ").append(throwable == null ? "N/A" : throwable.getMessage());
        }
    }
}
