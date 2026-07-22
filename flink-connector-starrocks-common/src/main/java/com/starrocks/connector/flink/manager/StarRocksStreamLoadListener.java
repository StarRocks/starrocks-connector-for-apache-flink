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

package com.starrocks.connector.flink.manager;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.mergecommit.MetricListener;
import com.starrocks.data.load.stream.v2.StreamLoadListener;

import java.util.concurrent.atomic.AtomicLong;

public class StarRocksStreamLoadListener implements StreamLoadListener, MetricListener {

    private transient Counter totalFlushBytes;
    private transient Counter totalFlushRows;
    private transient Counter totalFlushTime;
    private transient Counter totalFlushTimeWithoutRetries;
    private transient Counter totalFlushSucceededTimes;
    private transient Counter totalFlushFailedTimes;
    private transient Histogram flushTimeNs;
    private transient Histogram offerTimeNs;

    private transient Counter totalFilteredRows;
    private transient Histogram commitAndPublishTimeMs;
    private transient Histogram streamLoadPlanTimeMs;
    private transient Histogram readDataTimeMs;
    private transient Histogram writeDataTimeMs;
    private transient Histogram loadTimeMs;

    private transient AtomicLong maxBufferSize;
    private transient AtomicLong currentBufferSize;
    private transient Counter numBufferFull;
    private transient Histogram waitBufferTimeMs;
    private transient Counter numFlush;
    private transient AtomicLong numFlushTable;
    private transient Counter numFailLoads;
    private transient Counter numSuccessLoads;
    private transient Counter numLoadRetry;
    private transient Histogram serverTimeMs;
    private transient AtomicLong numInflightLoads;
    private transient AtomicLong numInflightRows;
    private transient AtomicLong numInflightBytes;

    public StarRocksStreamLoadListener(MetricGroup metricGroup, StarRocksSinkOptions sinkOptions) {
        totalFlushBytes = metricGroup.counter(COUNTER_TOTAL_FLUSH_BYTES);
        totalFlushRows = metricGroup.counter(COUNTER_TOTAL_FLUSH_ROWS);
        totalFlushTime = metricGroup.counter(COUNTER_TOTAL_FLUSH_COST_TIME);
        totalFlushTimeWithoutRetries = metricGroup.counter(COUNTER_TOTAL_FLUSH_COST_TIME_WITHOUT_RETRIES);
        totalFlushSucceededTimes = metricGroup.counter(COUNTER_TOTAL_FLUSH_SUCCEEDED_TIMES);
        totalFlushFailedTimes = metricGroup.counter(COUNTER_TOTAL_FLUSH_FAILED_TIMES);
        flushTimeNs = metricGroup.histogram(HISTOGRAM_FLUSH_TIME, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        offerTimeNs = metricGroup.histogram(HISTOGRAM_OFFER_TIME_NS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));

        totalFilteredRows = metricGroup.counter(COUNTER_NUMBER_FILTERED_ROWS);
        commitAndPublishTimeMs = metricGroup.histogram(HISTOGRAM_COMMIT_AND_PUBLISH_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        streamLoadPlanTimeMs = metricGroup.histogram(HISTOGRAM_STREAM_LOAD_PLAN_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        readDataTimeMs = metricGroup.histogram(HISTOGRAM_READ_DATA_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        writeDataTimeMs = metricGroup.histogram(HISTOGRAM_WRITE_DATA_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        loadTimeMs = metricGroup.histogram(HISTOGRAM_LOAD_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));

        maxBufferSize = new AtomicLong(0L);
        currentBufferSize = new AtomicLong(0L);
        metricGroup.gauge(GAUGE_MAX_BUFFER_BYTES, () -> maxBufferSize.get());
        metricGroup.gauge(GAUGE_CURRENT_BUFFER_BYTES, () -> currentBufferSize.get());
        numBufferFull = metricGroup.counter(COUNTER_NUM_BUFFER_FULL);
        waitBufferTimeMs = metricGroup.histogram(HISTOGRAM_WAIT_BUFFER_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        numFlush = metricGroup.counter(COUNTER_NUM_FLUSH);
        numFlushTable = new AtomicLong(0);
        metricGroup.gauge(GAUGE_NUM_FLUSH_TABLE, () -> numFlushTable.get());
        numFailLoads = metricGroup.counter(COUNTER_NUM_FAIL_LOADS);
        numSuccessLoads = metricGroup.counter(COUNTER_NUM_SUCCESS_LOADS);
        numLoadRetry = metricGroup.counter(COUNTER_NUM_LOAD_RETRY);
        serverTimeMs = metricGroup.histogram(HISTOGRAM_SERVER_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        numInflightLoads = new AtomicLong(0);
        numInflightRows = new AtomicLong(0);
        numInflightBytes = new AtomicLong(0);
        metricGroup.gauge(GAUGE_NUM_INFLIGHT_LOADS, () -> numInflightLoads.get());
        metricGroup.gauge(GAUGE_NUM_INFLIGHT_ROWS, () -> numInflightRows.get());
        metricGroup.gauge(GAUGE_NUM_INFLIGHT_BYTES, () -> numInflightBytes.get());
    }

    @Override
    public void onResponse(StreamLoadResponse response) {
        if (response.getException() != null) {
            flushFailedRecord();
        } else {
            flushSucceedRecord(response);
        }
    }

    public void flushSucceedRecord(StreamLoadResponse response) {
        if (response.getFlushBytes() != null) {
            totalFlushBytes.inc(response.getFlushBytes());
        }
        if (response.getFlushRows() != null) {
            totalFlushRows.inc(response.getFlushRows());
        }
        if (response.getCostNanoTime() != null) {
            totalFlushTime.inc(response.getCostNanoTime());
            flushTimeNs.update(response.getCostNanoTime());
        }

        totalFlushSucceededTimes.inc();

        StreamLoadResponse.StreamLoadResponseBody responseBody = response.getBody();
        if (responseBody == null) {
            return;
        }
        if (responseBody.getCommitAndPublishTimeMs() != null) {
            commitAndPublishTimeMs.update(responseBody.getCommitAndPublishTimeMs());
        }
        if (responseBody.getStreamLoadPlanTimeMs() != null) {
            streamLoadPlanTimeMs.update(responseBody.getStreamLoadPlanTimeMs());
        }
        if (responseBody.getReadDataTimeMs() != null) {
            readDataTimeMs.update(responseBody.getReadDataTimeMs());
        }
        if (responseBody.getWriteDataTimeMs() != null) {
            writeDataTimeMs.update(responseBody.getWriteDataTimeMs());
        }
        if (responseBody.getLoadTimeMs() != null) {
            loadTimeMs.update(responseBody.getLoadTimeMs());
        }
        if (responseBody.getNumberFilteredRows() != null) {
            totalFilteredRows.inc(responseBody.getNumberFilteredRows());
        }
    }

    public static void flushFailedRecord(StarRocksStreamLoadListener context) {
        if (context != null) {
            context.flushFailedRecord();
        }
    }

    public void flushFailedRecord() {
        totalFlushFailedTimes.inc();
    }

    private static final String COUNTER_TOTAL_FLUSH_BYTES = "totalFlushBytes";
    private static final String COUNTER_TOTAL_FLUSH_ROWS = "totalFlushRows";
    private static final String COUNTER_TOTAL_FLUSH_COST_TIME_WITHOUT_RETRIES = "totalFlushTimeNsWithoutRetries";
    private static final String COUNTER_TOTAL_FLUSH_COST_TIME = "totalFlushTimeNs";
    private static final String COUNTER_TOTAL_FLUSH_SUCCEEDED_TIMES = "totalFlushSucceededTimes";
    private static final String COUNTER_TOTAL_FLUSH_FAILED_TIMES = "totalFlushFailedTimes";
    private static final String HISTOGRAM_FLUSH_TIME= "flushTimeNs";
    private static final String HISTOGRAM_OFFER_TIME_NS = "offerTimeNs";

    // from stream load result
    private static final String COUNTER_NUMBER_FILTERED_ROWS = "totalFilteredRows";
    private static final String HISTOGRAM_COMMIT_AND_PUBLISH_TIME_MS = "commitAndPublishTimeMs";
    // No change of the metric name to ensure compatibility.
    private static final String HISTOGRAM_STREAM_LOAD_PLAN_TIME_MS = "streamLoadPutTimeMs";
    private static final String HISTOGRAM_READ_DATA_TIME_MS = "readDataTimeMs";
    private static final String HISTOGRAM_WRITE_DATA_TIME_MS = "writeDataTimeMs";
    private static final String HISTOGRAM_LOAD_TIME_MS = "loadTimeMs";

    private static final String GAUGE_MAX_BUFFER_BYTES = "maxBufferBytes";
    private static final String GAUGE_CURRENT_BUFFER_BYTES = "currentBufferBytes";
    private static final String COUNTER_NUM_BUFFER_FULL = "numBufferFull";
    private static final String HISTOGRAM_WAIT_BUFFER_TIME_MS = "waitBufferTimeMs";
    private static final String COUNTER_NUM_FLUSH = "numFlush";
    private static final String GAUGE_NUM_FLUSH_TABLE = "numFlushTable";
    private static final String COUNTER_NUM_FAIL_LOADS = "numFailLoads";
    private static final String COUNTER_NUM_SUCCESS_LOADS = "numSuccessLoads";
    private static final String COUNTER_NUM_LOAD_RETRY = "numLoadRetry";
    private static final String HISTOGRAM_SERVER_TIME_MS = "serverTimeMs";
    private static final String GAUGE_NUM_INFLIGHT_LOADS = "numInflightLoads";
    private static final String GAUGE_NUM_INFLIGHT_ROWS = "numInflightRows";
    private static final String GAUGE_NUM_INFLIGHT_BYTES = "numInflightBytes";

    // merge commit metrics =============================

    @Override
    public void onWrite(int numRows, int dataSize) {
        totalFlushRows.inc(numRows);
        totalFlushBytes.inc(dataSize);
    }

    @Override
    public void onCacheChange(long maxCacheBytes, long currentCacheBytes) {
        maxBufferSize.set(maxCacheBytes);
        currentBufferSize.set(currentCacheBytes);
    }

    @Override
    public void onCacheFull(long blockTimeMs) {
        numBufferFull.inc(1);
        waitBufferTimeMs.update(blockTimeMs);
    }

    @Override
    public void onFlush(int numTables, long flushTimeMs) {
        numFlush.inc(1);
        this.numFlushTable.set(numTables);
        this.flushTimeNs.update(flushTimeMs * 1000000);
    }

    @Override
    public void onLoadStart(long dataSize, int numRows) {
        updateInflightLoads(1, numRows, dataSize);
    }

    @Override
    public void onLoadFailure(long dataSize, int numRows, int numRetry, long totalTimeMs) {
        updateInflightLoads(-1, -numRows, -dataSize);
        this.numFailLoads.inc(1);
        this.numLoadRetry.inc(numRetry);
    }

    @Override
    public void onLoadSuccess(long dataSize, int numRows, int numRetry, long totalTimeMs, long serverTimeMs) {
        updateInflightLoads(-1, -numRows, -dataSize);
        this.numSuccessLoads.inc(1);
        this.numLoadRetry.inc(numRetry);
        this.loadTimeMs.update(totalTimeMs);
        this.serverTimeMs.update(serverTimeMs);
    }

    private void updateInflightLoads(int deltaNum, int deltaRows, long deltaBytes) {
        this.numInflightLoads.addAndGet(deltaNum);
        this.numInflightRows.addAndGet(deltaRows);
        this.numInflightBytes.addAndGet(deltaBytes);
    }
}
