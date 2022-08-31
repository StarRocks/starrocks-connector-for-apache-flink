package com.starrocks.connector.flink.manager;

import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.starrocks.data.load.stream.StreamLoadResponse;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

public class StarRocksSinkRuntimeContext {

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
    private transient Histogram streamLoadPutTimeMs;
    private transient Histogram readDataTimeMs;
    private transient Histogram writeDataTimeMs;
    private transient Histogram loadTimeMs;

    public StarRocksSinkRuntimeContext(RuntimeContext context, StarRocksSinkOptions sinkOptions) {
        totalFlushBytes = context.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_BYTES);
        totalFlushRows = context.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_ROWS);
        totalFlushTime = context.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_COST_TIME);
        totalFlushTimeWithoutRetries = context.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_COST_TIME_WITHOUT_RETRIES);
        totalFlushSucceededTimes = context.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_SUCCEEDED_TIMES);
        totalFlushFailedTimes = context.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_FAILED_TIMES);
        flushTimeNs = context.getMetricGroup().histogram(HISTOGRAM_FLUSH_TIME, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        offerTimeNs = context.getMetricGroup().histogram(HISTOGRAM_OFFER_TIME_NS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));

        totalFilteredRows = context.getMetricGroup().counter(COUNTER_NUMBER_FILTERED_ROWS);
        commitAndPublishTimeMs = context.getMetricGroup().histogram(HISTOGRAM_COMMIT_AND_PUBLISH_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        streamLoadPutTimeMs = context.getMetricGroup().histogram(HISTOGRAM_STREAM_LOAD_PUT_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        readDataTimeMs = context.getMetricGroup().histogram(HISTOGRAM_READ_DATA_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        writeDataTimeMs = context.getMetricGroup().histogram(HISTOGRAM_WRITE_DATA_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
        loadTimeMs = context.getMetricGroup().histogram(HISTOGRAM_LOAD_TIME_MS, new DescriptiveStatisticsHistogram(sinkOptions.getSinkHistogramWindowSize()));
    }

    public static void flushSucceedRecord(StarRocksSinkRuntimeContext context,
                                          StreamLoadResponse response) {
        if (context != null) {
            context.flushSucceedRecord(response);
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
        if (responseBody.getStreamLoadPutTimeMs() != null) {
            streamLoadPutTimeMs.update(responseBody.getStreamLoadPutTimeMs());
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

    public static void flushFailedRecord(StarRocksSinkRuntimeContext context) {
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
    private static final String HISTOGRAM_STREAM_LOAD_PUT_TIME_MS = "streamLoadPutTimeMs";
    private static final String HISTOGRAM_READ_DATA_TIME_MS = "readDataTimeMs";
    private static final String HISTOGRAM_WRITE_DATA_TIME_MS = "writeDataTimeMs";
    private static final String HISTOGRAM_LOAD_TIME_MS = "loadTimeMs";
}
