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

package com.starrocks.connector.flink.table.sink.v2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;

import com.starrocks.connector.flink.manager.StarRocksStreamLoadListener;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.ExactlyOnceLabelGeneratorFactory;
import com.starrocks.connector.flink.table.sink.ExactlyOnceLabelGeneratorSnapshot;
import com.starrocks.connector.flink.table.sink.LingeringTransactionAborter;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import com.starrocks.connector.flink.tools.EnvUtils;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class StarRocksWriter<InputT>
        implements StatefulSink.StatefulSinkWriter<InputT, StarRocksWriterState>,
        TwoPhaseCommittingSink.PrecommittingSinkWriter<InputT, StarRocksCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksWriter.class);

    private final StarRocksSinkOptions sinkOptions;
    private final StarRocksRecordSerializationSchema<InputT> serializationSchema;
    private final StarRocksStreamLoadListener streamLoadListener;
    private final LabelGeneratorFactory labelGeneratorFactory;
    private final StreamLoadManagerV2 sinkManager;
    private long totalReceivedRows = 0;

    public StarRocksWriter(
            StarRocksSinkOptions sinkOptions,
            StarRocksRecordSerializationSchema<InputT> serializationSchema,
            StreamLoadProperties streamLoadProperties,
            Sink.InitContext initContext,
            Collection<StarRocksWriterState> recoveredState) throws Exception {
        this.sinkOptions = sinkOptions;
        this.serializationSchema = serializationSchema;
        this.serializationSchema.open();
        this.streamLoadListener = new StarRocksStreamLoadListener(initContext.metricGroup(), sinkOptions);
        long restoredCheckpointId = initContext.getRestoredCheckpointId()
                .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        List<ExactlyOnceLabelGeneratorSnapshot> restoredGeneratorSnapshots = new ArrayList<>();
        for (StarRocksWriterState writerState : recoveredState) {
            restoredGeneratorSnapshots.addAll(writerState.getLabelSnapshots());
        }
        String labelPrefix = sinkOptions.getLabelPrefix();
        if (labelPrefix == null ||
                sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE ||
                !sinkOptions.isEnableExactlyOnceLabelGen()) {
            this.labelGeneratorFactory = new LabelGeneratorFactory.DefaultLabelGeneratorFactory(
                    labelPrefix == null ? "flink" : labelPrefix);
        } else {
            this.labelGeneratorFactory = new ExactlyOnceLabelGeneratorFactory(
                    labelPrefix,
                    initContext.getNumberOfParallelSubtasks(),
                    initContext.getSubtaskId(),
                    restoredCheckpointId);
        }

        this.sinkManager = new StreamLoadManagerV2(streamLoadProperties,
                sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE);
        sinkManager.setStreamLoadListener(streamLoadListener);
        sinkManager.setLabelGeneratorFactory(labelGeneratorFactory);
        try {
            sinkManager.init();
        } catch (Exception e) {
            LOG.error("Failed to init sink manager.", e);
            try {
                sinkManager.close();
            } catch (Exception ie) {
                LOG.error("Failed to close sink manager after init failure.", ie);
            }
            throw new RuntimeException("Failed to init sink manager", e);
        }

        try {
            if (sinkOptions.getSemantic() == StarRocksSinkSemantic.EXACTLY_ONCE
                    && sinkOptions.isAbortLingeringTxns()) {
                LingeringTransactionAborter aborter = new LingeringTransactionAborter(
                        sinkOptions.getLabelPrefix(),
                        restoredCheckpointId,
                        initContext.getSubtaskId(),
                        sinkOptions.getAbortCheckNumTxns(),
                        sinkOptions.getDbTables(),
                        restoredGeneratorSnapshots,
                        sinkManager.getStreamLoader());

                aborter.execute();
            }
        } catch (Exception e) {
            LOG.error("Failed to abort lingering transactions.", e);
            try {
                sinkManager.close();
            } catch (Exception ie) {
                LOG.error("Failed to close sink manager after aborting lingering transaction failure.", ie);
            }
            throw new RuntimeException("Failed to abort lingering transactions", e);
        }

        LOG.info("Create StarRocksWriter. {}", EnvUtils.getGitInformation());
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        StarRocksRowData rowData = serializationSchema.serialize(element);
        sinkManager.write(rowData.getUniqueKey(), rowData.getDatabase(), rowData.getTable(), rowData.getRow());
        totalReceivedRows += 1;
        if (totalReceivedRows % 100 == 1) {
            LOG.debug("Received raw record: {}", element);
            LOG.debug("Received serialized record: {}", rowData.getRow());
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        sinkManager.flush();
    }

    @Override
    public Collection<StarRocksCommittable> prepareCommit() throws IOException, InterruptedException {
        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE) {
            return Collections.emptyList();
        }

        StreamLoadSnapshot snapshot = sinkManager.snapshot();
        if (sinkManager.prepare(snapshot)) {
            return Collections.singleton(new StarRocksCommittable(snapshot));
        } else {
            sinkManager.abort(snapshot);
            throw new RuntimeException("Snapshot state failed by prepare");
        }
    }

    @Override
    public List<StarRocksWriterState> snapshotState(long checkpointId) throws IOException {
        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE ||
                !(labelGeneratorFactory instanceof ExactlyOnceLabelGeneratorFactory)) {
            return Collections.emptyList();
        }

        List<ExactlyOnceLabelGeneratorSnapshot> labelSnapshots =
                ((ExactlyOnceLabelGeneratorFactory) labelGeneratorFactory).snapshot(checkpointId);
        return Collections.singletonList(new StarRocksWriterState(labelSnapshots));
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close StarRocksWriter");
        serializationSchema.close();
        if (sinkManager != null) {
            try {
                StreamLoadSnapshot snapshot = sinkManager.snapshot();
                sinkManager.abort(snapshot);
            } finally {
                sinkManager.close();
            }
        }
    }
}
