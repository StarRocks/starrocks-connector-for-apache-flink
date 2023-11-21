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

import com.google.common.base.Strings;
import com.starrocks.connector.flink.manager.StarRocksStreamLoadListener;
import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.ExactlyOnceLabelGeneratorFactory;
import com.starrocks.connector.flink.table.sink.ExactlyOnceLabelGeneratorSnapshot;
import com.starrocks.connector.flink.table.sink.LingeringTransactionAborter;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkRowDataWithMeta;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import com.starrocks.connector.flink.tools.EnvUtils;
import com.starrocks.connector.flink.tools.JsonWrapper;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class StarRocksWriter<InputT>
        implements StatefulSink.StatefulSinkWriter<InputT, StarRocksWriterState>,
        TwoPhaseCommittingSink.PrecommittingSinkWriter<InputT, StarRocksCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksWriter.class);

    private final StarRocksSinkOptions sinkOptions;
    private final StarRocksISerializer serializer;
    private final StarRocksIRowTransformer<InputT> rowTransformer;
    private final JsonWrapper jsonWrapper;
    private final StarRocksStreamLoadListener streamLoadListener;
    private final LabelGeneratorFactory labelGeneratorFactory;
    private final StreamLoadManagerV2 sinkManager;
    private long totalReceivedRows = 0;

    public StarRocksWriter(
            StarRocksSinkOptions sinkOptions,
            StarRocksISerializer serializer,
            StarRocksIRowTransformer<InputT> rowTransformer,
            StreamLoadProperties streamLoadProperties,
            Sink.InitContext initContext,
            Collection<StarRocksWriterState> recoveredState) throws Exception {
        this.sinkOptions = sinkOptions;
        this.serializer = serializer;
        this.rowTransformer = rowTransformer;

        this.jsonWrapper = new JsonWrapper();
        if (this.serializer != null) {
            this.serializer.open(new StarRocksISerializer.SerializerContext(jsonWrapper));
        }
        if (this.rowTransformer != null) {
            this.rowTransformer.setRuntimeContext(null);
            this.rowTransformer.setFastJsonWrapper(jsonWrapper);
        }
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
        if (serializer == null) {
            if (element instanceof StarRocksSinkRowDataWithMeta) {
                StarRocksSinkRowDataWithMeta data = (StarRocksSinkRowDataWithMeta) element;
                if (Strings.isNullOrEmpty(data.getDatabase())
                        || Strings.isNullOrEmpty(data.getTable())
                        || data.getDataRows() == null) {
                    LOG.warn(String.format("json row data not fulfilled. {database: %s, table: %s, dataRows: %s}",
                            data.getDatabase(), data.getTable(), Arrays.toString(data.getDataRows())));
                    return;
                }
                sinkManager.write(null, data.getDatabase(), data.getTable(), data.getDataRows());
                return;
            } else if (element instanceof StarRocksRowData) {
                StarRocksRowData data = (StarRocksRowData) element;
                if (Strings.isNullOrEmpty(data.getDatabase())
                        || Strings.isNullOrEmpty(data.getTable())
                        || data.getRow() == null) {
                    LOG.warn(String.format("json row data not fulfilled. {database: %s, table: %s, dataRows: %s}",
                            data.getDatabase(), data.getTable(), data.getRow()));
                    return;
                }
                sinkManager.write(data.getUniqueKey(), data.getDatabase(), data.getTable(), data.getRow());
                return;
            }
            // raw data sink
            sinkManager.write(null, sinkOptions.getDatabaseName(), sinkOptions.getTableName(), element.toString());
            return;
        }

        if (element instanceof RowData) {
            if (RowKind.UPDATE_BEFORE.equals(((RowData) element).getRowKind()) &&
                    (!sinkOptions.supportUpsertDelete() || sinkOptions.getIgnoreUpdateBefore())) {
                return;
            }
            if (!sinkOptions.supportUpsertDelete() && RowKind.DELETE.equals(((RowData) element).getRowKind())) {
                // let go the UPDATE_AFTER and INSERT rows for tables who have a group of `unique` or `duplicate` keys.
                return;
            }
        }
        String serializedValue = serializer.serialize(rowTransformer.transform(element, sinkOptions.supportUpsertDelete()));
        sinkManager.write(
                null,
                sinkOptions.getDatabaseName(),
                sinkOptions.getTableName(),
                serializedValue);

        totalReceivedRows += 1;
        if (totalReceivedRows % 100 == 1) {
            LOG.debug("Received raw record: {}", element);
            LOG.debug("Received serialized record: {}", serializedValue);
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
