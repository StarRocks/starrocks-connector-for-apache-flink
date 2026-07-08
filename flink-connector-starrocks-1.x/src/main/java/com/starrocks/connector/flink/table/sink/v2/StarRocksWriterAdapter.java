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

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/** Bridges the Flink 1.x writer interfaces to the shared {@link StarRocksWriter}. */
public class StarRocksWriterAdapter<InputT>
        implements StatefulSink.StatefulSinkWriter<InputT, StarRocksWriterState>,
        TwoPhaseCommittingSink.PrecommittingSinkWriter<InputT, StarRocksCommittable> {

    private final StarRocksWriter<InputT> delegate;

    public StarRocksWriterAdapter(
            StarRocksSinkOptions sinkOptions,
            Sink.InitContext initContext,
            RecordSerializationSchema<InputT> serializationSchema,
            StreamLoadProperties streamLoadProperties,
            Collection<StarRocksWriterState> recoveredState) throws Exception {
        this.delegate = new StarRocksWriter<>(
                sinkOptions,
                initContext.getRestoredCheckpointId().orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1),
                initContext.getNumberOfParallelSubtasks(),
                initContext.getSubtaskId(),
                initContext.metricGroup(),
                initContext.asSerializationSchemaInitializationContext(),
                serializationSchema,
                streamLoadProperties,
                recoveredState);
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        delegate.write(element, context);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        delegate.flush(endOfInput);
    }

    @Override
    public Collection<StarRocksCommittable> prepareCommit() throws IOException, InterruptedException {
        return delegate.prepareCommit();
    }

    @Override
    public List<StarRocksWriterState> snapshotState(long checkpointId) throws IOException {
        return delegate.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
