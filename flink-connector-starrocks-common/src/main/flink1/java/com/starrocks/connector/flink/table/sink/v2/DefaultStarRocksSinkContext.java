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

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;

/** Default implementation for {@link StarRocksSinkContext}. */
public class DefaultStarRocksSinkContext implements StarRocksSinkContext {

    private final Sink.InitContext initContext;
    private final int numberOfParallelSubtasks;
    private final int subtaskIndex;
    private final StarRocksSinkOptions sinkOptions;

    public DefaultStarRocksSinkContext(Sink.InitContext initContext, StarRocksSinkOptions sinkOptions) {
        this.initContext = initContext;
        this.numberOfParallelSubtasks = initContext.getNumberOfParallelSubtasks();
        this.subtaskIndex = initContext.getSubtaskId();
        this.sinkOptions = sinkOptions;
    }

    // for callers outside a Flink sink, which have no init context
    public DefaultStarRocksSinkContext(
            int numberOfParallelSubtasks, int subtaskIndex, StarRocksSinkOptions sinkOptions) {
        this.initContext = null;
        this.numberOfParallelSubtasks = numberOfParallelSubtasks;
        this.subtaskIndex = subtaskIndex;
        this.sinkOptions = sinkOptions;
    }

    @Override
    public Sink.InitContext getInitContext() {
        if (initContext == null) {
            throw new UnsupportedOperationException("This sink context was created without a Sink.InitContext");
        }
        return initContext;
    }

    @Override
    public StarRocksSinkOptions getSinkOptions() {
        return sinkOptions;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return numberOfParallelSubtasks;
    }

    @Override
    public int getSubtaskIndex() {
        return subtaskIndex;
    }
}
