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

package com.starrocks.data.load.stream;

import com.starrocks.data.load.stream.mergecommit.MetricListener;
import com.starrocks.data.load.stream.v2.StreamLoadListener;

public interface StreamLoadManager {

    void init();
    void write(String uniqueKey, String database, String table, String... rows);
    void callback(StreamLoadResponse response);
    void callback(Throwable e);
    void flush();

    StreamLoadSnapshot snapshot();
    boolean prepare(StreamLoadSnapshot snapshot);
    boolean commit(StreamLoadSnapshot snapshot);
    boolean abort(StreamLoadSnapshot snapshot);
    void close();

    default StreamLoader getStreamLoader() {
        throw new UnsupportedOperationException();
    }

    default void setStreamLoadListener(StreamLoadListener streamLoadListener) {
       // ignore
    }

    default void setLabelGeneratorFactory(LabelGeneratorFactory labelGeneratorFactory) {
        // ignore
    }

    default void setMetricListener(MetricListener metricListener) {}

    default void setCommitAllowed(boolean allowed) {}

    /**
     * Partition-aware variant for multi-table transaction mode.
     * Signals that partition {@code partition} has reached a transaction boundary.
     */
    default void setCommitAllowed(int partition, boolean allowed) {
        setCommitAllowed(allowed);
    }

    /**
     * Partition-aware write for multi-table transaction mode.
     * Routes data to a per-partition region so that switchChunk can be
     * scoped to a single partition without affecting other partitions' data.
     */
    default void write(int partition, String database, String table, String... rows) {
        write(null, database, table, rows);
    }

    default Throwable getException() {
        return null;
    }
}
