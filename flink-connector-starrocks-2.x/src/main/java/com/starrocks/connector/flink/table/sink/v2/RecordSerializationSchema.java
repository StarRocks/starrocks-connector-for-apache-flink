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

import org.apache.flink.api.common.serialization.SerializationSchema;

import com.starrocks.connector.flink.table.data.StarRocksRowData;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A serialization schema which defines how to convert a value
 * of type {@code T} to {@link StarRocksRowData}.
 *
 * @param <T> the type of input record being serialized
 */
public interface RecordSerializationSchema<T> extends Serializable {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize(Object)} and thus suitable for one-time setup work.
     *
     * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     * @param sinkContext runtime information i.e. partitions, subtaskId
     */
    void open(SerializationSchema.InitializationContext context, StarRocksSinkContext sinkContext);

    /**
     * Serializes the given record and returns it as a {@link StarRocksRowData}.
     *
     * @param record element to be serialized
     * @return a {@link StarRocksRowData} or null if the given record cannot be serialized
     */
    @Nullable
    StarRocksRowData serialize(T record);

    /** Close the serializer */
    void close();
}
