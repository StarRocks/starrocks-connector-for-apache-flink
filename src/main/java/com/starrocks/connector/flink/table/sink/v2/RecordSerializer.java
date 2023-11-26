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

import com.starrocks.connector.flink.table.data.StarRocksRowData;

import java.io.Serializable;

/**
 * Interface for the input record serialization.
 *
 * @param <T> the type of input record being serialized
 */
public interface RecordSerializer<T> extends Serializable {

    /** Open the serializer. */
    void open();

    /** Serialize the input record. */
    StarRocksRowData serialize(T record);

    /** Close the serializer */
    void close();
}
