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

import com.starrocks.data.load.stream.http.StreamLoadEntityMeta;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;

import java.util.concurrent.Future;

public interface TableRegion {

    StreamLoadTableProperties getProperties();
    String getUniqueKey();
    String getDatabase();
    String getTable();

    void setLabel(String label);
    String getLabel();

    long getCacheBytes();
    long getFlushBytes();
    StreamLoadEntityMeta getEntityMeta();
    long getLastWriteTimeMillis();

    void resetAge();
    long getAndIncrementAge();
    long getAge();

    int write(byte[] row);
    byte[] read();

    boolean testPrepare();
    boolean prepare();
    boolean flush();
    boolean cancel();

    void callback(StreamLoadResponse response);
    void callback(Throwable e);
    void complete(StreamLoadResponse response);

    void setResult(Future<?> result);
    Future<?> getResult();

    boolean isReadable();
    boolean isFlushing();
}
