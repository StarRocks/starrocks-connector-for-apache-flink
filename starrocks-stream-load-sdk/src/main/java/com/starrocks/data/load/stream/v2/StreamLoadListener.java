/*
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

package com.starrocks.data.load.stream.v2;

import com.starrocks.data.load.stream.StreamLoadResponse;

public interface StreamLoadListener {

    void onResponse(StreamLoadResponse response);

    /**
     * Called after a shared transaction commits in multi-table transaction mode.
     * Only called when the first-row time of the transaction is known.
     *
     * @param label the shared transaction label
     * @param firstRowTimeMs epoch millis when the earliest row committed in this
     *                       transaction entered the sink buffer
     * @param latencyMs time from {@code firstRowTimeMs} until the commit returned
     */
    default void onSharedTransactionCommitted(String label, long firstRowTimeMs, long latencyMs) {
    }
}
