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

package com.starrocks.data.load.stream.auth;

import java.io.Serializable;

/**
 * Supplies the credentials used to authenticate HTTP requests to StarRocks.
 *
 * <p>The SDK calls {@link #getCredentials()} once for every request it sends (stream load,
 * transaction begin/prepare/commit/rollback, label state queries and merge commit requests), and
 * builds that request's {@code Authorization} header from the returned pair. An implementation
 * can therefore return different credentials over time, for example after reading a secret that
 * is rotated outside the application, and the next request picks them up without restarting the
 * loader.
 *
 * <p>The provider is part of {@link com.starrocks.data.load.stream.properties.StreamLoadProperties},
 * which is serialized when it is shipped to remote workers such as Flink task managers, so an
 * implementation must be serializable. Implementations are called concurrently from the loader's
 * I/O threads and must be thread-safe. They are called on the request path, so an expensive
 * lookup should be cached by the implementation.
 */
@FunctionalInterface
public interface CredentialsProvider extends Serializable {

    /**
     * Returns the credentials to use for the next request. Must not return {@code null}.
     */
    Credentials getCredentials();
}
