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

/**
 * A {@link CredentialsProvider} that always returns the same credentials. This is the provider
 * used when credentials are configured with a fixed user name and password.
 */
public final class StaticCredentialsProvider implements CredentialsProvider {

    private static final long serialVersionUID = 1L;

    private final Credentials credentials;

    public StaticCredentialsProvider(String username, String password) {
        this.credentials = new Credentials(username, password);
    }

    @Override
    public Credentials getCredentials() {
        return credentials;
    }

    @Override
    public String toString() {
        return "StaticCredentialsProvider{" + credentials + "}";
    }
}
