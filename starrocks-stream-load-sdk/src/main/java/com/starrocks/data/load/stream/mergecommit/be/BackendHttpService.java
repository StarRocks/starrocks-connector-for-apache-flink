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

package com.starrocks.data.load.stream.mergecommit.be;

import com.starrocks.data.load.stream.mergecommit.Pair;
import com.starrocks.data.load.stream.mergecommit.SharedService;
import com.starrocks.data.load.stream.mergecommit.SharedServiceRegistry;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.IdleConnectionEvictor;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class BackendHttpService extends SharedService {

    private static final Logger LOG = LoggerFactory.getLogger(BackendHttpService.class);

    // Loaders with equal connection settings share a service, so that they share the connections
    private static final SharedServiceRegistry<Key, BackendHttpService> REGISTRY = new SharedServiceRegistry<>();

    private final Config config;
    private final Key key;
    private CloseableHttpClient httpClient;
    private IdleConnectionEvictor idleConnectionEvictor;
    private volatile String currentUuid;

    private BackendHttpService(Config config, Key key) {
        this.config = config;
        this.key = key;
    }

    // Takes a reference to the service of the config, which is shared with equal connection settings.
    // Call release() to give it back.
    public static BackendHttpService acquire(Config config) throws Exception {
        Key key = new Key(config);
        return REGISTRY.acquire(key, () -> new BackendHttpService(config, key));
    }

    public void release() {
        REGISTRY.release(key, this);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected void init() {
        currentUuid = UUID.randomUUID().toString();
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(config.maxConnectionsPerRoute);
        cm.setMaxTotal(config.totalMaxConnections);
        cm.setValidateAfterInactivity(config.validateAfterInactivityMs);
        httpClient = HttpClients.custom().setConnectionManager(cm)
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                }).build();
        idleConnectionEvictor = new IdleConnectionEvictor(cm, 10, TimeUnit.SECONDS,
                config.idleConnectionTimeoutMs, TimeUnit.MILLISECONDS);
        idleConnectionEvictor.start();
        LOG.info("Init backend http service, uuid: {}, maxConnectionsPerRoute: {}, totalMaxConnections: {}, " +
                "idleConnectionTimeoutMs: {}", currentUuid, config.maxConnectionsPerRoute,
                config.totalMaxConnections, config.idleConnectionTimeoutMs);
    }

    @Override
    protected void reset() {
        if (httpClient != null) {
            try {
                httpClient.close();
                LOG.info("Close http client");
            } catch (Exception e) {
                LOG.error("Failed to close http client", e);
            } finally {
                httpClient = null;
            }
        }
        if (idleConnectionEvictor != null) {
            idleConnectionEvictor.shutdown();
            idleConnectionEvictor = null;
        }
        LOG.info("Reset backend http service, uuid: {}", currentUuid);
    }

    public Pair<Integer, String> streamLoad(HttpPut httpPut) throws Exception {
        try (CloseableHttpResponse response = httpClient.execute(httpPut)) {
            int code = response.getStatusLine().getStatusCode();
            String content = EntityUtils.toString(response.getEntity());
            return Pair.of(code, content);
        }
    }

    public static class Config {
        public int maxConnectionsPerRoute = 3;
        public int totalMaxConnections = 30;
        public int idleConnectionTimeoutMs = 60000;
        public int validateAfterInactivityMs = 2000;
        public String username;
        public String password;
    }

    // The connection settings that the service uses. Configs with equal settings share a service.
    // The credentials are not part of it, because each request carries its own Authorization header.
    private static final class Key {

        private final List<Integer> values;

        Key(Config config) {
            this.values = Arrays.asList(config.maxConnectionsPerRoute, config.totalMaxConnections,
                    config.idleConnectionTimeoutMs, config.validateAfterInactivityMs);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Key && values.equals(((Key) o).values);
        }

        @Override
        public int hashCode() {
            return values.hashCode();
        }
    }
}
