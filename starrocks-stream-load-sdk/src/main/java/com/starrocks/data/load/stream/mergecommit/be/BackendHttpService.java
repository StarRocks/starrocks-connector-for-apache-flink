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

import com.starrocks.data.load.stream.mergecommit.SharedService;
import com.starrocks.data.load.stream.mergecommit.WorkerAddress;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.IdleConnectionEvictor;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class BackendHttpService extends SharedService {

    private static final Logger LOG = LoggerFactory.getLogger(BackendHttpService.class);

    private static final String LOAD_URL_PATTERN = "http://%s:%s/api/%s/%s/_stream_load";

    private static volatile BackendHttpService INSTANCE;

    private final Config config;
    private CloseableHttpClient httpClient;
    private IdleConnectionEvictor idleConnectionEvictor;
    private volatile String currentUuid;

    public BackendHttpService(Config config) {
        this.config = config;
    }

    public static BackendHttpService getInstance(Config config) {
        if (INSTANCE == null) {
            synchronized (BackendHttpService.class) {
                if (INSTANCE == null) {
                    INSTANCE = new BackendHttpService(config);
                }
            }
        }
        return INSTANCE;
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
        httpClient = HttpClients.custom().setConnectionManager(cm).build();
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

    public String getLoadUrl(WorkerAddress worker, String database, String table) {
        return String.format(LOAD_URL_PATTERN, worker.getHost(), worker.getPort(), database, table);
    }

    public static class Config {
        public int maxConnectionsPerRoute = 3;
        public int totalMaxConnections = 30;
        public int idleConnectionTimeoutMs = 60000;
        public int validateAfterInactivityMs = 2000;
        public String username;
        public String password;
    }
}
