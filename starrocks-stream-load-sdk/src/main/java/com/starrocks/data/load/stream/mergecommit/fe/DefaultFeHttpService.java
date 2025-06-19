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

package com.starrocks.data.load.stream.mergecommit.fe;

import com.starrocks.data.load.stream.StreamLoadUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.IdleConnectionEvictor;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DefaultFeHttpService implements FeHttpService, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultFeHttpService.class);

    private static final String NODES_URL_PATTERN = "%s/api/%s/%s/_stream_load_meta?type=nodes";
    private static final String LABEL_STATE_URL_PATTERN = "%s/api/%s/get_load_state?label=%s";

    private final Config config;
    private CloseableHttpClient httpClient;
    private IdleConnectionEvictor idleConnectionEvictor;

    public DefaultFeHttpService(Config config) {
        this.config = config;
    }

    public void start() throws Exception {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(config.maxConnectionsPerRoute);
        int totalMaxConnections = Math.max(config.totalMaxConnections,
                config.maxConnectionsPerRoute * config.candidateHosts.size());
        cm.setMaxTotal(totalMaxConnections);
        cm.setValidateAfterInactivity(config.validateAfterInactivityMs);
        httpClient = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                })
                .setConnectionManager(cm)
                .build();
        idleConnectionEvictor = new IdleConnectionEvictor(cm, 10, TimeUnit.SECONDS,
                config.idleConnectionTimeoutMs, TimeUnit.MILLISECONDS);
        LOG.info("Init http client, maxConnectionsPerRoute: {}, totalMaxConnections: {}, " +
                "idleConnectionTimeoutMs: {}, candidate hosts: {}",
                config.maxConnectionsPerRoute, totalMaxConnections,
                config.idleConnectionTimeoutMs, config.candidateHosts);
    }

    @Override
    public void close() {
        if (httpClient != null) {
            try {
                httpClient.close();
                LOG.info("Close http client");
            } catch (Exception e) {
                LOG.error("Failed to close http client", e);
            }
        }
        if (idleConnectionEvictor != null) {
            idleConnectionEvictor.shutdown();
            idleConnectionEvictor = null;
        }
    }

    public Pair<Integer, String> getNodes(String db, String table, Map<String, String> headers) throws Exception {
        String url = String.format(NODES_URL_PATTERN, getHost(), db, table);
        HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Authorization", StreamLoadUtils.getBasicAuthHeader(config.username, config.password));
        headers.forEach(httpGet::addHeader);
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            int code = response.getStatusLine().getStatusCode();
            String content = EntityUtils.toString(response.getEntity());
            return Pair.of(code, content);
        }
    }

    public Pair<Integer, String> getLabelState(String db, String label) throws Exception {
        String url = String.format(LABEL_STATE_URL_PATTERN, getHost(), db, label);
        HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Authorization", StreamLoadUtils.getBasicAuthHeader(config.username, config.password));
        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            int code = response.getStatusLine().getStatusCode();
            String content = EntityUtils.toString(response.getEntity());
            return Pair.of(code, content);
        }
    }

    private String getHost() {
        int i = ThreadLocalRandom.current().nextInt(config.candidateHosts.size());
        return config.candidateHosts.get(i);
    }

    public static class Config {
        public int maxConnectionsPerRoute = 3;
        public int totalMaxConnections = 30;
        public int idleConnectionTimeoutMs = 60000;
        public int validateAfterInactivityMs = 2000;
        public String username;
        public String password;
        public List<String> candidateHosts;
    }

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.username = "root";
        config.password = "";
        config.candidateHosts = Arrays.asList("http://127.0.0.1:11901", "http://127.0.0.1:11901");
        try (DefaultFeHttpService service = new DefaultFeHttpService(config)) {
            Map<String, String> headers = new HashMap<>();
            System.out.println(service.getNodes("test", "tbl", headers));
            headers.put("enable_merge_commit", "true");
            headers.put("merge_commit_interval_ms", "1000");
            headers.put("merge_commit_parallel", "4");
            System.out.println(service.getNodes("test", "tbl", headers));
            System.out.println(service.getLabelState("test", "insert_1bd3005b-a559-11ef-b5ea-5e0024ae5de7"));
        }
    }
}
