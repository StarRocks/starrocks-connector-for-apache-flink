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

import org.apache.http.HttpClientConnection;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultClientConnectionReuseStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.IdleConnectionEvictor;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * An HTTP client that keeps its connections open across requests (HTTP keep-alive). A stream loader
 * sends its loads and transaction requests through one of these, instead of opening a new TCP (and
 * TLS) connection for every request.
 *
 * <p>Reusing connections brings three problems, which this class takes care of:
 * <ul>
 *   <li><b>Requests whose body was not sent.</b> A load is a PUT with {@code Expect: 100-continue}.
 *       The FE answers a transaction load with a 307 redirect before the body is sent and keeps the
 *       connection open, while its HTTP decoder still waits for the body announced by the request
 *       headers. The next request on that connection would be read as that body. So a connection on
 *       which the final response arrived before the request body was sent is closed, not reused.</li>
 *   <li><b>Idle connections dropped in between.</b> A load balancer can forget an idle connection
 *       without telling either end, and reset it when it is used again. An AWS Network Load Balancer
 *       does so after 350 seconds by default. Connections idle for longer than the idle timeout are
 *       closed, so it must be shorter than that of any load balancer in between. A connection idle for
 *       more than {@link #VALIDATE_AFTER_INACTIVITY_MS} is also checked before it is reused, so that
 *       one the server closed in the meantime is not used.</li>
 *   <li><b>Socket timeouts left over from an earlier request.</b> HttpClient sets the socket timeout
 *       of a connection only for a request that specifies one, so a request without one would keep the
 *       timeout of the request the connection served before. Such a request gets no timeout, which is
 *       what it gets on a new connection.</li>
 * </ul>
 */
final class KeepAliveHttpClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KeepAliveHttpClient.class);

    /** A connection idle for longer than this is checked for a close by the server before it is reused. */
    static final int VALIDATE_AFTER_INACTIVITY_MS = 2000;

    /**
     * Idle connections are kept for at most this many hosts. When the pool is full, a request to yet
     * another host closes the idle connection that was used least recently instead of waiting.
     */
    static final int MAX_HOSTS = 8;

    /** Bounds of the interval at which idle connections are looked for and closed. */
    private static final long MIN_IDLE_CHECK_INTERVAL_MS = 100L;
    private static final long MAX_IDLE_CHECK_INTERVAL_MS = 10_000L;

    /**
     * Context attribute that is {@code true} when the final response to a request arrived before its
     * body was sent.
     */
    static final String REQUEST_BODY_SKIPPED = "starrocks.request-body-skipped";

    private final int maxConnectionsPerHost;
    private final long idleTimeoutMs;
    private final PoolingHttpClientConnectionManager connectionManager;
    private final CloseableHttpClient client;
    private final IdleConnectionEvictor idleConnectionEvictor;

    /**
     * @param maxConnectionsPerHost the most connections to one host, in use and idle together
     * @param waitForContinueTimeoutMs how long to wait for {@code 100 Continue} before sending a body
     * @param idleTimeoutMs a connection idle for longer than this is closed
     * @param redirectStrategy which redirects to follow
     */
    KeepAliveHttpClient(int maxConnectionsPerHost, int waitForContinueTimeoutMs, long idleTimeoutMs,
                        RedirectStrategy redirectStrategy) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
        this.idleTimeoutMs = idleTimeoutMs;
        this.connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setDefaultMaxPerRoute(maxConnectionsPerHost);
        connectionManager.setMaxTotal(maxConnectionsPerHost * MAX_HOSTS);
        connectionManager.setValidateAfterInactivity(VALIDATE_AFTER_INACTIVITY_MS);
        this.client = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setRequestExecutor(new KeepAliveRequestExecutor(waitForContinueTimeoutMs))
                .setConnectionReuseStrategy(new KeepAliveReuseStrategy())
                .setRedirectStrategy(redirectStrategy)
                .build();
        long checkIntervalMs = Math.max(MIN_IDLE_CHECK_INTERVAL_MS, Math.min(idleTimeoutMs, MAX_IDLE_CHECK_INTERVAL_MS));
        this.idleConnectionEvictor = new IdleConnectionEvictor(connectionManager,
                checkIntervalMs, TimeUnit.MILLISECONDS, idleTimeoutMs, TimeUnit.MILLISECONDS);
        idleConnectionEvictor.start();
    }

    /**
     * Sends {@code request} on a pooled connection and returns what {@code reader} reads from the
     * response. The response is closed afterwards, which returns the connection to the pool if the
     * response was read to the end and the connection can be reused, and closes it otherwise.
     */
    <T> T execute(HttpRequestBase request, DefaultStreamLoader.ResponseReader<T> reader) throws IOException {
        HttpClientContext context = HttpClientContext.create();
        // Cookies last for one request and its redirects, as with the client built per request before
        context.setCookieStore(new BasicCookieStore());
        try (CloseableHttpResponse response = client.execute(request, context)) {
            return reader.read(response);
        }
    }

    int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    /** Connections of all hosts: leased (in use), available (idle) and requests waiting for one. */
    PoolStats getTotalStats() {
        return connectionManager.getTotalStats();
    }

    @Override
    public void close() {
        idleConnectionEvictor.shutdown();
        try {
            // Also shuts down the connection manager, which closes every connection
            client.close();
        } catch (IOException e) {
            LOG.warn("Failed to close the HTTP connection pool", e);
        }
    }

    @Override
    public String toString() {
        return "maxConnectionsPerHost=" + maxConnectionsPerHost + ", idleTimeoutMs=" + idleTimeoutMs;
    }

    /**
     * Records whether the server answered before the request body was sent, and gives a request
     * without a socket timeout no timeout, instead of the one its connection was left with.
     */
    static final class KeepAliveRequestExecutor extends HttpRequestExecutor {

        KeepAliveRequestExecutor(int waitForContinueTimeoutMs) {
            super(waitForContinueTimeoutMs);
        }

        @Override
        public HttpResponse execute(HttpRequest request, HttpClientConnection conn, HttpContext context)
                throws IOException, HttpException {
            if (HttpClientContext.adapt(context).getRequestConfig().getSocketTimeout() < 0) {
                // HttpClient leaves the socket timeout alone for -1. A new connection starts with 0 (none).
                conn.setSocketTimeout(0);
            }
            return super.execute(request, conn, context);
        }

        @Override
        protected HttpResponse doSendRequest(HttpRequest request, HttpClientConnection conn, HttpContext context)
                throws IOException, HttpException {
            HttpResponse response = super.doSendRequest(request, conn, context);
            // The superclass returns a response only when a final response arrived while it waited for
            // 100 Continue, in which case it did not send the body. Redirects reuse the context, and each
            // hop sets the attribute again.
            context.setAttribute(REQUEST_BODY_SKIPPED, response != null);
            return response;
        }
    }

    /** Does not reuse a connection on which the server answered before the request body was sent. */
    static final class KeepAliveReuseStrategy extends DefaultClientConnectionReuseStrategy {

        @Override
        public boolean keepAlive(HttpResponse response, HttpContext context) {
            if (Boolean.TRUE.equals(context.getAttribute(REQUEST_BODY_SKIPPED))) {
                return false;
            }
            return super.keepAlive(response, context);
        }
    }
}
