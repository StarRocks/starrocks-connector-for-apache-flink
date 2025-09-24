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

import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.HttpVersion;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.*;

public class ContainerAwareRedirectStrategyTest {

    private ContainerAwareRedirectStrategy strategy;
    private String[] loadUrls;
    private String originalHost;

    @Before
    public void setUp() {
        originalHost = "starrocks:8030";
        loadUrls = new String[]{"starrocks:8030", "starrocks:8040"};
        strategy = new ContainerAwareRedirectStrategy(originalHost, loadUrls);
    }

    @Test
    public void testNormalRedirectUnchanged() throws Exception {
        // Test normal redirect to external host - should remain unchanged
        HttpPut originalRequest = new HttpPut("http://starrocks:8030/api/test_db/test_table/_stream_load");
        
        HttpResponse response = createRedirectResponse("http://external-host:8040/api/test_db/test_table/_stream_load");
        
        HttpUriRequest redirectRequest = strategy.getRedirect(originalRequest, response, new BasicHttpContext());
        
        assertEquals("external-host", redirectRequest.getURI().getHost());
        assertEquals(8040, redirectRequest.getURI().getPort());
    }

    @Test
    public void testLocalhostRedirectFixed() throws Exception {
        // Test localhost redirect - should be fixed to container host
        HttpPut originalRequest = new HttpPut("http://starrocks:8030/api/test_db/test_table/_stream_load");
        
        // 创建一个重定向响应
        BasicHttpResponse response = new BasicHttpResponse(
            new BasicStatusLine(HttpVersion.HTTP_1_1, 307, "Temporary Redirect")
        );
        response.setHeader("Location", "http://127.0.0.1:8040/api/test_db/test_table/_stream_load");
        
        HttpUriRequest redirectRequest = strategy.getRedirect(originalRequest, response, new BasicHttpContext());
        
        // Should be fixed to use original hostname
        assertEquals("starrocks", redirectRequest.getURI().getHost());
        assertEquals(8040, redirectRequest.getURI().getPort());
        assertEquals("/api/test_db/test_table/_stream_load", redirectRequest.getURI().getPath());
    }

    @Test
    public void testHostMappingWithMultipleUrls() throws Exception {
        // Test with multiple load URLs for port mapping
        String[] multipleUrls = {"starrocks:8030", "starrocks-be1:8040", "starrocks-be2:8041"};
        ContainerAwareRedirectStrategy multiStrategy = new ContainerAwareRedirectStrategy("starrocks:8030", multipleUrls);
        
        HttpPut originalRequest = new HttpPut("http://starrocks:8030/api/test_db/test_table/_stream_load");
        
        // Test redirect to port 8041 should map to starrocks-be2
        HttpResponse response = createRedirectResponse("http://127.0.0.1:8041/api/test_db/test_table/_stream_load");
        
        HttpUriRequest redirectRequest = multiStrategy.getRedirect(originalRequest, response, new BasicHttpContext());
        
        assertEquals("starrocks-be2", redirectRequest.getURI().getHost());
        assertEquals(8041, redirectRequest.getURI().getPort());
    }

    @Test
    public void testIsRedirectable() {
        assertTrue(strategy.isRedirectable("PUT"));
        assertTrue(strategy.isRedirectable("POST"));
        assertTrue(strategy.isRedirectable("GET"));
    }

    @Test
    public void testWithHttpsUrls() throws Exception {
        String[] httpsUrls = {"https://starrocks:8030", "https://starrocks:8040"};
        ContainerAwareRedirectStrategy httpsStrategy = new ContainerAwareRedirectStrategy("https://starrocks:8030", httpsUrls);
        
        HttpPut originalRequest = new HttpPut("https://starrocks:8030/api/test_db/test_table/_stream_load");
        
        HttpResponse response = createRedirectResponse("https://127.0.0.1:8040/api/test_db/test_table/_stream_load");
        
        HttpUriRequest redirectRequest = httpsStrategy.getRedirect(originalRequest, response, new BasicHttpContext());
        
        assertEquals("starrocks", redirectRequest.getURI().getHost());
        assertEquals(8040, redirectRequest.getURI().getPort());
        assertEquals("https", redirectRequest.getURI().getScheme());
    }

    private HttpResponse createRedirectResponse(String location) {
        BasicHttpResponse response = new BasicHttpResponse(
            new BasicStatusLine(HttpVersion.HTTP_1_1, 307, "Temporary Redirect")
        );
        response.setHeader("Location", location);
        return response;
    }
} 