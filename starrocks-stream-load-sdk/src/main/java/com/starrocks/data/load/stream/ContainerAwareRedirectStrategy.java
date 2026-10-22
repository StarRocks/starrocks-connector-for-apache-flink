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

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Container-aware redirect strategy that handles Docker environment redirects.
 * This strategy fixes the issue where StarRocks FE redirects to 127.0.0.1:8040
 * which is not accessible from other containers.
 */
public class ContainerAwareRedirectStrategy extends DefaultRedirectStrategy {

    private static final Logger log = LoggerFactory.getLogger(ContainerAwareRedirectStrategy.class);
    
    private final Map<String, String> hostMapping;
    private final String originalHost;
    
    public ContainerAwareRedirectStrategy(String originalHost, String[] loadUrls) {
        this.originalHost = originalHost;
        this.hostMapping = buildHostMapping(loadUrls);
    }
    
    @Override
    public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context) 
            throws ProtocolException {
        HttpUriRequest redirectRequest = super.getRedirect(request, response, context);
        
        // Get the redirect URI
        URI redirectUri = redirectRequest.getURI();
        String redirectHost = redirectUri.getHost();
        int redirectPort = redirectUri.getPort();
        
        log.debug("Redirect detected: {} -> host={}, port={}", request.getRequestLine().getUri(), redirectHost, redirectPort);
        
        // Check if this is a problematic localhost redirect
        if (isLocalhostRedirect(redirectHost, redirectPort)) {
            log.info("Localhost redirect detected, attempting to fix");
            String newHost = resolveContainerHost(redirectHost, redirectPort);
            log.debug("Resolved new host: {}", newHost);
            if (newHost != null) {
                try {
                    URI newUri = new URI(
                        redirectUri.getScheme(),
                        redirectUri.getUserInfo(),
                        newHost,
                        redirectPort,
                        redirectUri.getPath(),
                        redirectUri.getQuery(),
                        redirectUri.getFragment()
                    );
                    
                    log.info("Redirect URL fixed: {} -> {}", redirectUri, newUri);
                    
                    // Create new request with fixed URI using reflection
                    try {
                        HttpUriRequest newRequest = (HttpUriRequest) redirectRequest.getClass()
                            .getConstructor(URI.class)
                            .newInstance(newUri);
                        
                        // Copy headers
                        newRequest.setHeaders(redirectRequest.getAllHeaders());
                        
                        log.debug("Created new request with URI: {}", newRequest.getURI());
                        return newRequest;
                    } catch (Exception reflectionEx) {
                        log.debug("Reflection failed, trying alternative approach: {}", reflectionEx.getMessage());
                        
                        // Alternative approach: Create a new HttpPut with the fixed URI
                        if (redirectRequest.getMethod().equals("PUT")) {
                            org.apache.http.client.methods.HttpPut newPut = new org.apache.http.client.methods.HttpPut(newUri);
                            newPut.setHeaders(redirectRequest.getAllHeaders());
                            log.debug("Created new HttpPut with URI: {}", newPut.getURI());
                            return newPut;
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to fix redirect URL, using original: " + redirectUri, e);
                }
            }
        } else {
            log.debug("Normal redirect, no fix needed");
        }
        
        return redirectRequest;
    }
    
    @Override
    protected boolean isRedirectable(String method) {
        return true;
    }
    
    private boolean isLocalhostRedirect(String host, int port) {
        return ("127.0.0.1".equals(host) || "localhost".equals(host)) && port > 0;
    }
    
    private String resolveContainerHost(String localhostHost, int port) {
        String portKey = String.valueOf(port);
        
        // First try direct port mapping
        if (hostMapping.containsKey(portKey)) {
            return hostMapping.get(portKey);
        }
        
        // For 8040 port (BE), try to use the original FE host
        if (port == 8040 && originalHost != null) {
            String originalHostName = extractHostName(originalHost);
            if (originalHostName != null) {
                log.info("Using original host {} for BE port {}", originalHostName, port);
                return originalHostName;
            }
        }
        
        return null;
    }
    
    private String extractHostName(String hostUrl) {
        try {
            // Remove http:// or https:// prefix if present
            String cleanHost = hostUrl.replaceAll("^https?://", "");
            // Remove port if present
            return cleanHost.split(":")[0];
        } catch (Exception e) {
            log.warn("Failed to extract hostname from: " + hostUrl, e);
            return null;
        }
    }
    
    private Map<String, String> buildHostMapping(String[] loadUrls) {
        Map<String, String> mapping = new HashMap<>();
        
        if (loadUrls != null) {
            for (String url : loadUrls) {
                try {
                    String cleanUrl = url.replaceAll("^https?://", "");
                    String[] parts = cleanUrl.split(":");
                    if (parts.length == 2) {
                        String host = parts[0];
                        String port = parts[1];
                        mapping.put(port, host);
                        log.debug("Host mapping: port {} -> host {}", port, host);
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse load URL: " + url, e);
                }
            }
        }
        
        return mapping;
    }
} 