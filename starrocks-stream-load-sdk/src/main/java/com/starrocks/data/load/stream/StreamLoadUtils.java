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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.data.load.stream.auth.Credentials;
import com.starrocks.data.load.stream.auth.CredentialsProvider;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamLoadUtils {

    private static final Logger LOG = LoggerFactory.getLogger(StreamLoadUtils.class);

    private static final int ERROR_LOG_MAX_LENGTH = 3000;

    public static String getTableUniqueKey(String database, String table) {
        return database + "-" + table;
    }

    public static String getStreamLoadUrl(String host, String database, String table) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host +
                "/api/" +
                database +
                "/" +
                table +
                "/_stream_load";
    }

    /**
     * Asks the provider for credentials once and builds the Basic {@code Authorization} header value.
     *
     * @throws IllegalStateException if the provider returns {@code null}
     */
    public static String getBasicAuthHeader(CredentialsProvider credentialsProvider) {
        Credentials credentials = credentialsProvider.getCredentials();
        if (credentials == null) {
            throw new IllegalStateException(
                    "CredentialsProvider " + credentialsProvider.getClass().getName() + " returned null credentials");
        }
        return getBasicAuthHeader(credentials);
    }

    public static String getBasicAuthHeader(Credentials credentials) {
        return getBasicAuthHeader(credentials.getUsername(), credentials.getPassword());
    }

    public static String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    /**
     * Sanitizes error logs by removing sensitive row data and column values while preserving
     * essential debugging information for data validation errors.
     * 
     * @param errorLog the raw error log from StarRocks
     * @return sanitized error log with sensitive data removed
     */
    public static String sanitizeErrorLog(String errorLog) {
        if (errorLog == null || errorLog.trim().isEmpty()) {
            return errorLog;
        }

        // Split by any combination of \r and \n
        String[] lines = errorLog.split("\\r?\\n|\\r");
        StringBuilder sanitized = new StringBuilder();

        for (String line : lines) {
            if (line.trim().isEmpty()) {
                continue;
            }
            
            String sanitizedLine = line;
            
            // First, sanitize column values in all lines
            sanitizedLine = sanitizedLine.replaceAll("Value\\s+''[^']*''", "column value");
            sanitizedLine = sanitizedLine.replaceAll("Value\\s+'[^']*'", "column value");
            sanitizedLine = sanitizedLine.replaceAll("Value\\s+\"[^\"]*\"", "column value");
            
            // Then, if line contains Row:, remove the row data
            if (sanitizedLine.contains("Row:")) {
                // Remove all types of Row data (array, JSON, or any other format)
                sanitizedLine = sanitizedLine.replaceAll("Row:\\s*\\[.*?].*$", "");
                sanitizedLine = sanitizedLine.replaceAll("Row:\\s*\\{.*?}.*$", "");
                sanitizedLine = sanitizedLine.replaceAll("Row:\\s*.*$", "");
            }
            
            if (!sanitizedLine.trim().isEmpty()) {
                sanitized.append(sanitizedLine).append("\n");
            }
        }

        String result = sanitized.toString().trim();
        return result.isEmpty() ? "Data validation errors detected. Row data has been redacted for security." : result;
    }

    public static boolean isStarRocksSupportTransactionLoad(List<String> httpUrls, int connectTimeout, String userName, String password) {
        String host = selectAvailableHttpHost(httpUrls, connectTimeout);
        if (host == null) {
            throw new RuntimeException("Can't find an available host in " + httpUrls);
        }

        String beginUrlStr = StreamLoadConstants.getBeginUrl(host);
        HttpPost httpPost = new HttpPost(beginUrlStr);
        httpPost.addHeader(HttpHeaders.AUTHORIZATION,
                StreamLoadUtils.getBasicAuthHeader(userName, password));
        httpPost.setConfig(RequestConfig.custom().setExpectContinueEnabled(true).setRedirectsEnabled(true).build());
        LOG.debug("Transaction load probe post {}", httpPost);

        HttpClientBuilder clientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });

        try (CloseableHttpClient client = clientBuilder.build()) {
            CloseableHttpResponse response = client.execute(httpPost);
            String responseBody = EntityUtils.toString(response.getEntity());
            LOG.debug("Transaction load probe response {}", responseBody);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(responseBody);
            JsonNode statusNode = node.get("status");
            String status = statusNode == null ? null : statusNode.asText();
            JsonNode msgNode = node.get("msg");
            String msg = msgNode == null ? null : msgNode.asText();

            // If StarRocks does not support transaction load, FE's NotFoundAction#executePost
            // will be called where you can know how the response json is constructed
            if ("FAILED".equals(status) && "Not implemented".equals(msg)) {
                return false;
            }
            return true;
        } catch (IOException e) {
            String errMsg = "Failed to probe transaction load for " + host;
            LOG.warn("{}", errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }


    /**
     * Select an available host from the list. Each host is like 'ip:port'.
     *
     * @param hostList the list of hosts to select from
     * @param connectionTimeout the connection timeout in milliseconds
     * @return an available host, or null if no available host is found
     */
    public static String selectAvailableHttpHost(List<String> hostList, int connectionTimeout) {
        for (String host : hostList) {
            if (host == null) {
                continue;
            }
            if (!host.startsWith("http")) {
                host = "http://" + host;
            }
            if (testHttpConnection(host, connectionTimeout)) {
                return host;
            }
        }

        return null;
    }

    public static boolean testHttpConnection(String urlStr, int connectionTimeout) {
        try {
            URL url = new URL(urlStr);
            HttpURLConnection co =  (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(connectionTimeout);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to connect to {}", urlStr, e);
            return false;
        }
    }

    public static String getErrorLog(String errorUrl, boolean sanitizeErrorLog) {
        if (errorUrl == null || !errorUrl.startsWith("http")) {
            return null;
        }

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(errorUrl);
            try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                int code = resp.getStatusLine().getStatusCode();
                if (200 != code) {
                    LOG.warn("Request error log failed with error code: {}, errorUrl: {}", code, errorUrl);
                    return null;
                }

                HttpEntity respEntity = resp.getEntity();
                if (respEntity == null) {
                    LOG.warn("Request error log failed with null entity, errorUrl: {}", errorUrl);
                    return null;
                }
                String errorLog = EntityUtils.toString(respEntity);
                if (errorLog != null && errorLog.length() > ERROR_LOG_MAX_LENGTH) {
                    errorLog = errorLog.substring(0, ERROR_LOG_MAX_LENGTH);
                }
                return sanitizeErrorLog ? StreamLoadUtils.sanitizeErrorLog(errorLog) : errorLog;
            }
        } catch (Exception e) {
            LOG.warn("Failed to get error log: {}.", errorUrl, e);
            return String.format("Failed to get error log: %s, exception message: %s", errorUrl, e.getMessage());
        }
    }

    // Error log url pattern in transaction aborted reason. This is only for merge commit
    private static final Pattern ERROR_LOG_URL_PATTERN = Pattern.compile(
            "(?:Tracking URL:|The tracking url:)\\s+" +
                    "(https?://\\S*/api/_load_error_log\\?file=error_log_[0-9a-fA-F]+_[0-9a-fA-F]+)",
            Pattern.CASE_INSENSITIVE
    );

    public static Optional<String> tryGetErrorLogUrlFromTxnAbortReason(String abortReason) {
        if (abortReason == null) {
            return Optional.empty();
        }
        Matcher matcher = ERROR_LOG_URL_PATTERN.matcher(abortReason);
        return matcher.find() ? Optional.ofNullable(matcher.group(1)) : Optional.empty();
    }

    public static Optional<String> tryGetErrorLogForMergeCommit(String txnAbortReason, boolean sanitizeErrorLog) {
        Optional<String> errorLogUrl = tryGetErrorLogUrlFromTxnAbortReason(txnAbortReason);
        return errorLogUrl.map(s -> getErrorLog(s, sanitizeErrorLog));
    }
}
