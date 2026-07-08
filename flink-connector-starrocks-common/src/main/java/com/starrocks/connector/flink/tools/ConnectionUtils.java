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

package com.starrocks.connector.flink.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.annotation.Nullable;

/** Utilities for HTTP connection. */
public class ConnectionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionUtils.class);

    /** Select an available host from the list. Each host is like 'ip:port'. */
    @Nullable
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
}
