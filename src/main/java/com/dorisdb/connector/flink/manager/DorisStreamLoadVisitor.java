/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dorisdb.connector.flink.manager;

import java.io.IOException;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.dorisdb.connector.flink.table.DorisSinkOptions;
import com.alibaba.fastjson.JSON;

import org.apache.commons.codec.binary.Base64;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

 
public class DorisStreamLoadVisitor implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoadVisitor.class);

    private final DorisSinkOptions sinkOptions;
    private final String[] fieldNames;
    private int pos;

    public DorisStreamLoadVisitor(DorisSinkOptions sinkOptions, String[] fieldNames) {
        this.fieldNames = fieldNames;
        this.sinkOptions = sinkOptions;
    }

    public void doStreamLoad(Tuple3<String, Long, ArrayList<String>> labeledRows) throws IOException {
        String host = getAvailableHost();
        if (null == host) {
            throw new IOException("None of the host in `load_url` could be connected.");
        }
        String loadUrl = new StringBuilder(host)
            .append("/api/")
            .append(sinkOptions.getDatabaseName())
            .append("/")
            .append(sinkOptions.getTableName())
            .append("/_stream_load")
            .toString();
        LOG.info(String.format("Start to join batch data: rows[%d] bytes[%d] label[%s].", labeledRows.f2.size(), labeledRows.f1, labeledRows.f0));
        Map<String, Object> loadResult = doHttpPut(loadUrl, labeledRows.f0, joinRows(labeledRows.f2));
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new IOException("Unable to flush data to doris: unknown result status.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
        }
        if (loadResult.get(keyStatus).equals("Fail")) {
            LOG.error(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
            throw new DorisStreamLoadFailedException("Failed to flush data to doris.", loadResult);
        }
    }

    private String getAvailableHost() {
        List<String> hostList = sinkOptions.getLoadUrlList();
        if (pos >= hostList.size()) {
            pos = 0;
        }
        for (; pos < hostList.size(); pos++) {
            String host = new StringBuilder("http://").append(hostList.get(pos)).toString();
            if (tryHttpConnection(host)) {
                return host;
            }
        }
        return null;
    }

    private boolean tryHttpConnection(String host) {
        try {  
            URL url = new URL(host);
            HttpURLConnection co =  (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(1000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            LOG.warn("Failed to connect to address:{}", host, e1);
            return false;
        }
    }

    private byte[] joinRows(List<String> rows) {
        if (DorisSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat())) {
            return String.join("\n", rows).getBytes(StandardCharsets.UTF_8);
        }
        if (DorisSinkOptions.StreamLoadFormat.JSON.equals(sinkOptions.getStreamLoadFormat())) {
            return new StringBuilder("[").append(String.join(",", rows)).append("]").toString().getBytes(StandardCharsets.UTF_8);
        }
        throw new RuntimeException("Failed to join rows data, unsupported `format` from stream load properties:");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> doHttpPut(String loadUrl, String label, byte[] data) throws IOException {
        LOG.info(String.format("Executing stream load to: '%s', size: '%s'", loadUrl, data.length));
        final HttpClientBuilder httpClientBuilder = HttpClients.custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });
        try (CloseableHttpClient httpclient = httpClientBuilder.build()) {
            HttpPut httpPut = new HttpPut(loadUrl);
            Map<String, String> props = sinkOptions.getSinkStreamLoadProperties();
            for (Map.Entry<String,String> entry : props.entrySet()) {
                httpPut.setHeader(entry.getKey(), entry.getValue());
            }
            if (!props.containsKey("columns") && DorisSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat())) {
                httpPut.setHeader("columns", String.join(",", fieldNames));
            }
            httpPut.setHeader("Expect", "100-continue");
            httpPut.setHeader("label", label);
            httpPut.setHeader("Content-Type", "application/x-www-form-urlencoded");
            httpPut.setHeader("Authorization", getBasicAuthHeader(sinkOptions.getUsername(), sinkOptions.getPassword()));
            httpPut.setEntity(new ByteArrayEntity(data));
            httpPut.setConfig(RequestConfig.custom().setRedirectsEnabled(true).build());
            try (CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                int code = resp.getStatusLine().getStatusCode();
                if (200 != code) {
                    LOG.warn("Request failed with code:{}", code);
                    return null;
                }
                HttpEntity respEntity = resp.getEntity();
                if (null == respEntity) {
                    LOG.warn("Request failed with empty response.");
                    return null;
                }
                return (Map<String, Object>)JSON.parse(EntityUtils.toString(respEntity));
            }
        }
    }
    
    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes());
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }

}
