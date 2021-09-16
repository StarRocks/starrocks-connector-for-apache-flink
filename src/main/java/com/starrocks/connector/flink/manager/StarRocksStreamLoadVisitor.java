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

package com.starrocks.connector.flink.manager;

import java.io.IOException;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.alibaba.fastjson.JSON;
import com.starrocks.connector.flink.row.StarRocksDelimiterParser;
import com.starrocks.connector.flink.row.StarRocksSinkOP;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;

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

 
public class StarRocksStreamLoadVisitor implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksStreamLoadVisitor.class);

    private final StarRocksSinkOptions sinkOptions;
    private final String[] fieldNames;
    private int pos;

    public StarRocksStreamLoadVisitor(StarRocksSinkOptions sinkOptions, String[] fieldNames) {
        this.fieldNames = fieldNames;
        this.sinkOptions = sinkOptions;
    }

    public void doStreamLoad(Tuple3<String, Long, ArrayList<String>> labeledRows) throws IOException {
        String host = getAvailableHost();
        if (null == host) {
            throw new IOException("None of the hosts in `load_url` could be connected.");
        }
        String loadUrl = new StringBuilder(host)
            .append("/api/")
            .append(sinkOptions.getDatabaseName())
            .append("/")
            .append(sinkOptions.getTableName())
            .append("/_stream_load")
            .toString();
        LOG.info(String.format("Start to join batch data: rows[%d] bytes[%d] label[%s].", labeledRows.f2.size(), labeledRows.f1, labeledRows.f0));
        Map<String, Object> loadResult = doHttpPut(loadUrl, labeledRows.f0, joinRows(labeledRows.f2, labeledRows.f1.intValue()));
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new IOException("Unable to flush data to StarRocks: unknown result status, usually caused by authentication or permission related problems.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
        }
        if (loadResult.get(keyStatus).equals("Fail")) {
            LOG.error(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
            throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error response: \n%s\n", JSON.toJSONString(loadResult)), loadResult);
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
            co.setConnectTimeout(sinkOptions.getConnectTimout());
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            LOG.warn("Failed to connect to address:{}", host, e1);
            return false;
        }
    }

    private byte[] joinRows(List<String> rows, int totalBytes) throws IOException {
        if (StarRocksSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat())) {
            byte[] lineDelimiter = StarRocksDelimiterParser.parse(sinkOptions.getSinkStreamLoadProperties().get("row_delimiter"), "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (String row : rows) {
                bos.put(row.getBytes(StandardCharsets.UTF_8));
                bos.put(lineDelimiter);
            }
            return bos.array();
        }
       
        if (StarRocksSinkOptions.StreamLoadFormat.JSON.equals(sinkOptions.getStreamLoadFormat())) {
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : rows.size() + 1));
            bos.put("[".getBytes(StandardCharsets.UTF_8));
            byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
            boolean isFirstElement = true;
            for (String row : rows) {
                if (!isFirstElement) {
                    bos.put(jsonDelimiter);
                }
                bos.put(row.getBytes(StandardCharsets.UTF_8));
                isFirstElement = false;
            }
            bos.put("]".getBytes(StandardCharsets.UTF_8));
            return bos.array();
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
            if (!props.containsKey("columns") && (sinkOptions.supportUpsertDelete() || StarRocksSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat()))) {
                String cols = String.join(",", fieldNames);
                if (cols.length() > 0 && sinkOptions.supportUpsertDelete()) {
                    cols += String.format(",%s", StarRocksSinkOP.COLUMN_KEY);
                }
                httpPut.setHeader("columns", cols);
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
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }

}