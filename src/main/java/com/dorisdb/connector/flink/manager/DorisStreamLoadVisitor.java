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


import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.dorisdb.connector.flink.table.DorisSinkOptions;
import com.alibaba.fastjson.JSON;

import org.apache.commons.codec.binary.Base64;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void doStreamLoad(Tuple2<String, List<String>> labeledRows) throws IOException {
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
        Map<String, Object> loadResult = doHttpPut(loadUrl, labeledRows.f0, joinRows(labeledRows.f1));
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new IOException("Unable to flush data to doris: unknown result status.");
        }
        if (loadResult.get(keyStatus).equals("Fail")) {
            throw new IOException(
                new StringBuilder("Failed to flush data to doris.").append(loadResult.get("Message").toString()).toString()
            );
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
            co.setConnectTimeout(100);
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
        URL url = null;
        HttpURLConnection httpurlconnection = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Executing stream load to: '%s'", loadUrl));
        }
        try {
            url = new URL(loadUrl);
            httpurlconnection = (HttpURLConnection) url.openConnection();
            httpurlconnection.setConnectTimeout(1000);
            httpurlconnection.setReadTimeout(300000);
            httpurlconnection.setRequestMethod("PUT");
            Map<String, String> props = sinkOptions.getSinkStreamLoadProperties();
            for (Map.Entry<String,String> entry : props.entrySet()) {
                httpurlconnection.setRequestProperty(entry.getKey(), entry.getValue());
            }
            if (!props.containsKey("columns") && DorisSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat())) {
                httpurlconnection.setRequestProperty("columns", String.join(",", fieldNames));
            }
            httpurlconnection.setRequestProperty("Expect", "100-continue");
            httpurlconnection.setRequestProperty("label", label);
            httpurlconnection.setRequestProperty("Authorization", getBasicAuthHeader(sinkOptions.getUsername(), sinkOptions.getPassword()));
            httpurlconnection.setDoInput(true);
            httpurlconnection.setDoOutput(true);
            httpurlconnection.setInstanceFollowRedirects(false);
            httpurlconnection.getOutputStream().write(data);
            httpurlconnection.getOutputStream().flush();
            httpurlconnection.getOutputStream().close();
            int code = httpurlconnection.getResponseCode();

            if(307 == httpurlconnection.getResponseCode()){
                return doHttpPut(httpurlconnection.getHeaderField("Location"), label, data);
            }
            if (200 != code) {
                LOG.warn("Request failed with code:{}", code);
                return null;
            }
            try(DataInputStream in = new DataInputStream(httpurlconnection.getInputStream())) {
                int len = in.available();
                byte[] by = new byte[len];
                in.readFully(by);
                String result = new String(by);
                return (Map<String, Object>)JSON.parse(result);
            }
        } catch (MalformedURLException e) {
            LOG.warn("Unable to parse url:{}", loadUrl.toString(), e);
        } catch (Exception e) {
            throw new IOException("Failed to do stream load with exception.", e);
        } finally {
            url = null;
            if (httpurlconnection != null) {
                httpurlconnection.disconnect();
            }
        }
        return null;
    }
    
    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes());
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }

}
