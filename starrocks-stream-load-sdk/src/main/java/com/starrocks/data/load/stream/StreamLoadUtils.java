package com.starrocks.data.load.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
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

public class StreamLoadUtils {

    private static final Logger LOG = LoggerFactory.getLogger(StreamLoadUtils.class);

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

    public static String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
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

            JSONObject bodyJson = JSON.parseObject(responseBody);
            String status = bodyJson.getString("status");
            String msg = bodyJson.getString("msg");

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


    /** Select an available host from the list. Each host is like 'ip:port'. */
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
