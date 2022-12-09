package com.starrocks.data.load.stream;

import com.alibaba.fastjson.JSON;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.http.StreamLoadEntity;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultStreamLoader implements StreamLoader, Serializable {

    private static final Logger log = LoggerFactory.getLogger(DefaultStreamLoader.class);

    private StreamLoadProperties properties;
    private StreamLoadManager manager;

    private HttpClientBuilder clientBuilder;
    private Header[] defaultHeaders;

    private ExecutorService executorService;

    private boolean enableTransaction = false;

    private volatile long availableHostPos;

    private final AtomicBoolean start = new AtomicBoolean(false);

    protected void enableTransaction() {
        this.enableTransaction = true;
    }

    @Override
    public void start(StreamLoadProperties properties, StreamLoadManager manager) {
        if (start.compareAndSet(false, true)) {
            this.properties = properties;
            this.manager = manager;

            initDefaultHeaders(properties);

            this.clientBuilder  = HttpClients.custom()
                    .setRedirectStrategy(new DefaultRedirectStrategy() {
                        @Override
                        protected boolean isRedirectable(String method) {
                            return true;
                        }
                    });
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                    properties.getIoThreadCount(), properties.getIoThreadCount(), 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                    r -> {
                        Thread thread = new Thread(null, r, "I/O client dispatch - " + UUID.randomUUID());
                        thread.setDaemon(true);
                        thread.setUncaughtExceptionHandler((t, e) -> {
                            log.error("Stream loader " + Thread.currentThread().getName() + " error", e);
                            manager.callback(e);
                        });
                        return thread;
                    });
            threadPoolExecutor.allowCoreThreadTimeOut(true);
            this.executorService = threadPoolExecutor;

            log.info("Default Stream Loader start, properties : {}, defaultHeaders : {}",
                    JSON.toJSONString(properties), JSON.toJSONString(defaultHeaders));
        }
    }

    @Override
    public void close() {
        if (start.compareAndSet(true, false)) {
            executorService.shutdownNow();
            log.warn("Default Stream loader closed");
        }
    }

    @Override
    public boolean begin(TableRegion region) {
        region.setLabel(genLabel(region));
        return true;
    }

    @Override
    public Future<StreamLoadResponse> send(TableRegion region) {
        if (!start.get()) {
            log.warn("Stream load not start");
        }
        if (begin(region)) {
            StreamLoadTableProperties tableProperties = properties.getTableProperties(region.getUniqueKey());
            return executorService.submit(() -> send(tableProperties, region));
        } else {
            region.callback(new StreamLoadFailException("Transaction start failed, db : " + region.getDatabase()));
        }

        return null;
    }

    @Override
    public boolean prepare(StreamLoadSnapshot.Transaction transaction) {
        return true;
    }

    @Override
    public boolean commit(StreamLoadSnapshot.Transaction transaction) {
        return true;
    }

    @Override
    public boolean rollback(StreamLoadSnapshot.Transaction transaction) {
        return true;
    }

    @Override
    public boolean prepare(StreamLoadSnapshot snapshot) {
        boolean succeed = true;
        for (StreamLoadSnapshot.Transaction transaction : snapshot.getTransactions()) {
            boolean prepared = false;
            for (int i = 0; i < 3; i++) {
                try {
                    Thread.sleep(i * 1000);
                } catch (InterruptedException e) {
                    log.warn("prepare interrupted");
                    return false;
                }
                if (prepare(transaction)) {
                    prepared = true;
                    break;
                }
            }
            if (!prepared) {
                succeed = false;
                break;
            }
        }

        return succeed;
    }

    @Override
    public boolean commit(StreamLoadSnapshot snapshot) {
        boolean committed = true;
        for (StreamLoadSnapshot.Transaction transaction : snapshot.getTransactions()) {
            if (transaction.isFinish()) {
                continue;
            }
            for (int i = 0; i < 3; i++) {
                try {
                    Thread.sleep(i * 1000);
                } catch (InterruptedException e) {
                    log.warn("commit interrupted");
                    return false;
                }
                if (commit(transaction)) {
                    transaction.setFinish(true);
                    break;
                }
            }
            if (!transaction.isFinish()) {
                committed = false;
            }
        }

        return committed;
    }

    @Override
    public boolean rollback(StreamLoadSnapshot snapshot) {
        boolean rollback = true;
        for (StreamLoadSnapshot.Transaction transaction : snapshot.getTransactions()) {
            if (transaction.isFinish()) {
                continue;
            }
            for (int i = 0; i < 3; i++) {
                if (rollback(transaction)) {
                    transaction.setFinish(true);
                    break;
                }
                if (!transaction.isFinish()) {
                    rollback = false;
                }
            }
        }
        return rollback;
    }


    protected void initDefaultHeaders(StreamLoadProperties properties) {
        Map<String, String> headers = new HashMap<>(properties.getHeaders());
        if (!headers.containsKey("timeout")) {
            headers.put("timeout", "600");
        }
        headers.put(HttpHeaders.AUTHORIZATION, StreamLoadUtils.getBasicAuthHeader(properties.getUsername(), properties.getPassword()));
        headers.put(HttpHeaders.EXPECT, "100-continue");
        this.defaultHeaders = headers.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);
    }

    protected StreamLoadResponse send(StreamLoadTableProperties tableProperties, TableRegion region) {
        try {
            StreamLoadDataFormat dataFormat = tableProperties.getDataFormat();
            String host = getAvailableHost();
            String sendUrl = getSendUrl(host, region.getDatabase(), region.getTable());
            String label = region.getLabel();

            log.info("Stream loading, label : {}, region : {}", label, region.getUniqueKey());

            HttpPut httpPut = new HttpPut(sendUrl);
            httpPut.setConfig(RequestConfig.custom().setExpectContinueEnabled(true).setRedirectsEnabled(true).build());
            httpPut.setEntity(new StreamLoadEntity(region, dataFormat, region.getEntityMeta()));

            httpPut.setHeaders(defaultHeaders);

            for (Map.Entry<String, String> entry : tableProperties.getProperties().entrySet()) {
                httpPut.removeHeaders(entry.getKey());
                httpPut.addHeader(entry.getKey(), entry.getValue());
            }

            httpPut.addHeader("label", label);

            try (CloseableHttpClient client = clientBuilder.build()) {
                log.info("Stream loading, label : {}, request : {}", label, httpPut);
                long startNanoTime = System.currentTimeMillis();
                CloseableHttpResponse response = client.execute(httpPut);
                String responseBody = EntityUtils.toString(response.getEntity());

                log.info("Stream load completed, label : {}, database : {}, table : {}, body : {}",
                        label, region.getDatabase(), region.getTable(), responseBody);

                StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
                StreamLoadResponse.StreamLoadResponseBody streamLoadBody = JSON.parseObject(responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
                streamLoadResponse.setBody(streamLoadBody);

                String status = streamLoadBody.getStatus();

                if (StreamLoadConstants.RESULT_STATUS_SUCCESS.equals(status)
                        || StreamLoadConstants.RESULT_STATUS_OK.equals(status)
                        || StreamLoadConstants.RESULT_STATUS_TRANSACTION_PUBLISH_TIMEOUT.equals(status)) {
                    streamLoadResponse.setCostNanoTime(System.nanoTime() - startNanoTime);
                    region.complete(streamLoadResponse);
                } else if (StreamLoadConstants.RESULT_STATUS_LABEL_EXISTED.equals(status)) {
                    boolean succeed = checkLabelState(host, region.getDatabase(), label);
                    if (!succeed) {
                        throw new StreamLoadFailException("Stream load failed");
                    }
                } else {
                    throw new StreamLoadFailException(responseBody, streamLoadBody);
                }
                return streamLoadResponse;
            } catch (Exception e) {
                log.error("Stream load failed unknown, label : " + label, e);
                throw e;
            }
        } catch (Exception e) {
            log.error("Stream load failed, thread : " + Thread.currentThread().getName(), e);
            region.callback(e);
        }
        return null;
    }

    protected String getAvailableHost() {
        String[] hosts = properties.getLoadUrls();
        int size = hosts.length;
        long pos = availableHostPos;
        while (pos < pos + size) {
            String host = "http://" + hosts[(int) (pos % size)];
            if (testHttpConnection(host)) {
                pos++;
                availableHostPos = pos;
                return host;
            }
        }

        return null;
    }

    private boolean testHttpConnection(String host) {
        try {
            URL url = new URL(host);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(properties.getConnectTimeout());
            connection.connect();
            connection.disconnect();
            return true;
        } catch (Exception e) {
            log.warn("Failed to connect to address:{}", host, e);
            return false;
        }
    }

    protected boolean checkLabelState(String host, String database, String label) throws Exception {
        int idx = 0;
        for (;;) {
            TimeUnit.SECONDS.sleep(Math.min(++idx, 5));
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                String url = host + "/api/" + database + "/get_load_state?label=" + label;
                HttpGet httpGet = new HttpGet(url);
                httpGet.addHeader("Authorization", StreamLoadUtils.getBasicAuthHeader(properties.getUsername(), properties.getPassword()));
                httpGet.setHeader("Connection", "close");
                try (CloseableHttpResponse response = client.execute(httpGet)) {
                    String entityContent = EntityUtils.toString(response.getEntity());

                    if (response.getStatusLine().getStatusCode() != 200) {
                        throw new StreamLoadFailException("Failed to flush data to StarRocks, Error " +
                                "could not get the final state of label : `" + label + "`, body : " + entityContent);
                    }

                    log.info("Label `{}` check, body : {}", label, entityContent);
                    StreamLoadResponse.StreamLoadResponseBody responseBody =
                            JSON.parseObject(entityContent, StreamLoadResponse.StreamLoadResponseBody.class);
                    String state = responseBody.getState();
                    if (state == null) {
                        log.error("Get label state failed, body : {}", JSON.toJSONString(responseBody));
                        throw new StreamLoadFailException(String.format("Failed to flush data to StarRocks, Error " +
                                "could not get the final state of label[%s]. response[%s]\n", label, entityContent));
                    }
                    switch (state) {
                        case StreamLoadConstants.LABEL_STATE_VISIBLE:
                        case StreamLoadConstants.LABEL_STATE_PREPARED:
                        case StreamLoadConstants.LABEL_STATE_COMMITTED:
                            return true;
                        case StreamLoadConstants.LABEL_STATE_PREPARE:
                            continue;
                        case StreamLoadConstants.LABEL_STATE_ABORTED:
                            return false;
                        case StreamLoadConstants.LABEL_STATE_UNKNOWN:
                        default:
                            throw new StreamLoadFailException(String.format("Failed to flush data to StarRocks, Error " +
                                    "label[%s] state[%s]\n", label, state));
                    }
                }
            }
        }
    }

    protected String getSendUrl(String host, String database, String table) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host + "/api/" + database + "/" + table + "/_stream_load";
    }

    protected String genLabel(TableRegion region) {
        if (properties.getLabelPrefix() != null) {
            return properties.getLabelPrefix() + UUID.randomUUID();
        }
        return UUID.randomUUID().toString();
    }
}
