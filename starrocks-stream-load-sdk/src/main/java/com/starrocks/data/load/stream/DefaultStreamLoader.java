package com.starrocks.data.load.stream;

import com.alibaba.fastjson.JSON;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.http.StreamLoadEntity;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
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
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultStreamLoader implements StreamLoader, Serializable {

    private static final Logger log = LoggerFactory.getLogger(DefaultStreamLoader.class);

    private static final int ERROR_LOG_MAX_LENGTH = 3000;

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
                    .setRequestExecutor(new HttpRequestExecutor(properties.getWaitForContinueTimeoutMs()))
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

            HttpPut httpPut = new HttpPut(sendUrl);
            httpPut.setConfig(RequestConfig.custom().setExpectContinueEnabled(true).setRedirectsEnabled(true).build());
            httpPut.setEntity(new StreamLoadEntity(region, dataFormat, region.getEntityMeta()));

            httpPut.setHeaders(defaultHeaders);

            for (Map.Entry<String, String> entry : tableProperties.getProperties().entrySet()) {
                httpPut.removeHeaders(entry.getKey());
                httpPut.addHeader(entry.getKey(), entry.getValue());
            }

            httpPut.addHeader("label", label);
            httpPut.addHeader("format", dataFormat.toString());

            log.info("Stream loading, label : {}, region : {}, request : {}", label, region.getUniqueKey(), httpPut);
            try (CloseableHttpClient client = clientBuilder.build()) {
                long startNanoTime = System.nanoTime();
                String responseBody;
                try (CloseableHttpResponse response = client.execute(httpPut)) {
                    responseBody = parseHttpResponse("load", region.getDatabase(), region.getTable(), label, response);
                }

                log.info("Stream load completed, label : {}, database : {}, table : {}, body : {}",
                        label, region.getDatabase(), region.getTable(), responseBody);

                StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
                StreamLoadResponse.StreamLoadResponseBody streamLoadBody = JSON.parseObject(responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
                streamLoadResponse.setBody(streamLoadBody);
                String status = streamLoadBody.getStatus();
                if (status == null) {
                    throw new StreamLoadFailException(String.format("Stream load status is null. db: %s, table: %s, " +
                            "label: %s, response body: %s", region.getDatabase(), region.getTable(), label, responseBody));
                }

                if (StreamLoadConstants.RESULT_STATUS_SUCCESS.equals(status)
                        || StreamLoadConstants.RESULT_STATUS_OK.equals(status)
                        || StreamLoadConstants.RESULT_STATUS_TRANSACTION_PUBLISH_TIMEOUT.equals(status)) {
                    streamLoadResponse.setCostNanoTime(System.nanoTime() - startNanoTime);
                    region.complete(streamLoadResponse);
                } else if (StreamLoadConstants.RESULT_STATUS_LABEL_EXISTED.equals(status)) {
                    String existingJobStatus = streamLoadBody.getExistingJobStatus();
                    if (StreamLoadConstants.EXISTING_JOB_STATUS_FINISHED.equals(existingJobStatus)) {
                        streamLoadResponse.setCostNanoTime(System.nanoTime() - startNanoTime);
                        region.complete(streamLoadResponse);
                    } else {
                        String errorMsage = String.format("Stream load failed because label existed, " +
                                "db: %s, table: %s, label: %s, existingJobStatus: %s", region.getDatabase(), region.getTable(), label, existingJobStatus);
                        throw new StreamLoadFailException(errorMsage);
                    }
                } else {
                    String errorLog = getErrorLog(streamLoadBody.getErrorURL());
                    String errorMsg = String.format("Stream load failed because of error, db: %s, table: %s, label: %s, " +
                                    "\nresponseBody: %s\nerrorLog: %s", region.getDatabase(), region.getTable(), label,
                                    responseBody, errorLog);
                    throw new StreamLoadFailException(errorMsg);
                }
                return streamLoadResponse;
            } catch (StreamLoadFailException e) {
                throw e;
            }  catch (Exception e) {
                String errorMsg = String.format("Stream load failed because of unknown exception, db: %s, table: %s, " +
                        "label: %s", region.getDatabase(), region.getTable(), label);
                throw new StreamLoadFailException(errorMsg, e);
            }
        } catch (Exception e) {
            log.error("Exception happens when sending data, thread: {}", Thread.currentThread().getName(), e);
            region.callback(e);
        }
        return null;
    }

    protected String getAvailableHost() {
        String[] hosts = properties.getLoadUrls();
        int size = hosts.length;
        long pos = availableHostPos;
        long tmp = pos + size;
        while (pos < tmp) {
            String host = hosts[(int) (pos % size)];
            pos++;
            if (testHttpConnection(host)) {
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

    protected String parseHttpResponse(String requestType, String db, String table, String label, CloseableHttpResponse response) throws StreamLoadFailException {
        int code = response.getStatusLine().getStatusCode();
        if (307 == code) {
            String errorMsg = String.format("Request %s failed because http response code is 307 which means 'Temporary Redirect'. " +
                    "This can happen when FE responds the request slowly , you should find the reason first. The reason may be " +
                    "StarRocks FE/Flink GC, network delay, or others. db: %s, table: %s, label: %s, response status line: %s",
                    requestType, db, table, label, response.getStatusLine());
            log.error("{}", errorMsg);
            throw new StreamLoadFailException(errorMsg);
        } else if (200 != code) {
            String errorMsg = String.format("Request %s failed because http response code is not 200. db: %s, table: %s," +
                    "label: %s, response status line: %s", requestType, db, table, label, response.getStatusLine());
            log.error("{}", errorMsg);
            throw new StreamLoadFailException(errorMsg);
        }

        HttpEntity respEntity = response.getEntity();
        if (respEntity == null) {
            String errorMsg = String.format("Request %s failed because response entity is null. db: %s, table: %s," +
                    "label: %s, response status line: %s", requestType, db, table, label, response.getStatusLine());
            log.error("{}", errorMsg);
            throw new StreamLoadFailException(errorMsg);
        }

        try {
            return EntityUtils.toString(respEntity);
        } catch (Exception e) {
            String errorMsg = String.format("Request %s failed because fail to convert response entity to string. " +
                    "db: %s, table: %s, label: %s, response status line: %s, response entity: %s", requestType, db,
                    table, label, response.getStatusLine(), response.getEntity());
            log.error("{}", errorMsg, e);
            throw new StreamLoadFailException(errorMsg, e);
        }
    }

    protected String getLabelState(String host, String database, String table, String label, Set<String> retryStates) throws Exception {
        int totalSleepSecond = 0;
        String lastState = null;
        for (int sleepSecond = 0;;sleepSecond++) {
            if (totalSleepSecond >= 60) {
                log.error("Fail to get expected load state because of timeout, db: {}, table: {}, label: {}, current state {}",
                        database, table, label, lastState);
                throw new StreamLoadFailException(String.format("Could not get expected load state because of timeout, " +
                        "db: %s, table: %s, label: %s", database, table, label));
            }
            TimeUnit.SECONDS.sleep(Math.min(sleepSecond, 5));
            totalSleepSecond += sleepSecond;
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                String url = host + "/api/" + database + "/get_load_state?label=" + label;
                HttpGet httpGet = new HttpGet(url);
                httpGet.addHeader("Authorization", StreamLoadUtils.getBasicAuthHeader(properties.getUsername(), properties.getPassword()));
                httpGet.setHeader("Connection", "close");
                try (CloseableHttpResponse response = client.execute(httpGet)) {
                    int responseStatusCode = response.getStatusLine().getStatusCode();
                    String entityContent = EntityUtils.toString(response.getEntity());
                    log.info("Response for get_load_state, label: {}, response status code: {}, response body : {}",
                            label, responseStatusCode, entityContent);
                    if (responseStatusCode != 200) {
                        throw new StreamLoadFailException(String.format("Could not get load state because of incorrect response status code %s, " +
                                "label: %s, response body: %s", responseStatusCode, label, entityContent));
                    }

                    StreamLoadResponse.StreamLoadResponseBody responseBody =
                            JSON.parseObject(entityContent, StreamLoadResponse.StreamLoadResponseBody.class);
                    String state = responseBody.getState();
                    if (state == null) {
                        log.error("Fail to get load state, label: {}, load information: {}", label, JSON.toJSONString(responseBody));
                        throw new StreamLoadFailException(String.format("Could not get load state because of state is null," +
                                "label: %s, load information: %s", label, entityContent));
                    }

                    lastState = state;
                    if (retryStates.contains(state)) {
                        continue;
                    }

                    return state;
                }
            }
        }
    }

    protected String getErrorLog(String errorUrl) {
        if (errorUrl == null || !errorUrl.startsWith("http")) {
            return null;
        }

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(errorUrl);
            try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                int code = resp.getStatusLine().getStatusCode();
                if (200 != code) {
                    log.warn("Request error log failed with error code: {}, errorUrl: {}", code, errorUrl);
                    return null;
                }

                HttpEntity respEntity = resp.getEntity();
                if (respEntity == null) {
                    log.warn("Request error log failed with null entity, errorUrl: {}", errorUrl);
                    return null;
                }
                String errorLog = EntityUtils.toString(respEntity);
                if (errorLog != null && errorLog.length() > ERROR_LOG_MAX_LENGTH) {
                    errorLog = errorLog.substring(0, ERROR_LOG_MAX_LENGTH);
                }
                return errorLog;
            }
        } catch (Exception e) {
            log.warn("Failed to get error log: {}.", errorUrl, e);
            return String.format("Failed to get error log: %s, exception message: %s", errorUrl, e.getMessage());
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
