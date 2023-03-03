package com.starrocks.data.load.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.starrocks.data.load.stream.StreamLoadConstants.getBeginUrl;
import static com.starrocks.data.load.stream.StreamLoadConstants.getCommitUrl;
import static com.starrocks.data.load.stream.StreamLoadConstants.getPrepareUrl;
import static com.starrocks.data.load.stream.StreamLoadConstants.getRollbackUrl;

public class TransactionStreamLoader extends DefaultStreamLoader {

    private static final Logger log = LoggerFactory.getLogger(TransactionStreamLoader.class);

    private Header[] txHeaders;

    private HttpClientBuilder clientBuilder;

    private StreamLoadManager manager;

    protected void initTxHeaders(StreamLoadProperties properties) {
        Map<String, String> headers = new HashMap<>();
        headers.put(HttpHeaders.AUTHORIZATION, StreamLoadUtils.getBasicAuthHeader(properties.getUsername(), properties.getPassword()));
        this.txHeaders = headers.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);
    }

    @Override
    public void start(StreamLoadProperties properties, StreamLoadManager manager) {
        super.start(properties, manager);
        this.manager = manager;
        enableTransaction();
        initTxHeaders(properties);
        clientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });
    }

    @Override
    public boolean begin(TableRegion region) {
        if (region.getLabel() == null) {
            for (int i = 0; i < 5; i++) {
                region.setLabel(genLabel(region));
                if (doBegin(region)) {
                    return true;
                } else {
                    region.setLabel(null);
                }
            }
            return false;
        }
        return true;
    }

    protected boolean doBegin(TableRegion region) {
        String host = getAvailableHost();
        String beginUrl = getBeginUrl(host);
        String label = region.getLabel();
        log.info("Transaction start, label : {}", label);

        HttpPost httpPost = new HttpPost(beginUrl);
        httpPost.setHeaders(txHeaders);
        httpPost.addHeader("label", label);
        httpPost.addHeader("db", region.getDatabase());
        httpPost.addHeader("table", region.getTable());

        httpPost.setConfig(RequestConfig.custom().setExpectContinueEnabled(true).setRedirectsEnabled(true).build());

        log.info("Transaction start, request : {}", httpPost);

        try (CloseableHttpClient client = clientBuilder.build()) {
            CloseableHttpResponse response = client.execute(httpPost);
            String responseBody = EntityUtils.toString(response.getEntity());
            log.info("Transaction started,  body : {}", responseBody);

            JSONObject bodyJson = JSON.parseObject(responseBody);
            String status = bodyJson.getString("Status");

            if (status == null) {
                String errMsg = "Can't find 'Status' in the response of transaction begin request. " +
                        "Transaction load is supported since StarRocks 2.4, and please make sure your " +
                        "StarRocks version support transaction load first. The response json is '" + responseBody + "'";
                throw new IllegalStateException(errMsg);
            }

            switch (status) {
                case StreamLoadConstants.RESULT_STATUS_OK:
                    return true;
                case StreamLoadConstants.RESULT_STATUS_LABEL_EXISTED:
                    return false;
                default:
                    log.error("Transaction start failed, db : {}, label : {}", region.getDatabase(), label);
                    return false;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean prepare(StreamLoadSnapshot.Transaction transaction) {
        String host = getAvailableHost();
        String prepareUrl = getPrepareUrl(host);

        HttpPost httpPost = new HttpPost(prepareUrl);
        httpPost.setHeaders(txHeaders);
        httpPost.addHeader("label", transaction.getLabel());
        httpPost.addHeader("db", transaction.getDatabase());
        httpPost.addHeader("table", transaction.getTable());

        httpPost.setConfig(RequestConfig.custom().setExpectContinueEnabled(true).setRedirectsEnabled(true).build());


        log.info("Transaction prepare, label : {}, request : {}", transaction.getLabel(), httpPost);

        try (CloseableHttpClient client = clientBuilder.build()) {
            CloseableHttpResponse response = client.execute(httpPost);
            String responseBody = EntityUtils.toString(response.getEntity());
            log.info("Transaction prepared,  body : {}", responseBody);

            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            StreamLoadResponse.StreamLoadResponseBody streamLoadBody = JSON.parseObject(responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
            streamLoadResponse.setBody(streamLoadBody);
            String status = streamLoadBody.getStatus();

            switch (status) {
                case StreamLoadConstants.RESULT_STATUS_OK:
                    manager.callback(streamLoadResponse);
                    return true;
                case StreamLoadConstants.RESULT_STATUS_TRANSACTION_NOT_EXISTED: {
                    // currently this could happen after timeout which is specified in http header,
                    // but as a protection we check the state again
                    String labelState = getLabelState(host, transaction.getDatabase(), transaction.getLabel(),
                            Collections.singleton(StreamLoadConstants.LABEL_STATE_PREPARE));
                    if (!StreamLoadConstants.LABEL_STATE_PREPARED.equals(labelState)) {
                       String errMsg = String.format("Transaction prepare failed because of unexpected state, " +
                                       "label: %s, state: %s", transaction.getLabel(), labelState);
                       log.error(errMsg);
                       throw new StreamLoadFailException(errMsg);
                    } else {
                        return true;
                    }
                }
            }

            String errorLog = getErrorLog(streamLoadBody.getErrorURL());
            log.error("Transaction prepare failed, db: {}, table: {}, label: {}, \nresponseBody: {}\nerrorLog: {}",
                    transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), responseBody, errorLog);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public boolean commit(StreamLoadSnapshot.Transaction transaction) {
        String host = getAvailableHost();
        String commitUrl = getCommitUrl(host);

        HttpPost httpPost = new HttpPost(commitUrl);
        httpPost.setHeaders(txHeaders);
        httpPost.addHeader("label", transaction.getLabel());
        httpPost.addHeader("db", transaction.getDatabase());
        httpPost.addHeader("table", transaction.getTable());

        httpPost.setConfig(RequestConfig.custom().setExpectContinueEnabled(true).setRedirectsEnabled(true).build());


        log.info("Transaction commit, label : {}, request : {}", transaction.getLabel(), httpPost);

        try (CloseableHttpClient client = clientBuilder.build()) {
            CloseableHttpResponse response = client.execute(httpPost);
            String responseBody = EntityUtils.toString(response.getEntity());
            log.info("Transaction committed,  body : {}", responseBody);

            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            StreamLoadResponse.StreamLoadResponseBody streamLoadBody = JSON.parseObject(responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
            streamLoadResponse.setBody(streamLoadBody);
            String status = streamLoadBody.getStatus();

            if (StreamLoadConstants.RESULT_STATUS_OK.equals(status)) {
                manager.callback(streamLoadResponse);
                return true;
            }

            // there are many corner cases that can lead to non-ok status. some of them are
            // 1. TXN_NOT_EXISTS: transaction timeout and the label is cleanup up
            // 2. Failed: the error message can be "has no backend", The case is that FE leader restarts, and after
            //    that commit the transaction repeatedly because flink job continues failover for some reason , but
            //    the transaction actually success, and this commit should be successful
            // To reduce the dependency for the returned status type, always check the label state
            String labelState = getLabelState(host, transaction.getDatabase(), transaction.getLabel(), Collections.emptySet());
            if (StreamLoadConstants.LABEL_STATE_COMMITTED.equals(labelState) || StreamLoadConstants.LABEL_STATE_VISIBLE.equals(labelState)) {
                return true;
            }

            String errorLog = getErrorLog(streamLoadBody.getErrorURL());
            log.error("Transaction commit failed, db: {}, table: {}, label: {}, label state: {}, \nresponseBody: {}\nerrorLog: {}",
                    transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), labelState, responseBody, errorLog);

            String exceptionMsg = String.format("Transaction commit failed, db: %s, table: %s, label: %s, commit response status: %s," +
                   " label state: %s", transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), status, labelState);
            // transaction not exist often happens after transaction timeouts
            if (StreamLoadConstants.RESULT_STATUS_TRANSACTION_NOT_EXISTED.equals(status) ||
                StreamLoadConstants.LABEL_STATE_UNKNOWN.equals(labelState)) {
                exceptionMsg += ". commit response status with TXN_NOT_EXISTS or label state with UNKNOWN often happens when transaction" +
                        " timeouts, and please check StarRocks FE leader's log to confirm it. You can find the transaction id for the label" +
                        " in the FE log first, and search with the transaction id and the keyword 'expired'";
            }
            throw new StreamLoadFailException(exceptionMsg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean rollback(StreamLoadSnapshot.Transaction transaction) {
        String host = getAvailableHost();
        String rollbackUrl = getRollbackUrl(host);
        log.info("Transaction rollback, label : {}", transaction.getLabel());

        HttpPost httpPost = new HttpPost(rollbackUrl);
        httpPost.setHeaders(txHeaders);
        httpPost.addHeader("label", transaction.getLabel());
        httpPost.addHeader("db", transaction.getDatabase());
        httpPost.addHeader("table", transaction.getTable());

        try (CloseableHttpClient client = clientBuilder.build()) {
            CloseableHttpResponse response = client.execute(httpPost);
            String responseBody = EntityUtils.toString(response.getEntity());
            log.info("Transaction rollback,  body : {}", responseBody);

            JSONObject bodyJson = JSON.parseObject(responseBody);
            String status = bodyJson.getString("Status");

            if (StreamLoadConstants.RESULT_STATUS_SUCCESS.equals(status)) {
                return true;
            }
            log.error("Transaction rollback failed, db: {}, table: {}, label : {}",
                    transaction.getDatabase(), transaction.getTable(), transaction.getLabel());
            return false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getSendUrl(String host, String database, String table) {
       return StreamLoadConstants.getSendUrl(host);
    }
}
