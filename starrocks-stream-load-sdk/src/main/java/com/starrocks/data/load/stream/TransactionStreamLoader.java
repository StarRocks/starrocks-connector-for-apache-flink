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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.data.load.stream.StreamLoadConstants.getBeginUrl;
import static com.starrocks.data.load.stream.StreamLoadConstants.getCommitUrl;
import static com.starrocks.data.load.stream.StreamLoadConstants.getPrepareUrl;
import static com.starrocks.data.load.stream.StreamLoadConstants.getRollbackUrl;
import static com.starrocks.data.load.stream.StreamLoadUtils.getErrorLog;

public class TransactionStreamLoader extends DefaultStreamLoader {

    private static final Logger log = LoggerFactory.getLogger(TransactionStreamLoader.class);

    /**
     * Transaction states that mean "commit still in progress" — the reconciliation after a lost
     * commit response polls (rather than fails) while the label is in one of these, so a commit
     * that is merely slow (not lost) is not prematurely failed.
     */
    private static final Set<String> COMMIT_IN_PROGRESS_STATES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(TransactionStatus.PREPARE.name(), TransactionStatus.PREPARED.name())));

    private final boolean enableAutoCommit;
    private Header[] defaultTxnHeaders;
    private Header[] beginTxnHeader;
    private Header[] preparedTxnHeader;

    private HttpClientBuilder clientBuilder;

    private StreamLoadManager manager;

    public TransactionStreamLoader(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    protected void initTxHeaders(StreamLoadProperties properties) {
        Map<String, String> headers = new HashMap<>();
        headers.put(HttpHeaders.AUTHORIZATION, StreamLoadUtils.getBasicAuthHeader(properties.getUsername(), properties.getPassword()));
        this.defaultTxnHeaders = headers.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);

        Map<String, String> beginHeaders = new HashMap<>(headers);
        String timeout = properties.getHeaders().get("timeout");
        if (timeout == null) {
            beginHeaders.put("timeout", "600");
        } else {
            beginHeaders.put("timeout", timeout);
        }
        String warehouse = properties.getHeaders().get("warehouse");
        if (warehouse != null) {
            beginHeaders.put("warehouse", warehouse);
        }
        this.beginTxnHeader = beginHeaders.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);

        Map<String, String> preparedHeaders = new HashMap<>(headers);
        String preparedTimeout = properties.getHeaders().get("prepared_timeout");
        if (preparedTimeout != null) {
            preparedHeaders.put("prepared_timeout", preparedTimeout);
        } else if (enableAutoCommit) {
            preparedHeaders.put("prepared_timeout", "180");
        }
        this.preparedTxnHeader = preparedHeaders.entrySet().stream()
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
            // Multi-table transaction mode: a region must only ever load under the
            // coordinator's injected shared label. A null label here means the shared
            // label was not yet injected or was cleared concurrently by the manager
            // thread; minting an independent label would open an orphan single-table
            // transaction and split a source transaction across labels (breaking
            // cross-table atomicity). Refuse the load — the caller (streamLoad) releases
            // FLUSHING so the manager reconciles the shared label and re-triggers.
            if (properties.isEnableMultiTableTransaction()) {
                log.warn("Refusing to begin an independent transaction for a multi-table region with a " +
                        "null shared label, db: {}, table: {}; awaiting manager reconcile",
                        region.getDatabase(), region.getTable());
                return false;
            }
            region.setLabel(region.getLabelGenerator().next());
            if (doBegin(region)) {
                return true;
            } else {
                region.setLabel(null);
                return false;
            }
        }
        return true;
    }

    @Override
    protected void onBeginRefused(TableRegion region) {
        // Distinguish the multi-table null-label deferral above from a genuine begin()
        // failure. The two are mutually exclusive: the doBegin() failure path in begin()
        // is only reachable when multi-table transactions are disabled.
        //
        // Failing the region here would be terminal, not a retry: in multi-table mode
        // TransactionTableRegion.fail() only treats TXN_IN_PROCESSING as retryable, so a
        // synthetic "Transaction start failed" reaches manager.callback() and aborts the
        // job — defeating the very race this guard exists to survive. Return quietly
        // instead; send() still yields null, and the caller releases FLUSHING without
        // consuming the chunk so the manager's reconcile pass restores the shared label
        // and re-triggers the load.
        if (properties.isEnableMultiTableTransaction() && region.getLabel() == null) {
            return;
        }
        super.onBeginRefused(region);
    }

    @Override
    public boolean beginTransaction(String label, String database) {
        return doBegin(label, database, null);
    }

    protected boolean doBegin(TableRegion region) {
        return doBegin(region.getLabel(), region.getDatabase(), region.getTable());
    }

    protected boolean doBegin(String label, String database, String table) {
        String host = getAvailableHost();
        String beginUrl = getBeginUrl(host);
        log.info("Transaction start, label : {}", label);

        HttpPost httpPost = new HttpPost(beginUrl);
        httpPost.setHeaders(beginTxnHeader);
        httpPost.addHeader("label", label);
        httpPost.addHeader("db", database);
        if (table != null) {
            httpPost.addHeader("table", table);
        } else {
            httpPost.addHeader("transaction_type", "multi");
        }

        httpPost.setConfig(RequestConfig.custom()
                        .setConnectTimeout(properties.getConnectTimeout())
                        .setSocketTimeout(boundedRpcSocketTimeoutMs(properties))
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());

        log.info("Transaction start, db: {}, table: {}, label: {}, request : {}", database, table, label, httpPost);

        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody = parseHttpResponse("begin transaction", database, table, label, response);
            }
            log.info("Transaction started, db: {}, table: {}, label: {}, body : {}", database, table, label, responseBody);

            JsonNode node = objectMapper.readTree(responseBody);
            JsonNode statusNode = node.get("Status");
            String status = statusNode == null ? null : statusNode.asText();

            if (status == null) {
                String errMsg = String.format("Can't find 'Status' in the response of transaction begin request. " +
                        "Transaction load is supported since StarRocks 2.4, and please make sure your " +
                        "StarRocks version support transaction load first. db: %s, table: %s, label: %s, response: %s",
                        database, table, label, responseBody);
                log.error(errMsg);
                throw new StreamLoadFailException(errMsg);
            }

            if (StreamLoadConstants.RESULT_STATUS_OK.equals(status)) {
                return true;
            }

            String errMsg = String.format("Transaction start failed, db: %s, table: %s, label: %s, responseBody: %s",
                    database, table, label, responseBody);
            throw new StreamLoadFailException(errMsg);
        } catch (StreamLoadFailException se) {
            throw se;
        } catch (Exception e) {
            throw new RuntimeException("Failed to begin transaction, label: " + label
                    + ", db: " + database + ", table: " + table, e);
        }
    }

    @Override
    public boolean prepare(StreamLoadSnapshot.Transaction transaction) {
        String host = getAvailableHost();
        String prepareUrl = getPrepareUrl(host);

        HttpPost httpPost = new HttpPost(prepareUrl);
        httpPost.setHeaders(preparedTxnHeader);
        httpPost.addHeader("label", transaction.getLabel());
        httpPost.addHeader("db", transaction.getDatabase());
        if (transaction.isMultiTable()) {
            httpPost.addHeader("transaction_type", "multi");
        } else {
            httpPost.addHeader("table", transaction.getTable());
        }

        httpPost.setConfig(RequestConfig.custom()
                        .setConnectTimeout(properties.getConnectTimeout())
                        .setSocketTimeout(boundedRpcSocketTimeoutMs(properties))
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());

        log.info("Transaction prepare, label : {}, request : {}", transaction.getLabel(), httpPost);

        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody = parseHttpResponse("prepare transaction", transaction.getDatabase(), transaction.getTable(),
                        transaction.getLabel(), response);
            }
            log.info("Transaction prepared, label : {}, body : {}", transaction.getLabel(), responseBody);

            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            StreamLoadResponse.StreamLoadResponseBody streamLoadBody =
                    objectMapper.readValue(responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
            streamLoadResponse.setBody(streamLoadBody);
            String status = streamLoadBody.getStatus();
            if (status == null) {
                throw new StreamLoadFailException(String.format("Prepare transaction status is null. db: %s, table: %s, " +
                        "label: %s, response body: %s", transaction.getDatabase(), transaction.getTable(), transaction.getLabel(),
                        responseBody));
            }

            switch (status) {
                case StreamLoadConstants.RESULT_STATUS_OK:
                    manager.callback(streamLoadResponse);
                    return true;
                case StreamLoadConstants.RESULT_STATUS_TRANSACTION_NOT_EXISTED: {
                    // currently this could happen after timeout which is specified in http header,
                    // but as a protection we check the state again
                    String labelState = getLabelState(host, transaction.getDatabase(), transaction.getTable(), transaction.getLabel(),
                            Collections.singleton(TransactionStatus.PREPARE.name()));
                    if (!TransactionStatus.PREPARED.isSame(labelState)) {
                       String errMsg = String.format("Transaction prepare failed because of unexpected state, " +
                                       "label: %s, state: %s", transaction.getLabel(), labelState);
                       log.error(errMsg);
                       throw new StreamLoadFailException(errMsg);
                    } else {
                        return true;
                    }
                }
            }

            String errorLog = getErrorLog(streamLoadBody.getErrorURL(), properties.isSanitizeErrorLog());
            String errorMsg = String.format("Transaction prepare failed, db: %s, table: %s, label: %s, " +
                            "\nresponseBody: %s\nerrorLog: %s", transaction.getDatabase(), transaction.getTable(),
                            transaction.getLabel(), responseBody, errorLog);
            log.error(errorMsg);
            throw new StreamLoadFailException(errorMsg);
        } catch (StreamLoadFailException se) {
            throw se;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean commit(StreamLoadSnapshot.Transaction transaction) {
        String host = getAvailableHost();
        String commitUrl = getCommitUrl(host);

        HttpPost httpPost = new HttpPost(commitUrl);
        httpPost.setHeaders(defaultTxnHeaders);
        httpPost.addHeader("label", transaction.getLabel());
        httpPost.addHeader("db", transaction.getDatabase());
        if (transaction.isMultiTable()) {
            httpPost.addHeader("transaction_type", "multi");
        } else {
            httpPost.addHeader("table", transaction.getTable());
        }
        if (properties.getPublishTimeoutMs() > 0) {
            int timeoutSeconds = Math.max(1, properties.getPublishTimeoutMs() / 1000);
            httpPost.addHeader("timeout", String.valueOf(timeoutSeconds));
        }

        httpPost.setConfig(RequestConfig.custom()
                        .setConnectTimeout(properties.getConnectTimeout())
                        .setSocketTimeout(boundedRpcSocketTimeoutMs(properties))
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());

        log.info("Transaction commit, label: {}, request : {}", transaction.getLabel(), httpPost);

        StreamLoadResponse.StreamLoadResponseBody streamLoadBody;
        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody = parseHttpResponse("commit transaction", transaction.getDatabase(), transaction.getTable(),
                        transaction.getLabel(), response);
            }
            log.info("Transaction committed, label: {}, body : {}", transaction.getLabel(), responseBody);
            streamLoadBody = objectMapper.readValue(responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
            if (streamLoadBody == null || streamLoadBody.getStatus() == null) {
                // Syntactically valid JSON that carries no FE status — e.g. `{}` or a bare `null`
                // substituted by an intermediary. That is NOT a decision from the FE, so it belongs
                // with the lost-response cases below rather than on the fail-fast path: the FE may
                // well have committed. Thrown here so it lands in the reconciliation catch.
                throw new StreamLoadFailException(String.format("Commit transaction response carries no status. " +
                        "db: %s, table: %s, label: %s, response body: %s", transaction.getDatabase(),
                        transaction.getTable(), transaction.getLabel(), responseBody));
            }
        } catch (Exception e) {
            // No definitive FE commit status was read: socket timeout, connection reset, a non-200
            // from an intermediary (proxy/LB), or an unparseable/absent body. The transaction may
            // nonetheless have COMMITTED server-side — the FE commits, then the reply is dropped by
            // the LB/network. Reconcile against the real label state before failing; a committed/
            // visible label means the commit actually succeeded and we must NOT fail the job (which
            // would otherwise stall the manager thread up to flushTimeoutMs and then restart-storm).
            return reconcileLostCommit(host, transaction, e);
        }

        // A definitive FE response with a parseable, non-null status was read below this point
        // (a missing status was routed to reconciliation above).
        String status = streamLoadBody.getStatus();
        if (StreamLoadConstants.RESULT_STATUS_OK.equals(status)) {
            StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            streamLoadResponse.setBody(streamLoadBody);
            manager.callback(streamLoadResponse);
            return true;
        }

        // Definitive non-OK FE status. Double-check the label state once (a non-OK response can
        // accompany an actually-committed txn — e.g. FE leader failover, or a repeated commit), but
        // this IS the FE's decision, so do NOT poll in-progress states here (unlike the lost-response
        // path above): a genuine rejection (e.g. "disk full") must fail fast, not wait ~60s.
        // corner cases: TXN_NOT_EXISTS (txn timed out and the label was cleaned up); or a Failed
        // status whose txn actually succeeded (FE leader restart + job failover re-committing).
        String labelState;
        try {
            labelState = getLabelState(host, transaction.getDatabase(), transaction.getTable(),
                    transaction.getLabel(), Collections.emptySet());
        } catch (Exception stateEx) {
            throw new StreamLoadFailException(String.format("Transaction commit failed, db: %s, table: %s, label: %s, " +
                    "commit response status: %s; the label-state re-check also failed: %s", transaction.getDatabase(),
                    transaction.getTable(), transaction.getLabel(), status, stateEx));
        }
        if (TransactionStatus.COMMITTED.isSame(labelState) || TransactionStatus.VISIBLE.isSame(labelState)) {
            return true;
        }

        String errorLog = getErrorLog(streamLoadBody.getErrorURL(), properties.isSanitizeErrorLog());
        log.error("Transaction commit failed, db: {}, table: {}, label: {}, label state: {}, \nerrorLog: {}",
                transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), labelState, errorLog);
        String exceptionMsg = String.format("Transaction commit failed, db: %s, table: %s, label: %s, commit response status: %s," +
                " label state: %s", transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), status, labelState);
        if (StreamLoadConstants.RESULT_STATUS_TRANSACTION_NOT_EXISTED.equals(status) ||
                TransactionStatus.UNKNOWN.isSame(labelState)) {
            exceptionMsg += ". commit response status with TXN_NOT_EXISTS or label state with UNKNOWN often happens when transaction" +
                    " timeouts, and please check StarRocks FE leader's log to confirm it. You can find the transaction id for the label" +
                    " in the FE log first, and search with the transaction id and the keyword 'expired'";
        }
        throw new StreamLoadFailException(exceptionMsg);
    }

    /**
     * Reconciles a commit whose response was lost/errored before a definitive FE status could be
     * read (socket timeout, connection reset, non-200 from an LB/proxy, unparseable body). Polls
     * the real label state — retrying while the commit is still IN PROGRESS ({@code PREPARE}/
     * {@code PREPARED}) so a commit that is merely slow (not lost) is not prematurely failed — and
     * treats {@code COMMITTED}/{@code VISIBLE} as success; otherwise rethrows the original cause.
     *
     * <p>Relies on {@code get_load_state} reporting the true terminal state for a (multi-statement)
     * label. Verified on a real cluster that a committed multi-table label reports {@code VISIBLE}
     * here (the {@code information_schema.loads} / {@code SHOW STREAM LOAD} {@code PREPARING} display
     * lag is a different FE path; {@code get_load_state} reads the transaction state directly).
     *
     * <p>The query is not pinned to the FE that took the commit: the very failure being reconciled
     * can be that FE going away right after it committed, and {@link #getLabelState} performs no
     * host selection of its own. The commit host is tried first (it is the most likely to answer),
     * then every other configured load URL, so a single dead FE cannot turn a durably committed
     * transaction into a job failure. Only a FAILED query moves on to the next host — any FE that
     * answers reports the same transaction state, so the first answer is taken as definitive.
     */
    private boolean reconcileLostCommit(String host, StreamLoadSnapshot.Transaction transaction, Exception cause) {
        Exception lastStateEx = null;
        for (String candidate : reconciliationHosts(host)) {
            String labelState;
            try {
                labelState = getLabelState(candidate, transaction.getDatabase(), transaction.getTable(),
                        transaction.getLabel(), COMMIT_IN_PROGRESS_STATES);
            } catch (Exception stateEx) {
                lastStateEx = stateEx;
                log.warn("Commit response lost for label {}; the label-state query via {} failed, " +
                        "trying the next configured FE if there is one.", transaction.getLabel(), candidate, stateEx);
                continue;
            }
            if (TransactionStatus.COMMITTED.isSame(labelState) || TransactionStatus.VISIBLE.isSame(labelState)) {
                log.warn("Commit response lost for label {} ({}), but the label is already {} server-side; " +
                        "treating the commit as successful.", transaction.getLabel(), cause.toString(), labelState);
                return true;
            }
            log.error("Commit response lost for label {}, and label state is {} (not committed); failing.",
                    transaction.getLabel(), labelState, cause);
            throw new RuntimeException(cause);
        }
        log.error("Commit response lost for label {}, and the label-state re-check failed on every configured FE; " +
                "failing.", transaction.getLabel(), lastStateEx);
        throw new RuntimeException(cause);
    }

    /**
     * The hosts to try for a lost-commit label-state query: the FE that took the commit first, then
     * every other configured load URL, de-duplicated. Falls back to the configured URLs alone when
     * the commit host is unknown (host probing can return {@code null} when every FE failed a probe).
     */
    private List<String> reconciliationHosts(String commitHost) {
        List<String> hosts = new ArrayList<>();
        if (commitHost != null) {
            hosts.add(commitHost);
        }
        String[] loadUrls = properties.getLoadUrls();
        if (loadUrls != null) {
            for (String url : loadUrls) {
                if (url != null && !hosts.contains(url)) {
                    hosts.add(url);
                }
            }
        }
        return hosts;
    }

    @Override
    public boolean rollback(StreamLoadSnapshot.Transaction transaction) {
        String host = getAvailableHost();
        String rollbackUrl = getRollbackUrl(host);
        log.info("Transaction rollback, label : {}", transaction.getLabel());

        HttpPost httpPost = new HttpPost(rollbackUrl);
        httpPost.setHeaders(defaultTxnHeaders);
        httpPost.addHeader("label", transaction.getLabel());
        httpPost.addHeader("db", transaction.getDatabase());
        if (transaction.isMultiTable()) {
            httpPost.addHeader("transaction_type", "multi");
        } else {
            httpPost.addHeader("table", transaction.getTable());
        }

        httpPost.setConfig(RequestConfig.custom()
                        .setConnectTimeout(properties.getConnectTimeout())
                        .build());

        try (CloseableHttpClient client = clientBuilder.build()) {
            String responseBody;
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                responseBody = parseHttpResponse("abort transaction", transaction.getDatabase(), transaction.getTable(),
                        transaction.getLabel(), response);
            }
            log.info("Transaction rollback, label: {}, body : {}", transaction.getLabel(), responseBody);

            JsonNode node = objectMapper.readTree(responseBody);
            JsonNode statusNode = node.get("Status");
            String status = statusNode == null ? null : statusNode.asText();

            if (status == null) {
                String errMsg = String.format("Abort transaction status is null. db: %s, table: %s, label: %s, response: %s",
                        transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), responseBody);
                log.error(errMsg);
                throw new StreamLoadFailException(errMsg);
            }

            if (StreamLoadConstants.RESULT_STATUS_SUCCESS.equals(status) || StreamLoadConstants.RESULT_STATUS_OK.equals(status)) {
                return true;
            }

            JsonNode msgNode = node.get("Message");
            String msg = msgNode == null ? "" : msgNode.asText();
            log.error("Transaction rollback failed, db: {}, table: {}, label : {}, message: {}",
                    transaction.getDatabase(), transaction.getTable(), transaction.getLabel(), msg);
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
