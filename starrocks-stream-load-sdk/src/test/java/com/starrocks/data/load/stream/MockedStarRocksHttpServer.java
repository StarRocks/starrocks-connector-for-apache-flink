/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Minimal mock of StarRocks FE HTTP APIs for tests.
 *
 * Endpoints:
 * - POST /api/transaction/begin
 * - PUT  /api/transaction/load
 * - POST /api/transaction/prepare
 * - POST /api/transaction/commit
 * - POST /api/transaction/rollback
 * - PUT  /api/{db}/{table}/_stream_load
 * - GET  /api/{db}/get_load_state?label=xxx
 * - GET  /errors/{label}.log
 */
public class MockedStarRocksHttpServer {

    public static class Builder {
        private int port = 0;
        private boolean enforceAuth = false;
        private String username = "";
        private String password = "";

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder enforceAuth(String username, String password) {
            this.enforceAuth = true;
            this.username = Objects.requireNonNull(username, "username");
            this.password = Objects.requireNonNull(password, "password");
            return this;
        }

        public MockedStarRocksHttpServer build() throws IOException {
            return new MockedStarRocksHttpServer(port, enforceAuth, username, password);
        }
    }

    private static class LabelKey {
        final String db;
        final String table;
        final String label;

        LabelKey(String db, String table, String label) {
            this.db = db;
            this.table = table;
            this.label = label;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LabelKey labelKey = (LabelKey) o;
            return Objects.equals(db, labelKey.db) && Objects.equals(table, labelKey.table) && Objects.equals(label, labelKey.label);
        }

        @Override
        public int hashCode() {
            return Objects.hash(db, table, label);
        }
    }

    private static class LabelInfo {
        volatile TransactionStatus state = TransactionStatus.UNKNOWN;
        volatile long txnId;
        volatile long numberTotalRows;
        volatile long numberLoadedRows;
        volatile long numberFilteredRows;
        volatile long numberUnselectedRows;
    }

    public static class ResponseOverride {
        int httpCode = 200;
        public String status; // use values from StreamLoadConstants.RESULT_STATUS_*
        public String existingJobStatus;
        public String message = "";
        public boolean includeErrorURL = false;
        public String errorLogContent;
    }

    private final HttpServer server;
    private final InetSocketAddress bindAddress;
    private final boolean enforceAuth;
    private final String basicAuthHeaderValue;

    private final ConcurrentMap<LabelKey, LabelInfo> labels = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> errorLogs = new ConcurrentHashMap<>();

    // Endpoint specific overrides applying to ALL labels
    private volatile ResponseOverride beginOverride;
    private volatile ResponseOverride txnLoadOverride;
    private volatile ResponseOverride prepareOverride;
    private volatile ResponseOverride commitOverride;
    private volatile ResponseOverride rollbackOverride;
    private volatile ResponseOverride streamLoadOverride;

    private final Random random = new Random(1234);

    private MockedStarRocksHttpServer(int port, boolean enforceAuth, String username, String password) throws IOException {
        this.bindAddress = new InetSocketAddress("127.0.0.1", port);
        this.server = HttpServer.create(this.bindAddress, 0);
        this.enforceAuth = enforceAuth;
        if (enforceAuth) {
            String token = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
            this.basicAuthHeaderValue = "Basic " + token;
        } else {
            this.basicAuthHeaderValue = null;
        }

        // register handlers
        server.createContext("/api/transaction/begin", new BeginHandler());
        server.createContext("/api/transaction/load", new TransactionLoadHandler());
        server.createContext("/api/transaction/prepare", new PrepareHandler());
        server.createContext("/api/transaction/commit", new CommitHandler());
        server.createContext("/api/transaction/rollback", new RollbackHandler());
        server.createContext("/api", new ApiRootHandler()); // for /api/{db}/{table}/_stream_load and /api/{db}/get_load_state
        server.createContext("/errors", new ErrorLogHandler());
    }

    public static Builder builder() {
        return new Builder();
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public String getBaseUrl() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    // Configuration helpers for tests
    public void setBeginOverride(ResponseOverride override) { this.beginOverride = override; }

    public void setTxnLoadOverride(ResponseOverride override) { this.txnLoadOverride = override; }

    public void setPrepareOverride(ResponseOverride override) { this.prepareOverride = override; }

    public void setCommitOverride(ResponseOverride override) { this.commitOverride = override; }

    public void setRollbackOverride(ResponseOverride override) { this.rollbackOverride = override; }

    public void setStreamLoadOverride(ResponseOverride override) { this.streamLoadOverride = override; }

    public void putErrorLog(String label, String content) {
        errorLogs.put(label, content);
    }

    public void setLabelState(String db, String table, String label, TransactionStatus state) {
        LabelKey key = new LabelKey(db, table, label);
        labels.computeIfAbsent(key, k -> new LabelInfo()).state = state;
    }

    private boolean checkAuth(HttpExchange exchange) throws IOException {
        if (!enforceAuth) return true;
        List<String> authHeaders = exchange.getRequestHeaders().get("Authorization");
        if (authHeaders == null || authHeaders.isEmpty() || !authHeaders.get(0).equals(basicAuthHeaderValue)) {
            String body = toJson(mapOf(
                    "Status", StreamLoadConstants.RESULT_STATUS_FAILED,
                    "Message", "Access denied; you need (at least one of) the INSERT privilege(s) for this operation"
            ));
            sendJson(exchange, 401, body);
            return false;
        }
        return true;
    }

    private static Map<String, String> parseQuery(String q) {
        if (q == null || q.isEmpty()) return Collections.emptyMap();
        Map<String, String> map = new HashMap<>();
        for (String p : q.split("&")) {
            int idx = p.indexOf('=');
            if (idx >= 0) {
                String k = urlDecode(p.substring(0, idx));
                String v = urlDecode(p.substring(idx + 1));
                map.put(k, v);
            } else {
                map.put(urlDecode(p), "");
            }
        }
        return map;
    }

    private static String urlDecode(String s) {
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            return s;
        }
    }

    private static String readBody(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody(); ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buf = new byte[8192];
            int r;
            while ((r = in.read(buf)) != -1) {
                bos.write(buf, 0, r);
            }
            return bos.toString(StandardCharsets.UTF_8.name());
        }
    }

    private static void sendJson(HttpExchange exchange, int code, String body) throws IOException {
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "application/json; charset=utf-8");
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static void sendText(HttpExchange exchange, int code, String body) throws IOException {
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "text/plain; charset=utf-8");
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String toJson(Map<String, ?> map) {
        // Simple and safe JSON serializer for flat maps with primitive/String values
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, ?> e : map.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(escapeJson(e.getKey())).append('"').append(':');
            Object v = e.getValue();
            if (v == null) {
                sb.append("null");
            } else if (v instanceof Number || v instanceof Boolean) {
                sb.append(String.valueOf(v));
            } else {
                sb.append('"').append(escapeJson(String.valueOf(v))).append('"');
            }
        }
        sb.append('}');
        return sb.toString();
    }

    private static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        return sb.toString();
    }

    private static Map<String, Object> mapOf(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return m;
    }

    private LabelInfo getOrCreateLabel(LabelKey key) {
        return labels.computeIfAbsent(key, k -> new LabelInfo());
    }

    private class BeginHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!checkAuth(exchange)) return;
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Method Not Allowed")));
                return;
            }
            Headers h = exchange.getRequestHeaders();
            String label = h.getFirst("label");
            String db = h.getFirst("db");
            String table = h.getFirst("table");
            if (label == null || db == null || table == null) {
                sendJson(exchange, 400, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Missing label/db/table")));
                return;
            }

            ResponseOverride override = beginOverride;
            LabelKey key = new LabelKey(db, table, label);
            LabelInfo info = getOrCreateLabel(key);
            info.txnId = Math.abs(random.nextLong());
            info.state = TransactionStatus.PREPARE;

            if (override != null) {
                if (override.includeErrorURL && override.errorLogContent != null) {
                    errorLogs.put(label, override.errorLogContent);
                }
                Map<String, Object> resp = mapOf(
                        "Status", override.status == null ? StreamLoadConstants.RESULT_STATUS_OK : override.status,
                        "TxnId", info.txnId,
                        "Label", label,
                        "Message", override.message
                );
                sendJson(exchange, override.httpCode, toJson(resp));
                return;
            }

            Map<String, Object> resp = mapOf(
                    "Status", StreamLoadConstants.RESULT_STATUS_OK,
                    "TxnId", info.txnId,
                    "Label", label
            );
            sendJson(exchange, 200, toJson(resp));
        }
    }

    private class TransactionLoadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!checkAuth(exchange)) return;
            if (!"PUT".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Method Not Allowed")));
                return;
            }
            Headers h = exchange.getRequestHeaders();
            String label = h.getFirst("label");
            String db = h.getFirst("db");
            String table = h.getFirst("table");
            if (label == null) {
                sendJson(exchange, 400, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Missing label")));
                return;
            }

            // consume body
            readBody(exchange);

            ResponseOverride override = txnLoadOverride;
            if (override != null) {
                if (override.includeErrorURL && override.errorLogContent != null) {
                    errorLogs.put(label, override.errorLogContent);
                }
                Map<String, Object> resp = new HashMap<>();
                resp.put("Status", override.status == null ? StreamLoadConstants.RESULT_STATUS_SUCCESS : override.status);
                if (StreamLoadConstants.RESULT_STATUS_LABEL_EXISTED.equals(override.status)) {
                    resp.put("ExistingJobStatus", override.existingJobStatus == null ? StreamLoadConstants.EXISTING_JOB_STATUS_FINISHED : override.existingJobStatus);
                }
                if (override.includeErrorURL) {
                    resp.put("ErrorURL", getBaseUrl() + "/errors/" + label + ".log");
                }
                resp.put("Label", label);
                sendJson(exchange, override.httpCode, toJson(resp));
                return;
            }

            // default success and mark state to PREPARE
            if (db == null) db = "";
            if (table == null) table = "";
            LabelInfo info = getOrCreateLabel(new LabelKey(db, table, label));
            info.state = TransactionStatus.PREPARE;
            info.numberTotalRows += 0; // noop for now
            info.numberLoadedRows += 0;

            Map<String, Object> resp = mapOf(
                    "Status", StreamLoadConstants.RESULT_STATUS_SUCCESS,
                    "Label", label,
                    "NumberTotalRows", info.numberTotalRows,
                    "NumberLoadedRows", info.numberLoadedRows,
                    "NumberFilteredRows", info.numberFilteredRows,
                    "NumberUnselectedRows", info.numberUnselectedRows
            );
            sendJson(exchange, 200, toJson(resp));
        }
    }

    private class PrepareHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!checkAuth(exchange)) return;
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Method Not Allowed")));
                return;
            }
            Headers h = exchange.getRequestHeaders();
            String label = h.getFirst("label");
            String db = h.getFirst("db");
            String table = h.getFirst("table");
            if (label == null || db == null || table == null) {
                sendJson(exchange, 400, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Missing label/db/table")));
                return;
            }

            ResponseOverride override = prepareOverride;
            if (override != null) {
                if (override.includeErrorURL && override.errorLogContent != null) {
                    errorLogs.put(label, override.errorLogContent);
                }
                if (!StreamLoadConstants.RESULT_STATUS_TRANSACTION_NOT_EXISTED.equals(override.status)) {
                    setLabelState(db, table, label, TransactionStatus.PREPARED);
                }
                Map<String, Object> resp = mapOf(
                        "Status", override.status == null ? StreamLoadConstants.RESULT_STATUS_OK : override.status,
                        "Label", label,
                        "Message", override.message,
                        "ErrorURL", override.includeErrorURL ? (getBaseUrl() + "/errors/" + label + ".log") : null
                );
                sendJson(exchange, override.httpCode, toJson(resp));
                return;
            }

            setLabelState(db, table, label, TransactionStatus.PREPARED);
            Map<String, Object> resp = mapOf(
                    "Status", StreamLoadConstants.RESULT_STATUS_OK,
                    "Label", label
            );
            sendJson(exchange, 200, toJson(resp));
        }
    }

    private class CommitHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!checkAuth(exchange)) return;
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Method Not Allowed")));
                return;
            }
            Headers h = exchange.getRequestHeaders();
            String label = h.getFirst("label");
            String db = h.getFirst("db");
            String table = h.getFirst("table");
            if (label == null || db == null || table == null) {
                sendJson(exchange, 400, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Missing label/db/table")));
                return;
            }

            ResponseOverride override = commitOverride;
            if (override != null) {
                if (override.includeErrorURL && override.errorLogContent != null) {
                    errorLogs.put(label, override.errorLogContent);
                }
                Map<String, Object> resp = mapOf(
                        "Status", override.status == null ? StreamLoadConstants.RESULT_STATUS_OK : override.status,
                        "Label", label,
                        "Message", override.message,
                        "ErrorURL", override.includeErrorURL ? (getBaseUrl() + "/errors/" + label + ".log") : null
                );
                // Even if not OK, let tests pre-set state to COMMITTED/VISIBLE for client-side reconciliation
                sendJson(exchange, override.httpCode, toJson(resp));
                return;
            }

            setLabelState(db, table, label, TransactionStatus.VISIBLE);
            Map<String, Object> resp = mapOf(
                    "Status", StreamLoadConstants.RESULT_STATUS_OK,
                    "Label", label
            );
            sendJson(exchange, 200, toJson(resp));
        }
    }

    private class RollbackHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!checkAuth(exchange)) return;
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Method Not Allowed")));
                return;
            }
            Headers h = exchange.getRequestHeaders();
            String label = h.getFirst("label");
            String db = h.getFirst("db");
            String table = h.getFirst("table");
            if (label == null || db == null || table == null) {
                sendJson(exchange, 400, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Missing label/db/table")));
                return;
            }

            ResponseOverride override = rollbackOverride;
            if (override != null) {
                Map<String, Object> resp = mapOf(
                        "Status", override.status == null ? StreamLoadConstants.RESULT_STATUS_SUCCESS : override.status,
                        "Label", label,
                        "Message", override.message
                );
                sendJson(exchange, override.httpCode, toJson(resp));
                return;
            }

            setLabelState(db, table, label, TransactionStatus.ABORTED);
            Map<String, Object> resp = mapOf("Status", StreamLoadConstants.RESULT_STATUS_SUCCESS, "Label", label);
            sendJson(exchange, 200, toJson(resp));
        }
    }

    private class ApiRootHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!checkAuth(exchange)) return;
            URI uri = exchange.getRequestURI();
            String path = uri.getPath();
            // path starts with /api/
            List<String> segments = split(path);
            if (segments.size() >= 4 && "api".equals(segments.get(0))) {
                String db = segments.get(1);
                if (segments.size() == 3 && "get_load_state".equals(segments.get(2))) {
                    // /api/{db}/get_load_state
                    handleGetLoadState(exchange, db);
                    return;
                }
                if (segments.size() == 4 && "_stream_load".equals(segments.get(3))) {
                    // /api/{db}/{table}/_stream_load
                    handleStreamLoad(exchange, db, segments.get(2));
                    return;
                }
            }
            sendJson(exchange, 404, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Not Found")));
        }

        private List<String> split(String path) {
            String[] arr = path.split("/");
            List<String> list = new ArrayList<>();
            for (String s : arr) if (!s.isEmpty()) list.add(s);
            return list;
        }

        private void handleGetLoadState(HttpExchange exchange, String db) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Method Not Allowed")));
                return;
            }
            Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
            String label = query.get("label");
            if (label == null) {
                sendJson(exchange, 400, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Missing label")));
                return;
            }

            // find by any table for given db+label
            TransactionStatus state = TransactionStatus.UNKNOWN;
            for (Map.Entry<LabelKey, LabelInfo> e : labels.entrySet()) {
                if (db.equals(e.getKey().db) && label.equals(e.getKey().label)) {
                    state = e.getValue().state;
                    break;
                }
            }
            Map<String, Object> resp = mapOf("state", state.name());
            sendJson(exchange, 200, toJson(resp));
        }

        private void handleStreamLoad(HttpExchange exchange, String db, String table) throws IOException {
            if (!"PUT".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Method Not Allowed")));
                return;
            }
            Headers h = exchange.getRequestHeaders();
            String label = h.getFirst("label");
            if (label == null) {
                sendJson(exchange, 400, toJson(mapOf("Status", StreamLoadConstants.RESULT_STATUS_FAILED, "Message", "Missing label")));
                return;
            }

            // consume body
            readBody(exchange);

            ResponseOverride override = streamLoadOverride;
            if (override != null) {
                if (override.includeErrorURL && override.errorLogContent != null) {
                    errorLogs.put(label, override.errorLogContent);
                }
                Map<String, Object> resp = new HashMap<>();
                resp.put("Status", override.status == null ? StreamLoadConstants.RESULT_STATUS_SUCCESS : override.status);
                if (StreamLoadConstants.RESULT_STATUS_LABEL_EXISTED.equals(override.status)) {
                    resp.put("ExistingJobStatus", override.existingJobStatus == null ? StreamLoadConstants.EXISTING_JOB_STATUS_FINISHED : override.existingJobStatus);
                }
                if (override.includeErrorURL) {
                    resp.put("ErrorURL", getBaseUrl() + "/errors/" + label + ".log");
                }
                resp.put("Label", label);
                sendJson(exchange, override.httpCode, toJson(resp));
                return;
            }

            LabelInfo info = getOrCreateLabel(new LabelKey(db, table, label));
            info.state = TransactionStatus.VISIBLE;

            Map<String, Object> resp = mapOf(
                    "Status", StreamLoadConstants.RESULT_STATUS_SUCCESS,
                    "Label", label,
                    "NumberTotalRows", info.numberTotalRows,
                    "NumberLoadedRows", info.numberLoadedRows,
                    "NumberFilteredRows", info.numberFilteredRows,
                    "NumberUnselectedRows", info.numberUnselectedRows
            );
            sendJson(exchange, 200, toJson(resp));
        }
    }

    private class ErrorLogHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendText(exchange, 405, "Method Not Allowed");
                return;
            }
            String path = exchange.getRequestURI().getPath();
            String name = path.substring(path.lastIndexOf('/') + 1);
            String label = name.endsWith(".log") ? name.substring(0, name.length() - 4) : name;
            String content = errorLogs.get(label);
            if (content == null) content = "";
            sendText(exchange, 200, content);
        }
    }

    public static void main(String[] args) throws Exception {
        MockedStarRocksHttpServer server = MockedStarRocksHttpServer.builder()
                .port(0)
                .enforceAuth("root", "password")
                .build();
        server.start();
        String base = server.getBaseUrl();
        System.out.println("Mocked StarRocks FE HTTP server started at " + base);
        // configure overrides if needed, then run client code against base
        server.stop();
    }
}
