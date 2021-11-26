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

package com.starrocks.connector.flink;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;
import com.starrocks.connector.flink.manager.StarRocksQueryVisitor;
import com.starrocks.connector.flink.row.StarRocksDelimiterParser;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.StarRocksSinkSemantic;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchema.Builder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import mockit.Mocked;
import mockit.Expectations;

public abstract class StarRocksSinkBaseTest {

    @Mocked
    protected StarRocksQueryVisitor v;

    protected final int AVAILABLE_QUERY_PORT = 53328;
    protected final String JDBC_URL = "jdbc:mysql://127.0.0.1:53316,127.0.0.1:" + AVAILABLE_QUERY_PORT;
    protected final int AVAILABLE_HTTP_PORT = 29591;
    protected final String LOAD_URL = "127.0.0.1:28591;127.0.0.1:" + AVAILABLE_HTTP_PORT;
    protected final String DATABASE = "test";
    protected final String TABLE = "test_tbl";
    protected final String USERNAME = "root";
    protected final String PASSWORD = "root123";
    protected final StarRocksSinkSemantic SINK_SEMANTIC = StarRocksSinkSemantic.AT_LEAST_ONCE;
    protected final String SINK_MAX_INTERVAL = "2000";
    protected final String SINK_MAX_BYTES = "74002019";
    protected final String SINK_MAX_ROWS = "1002000";
    protected final String SINK_MAX_RETRIES = "2";
    protected final Map<String, String> SINK_PROPS = new HashMap<String, String>(){{
        put("filter-ratio", "0");
    }};
    protected final String SINK_PROPS_FILTER_RATIO = "0";

    protected StarRocksSinkOptions OPTIONS;
    protected StarRocksSinkOptions.Builder OPTIONS_BUILDER;
    protected TableSchema TABLE_SCHEMA;
    protected Map<String, String> STARROCKS_TABLE_META;
    protected String mockResonse = "";
    private ServerSocket serverSocket;

    @Before
    public void initializeOptions() {
        StarRocksSinkOptions.Builder builder = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", JDBC_URL)
            .withProperty("load-url", LOAD_URL)
            .withProperty("database-name", DATABASE)
            .withProperty("table-name", TABLE)
            .withProperty("username", USERNAME)
            .withProperty("password", PASSWORD)
            .withProperty("sink.semantic", SINK_SEMANTIC.getName())
            .withProperty("sink.buffer-flush.interval-ms", SINK_MAX_INTERVAL)
            .withProperty("sink.buffer-flush.max-bytes", SINK_MAX_BYTES)
            .withProperty("sink.buffer-flush.max-rows", SINK_MAX_ROWS)
            .withProperty("sink.max-retries", SINK_MAX_RETRIES)
            .withProperty("sink.connect.timeout-ms", "2000");
        SINK_PROPS.keySet().stream().forEach(k -> builder.withProperty("sink.properties." + k, SINK_PROPS.get(k)));
        OPTIONS = builder.build();
        OPTIONS_BUILDER = builder;
    }

    @Before
    public void initializeTableSchema() {
        TableSchema.Builder builder = createTableSchemaBuilder();
        STARROCKS_TABLE_META = new HashMap<String, String>(){{
            put("k1", "tinyint");
            put("k2", "varchar");
            put("v1", "datetime");
            put("v2", "date");
            put("v3", "decimal");
            put("v4", "smallint");
            put("v5", "char");
        }};
        TABLE_SCHEMA = builder.build();
    }

    protected Builder createTableSchemaBuilder() {
        return TableSchema.builder()
            .field("k1", DataTypes.TINYINT())
            .field("k2", DataTypes.VARCHAR(16))
            .field("v1", DataTypes.TIMESTAMP())
            .field("v2", DataTypes.DATE())
            .field("v3", DataTypes.DECIMAL(10, 2))
            .field("v4", DataTypes.SMALLINT())
            .field("v5", DataTypes.CHAR(2));
    }

    @Before
    public void createHttpServer() throws IOException {
        serverSocket = new ServerSocket(AVAILABLE_HTTP_PORT);
        new Thread(new Runnable(){
            @Override
            public void run() {
                try {
                    while (true) {
                        Socket socket = serverSocket.accept();
                        InputStream in = socket.getInputStream();
                        byte[] b = new byte[in.available()];
                        int len = in.read(b);
                        new String(b, 0, len);
                        Thread.sleep(100);
                        String res = "HTTP/1.1 200 OK\r\n" +
                                    "\r\n" +
                                    mockResonse;
                        OutputStream out = socket.getOutputStream();
                        if (0 == len) {
                            out.write("".getBytes());
                        } else {
                            out.write(res.getBytes());
                        }
                        out.flush();
                        out.close();
                        in.close();
                        socket.close();
                    }
                } catch (Exception e) {}

            }
        }).start();
    }

    @After
    public void stopHttpServer() throws IOException {
        if (serverSocket != null) {
            serverSocket.close();
        }
        serverSocket = null;
    }

    protected void mockTableStructure() {
        new Expectations(){
            {
                v.getTableColumnsMetaData();
                result = STARROCKS_TABLE_META.keySet().stream().map(k -> new HashMap<String, Object>(){{
                    put("COLUMN_NAME", k);
                    put("COLUMN_KEY", "");
                    put("DATA_TYPE", STARROCKS_TABLE_META.get(k).toString());
                }}).collect(Collectors.toList());;
            }
        };
    }

    protected void mockMapResponse(Map<String, Object> resp) {
        mockResonse = JSON.toJSONString(resp);
    }

    protected String mockSuccessResponse() {
        String label = UUID.randomUUID().toString();
        Map<String, Object> r = new HashMap<String, Object>();
        r.put("TxnId", new java.util.Date().getTime());
        r.put("Label", label);
        r.put("Status", "Success");
        r.put("Message", "OK");
        mockMapResponse(r);
        return label;
    }

    protected String mockFailedResponse() {
        String label = UUID.randomUUID().toString();
        Map<String, Object> r = new HashMap<String, Object>();
        r.put("TxnId", new java.util.Date().getTime());
        r.put("Label", label);
        r.put("Status", "Fail");
        r.put("Message", "Failed to do stream loading.");
        mockMapResponse(r);
        return label;
    }

    protected byte[] joinRows(List<byte[]> rows, int totalBytes) throws IOException {
        if (StarRocksSinkOptions.StreamLoadFormat.CSV.equals(OPTIONS.getStreamLoadFormat())) {
            byte[] lineDelimiter = StarRocksDelimiterParser.parse(OPTIONS.getSinkStreamLoadProperties().get("row_delimiter"), "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }

        if (StarRocksSinkOptions.StreamLoadFormat.JSON.equals(OPTIONS.getStreamLoadFormat())) {
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : rows.size() + 1));
            bos.put("[".getBytes(StandardCharsets.UTF_8));
            byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
            boolean isFirstElement = true;
            for (byte[] row : rows) {
                if (!isFirstElement) {
                    bos.put(jsonDelimiter);
                }
                bos.put(row);
                isFirstElement = false;
            }
            bos.put("]".getBytes(StandardCharsets.UTF_8));
            return bos.array();
        }
        throw new RuntimeException("Failed to join rows data, unsupported `format` from stream load properties:");
    }
}
