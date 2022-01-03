package com.starrocks.connector.flink.it.source;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.junit.After;
import org.junit.Before;

public abstract class StarRocksSourceBaseTest {

    protected TableSchema TABLE_SCHEMA;

    protected TableSchema TABLE_SCHEMA_NOT_NULL;

    protected StarRocksSourceOptions OPTIONS;

    protected int[][] PROJECTION_ARRAY;
    protected SelectColumn[] SELECT_COLUMNS;
    protected int[][] PROJECTION_ARRAY_NULL;

    protected final String DATABASE = "test";
    protected final String TABLE = "test_source";
    protected final String USERNAME = "root";
    protected final String PASSWORD = "root123";

    private ServerSocket serverSocket;
    protected final int AVAILABLE_QUERY_PORT = 53329;
    protected final String JDBC_URL = "jdbc:mysql://127.0.0.1:53329,127.0.0.1:" + AVAILABLE_QUERY_PORT;
    protected final int AVAILABLE_HTTP_PORT = 29592;
    protected final String SCAN_URL = "127.0.0.1:29592,127.0.0.1:" + AVAILABLE_HTTP_PORT;
    protected String mockResonse = "";
    protected String querySQL = "select * from sr";

    @Before
    public void initializeCommon() {

        PROJECTION_ARRAY = new int[2][1];
        PROJECTION_ARRAY[0] = new int[]{2};
        PROJECTION_ARRAY[1] = new int[]{7};

        PROJECTION_ARRAY_NULL = new int[0][0];

        SELECT_COLUMNS = new SelectColumn[]{
            new SelectColumn("char_1", 2),
            new SelectColumn("int_1", 7),
        };
    }

    @Before
    public void initializeOptions() {

        StarRocksSourceOptions options = StarRocksSourceOptions.builder()
                .withProperty("scan-url", SCAN_URL)
                .withProperty("jdbc-url", JDBC_URL)
                .withProperty("username", USERNAME)
                .withProperty("password", PASSWORD)
                .withProperty("table-name", DATABASE)
                .withProperty("database-name", TABLE)
                .build();
        OPTIONS = options;
    }

    @Before
    public void initializeTableSchema() {

        TableSchema tableSchema = TableSchema.builder()
            .field("date_1", DataTypes.DATE())
            .field("datetime_1", DataTypes.TIMESTAMP(6))
            .field("char_1", DataTypes.CHAR(20))
            .field("varchar_1", DataTypes.STRING())
            .field("boolean_1", DataTypes.BOOLEAN())
            .field("tinyint_1", DataTypes.TINYINT())
            .field("smallint_1", DataTypes.SMALLINT())
            .field("int_1", DataTypes.INT())
            .field("bigint_1", DataTypes.BIGINT())
            .field("largeint_1", DataTypes.STRING())
            .field("float_1", DataTypes.FLOAT())
            .field("double_1", DataTypes.DOUBLE())
            .field("decimal_1", DataTypes.DECIMAL(27, 9))
            .build();
        TABLE_SCHEMA = tableSchema;
    }

    @Before
    public void initializeTableSchemaNotNull() {

        TableSchema tableSchema = TableSchema.builder()
            .field("date_1", DataTypes.DATE().notNull())
            .field("datetime_1", DataTypes.TIMESTAMP(6).notNull())
            .field("char_1", DataTypes.CHAR(20).notNull())
            .field("varchar_1", DataTypes.STRING().notNull())
            .field("boolean_1", DataTypes.BOOLEAN().notNull())
            .field("tinyint_1", DataTypes.TINYINT().notNull())
            .field("smallint_1", DataTypes.SMALLINT().notNull())
            .field("int_1", DataTypes.INT().notNull())
            .field("bigint_1", DataTypes.BIGINT().notNull())
            .field("largeint_1", DataTypes.STRING().notNull())
            .field("float_1", DataTypes.FLOAT().notNull())
            .field("double_1", DataTypes.DOUBLE().notNull())
            .field("decimal_1", DataTypes.DECIMAL(27, 9).notNull())
            .build();
        TABLE_SCHEMA_NOT_NULL = tableSchema;
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
                        BufferedReader bd = new BufferedReader(new InputStreamReader(in));
                        String requestHeader;
                        int contentLength = 0;
                        while ((requestHeader = bd.readLine()) != null && !requestHeader.isEmpty()) {
                            if (requestHeader.startsWith("Content-Length")) {
                                int begin = requestHeader.indexOf("Content-Length") + "Content-Length:".length();
                                String postParamsLength = requestHeader.substring(begin).trim();
                                contentLength = Integer.parseInt(postParamsLength);                                
                            }
                        }
                        StringBuffer sb = new StringBuffer();
                        if (contentLength > 0) {
                            for (int i = 0; i < contentLength; i ++) {
                                sb.append((char) bd.read());
                            }
                        }
                        assertEquals("{\"sql\":\""+ querySQL +"\"}", sb.toString());
                        PrintWriter pw = new PrintWriter(socket.getOutputStream());
                        pw.println("HTTP/1.1 200 OK");
                        pw.println("Content-type:application/json");
                        pw.println();
                        pw.println(mockResonse);
                        pw.flush();
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
}