package com.dorisdb;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.alibaba.fastjson.JSON;
import com.dorisdb.table.DorisSinkOptions;
import com.dorisdb.table.DorisSinkSemantic;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class DorisSinkBaseTest {

	protected final int AVAILABLE_QUERY_PORT = 53317;
	protected final String JDBC_URL = "jdbc:mysql://127.0.0.1:53316,127.0.0.1:" + AVAILABLE_QUERY_PORT;
	protected final int AVAILABLE_HTTP_PORT = 28002;
	protected final String LOAD_URL = "127.0.0.1:28001;127.0.0.1:" + AVAILABLE_HTTP_PORT;
	protected final String DATABASE = "test";
	protected final String TABLE = "test_tbl";
	protected final String USERNAME = "root";
	protected final String PASSWORD = "root123";
	protected final DorisSinkSemantic SINK_SEMANTIC = DorisSinkSemantic.AT_LEAST_ONCE;
	protected final String SINK_MAX_INTERVAL = "2000";
	protected final String SINK_MAX_BYTES = "74002019";
	protected final String SINK_MAX_ROWS = "1002000";
	protected final String SINK_MAX_RETRIES = "2";
	protected final Map<String, String> SINK_PROPS = new HashMap<String, String>(){{
		put("columns", "a=a, b=b");
		put("filter-ratio", "0");
	}};
	protected final String SINK_PROPS_FILTER_RATIO = "0";

	protected DorisSinkOptions OPTIONS;
	protected TableSchema TABLE_SCHEMA;
	protected Map<String, String> DORIS_TABLE_META;
	protected String mockResonse = "";
	private ServerSocket serverSocket;

	@Before
	public void initializeOptiosn() {
		DorisSinkOptions.Builder builder = DorisSinkOptions.builder()
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
			.withProperty("sink.buffer-flush.max-retries", SINK_MAX_RETRIES);
		SINK_PROPS.keySet().stream().forEach(k -> builder.withProperty("sink.properties." + k, SINK_PROPS.get(k)));
		OPTIONS = builder.build();
	}

	@Before
	public void initializeTableSchema() {
		TableSchema.Builder builder = TableSchema.builder()
			.field("k1", DataTypes.TINYINT())
			.field("k2", DataTypes.VARCHAR(16))
			.field("v1", DataTypes.TIMESTAMP())
			.field("v2", DataTypes.DATE())
			.field("v3", DataTypes.DECIMAL(10, 2))
			.field("v4", DataTypes.SMALLINT())
			.field("v5", DataTypes.CHAR(2));
		DORIS_TABLE_META = new HashMap<String, String>(){{
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

	@Test
	public void testSinkCommonProperties() {
	
		OPTIONS.validate();
		assertEquals(JDBC_URL, OPTIONS.getJdbcUrl());
		assertEquals(DATABASE, OPTIONS.getDatabaseName());
		assertEquals(TABLE, OPTIONS.getTableName());
		assertEquals(Long.parseLong(SINK_MAX_INTERVAL), OPTIONS.getSinkMaxFlushInterval());
		assertEquals(Long.parseLong(SINK_MAX_BYTES), OPTIONS.getSinkMaxBytes());
		assertEquals(Long.parseLong(SINK_MAX_RETRIES), OPTIONS.getSinkMaxRetries());
		assertEquals(Long.parseLong(SINK_MAX_ROWS), OPTIONS.getSinkMaxRows());

		assertEquals(LOAD_URL.split(";").length, OPTIONS.getLoadUrlList().size());
		assertEquals(SINK_SEMANTIC, OPTIONS.getSemantic());
		assertEquals(SINK_PROPS.size(), OPTIONS.getSinkStreamLoadProperties().size());
		assertEquals(true, OPTIONS.hasColumnMappingProperty());
	}

	@After
	public void stopHttpServer() throws IOException {
		if (serverSocket != null) {
			serverSocket.close();
		}
		serverSocket = null;
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
}
