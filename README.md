# flink-connector-doris

## Quick Start

### Intgerate with your existing project:

Append this line `com.dorisdb.table.DorisDynamicTableSinkFactory` to the file located at:
`src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`

### Use it like below:

```java

fromElements(TEST_DATA)
  .addSink(
    DorisSink.sink(
      // the table structure
      TableSchema.builder()
        .field("score", DataTypes.INT())
        .field("name", DataTypes.VARCHAR(20))
        .build(),
      // the sink options
      DorisSinkOptions.builder()
        .withProperty("jdbc-url", "jdbc:mysql://ip:port,ip:port?xxxxx")
        .withProperty("load-url", "ip:port;ip:port")
        .withProperty("username", "xxx")
        .withProperty("password", "xxx")
        .withProperty("table-name", "xxx")
        .withProperty("database-name", "xxx")
        .build(),
      // set the slots with streamRowData
      (slots, streamRowData) -> {
        slots[0] = streamRowData.score;
        slots[1] = streamRowData.name;
      }
    )
  );

```

**OR**

```java

    // create a table with `structure` and `properties`
		tEnv.executeSql(
      "CREATE TABLE USER_RESULT(" +
			"name VARCHAR," +
			"score BIGINT" +
			") WITH ( " +
			"'connector' = 'dorisdb'," +
			"'jdbc-url'='jdbc:mysql://ip:port,ip:port?xxxxx'," +
			"'load-url'='ip:port;ip:port'," +
			"'database-name' = 'xxx'," +
			"'table-name' = 'xxx'," +
			"'username' = 'xxx'," +
			"'password' = 'xxx'," +
			"'sink.buffer-flush.max-rows' = '1000000'," +
			"'sink.buffer-flush.max-bytes' = '300000000'," +
			"'sink.buffer-flush.interval-ms' = '300000'," +
			"'sink.buffer-flush.max-retries' = '3'" +
			"'sink.properties.*' = '3'" + // stream load properties like `'sink.properties.columns' = 'k1=name, v1=score'`
			")"
    );

    // insert values into the table created above
    tEnv.executeSql(
      "INSERT INTO USER_RESULT\n" +
      "VALUES ('lebron', 99), ('stephen', 99)"
    ).await();

```

## Sink Options

| Option | Required | Default | Type | Description |
|  ----  | ----  | ----  | ----  | ----  |
| connector | YES | NONE | String |`dorisdb`|
| jdbc-url | YES | NONE | String | jdbc url used to execute queries with doris. |
| load-url | YES | NONE | String | http urls like `fe_ip:http_port;fe_ip:http_port` separated with `;`, used to batch sinking. |
| database-name | YES | NONE | String | doris database name |
| table-name | YES | NONE | String | doris table name |
| username | YES | NONE | String | doris connecting username |
| password | YES | NONE | String | doris connecting password |
| sink.semantic | NO | `at-least-once` | String | `at-least-once` or `exactly-once`(which only takes effect on `checkpoint-interval = sink.buffer-flush.interval-ms`). |
| sink.buffer-flush.max-bytes | NO | 67108864 | String | the max batching size of serialized data, range in `[64MB, 10GB]`. |
| sink.buffer-flush.max-rows | NO | 64000 | String | the max batching rows, range in `[64,000, 5000,000]`. |
| sink.buffer-flush.max-retries | NO | 1 | String | max retry times of the stream load request, range in `[0, 10]`. |
| sink.buffer-flush.interval-ms | NO | 1000 | String | the flushing time interval, range in `[1000ms, 3600000ms]`. |
| sink.properties.* | NO | NONE | String | the stream load properties like `'sink.properties.columns' = 'k1=name, v1=score'`. |