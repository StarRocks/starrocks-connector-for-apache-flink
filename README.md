# flink-connector-starrocks

## Prerequisites

### Intgerate with your existing project

- Add these to the `pom.xml`:

```xml

AND:

```xml

<dependency>
    <groupId>com.starrocks.connector</groupId>
    <artifactId>flink-connector-starrocks</artifactId>
    <version></version>
</dependency>
    
```

### Start using like

```java

// -------- sink with raw json string stream --------
fromElements(new String[]{
    "{\"score\": \"99\", \"name\": \"stephen\"}",
    "{\"score\": \"100\", \"name\": \"lebron\"}"
}).addSink(
    StarRocksSink.sink(
        // the sink options
        StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", "jdbc:mysql://ip:port,ip:port?xxxxx")
            .withProperty("load-url", "ip:port;ip:port")
            .withProperty("username", "xxx")
            .withProperty("password", "xxx")
            .withProperty("table-name", "xxx")
            .withProperty("database-name", "xxx")
            .withProperty("sink.properties.format", "json")
            .withProperty("sink.properties.strip_outer_array", "true")
            .build()
    )
);


// -------- sink with stream transformation --------
class RowData {
    public int score;
    public String name;
    public RowData(int score, String name) {
        ......
    }
}
fromElements(
    new RowData[]{
        new RowData(99, "stephen"),
        new RowData(100, "lebron")
    }
).addSink(
    StarRocksSink.sink(
        // the table structure
        TableSchema.builder()
            .field("score", DataTypes.INT())
            .field("name", DataTypes.VARCHAR(20))
            .build(),
        // the sink options
        StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", "jdbc:mysql://ip:port,ip:port?xxxxx")
            .withProperty("load-url", "ip:port;ip:port")
            .withProperty("username", "xxx")
            .withProperty("password", "xxx")
            .withProperty("table-name", "xxx")
            .withProperty("database-name", "xxx")
            .withProperty("sink.properties.column_separator", "\\x01")
            .withProperty("sink.properties.row_delimiter", "\\x02")
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
// Needed: Add `com.starrocks.connector.flink.table.StarRocksDynamicTableSinkFactory` to: `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`
tEnv.executeSql(
    "CREATE TABLE USER_RESULT(" +
        "name VARCHAR," +
        "score BIGINT" +
    ") WITH ( " +
        "'connector' = 'starrocks'," +
        "'jdbc-url'='jdbc:mysql://ip:port,ip:port?xxxxx'," +
        "'load-url'='ip:port;ip:port'," +
        "'database-name' = 'xxx'," +
        "'table-name' = 'xxx'," +
        "'username' = 'xxx'," +
        "'password' = 'xxx'," +
        "'sink.buffer-flush.max-rows' = '1000000'," +
        "'sink.buffer-flush.max-bytes' = '300000000'," +
        "'sink.buffer-flush.interval-ms' = '300000'," +
        "'sink.properties.column_separator' = '\\x01'," +
        "'sink.properties.row_delimiter' = '\\x02'," +
        "'sink.max-retries' = '3'" +
        "'sink.properties.*' = 'xxx'" + // stream load properties like `'sink.properties.columns' = 'k1, v1'`
    ")"
);
```

## Using flink-cdc as source

`Note that the SQL in steps 6,7,8 could be auto-generated using the` [starrocks-migrate-tool](http://dorisdb-release.dorisdb.com/dmt.tar.gz?Expires=1988476953&OSSAccessKeyId=LTAI4GFYjbX9e7QmFnAAvkt8&Signature=vpV727KMXTcaYqnjl0SrFadTFIk%3D).

1. [Download Flink](https://flink.apache.org/downloads.html)

2. [Download Flink CDC connector](https://github.com/ververica/flink-cdc-connectors/releases)

3. [Download Flink StarRocks connector](http://dorisdbvisitor:dorisdbvisitor134@nexus.dorisdb.com)

4. Untar flink and put `flink-sql-connector-mysql-cdc-xxx.jar`, `flink-connector-starrocks-xxx.jar` to `flink-xxx/lib/`

5. Execute `flink-xxxx/bin/sql-client.sh embedded`.

6. Create source table:

    ```sql
    CREATE TABLE mysql_src (
        name  VARCHAR,
        score  BIGINT,
        PRIMARY KEY (name) not enforced
    ) WITH (
        'connector' = 'mysql-cdc',
        'hostname'='127.0.0.1',
        'port'='3306',
        'username' = 'username',
        'password' = 'xxx',
        'database-name' = 'xxx',
        'table-name' = 'xxx'
    )
    ```

7. Create sink table:

    ```sql
    CREATE TABLE starrocks_sink (
        name  VARCHAR,
        score  BIGINT,
        PRIMARY KEY (name) not enforced
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url'='jdbc:mysql://fe_ip:query_port,fe_ip:query_port?xxxxx',
        'load-url'='fe_ip:http_port;fe_ip:http_port',
        'database-name' = 'xxx',
        'table-name' = 'xxx',
        'username' = 'xxx',
        'password' = 'xxx',
        'sink.properties.column_separator' = '\x01',
        'sink.properties.row_delimiter' = '\x02'
    )
    ```

8. Execute command to sync mysql data:

    ```sql
    INSERT INTO starrocks_sink select * from mysql_src;
    ```

## Sink Options

| Option | Required | Default | Type | Description |
|  :-:  | :-:  | :-:  | :-:  | :-:  |
| connector | YES | NONE | String |`starrocks`|
| jdbc-url | YES | NONE | String | this will be used to execute queries in starrocks. |
| load-url | YES | NONE | String | `fe_ip:http_port;fe_ip:http_port` separated with `;`, which would be used to do the batch sinking. |
| database-name | YES | NONE | String | starrocks database name |
| table-name | YES | NONE | String | starrocks table name |
| username | YES | NONE | String | starrocks connecting username |
| password | YES | NONE | String | starrocks connecting password |
| sink.semantic | NO | `at-least-once` | String | `at-least-once` or `exactly-once`(`flush at checkpoint only` and options like `sink.buffer-flush.*` won't work either). |
| sink.buffer-flush.max-bytes | NO | 94371840(90M) | String | the max batching size of the serialized data, range: `[64MB, 10GB]`. |
| sink.buffer-flush.max-rows | NO | 500000 | String | the max batching rows, range: `[64,000, 5000,000]`. |
| sink.buffer-flush.interval-ms | NO | 300000 | String | the flushing time interval, range: `[1000ms, 3600000ms]`. |
| sink.max-retries | NO | 1 | String | max retry times of the stream load request, range: `[0, 10]`. |
| sink.connect.timeout-ms | NO | 1000 | String | Timeout in millisecond for connecting to the `load-url`, range: `[100, 60000]`. |
| sink.properties.* | NO | NONE | String | the stream load properties like `'sink.properties.columns' = 'k1, v1'`. |
