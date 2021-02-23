# flink-connector-doris

## Quick Start

### Intgerate with your existing project:

- Add `com.dorisdb.table.DorisDynamicTableSinkFactory` to:
`src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`

- Add these to your `pom.xml`:

```xml

<repositories>
    <repository>
        <id>dorisdb-maven-releases</id>
        <url>http://username:password@nexus.dorisdb.com/repository/maven-releases/</url>
    </repository>
    <repository>
        <id>dorisdb-maven-snapshots</id>
        <url>http://username:password@nexus.dorisdb.com/repository/maven-snapshots/</url>
    </repository>
</repositories>
    
```

AND:

```xml

<dependency>
    <groupId>com.dorisdb.connector</groupId>
    <artifactId>flink-connector-doris</artifactId>
    <version></version>
</dependency>
    
```


### Use it like below:

```java
// sink with stream transformation
class RowData {
    public int k1;
    public String v1;
    public RowData(int k1, String v1) {
        ......
    }
}
fromElements(
    new RowData[]{
        new RowData(1024, "test1"),
        new RowData(2048, "test2"),
        new RowData(3072, "test3")
    }
)
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

// sink with raw json string stream
fromElements(new String[]{
    "{\"k1\": \"1024\", \"v1\": \"test1\"}",
    "{\"k1\": \"2048\", \"v1\": \"test2\"}",
    "{\"k1\": \"3072\", \"v1\": \"test3\"}"
})
.addSink(
    DorisSink.sink(
        // the sink options
        DorisSinkOptions.builder()
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

```

**OR**

```java

// create a table with `structure` and `properties`
tEnv.executeSql(
    "CREATE TABLE USER_RESULT(" +
        "name VARCHAR," +
        "score BIGINT" +
    ") WITH ( " +
        "'connector' = 'doris'," +
        "'jdbc-url'='jdbc:mysql://ip:port,ip:port?xxxxx'," +
        "'load-url'='ip:port;ip:port'," +
        "'database-name' = 'xxx'," +
        "'table-name' = 'xxx'," +
        "'username' = 'xxx'," +
        "'password' = 'xxx'," +
        "'sink.buffer-flush.max-rows' = '1000000'," +
        "'sink.buffer-flush.max-bytes' = '300000000'," +
        "'sink.buffer-flush.interval-ms' = '300000'," +
        "'sink.max-retries' = '3'" +
        "'sink.properties.*' = '3'" + // stream load properties like `'sink.properties.columns' = 'k1=name, v1=score'`
    ")"
);

```

## Sink Options

| Option | Required | Default | Type | Description |
|  :-:  | :-:  | :-:  | :-:  | :-:  |
| connector | YES | NONE | String |`doris`|
| jdbc-url | YES | NONE | String | this will be used to execute queries in doris. |
| load-url | YES | NONE | String | `fe_ip:http_port;fe_ip:http_port` separated with `;`, which would be used to do the batch sinking. |
| database-name | YES | NONE | String | doris database name |
| table-name | YES | NONE | String | doris table name |
| username | YES | NONE | String | doris connecting username |
| password | YES | NONE | String | doris connecting password |
| sink.semantic | NO | `at-least-once` | String | `at-least-once` or `exactly-once`(`flush at checkpoint only` and options like `sink.buffer-flush.*` won't work either). |
| sink.buffer-flush.max-bytes | NO | 67108864 | String | the max batching size of the serialized data, range: `[64MB, 10GB]`. |
| sink.buffer-flush.max-rows | NO | 64000 | String | the max batching rows, range: `[64,000, 5000,000]`. |
| sink.buffer-flush.interval-ms | NO | 1000 | String | the flushing time interval, range: `[1000ms, 3600000ms]`. |
| sink.max-retries | NO | 1 | String | max retry times of the stream load request, range: `[0, 10]`. |
| sink.properties.* | NO | NONE | String | the stream load properties like `'sink.properties.columns' = 'k1=name, v1=score'`. |
