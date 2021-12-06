# flink-connector-starrocks

## Prerequisites

### Intgerate into your existing project

```xml
<dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>flink-connector-starrocks</artifactId>
    <!-- for flink-1.11 -->
    <version>x.x.x_flink-1.11</version>
    <!-- for flink-1.12 -->
    <version>x.x.x_flink-1.12</version>
    <!-- for flink-1.13 -->
    <version>x.x.x_flink-1.13</version>
</dependency>
```

Click [HERE](https://search.maven.org/search?q=g:com.starrocks) to get the latest version.

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
            .withProperty("sink.parallelism", "1")
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
            .withProperty("sink.properties.format", "json")
            .withProperty("sink.properties.strip_outer_array", "true")
            .withProperty("sink.parallelism", "1")
            .build(),
        // set the slots with streamRowData
        (slots, streamRowData) -> {
            slots[0] = streamRowData.score;
            slots[1] = streamRowData.name;
        }
    )
);

```

### OR

```java

// create a table with `structure` and `properties`
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
        "'sink.parallelism' = '1'," +
        "'sink.max-retries' = '3'," +
        "'sink.properties.*' = 'xxx'" + // stream load properties like `'sink.properties.columns' = 'k1, v1'`
    ")"
);
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
| sink.parallelism | NO | NULL | String | Specify the parallelism of the sink individually. Remove it if you want to follow the global parallelism settings. |
| sink.connect.timeout-ms | NO | 1000 | String | Timeout in millisecond for connecting to the `load-url`, range: `[100, 60000]`. |
| sink.properties.* | NO | NONE | String | the stream load properties like `'sink.properties.columns' = 'k1, v1'`. |

## Metrics

| Name | Type | Description |
|  :-: | :-:  | :-:  |
| totalFlushBytes | counter | successfully flushed bytes. |
| totalFlushRows | counter | successfully flushed rows. |
| totalFlushSucceededTimes | counter | number of times that the data-batch been successfully flushed. |
| totalFlushFailedTimes | counter | number of times that the flushing been failed. |

### Notes

1. `Flush` action was triggered `at-least-once` when: `cachedRows >= ${sink.buffer-flush.max-rows} || cachedBytes >= ${sink.buffer-flush.max-bytes} || idleTime >= ${sink.buffer-flush.interval-ms}`
2. `sink.buffer-flush.{max-rows|max-bytes|interval-ms}` becomes invalid when it comes with the `exactly-once` semantic.
