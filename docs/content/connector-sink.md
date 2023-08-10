# Continuously load data from Apache Flink®

StarRocks provides a self-developed connector named Flink Connector for Apache Flink® (Flink connector for short) to help you load data into a StarRocks table by using Flink. The basic principle is to accumulate the data and then load it all at a time into StarRocks through [STREAM LOAD](https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-manipulation/STREAM%20LOAD).
The connector has a higher performance than [flink-connector-jdbc](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/) provided by Apache Flink®.

## Version requirements

| Fpark connector | Flink                    | StarRocks     | Java | Scala     |
|-----------------|--------------------------|---------------| ---- |-----------|
| 1.2.7           | 1.11,1.12,1.13,1.14,1.15 | 2.1 and later | 8    | 2.11,2.12 |

## Obtain Flink connector

You can obtain the Flink connector JAR file in the following ways:

- Directly download the compiled Flink Connector JAR file.
- Add the Flink connector as a dependency in your Maven project and then download the JAR file.
- Compile the source code of the Flink Connector into a JAR file by yourself.

The naming format of the Flink connector JAR file

* Since Flink 1.15, it's `flink-connector-starrocks-${connector_version}_flink-${flink_version}.jar`. For example, if you install Flink 1.15 and you want to use Flink connector 1.2.7, you can use `flink-connector-starrocks-1.2.7_flink-1.15.jar`

* Prior to Flink 1.15, it's `flink-connector-starrocks-${connector_version}_flink-${flink_version}_${scala_version}.jar`. For example, if you install Flink 1.14 and Scala 2.12 in your environment, and you want to use Flink connector 1.2.7, you can use `flink-connector-starrocks-1.2.7_flink-1.14_2.12.jar`.

> **NOTICE**
>
> In general, the latest version of the Flink connector only maintains compatibility with the three most recent versions of Flink.


### Download the compiled Jar file

Directly download the corresponding version of the Flink connector JAR from the [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks).

### Maven Dependency

In your Maven project's `pom.xml` file, add the Flink connector as a dependency according to the following format. Replace `flink_version`, `scala_version`, and `connector_version` with the respective versions.
* Since Flink 1.15
    ```xml
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}</version>
    </dependency>
    ```
* Prior to Flink 1.15
    ```xml
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}_${scala_version}</version>
    </dependency>
    ```

### Compile by yourself

1. Download the [Flink connector package](https://github.com/StarRocks/starrocks-connector-for-apache-flink).
2. Execute the following command to compile the source code of Flink connector into a JAR file. Note that  `flink_version` is replaced with the corresponding Flink version.

      ```bash
      sh build.sh <flink_version>
      ```

   For example, if the Flink version in your environment is 1.15, you need to execute the following command:

      ```bash
      sh build.sh 1.15
      ```

3. Go to the `target/`  directory to find the Flink connector JAR file, such as `flink-connector-starrocks-1.2.7_flink-1.15-SNAPSHOT.jar` , generated upon compilation.

> **NOTE**
>
> The name of Flink connector which is not formally released contains the `SNAPSHOT` suffix.

## Options

| **Option**                        | **Required** | **Default value** | **Description**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|-----------------------------------|--------------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector                         | Yes          | NONE              | The connector that you want to use. The value must be "starrocks".                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| jdbc-url                          | Yes          | NONE              | The address that is used to connect to the MySQL server of the FE. Format: `jdbc:mysql://<fe_host>:<fe_query_port>`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| load-url                          | Yes          | NONE              | The HTTP URL of the FE in your StarRocks cluster. You can specify multiple URLs, which must be separated by a semicolon (;). Format: `<fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| database-name                     | Yes          | NONE              | The name of the StarRocks database into which you want to load data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| table-name                        | Yes          | NONE              | The name of the table that you want to use to load data into StarRocks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| username                          | Yes          | NONE              | The username of the account that you want to use to load data into StarRocks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| password                          | Yes          | NONE              | The password of the preceding account.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| sink.version                      | No           | AUTO              | Supported since 1.2.4. Choose which implementation to use. <ul><li>`V1`: use [Stream Load](https://docs.starrocks.io/en-us/latest/loading/StreamLoad) interface to load data. Connectors before 1.2.4 only support this mode. </li> <li>`V2`: use [Transaction Stream Load](https://docs.starrocks.io/en-us/latest/loading/Stream_Load_transaction_interface) interface to load data. It requires StarRocks to be at least version 2.4. Recommends `V2` because it optimizes the memory usage and provide a more stable exactly-once implementation. </li> <li>`AUTO`: if the version of StarRocks supports transaction stream load, will choose `V2` automatically, otherwise choose `V1` </li></ul> |
| sink.label-prefix                 | No           | NONE              | The label prefix used by Stream Load.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| sink.semantic                     | No           | at-least-once     | The semantics that is supported by your sink. Valid values: **at-least-once** and **exactly-once**.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| sink.buffer-flush.max-bytes       | No           | 94371840(90M)     | The maximum size of data that can be accumulated in memory before being sent to StarRocks at a time. Valid values: 64 MB to 10 GB. Setting this parameter to a larger value can improve loading performance but may increase loading latency.                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| sink.buffer-flush.max-rows        | No           | 500000            | The maximum number of rows that can be accumulated in memory before being sent to StarRocks at a time. Only available for version `V1` when using at-least-once. Valid values: 64000 to 5000000.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| sink.buffer-flush.interval-ms     | No           | 300000            | The interval at which data is flushed. Only available for at-least-once mode. Valid values: 1000 to 3600000. Unit: ms.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| sink.max-retries                  | No           | 3                 | The number of times that the system retries to perform the Stream Load. Only available for sink version `V1` currently. Valid values: 0 to 10.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| sink.connect.timeout-ms           | No           | 1000              | The timeout for establishing HTTP connection. Valid values: 100 to 60000. Unit: ms.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| sink.wait-for-continue.timeout-ms | No           | 10000             | Supported since 1.2.7. The timeout for waiting response of HTTP 100-continue from FE. Valid values: 3000 to 600000. Unit: ms                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| sink.ignore.update-before         | No           | true              | Supported since version 1.2.8. Whether to ignore `UPDATE_BEFORE` records from Flink when loading data to primary key table. If false, will treat the record as a delete operation to StarRocks table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| sink.properties.*                 | No           | NONE              | The parameters that are used to control Stream Load behavior.  For example, the parameter `sink.properties.format` specifies the format used for stream load, such as CSV or JSON. For a list of supported parameters and their descriptions, see [STREAM LOAD](https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-manipulation/STREAM%20LOAD).                                                                                                                                                                                                                                                                                                                                 |
| sink.properties.format            | No           | csv               | The format used for stream load. The connector will transform each batch of data to the format before sending them to StarRocks. Valid values: csv and json.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| sink.properties.row_delimiter     | No           | \n                | The row delimiter for CSV-formatted data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| sink.properties.column_separator  | No           | \t                | The column separator for CSV-formatted data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| sink.properties.max_filter_ratio  | No           | 0                 | The maximum error tolerance of the stream load. It's the maximum percentage of data records that can be filtered out due to inadequate data quality. Valid values: 0 to 1. Default value: 0. See [Stream Load](https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-manipulation/STREAM%20LOAD) for details.                                                                                                                                                                                                                                                                                                                                                                      |
| sink.parallelism                  | No           | NONE              | The parallelism of the connector. Only available for Flink SQL. If not set, Flink planner will decide the parallelism.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |


## Data type mapping between Flink and StarRocks

| Flink data type                   | StarRocks data type   |
|-----------------------------------|-----------------------|
| BOOLEAN                           | BOOLEAN               |
| TINYINT                           | TINYINT               |
| SMALLINT                          | SMALLINT              |
| INTEGER                           | INTEGER               |
| BIGINT                            | BIGINT                |
| FLOAT                             | FLOAT                 |
| DOUBLE                            | DOUBLE                |
| DECIMAL                           | DECIMAL               |
| BINARY                            | INT                   |
| CHAR                              | STRING                |
| VARCHAR                           | STRING                |
| STRING                            | STRING                |
| DATE                              | DATE                  |
| TIMESTAMP_WITHOUT_TIME_ZONE(N)    | DATETIME              |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE(N) | DATETIME              |
| ARRAY\<T\>                        | ARRAY\<T\>            |
| MAP\<KT,VT\>                      | JSON STRING           |
| ROW\<arg T...\>                   | JSON STRING           |

## Usage notes

When you load data from Apache Flink® into StarRocks, take note of the following points:

- Loading data into StarRocks tables needs INSERT privilege. If you do not have the INSERT privilege, follow the instructions provided in [GRANT](https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/account-management/GRANT) to grant the INSERT privilege to the user that you use to connect to your StarRocks cluster.

- If you specify the exactly-once semantics, the two-phase commit (2PC) protocol must be supported to ensure efficient data loading. StarRocks does not support this protocol. Therefore we need to rely on Apache Flink® to achieve exactly-once. The overall process is as follows:
    1. Save data and its label at each checkpoint that is completed at a specific checkpoint interval.
    2. After data and labels are saved, block the flushing of data cached in the state at the first invoke after each checkpoint is completed.

  If StarRocks unexpectedly exits, the operators for Apache Flink® sink streaming are blocked for a long time and Apache Flink® issues a monitoring alert or shuts down.

- By default, data is loaded in the CSV format. You can set the `sink.properties.row_delimiter` parameter to `\\x02` to specify a row separator and set the `sink.properties.column_separator` parameter to `\\x01` to specify a column separator.

- If data loading pauses, you can increase the memory of the Flink task.

- If the preceding code runs as expected and StarRocks can receive data, but the data loading fails, check whether your machine can access the HTTP port of the backends (BEs) in your StarRocks cluster. If you can successfully ping the HTTP port returned by the execution of the SHOW BACKENDS command in your StarRocks cluster, your machine can access the HTTP port of the BEs in your StarRocks cluster. For example, a machine has a public IP address and a private IP address, the HTTP ports of frontends (FEs) and BEs can be accessed through the public IP address of the FEs and BEs, the IP address that is bounded with your StarRocks cluster is the private IP address, and the value of `loadurl` for the Flink task is the HTTP port of the public IP address of the FEs. The FEs forwards the data loading task to the private IP address of the BEs. In this example, if the machine cannot ping the private IP address of the BEs, the data loading fails.

- If you specify the value as exactly-once, `sink.buffer-flush.max-bytes`, `sink.buffer-flush.max-bytes`, and `sink.buffer-flush.interval-ms` are invalid.

## Examples

The following examples show how to use the Flink connector to load data into a StarRocks table with Flink SQL or Flink DataStream.

### Preparations

#### Create a StarRocks table

Create a database `test` and create a Primary Key table `score_board`.

```sql
CREATE DATABASE `test`;

CREATE TABLE `test`.`score_board`
(
    `id` int(11) NOT NULL COMMENT "",
    `name` varchar(65533) NULL DEFAULT "" COMMENT "",
    `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
)
ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`);
```

#### Set up Flink environment

* Download Flink binary [Flink 1.15.2](https://archive.apache.org/dist/flink/flink-1.15.2/flink-1.15.2-bin-scala_2.12.tgz), and unzip it to directory `flink-1.15.2`
* Download [connector 1.2.7](https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/1.2.7_flink-1.15/flink-connector-starrocks-1.2.7_flink-1.15.jar), and put it into the directory `flink-1.15.2/lib`
* Run the following commands to start a Flink cluster
    ```shell
    cd flink-1.15.2
    ./bin/start-cluster.sh
    ```

### Run with Flink SQL

* Run the following command to start a Flink SQL client.
    ```shell
    ./bin/sql-client.sh
    ```
* Create a Flink table `score_board`, and insert values into the table via Flink SQL Client.

    ```sql
    CREATE TABLE `score_board` (
        `id` INT,
        `name` STRING,
        `score` INT,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:11903',
        'load-url' = '127.0.0.1:11901',
        'database-name' = 'test',
        'table-name' = 'score_board',
        'username' = 'root',
        'password' = ''
    );

    INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100);
    ```

### Run with Flink DataStream

There are several ways to implement a Flink DataStream job according to the type of the input records, such as a csv Java `String`, a json Java `String` or a custom Java object.    

* The input records are csv-format `String`. See [LoadCsvRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/main/examples/src/main/java/com/starrocks/connector/flink/examples/datastream/LoadCsvRecords.java) for a complete example.
    ```java
    // Generate csv-format records. Each record has three fields separated by "\t". These
    // fields correspond to the columns `id`, `name`, and `score` in StarRocks table.
    String[] records = new String[]{
            "1\tstarrocks-csv\t100",
            "2\tflink-csv\t100"
    };
    DataStream<String> source = env.fromElements(records);

    // Configure the connector with the required properties, and you also need to add properties
    // "sink.properties.format" and "sink.properties.column_separator" to tell the connector the
    // input records are csv-format, and the separator is "\t". You can also use other separators
    // in the records, but remember to modify the "sink.properties.column_separator" correspondingly
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .withProperty("sink.properties.format", "csv")
            .withProperty("sink.properties.column_separator", "\t")
            .build();
    // Create the sink with the options
    SinkFunction<String> starRockSink = StarRocksSink.sink(options);
    source.addSink(starRockSink);
    ```

* The input records are json-format `String`. See [LoadJsonRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/main/examples/src/main/java/com/starrocks/connector/flink/examples/datastream/LoadJsonRecords.java) for a complete example.
    ```java
    // Generate json-format records. Each record has three fields correspond to
    // the columns `id`, `name`, and `score` in StarRocks table.
    String[] records = new String[]{
            "{\"id\":1, \"name\":\"starrocks-json\", \"score\":100}",
            "{\"id\":2, \"name\":\"flink-json\", \"score\":100}",
    };
    DataStream<String> source = env.fromElements(records);

    // Configure the connector with the required properties, and you also need to add properties
    // "sink.properties.format" and "sink.properties.strip_outer_array" to tell the connector the
    // input records are json-format.
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .withProperty("sink.properties.format", "json")
            .withProperty("sink.properties.strip_outer_array", "true")
            .build();
    // Create the sink with the options
    SinkFunction<String> starRockSink = StarRocksSink.sink(options);
    source.addSink(starRockSink);
    ```

* The input records are custom Java objects. See [LoadCustomJavaRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/main/examples/src/main/java/com/starrocks/connector/flink/examples/datastream/LoadCustomJavaRecords.java) for a complete example.

  * In this example, the input record is a simple POJO `RowData`.

      ```java
      public static class RowData {
              public int id;
              public String name;
              public int score;
    
              public RowData() {}
    
              public RowData(int id, String name, int score) {
                  this.id = id;
                  this.name = name;
                  this.score = score;
              }
          }
      ```

  * The main program is
    ```java
    // Generate records which use RowData as the container.
    RowData[] records = new RowData[]{
            new RowData(1, "starrocks-rowdata", 100),
            new RowData(2, "flink-rowdata", 100),
        };
    DataStream<RowData> source = env.fromElements(records);

    // Configure the connector with the required properties
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .build();

    // connector will use a Java object array (Object[]) to represent a row of
    // StarRocks table, and each element is the value for a column. Need to
    // define the schema of the Object[] which matches that of StarRocks table
    TableSchema schema = TableSchema.builder()
            .field("id", DataTypes.INT().notNull())
            .field("name", DataTypes.STRING())
            .field("score", DataTypes.INT())
            // Must specify the primary key for StarRocks primary key table,
            // and DataTypes.INT().notNull() for `id` must specify notNull()
            .primaryKey("id")
            .build();
    // Transforms the RowData to the Object[] according to the schema
    RowDataTransformer transformer = new RowDataTransformer();
    // Create the sink with schema, options, and transformer
    SinkFunction<RowData> starRockSink = StarRocksSink.sink(schema, options, transformer);
    source.addSink(starRockSink);
    ```

  * The `RowDataTransformer` in the main program is defined as
    ```java
    private static class RowDataTransformer implements StarRocksSinkRowBuilder<RowData> {
    
        /**
         * Set each element of the object array according to the input RowData.
         * The schema of the array matches that of StarRocks table.
         */
        @Override
        public void accept(Object[] internalRow, RowData rowData) {
            internalRow[0] = rowData.id;
            internalRow[1] = rowData.name;
            internalRow[2] = rowData.score;
            // Only need for StarRocks primary key table. Set the last
            // element to tell whether the record is a UPSERT or DELETE.
            internalRow[internalRow.length - 1] = StarRocksSinkOP.UPSERT.ordinal();
        }
    }  
    ```

## Best Practices
TODO