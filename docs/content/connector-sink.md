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


> **NOTICE**
>
> You can load data into StarRocks tables only as a user who has the INSERT privilege on those StarRocks tables. If you do not have the INSERT privilege, follow the instructions provided in [GRANT](https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/account-management/GRANT) to grant the INSERT privilege to the user that you use to connect to your StarRocks cluster.

## Options

| **Option**                    | **Required** | **Default value** | **Data type** | **Description**                                              |
| ----------------------------- | ------------ | ----------------- | ------------- | ------------------------------------------------------------ |
| connector                     | Yes          | NONE              | STRING        | The connector that you want to use. The value must be starrocks. |
| jdbc-url                      | Yes          | NONE              | STRING        | The URL that is used to query data from StarRocks.           |
| load-url                      | Yes          | NONE              | STRING        | The URL that is used to load all data in a time. Format: fe_ip:http_port;fe_ip:http_port. |
| database-name                 | Yes          | NONE              | STRING        | The name of the StarRocks database into which you want to load data. |
| table-name                    | Yes          | NONE              | STRING        | The name of the table that you want to use to load data into StarRocks. |
| username                      | Yes          | NONE              | STRING        | The username of the account that you want to use to load data into StarRocks. |
| password                      | Yes          | NONE              | STRING        | The password of the preceding account.                       |
| sink.semantic                 | No           | at-least-once     | STRING        | The semantics that is supported by your sink. Valid values: **at-least-once** and **exactly-once**. If you specify the value as exactly-once, `sink.buffer-flush.max-bytes`, `sink.buffer-flush.max-bytes`, and `sink.buffer-flush.interval-ms` are invalid. |
| sink.buffer-flush.max-bytes   | No           | 94371840(90M)     | STRING        | The maximum size of data that can be loaded into StarRocks at a time. Valid values: 64 MB to 10 GB. |
| sink.buffer-flush.max-rows    | No           | 500000            | STRING        | The maximum number of rows that can be loaded into StarRocks at a time. Valid values: 64000 to 5000000. |
| sink.buffer-flush.interval-ms | No           | 300000            | STRING        | The interval at which data is flushed. Valid values: 1000 to 3600000. Unit: ms. |
| sink.max-retries              | No           | 3                 | STRING        | The number of times that the system retries to perform the Stream Load. Valid values: 0 to 10. |
| sink.connect.timeout-ms       | No           | 1000              | STRING        | The period of time after which the stream load times out. Valid values: 100 to 60000. Unit: ms. |
| sink.properties.*             | No           | NONE              | STRING        | The properties of the stream load. The properties include k1, k2, and k3. Since 2.4, the flink-connector-starrocks supports partial updates for Primary Key tables. |


## Data type mapping between Flink and StarRocks

| Flink type | StarRocks type |
|  :-: | :-: |
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INTEGER | INTEGER |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| BINARY | INT |
| CHAR | STRING |
| VARCHAR | STRING |
| STRING | STRING |
| DATE | DATE |
| TIMESTAMP_WITHOUT_TIME_ZONE(N) | DATETIME |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE(N) | DATETIME |
| ARRAY\<T\> | ARRAY\<T\> |
| MAP\<KT,VT\> | JSON STRING |
| ROW\<arg T...\> | JSON STRING |

## Usage notes

When you load data from Apache Flink® into StarRocks, take note of the following points:

- If you specify the exactly-once semantics, the two-phase commit (2PC) protocol must be supported to ensure efficient data loading. StarRocks does not support this protocol. Therefore we need to rely on Apache Flink® to achieve exactly-once. The overall process is as follows:
    1. Save data and its label at each checkpoint that is completed at a specific checkpoint interval.
    2. After data and labels are saved, block the flushing of data cached in the state at the first invoke after each checkpoint is completed.

  If StarRocks unexpectedly exits, the operators for Apache Flink® sink streaming are blocked for a long time and Apache Flink® issues a monitoring alert or shuts down.

- By default, data is loaded in the CSV format. You can set the `sink.properties.row_delimiter` parameter to `\\x02` to specify a row separator and set the `sink.properties.column_separator` parameter to `\\x01` to specify a column separator.

- If data loading pauses, you can increase the memory of the Flink task.

- If the preceding code runs as expected and StarRocks can receive data, but the data loading fails, check whether your machine can access the HTTP port of the backends (BEs) in your StarRocks cluster. If you can successfully ping the HTTP port returned by the execution of the SHOW BACKENDS command in your StarRocks cluster, your machine can access the HTTP port of the BEs in your StarRocks cluster. For example, a machine has a public IP address and a private IP address, the HTTP ports of frontends (FEs) and BEs can be accessed through the public IP address of the FEs and BEs, the IP address that is bounded with your StarRocks cluster is the private IP address, and the value of `loadurl` for the Flink task is the HTTP port of the public IP address of the FEs. The FEs forwards the data loading task to the private IP address of the BEs. In this example, if the machine cannot ping the private IP address of the BEs, the data loading fails.

## Examples

TODO


## Best Practices
TODO