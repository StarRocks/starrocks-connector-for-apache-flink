# mysql entire database is synchronized to Starrocks in real time

### obtain jar by compile source code

1. Download the [Flink connector source code](https://github.com/StarRocks/starrocks-connector-for-apache-flink).
2. Execute the following command to compile the source code of Flink connector into a JAR file. Note that `flink_version` is replaced with the corresponding Flink version.

      ```bash
      sh build.sh <flink_version>
      ```

   For example, if the Flink version in your environment is 1.15, you need to execute the following command:

      ```bash
      sh build.sh 1.15
      ```

3. Go to the `target/` directory to find the Flink connector JAR file, such as `flink-connector-starrocks-1.2.7_flink-1.15-SNAPSHOT.jar`, generated upon compilation.


## Prerequisites

Flink has been deployed. If Flink has not been deployed, follow these steps to deploy it:

1. Install Java 8 or Java 11 in your operating system to ensure Flink can run properly. You can use the following command to check the version of your Java installation:

   ```SQL
   java -version
   ```

   For example, if the following information is returned, Java 8 has been installed:

   ```SQL
   openjdk version "1.8.0_322"
   OpenJDK Runtime Environment (Temurin)(build 1.8.0_322-b06)
   OpenJDK 64-Bit Server VM (Temurin)(build 25.322-b06, mixed mode)
   ```

2. Download and unzip the [Flink package](https://flink.apache.org/downloads.html) of your choice.

   > **NOTE**
   >
   > We recommend that you use Flink v1.14 or later. The minimum Flink version supported is v1.11.

   ```SQL
   # Download the Flink package.
   wget https://dlcdn.apache.org/flink/flink-1.14.5/flink-1.14.5-bin-scala_2.11.tgz
   # Unzip the Flink package.
   tar -xzf flink-1.14.5-bin-scala_2.11.tgz
   # Go to the Flink directory.
   cd flink-1.14.5
   ```

3. move Flink CDC MySQL Connector and Flink Starrocks Connector to FLINK_HOME lib directory
    ```SQL
   mv flink-connector-starrocks-1.2.9_flink-1.17.jar lib/
   mv flink-sql-connector-mysql-cdc-2.4.2.jar lib/ 
    ```

4. Start your Flink cluster.

   ```SQL
   # Start your Flink cluster.
   ./bin/start-cluster.sh
         
   # When the following information is displayed, your Flink cluster has successfully started:
   Starting cluster.
   Starting standalonesession daemon on host.
   Starting taskexecutor daemon on host.
   ```

## Synchronize data
   ```SQL
    # Start sync
      bin/flink run -Dexecution.checkpointing.interval=2s -Dparallelism.default=1 -c com.starrocks.connector.flink.cdc.StarRocksCdcTools lib/flink-connector-starrocks-1.2.9_flink-1.17-SNAPSHOT.jar mysql-sync-database-starrocks  --database test_cdc \
      --mysql-conf hostname=ip \
      --mysql-conf username=UserName \
      --mysql-conf password=PassWord \
      --mysql-conf port=3306 \
      --mysql-conf database-name=test_cdc \
      --including-tables "bill.*" \
      --sink-conf load-url=ip:port \
      --sink-conf username=UserName1 \
      --sink-conf password= Password1 \
      --sink-conf jdbc-url=jdbc:mysql://ip:9030 \
      --sink-conf sink.label-prefix=superman \
      --table-conf replication_num=1
   ```

## Options

| **Option**                        | **Required** | **Default value** | **Description**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|-----------------------------------|--------------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| --mysql-conf hostname             | Yes          | STRING            | The IP of your MySQL 
| --mysql-conf username             | Yes          | STRING            | The username of your MySQL cluster account. 
| --mysql-conf password             | Yes          | STRING            | The password of your MySQL cluster account.              
| --mysql-conf port                 | Yes          | STRING            | The Port of the MySQL database 
| --mysql-conf database-name        | Yes          | STRING            | The DB name of the MySQL you want to read.            
| --including-tables                | Yes          | STRING            | Sync table Name, eg tableNameA | TableNameB 
| --sink-conf jdbc-url              | Yes          | NONE              | The address that is used to connect to the MySQL server of the FE. You can specify multiple addresses, which must be separated by a comma (,). Format: `jdbc:mysql://<fe_host1>:<fe_query_port1>,<fe_host2>:<fe_query_port2>,<fe_host3>:<fe_query_port3>`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| --sink-conf load-url              | Yes          | NONE              | The address that is used to connect to the HTTP server of the FE. You can specify multiple addresses, which must be separated by a semicolon (;). Format: `<fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |                                                                                                                                                                                                                                       |
| --sink-conf username              | Yes          | NONE              | The username of the account that you want to use to load data into StarRocks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| --sink-conf password              | Yes          | NONE              | The password of the StarRocks                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| --sink-conf sink.label-prefix     | Yes          | No                | stream load label                                                                                                                                                                                                                                                                                                                                                               |
| --table-conf replication_num      | Yes          | 3                 | table property                                                                                                                                                                                                                 |

