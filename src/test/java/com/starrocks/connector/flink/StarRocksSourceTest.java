package com.starrocks.connector.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class StarRocksSourceTest {

    // public static void main(String[] args) throws Exception {

    //     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //     env.setParallelism(1);
    //     StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    //     tEnv.executeSql(
    //             "CREATE TABLE flink_type_test (" +
    //                 "date_1 DATE," +
    //                 "datetime_1 TIMESTAMP(6),"+
    //                 "char_1 CHAR(20),"+
    //                 "varchar_1 VARCHAR,"+
    //                 "boolean_1 BOOLEAN,"+
    //                 "tinyint_1 TINYINT,"+
    //                 "smallint_1 SMALLINT,"+
    //                 "int_1 INT,"+
    //                 "bigint_1 BIGINT,"+
    //                 "largeint_1 STRING,"+
    //                 "float_1 FLOAT,"+
    //                 "double_1 DOUBLE,"+
    //                 "decimal_1 DECIMAL(27,9) \n"+
    //             ") WITH (\n" +
    //                 "  'connector' = 'starrocks',\n" +
                    
    //                 "  'source.connect.timeout-ms' = '2000',\n" +
    //                 "  'scan.params.batch-size' = '4',\n" +
    //                 "  'scan.params.properties' = '{ \"test_prop\" : \"cccccc\"}',\n" +
    //                 "  'scan.params.limit' = '10',\n" +
    //                 "  'scan.params.keep-alive-min' = '2',\n" +
    //                 "  'scan.params.query-timeout' = '1200',\n" +
    //                 "  'scan.params.mem-limit' = '2048',\n" +
    //                 "  'source.max-retries' = '3',\n" +

    //                 "  'scan-url' = '172.26.92.152:8634,172.26.92.152:8634,172.26.92.152:8634',\n" +
    //                 "  'jdbc-url' = 'jdbc:mysql://172.26.92.152:9632',\n" +
    //                 "  'username' = 'root',\n" +
    //                 "  'password' = '',\n" +                
    //                 "  'database-name' = 'cjs_test',\n" +
    //                 "  'table-name' = 'flink_type_test'\n" +
    //             ")"
    //             );
        
    //     // tEnv.executeSql("SELECT tinyint_1 from flink_type_test where tinyint_1 > 0").print();
    //     // tEnv.executeSql("SELECT tinyint_1, int_1 from flink_type_test where tinyint_1 = 100 and int_1 = -2147483648").print();
    //     // tEnv.executeSql("SELECT tinyint_1, char_1 from flink_type_test where tinyint_1 = 100 and char_1 = 'A'").print();
    //     // tEnv.executeSql("SELECT * from flink_type_test where int_1 < 1 and (char_1 = 'A' or varchar_1 = 'B')").print();
    //     // tEnv.executeSql("SELECT int_1, char_1 from flink_type_test where char_1 = 'A' and int_1 = -2147483648").print();
    //     // tEnv.executeSql("SELECT tinyint_1, char_1 from flink_type_test where char_1 = 'A' limit 1").print();
    //     // tEnv.executeSql("SELECT char_1, int_1 from flink_type_test where char_1 = 'A'").print();
    //     // tEnv.executeSql("SELECT int_1, char_1 from flink_type_test where char_1 = 'A'").print();
    //     // tEnv.executeSql("SELECT int_1, char_1 from flink_type_test where int_1 = -2147483648").print();
    //     // tEnv.executeSql("SELECT * from flink_type_test").print();
    //     // tEnv.executeSql("SELECT * from flink_type_test where tinyint_1 = 100").print();
    //     // tEnv.executeSql("SELECT int_1 from flink_type_test").print();
    //     // tEnv.executeSql("SELECT tinyint_1 as ccc from flink_type_test").print();
    //     tEnv.executeSql("SELECT tinyint_1 from flink_type_test where tinyint_1 = 100").print();
    //     // ---------------------- unsupport ----------------------
    //     // tEnv.executeSql("SELECT count(*) from flink_type_test where tinyint_1 = 100").print();
    //     // tEnv.executeSql("SELECT count(*) from flink_type_test").print();

        
    // }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table duplicate_table_with_null( k1 DATE, k2 TIMESTAMP,k3 char(20),k4 varchar,k5 boolean, v1 tinyint,v2 smallint,v3 int, v4 bigint,v5 string, v6 float,v7 double,v8 decimal(27,9)) with ('connector' = 'starrocks', 'jdbc-url' = 'jdbc:mysql://172.26.92.151:9232' ,'scan-url' = '172.26.92.151:8234', 'username'='root', 'password'='', 'database-name'='test_broker_load1635926631014', 'table-name'='duplicate_table_with_null')");
        tEnv.executeSql("select * from duplicate_table_with_null  where k1<='2020-06-23'").print();
    }
}
