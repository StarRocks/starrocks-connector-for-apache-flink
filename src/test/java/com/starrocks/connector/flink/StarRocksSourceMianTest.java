package com.starrocks.connector.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StarRocksSourceMianTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env;
        StreamTableEnvironment tEnv;

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE flink_type_test (" +
                    "date_1 DATE," +
                    "datetime_1 TIMESTAMP(6),"+
                    "char_1 CHAR(20),"+
                    "varchar_1 VARCHAR,"+
                    "boolean_1 BOOLEAN,"+
                    "tinyint_1 TINYINT,"+
                    "smallint_1 SMALLINT,"+
                    "int_1 INT,"+
                    "bigint_1 BIGINT,"+
                    "largeint_1 STRING,"+
                    "float_1 FLOAT,"+
                    "double_1 DOUBLE,"+
                    "decimal_1 DECIMAL(27,9) \n"+
                ") WITH (\n" +
                    "  'connector' = 'starrocks',\n" +
                    
                    "  'scan.connect.timeout-ms' = '2000',\n" +
                    "  'scan.params.batch-rows' = '4',\n" +
                    "  'scan.params.properties' = '{ \"test_prop\" : \"cccccc\"}',\n" +
                    "  'scan.params.limit' = '10',\n" +
                    "  'scan.params.keep-alive-min' = '2',\n" +
                    "  'scan.params.query-timeout' = '1200',\n" +
                    "  'scan.max-retries' = '3',\n" +

                    "  'scan-url' = '172.26.92.152:8634,172.26.92.152:8634,172.26.92.152:8634',\n" +
                    "  'jdbc-url' = 'jdbc:mysql://172.26.92.152:9632',\n" +
                    "  'username' = 'root',\n" +
                    "  'password' = '',\n" +                
                    "  'database-name' = 'cjs_test',\n" +
                    "  'table-name' = 'flink_type_test'\n" +
                ")"
                );

        tEnv.executeSql("SELECT * from flink_type_test where char_1 = 'A'").print();
    }
}
