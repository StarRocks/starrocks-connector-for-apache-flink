package com.starrocks.connector.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class StarRocksSourceTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
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
                    "  'connector' = 'starrocks-source',\n" +
                    "  'http-nodes' = '172.26.92.152:8634,172.26.92.152:8634,172.26.92.152:8634',\n" +
                    "  'username' = 'root',\n" +
                    "  'password' = '',\n" +                
                    "  'database-name' = 'cjs_test',\n" +
                    "  'table-name' = 'flink_type_test'\n" +
                ")"
                );
        tEnv.executeSql("SELECT * from flink_type_test").print();
    }
}
