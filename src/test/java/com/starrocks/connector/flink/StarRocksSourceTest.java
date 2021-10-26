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
                "CREATE TABLE test_1 (" +
                        "col1 TINYINT," +
                        "event_day STRING " +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'starrocks-source',\n" +
                        "  'http-nodes' = '172.26.92.152:8634,172.26.92.152:8634,172.26.92.152:8634',\n" +
                        "  'be-socket-timeout-ms' = '50000',\n" +
                        "  'be-connect-timeout-ms' = '50000',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '',\n" +
                        "  'database-name' = 'cjs_test',\n" +
                        "  'table-name' = 'test_1'\n" +
                        ")");
        final Table result = tEnv.sqlQuery("SELECT col1 from test_1");
        tEnv.toRetractStream(result, Row.class).print();
        env.execute();
    }
}
