package com.starrocks.connector.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

public class StarRocksSourceTest {

    StreamExecutionEnvironment env;
    StreamTableEnvironment tEnv;
    

    @Before
    public void createTable() {

        String scanUrl = "172.26.92.152:8634";
        String jdbcUrl = "172.26.92.152:9632";
        String tableName = "flink_type_test";
        String dbName = "flink_source";
        String username = "root";
        String pwd = "";

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
                    
                    "  'source.connect.timeout-ms' = '2000',\n" +
                    "  'scan.params.batch-size' = '4',\n" +
                    "  'scan.params.properties' = '{ \"test_prop\" : \"cccccc\"}',\n" +
                    "  'scan.params.limit' = '10',\n" +
                    "  'scan.params.keep-alive-min' = '2',\n" +
                    "  'scan.params.query-timeout' = '1200',\n" +
                    "  'scan.params.mem-limit' = '2048',\n" +
                    "  'source.max-retries' = '3',\n" +

                    "  'scan-url' = '"+ scanUrl +"',\n" +
                    "  'jdbc-url' = 'jdbc:mysql://" + jdbcUrl + "',\n" +
                    "  'username' = '"+ username +"',\n" +
                    "  'password' = '"+ pwd +"',\n" +                
                    "  'database-name' = '" + dbName + "',\n" +
                    "  'table-name' = '"+ tableName +"'\n" +
                ")"
                );
    }

    @Test
    public void test_SQL_1() {
        TableResult result = tEnv.executeSql("SELECT tinyint_1 from flink_type_test where tinyint_1 >= 0");
    }

    @Test
    public void test_SQL_2() {
        tEnv.executeSql("SELECT tinyint_1, int_1 from flink_type_test where tinyint_1 = 100 and int_1 = -2147483648").print();
    }

    @Test
    public void test_SQL_3() {
        tEnv.executeSql("SELECT tinyint_1, char_1 from flink_type_test where tinyint_1 = 100 and char_1 = 'A'").print();
    }

    @Test
    public void test_SQL_4() {
        tEnv.executeSql("SELECT * from flink_type_test where int_1 < 1 and (char_1 = 'A' or varchar_1 = 'B')").print();
    }

    @Test
    public void test_SQL_5() {
        tEnv.executeSql("SELECT int_1, char_1 from flink_type_test where char_1 = 'A' and int_1 = -2147483648").print();
    }

    @Test
    public void test_SQL_6() {
        tEnv.executeSql("SELECT tinyint_1, char_1 from flink_type_test where char_1 = 'A' limit 1").print();
    }

    @Test
    public void test_SQL_7() {
        tEnv.executeSql("SELECT char_1, int_1 from flink_type_test where char_1 = 'A'").print();
    }

    @Test
    public void test_SQL_8() {
        tEnv.executeSql("SELECT int_1, char_1 from flink_type_test where char_1 = 'A'").print();
    }

    @Test
    public void test_SQL_9() {
        tEnv.executeSql("SELECT int_1, char_1 from flink_type_test where int_1 = -2147483648").print();
    }

    @Test
    public void test_SQL_10() {
        tEnv.executeSql("SELECT * from flink_type_test").print();
    }

    @Test
    public void test_SQL_11() {
        tEnv.executeSql("SELECT * from flink_type_test where tinyint_1 = 100").print();
    }

    @Test
    public void test_SQL_12() {
        tEnv.executeSql("SELECT int_1 from flink_type_test").print();
    }

    @Test
    public void test_SQL_13() {
        tEnv.executeSql("SELECT tinyint_1 as ccc from flink_type_test").print();
    }

    @Test
    public void test_SQL_14() {
        tEnv.executeSql("SELECT tinyint_1 from flink_type_test where tinyint_1 = 100").print();
    }

    @Test
    public void test_SQL_15() {
        tEnv.executeSql("SELECT count(*) from flink_type_test where tinyint_1 = 100").print();
    }

    @Test
    public void test_SQL_16() {
        tEnv.executeSql("SELECT count(*) from flink_type_test").print();
    }
    
    @Test
    public void test_SQL_17() {
        tEnv.executeSql("SELECT decimal_1 from flink_type_test").print();
    }

    @Test
    public void test_SQL_18() {
        tEnv.executeSql("SELECT -3.141298000 as ccc from flink_type_test").print();
    }

    @Test
    public void test_SQL_19() {
        tEnv.executeSql("SELECT char_1, tinyint_1 from flink_type_test where tinyint_1 <> 100").print();
    }

    @Test
    public void test_SQL_20() {
        tEnv.executeSql("SELECT char_1 from flink_type_test where boolean_1 = true").print();
    }
}
