package com.starrocks.connector.flink.it.source;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import com.starrocks.connector.flink.StarRocksSource;
import com.starrocks.connector.flink.table.source.StarRocksSourceCommonFunc;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import mockit.Mock;
import mockit.MockUp;


public class StarRocksDynamicTableSourceITTest extends StarRocksSourceBaseTest {

    private Long dataCount = 30L;

    @Test
    public void testDataStream() throws Exception {
        new MockUp<StarRocksSourceCommonFunc>() {
            @Mock
            public Long getQueryCount(StarRocksSourceOptions sourceOptions, String SQL) {
                return dataCount;
            }
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<RowData> dList = env.addSource(StarRocksSource.source(OPTIONS_WITH_COLUMN_IS_COUNT, TABLE_SCHEMA)).setParallelism(5).executeAndCollect(50);
        assertTrue(dList.size() == dataCount);
    }

    @Test
    public void testTableAPI() {
        mockResonsefunc();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv;
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
                    "decimal_1 DECIMAL(27,9)\n"+
                ") WITH (\n" +
                    "  'connector' = 'starrocks',\n" +
                    "  'scan-url' = '" + OPTIONS.getScanUrl() + "',\n" +
                    "  'jdbc-url' = '" + OPTIONS.getJdbcUrl() + "',\n" +
                    "  'username' = '" + OPTIONS.getUsername() + "',\n" +
                    "  'password' = '" + OPTIONS.getPassword() + "',\n" +
                    "  'database-name' = '" + OPTIONS.getDatabaseName() + "',\n" +
                    "  'table-name' = '" + OPTIONS.getTableName() + "')"
                );

        boolean checkCause = false;
        try {
            tEnv.executeSql("select * from flink_type_test").print();
            Thread.sleep(2000);
        } catch (Exception e) {
            checkCause = checkCause(
                e.getCause(), 
                "Failed to create brpc source:java.net.SocketTimeoutException: connect timed out"
            );
        }
        assertTrue(checkCause);
    }

    private boolean checkCause(Throwable throwable, String causeStr) {
        if (null == throwable) {
            return false;
        }
        if (throwable.getMessage().contains(causeStr)) {
            return true;
        }
        return checkCause(throwable.getCause(), causeStr);
    }

    @Test
    public void testSourceCommonProperties() {
        assertEquals(JDBC_URL, OPTIONS.getJdbcUrl());
        assertEquals(SCAN_URL, OPTIONS.getScanUrl());
        assertEquals(DATABASE, OPTIONS.getDatabaseName());
        assertEquals(TABLE, OPTIONS.getTableName());
        assertEquals(USERNAME, OPTIONS.getUsername());
        assertEquals(PASSWORD, OPTIONS.getPassword());
    }
}
