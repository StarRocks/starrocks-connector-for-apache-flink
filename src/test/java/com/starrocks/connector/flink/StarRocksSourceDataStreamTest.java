package com.starrocks.connector.flink;


import com.starrocks.connector.flink.manager.StarRocksSourceManager;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.table.StarRocksDynamicSourceFunction;
import com.starrocks.connector.flink.table.StarRocksSourceOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StarRocksSourceDataStreamTest {


    public static void main(String[] args) throws Exception {

        StarRocksSourceOptions options = StarRocksSourceOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://172.26.92.152:9632")
                .withProperty("http-nodes", "172.26.92.152:8634,172.26.92.152:8634,172.26.92.152:8634")
                .withProperty("be-socket-timeout-ms", "5000")
                .withProperty("be-connect-timeout-ms", "5000")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("table-name", "test_1")
                .withProperty("database-name", "cjs_test")
                .withProperty("columns", "col1, event_day")
                // .withProperty("filter", "col1 = 0")
                .build();

        StarRocksSourceManager manager = new StarRocksSourceManager(options);
        QueryInfo queryInfo = manager.getQueryInfo();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(queryInfo.getBeXTablets().size());
        env.addSource(StarRocksSource.source(
            options, 
            queryInfo
            )).print();
        env.execute("StarRocks flink source");
    }
}
