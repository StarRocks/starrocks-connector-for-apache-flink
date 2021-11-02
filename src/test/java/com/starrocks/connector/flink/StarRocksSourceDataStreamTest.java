package com.starrocks.connector.flink;

import com.starrocks.connector.flink.manager.StarRocksSourceInfoVisitor;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.table.StarRocksSourceOptions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

public class StarRocksSourceDataStreamTest {

    public static void main(String[] args) throws Exception {

        StarRocksSourceOptions options = StarRocksSourceOptions.builder()
                .withProperty("scan-url", "172.26.92.152:8634,172.26.92.152:8634,172.26.92.152:8634")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("table-name", "flink_type_test")
                .withProperty("database-name", "cjs_test")
                .build();

        StarRocksSourceInfoVisitor visitor = new StarRocksSourceInfoVisitor(options);
        QueryInfo queryInfo = visitor.getQueryInfo("int_1", "int_1 < 0", 1);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(queryInfo.getBeXTablets().size());
        env.addSource(StarRocksSource.source(
            options, 
            queryInfo,
            TableSchema.builder()
            .field("int_1", DataTypes.INT())
            .build(),
            null
            )).print();
        env.execute("StarRocks flink source");
    }
}
