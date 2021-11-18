package com.starrocks.connector.flink;

import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;


public class StarRocksSourceDataStreamTest {

    public static void main(String[] args) throws Exception {

        StarRocksSourceOptions options = StarRocksSourceOptions.builder()
                .withProperty("scan-url", "172.26.92.152:8634,172.26.92.152:8634,172.26.92.152:8634")
                .withProperty("jdbc-url", "jdbc:mysql://172.26.92.152:9632'")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("table-name", "flink_type_test")
                .withProperty("database-name", "cjs_test")
                .withProperty("scan.params.mem-limit", "1024")
                .withProperty("scan.columns", "tinyint_1")
                .withProperty("scan.filter", "tinyint_1 = 100")
                .build();

        TableSchema tableSchema = TableSchema.builder().
                                        field("date_1", DataTypes.DATE()).
                                        field("datetime_1", DataTypes.TIMESTAMP(6)).
                                        field("char_1", DataTypes.CHAR(20)).
                                        field("varchar_1", DataTypes.STRING()).
                                        field("boolean_1", DataTypes.BOOLEAN()).
                                        field("tinyint_1", DataTypes.TINYINT()).
                                        field("smallint_1", DataTypes.SMALLINT()).
                                        field("int_1", DataTypes.INT()).
                                        field("bigint_1", DataTypes.BIGINT()).
                                        field("largeint_1", DataTypes.STRING()).
                                        field("float_1", DataTypes.FLOAT()).
                                        field("double_1", DataTypes.DOUBLE()).
                                        field("decimal_1", DataTypes.DECIMAL(27, 9)).
                                        build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(StarRocksSource.source(options, tableSchema)).print();
        env.execute("StarRocks flink source");
    }
}
