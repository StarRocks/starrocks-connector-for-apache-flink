package com.starrocks.connector.flink;

import java.util.ArrayList;
import java.util.List;

import com.starrocks.connector.flink.manager.StarRocksSourceInfoVisitor;
import com.starrocks.connector.flink.source.ColunmRichInfo;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.source.SelectColumn;
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
        QueryInfo queryInfo = visitor.getQueryInfo("select int_1 from cjs_test.flink_type_test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SelectColumn[] selectColumns = new SelectColumn[1];
        selectColumns[0] = new SelectColumn("int_1", 7, true);
        List<ColunmRichInfo> colunmRichInfos = new ArrayList<>();
        colunmRichInfos.add(new ColunmRichInfo("", 0, DataTypes.INT()));
        colunmRichInfos.add(new ColunmRichInfo("", 1, DataTypes.INT()));
        colunmRichInfos.add(new ColunmRichInfo("", 2, DataTypes.INT()));
        colunmRichInfos.add(new ColunmRichInfo("", 3, DataTypes.INT()));
        colunmRichInfos.add(new ColunmRichInfo("", 4, DataTypes.INT()));
        colunmRichInfos.add(new ColunmRichInfo("", 5, DataTypes.INT()));
        colunmRichInfos.add(new ColunmRichInfo("", 6, DataTypes.INT()));
        colunmRichInfos.add(new ColunmRichInfo("int_1", 7, DataTypes.INT()));
        env.setParallelism(queryInfo.getBeXTablets().size());
        env.addSource(StarRocksSource.source(
            options, 
            queryInfo,
            TableSchema.builder()
            .field("int_1", DataTypes.INT())
            .build(),
            selectColumns, colunmRichInfos
            )).print();
        env.execute("StarRocks flink source");
    }
}
