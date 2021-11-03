package com.starrocks.connector.flink;

import java.util.List;

import com.starrocks.connector.flink.source.ColunmRichInfo;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.source.SelectColumn;
import com.starrocks.connector.flink.table.StarRocksDynamicSourceFunction;
import com.starrocks.connector.flink.table.StarRocksSourceOptions;

import org.apache.flink.table.api.TableSchema;


public class StarRocksSource {
    
    /**
     * Create a StarRocks DataStream source.
     *
     * @param sourceOptions     StarRocksSourceOptions as the document listed, such as http-nodes, load-url, batch size and maximum retries
     * @param queryInfo         queryInfo includes some infomations, such as query_plan, be_nodes, you can get by use StarRocksSourceManager.getQueryInfo 
     */
    public static StarRocksDynamicSourceFunction source(
        StarRocksSourceOptions sourceOptions,
        QueryInfo queryInfo,
        TableSchema flinkSchema,
        SelectColumn[] selectColumns,
        List<ColunmRichInfo> colunmRichInfos
        ) {
        return new StarRocksDynamicSourceFunction(
            sourceOptions, 
            queryInfo,
            flinkSchema,
            selectColumns, 
            colunmRichInfos
            );
    }
}
