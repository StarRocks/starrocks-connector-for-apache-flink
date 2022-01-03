package com.starrocks.connector.flink;

import com.starrocks.connector.flink.table.source.StarRocksDynamicSourceFunction;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;

import org.apache.flink.table.api.TableSchema;


public class StarRocksSource {
    
    /**
     * Create a StarRocks DataStream source.
     *
     * @param sourceOptions     StarRocksSourceOptions as the document listed, such as http-nodes, load-url, batch size and maximum retries
     * @param flinkSchema       FlinkSchema
     * @return StarRocksDynamicSourceFunction Function of RichParallelSourceFunction
     */
    public static StarRocksDynamicSourceFunction source(StarRocksSourceOptions sourceOptions, TableSchema flinkSchema) {
        
        return new StarRocksDynamicSourceFunction(sourceOptions, flinkSchema);
    }
}
