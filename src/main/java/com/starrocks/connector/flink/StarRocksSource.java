package com.starrocks.connector.flink;

import com.starrocks.connector.flink.manager.StarRocksSourceManager;
import com.starrocks.connector.flink.table.StarRocksSourceOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.List;

public class StarRocksSource extends RichSourceFunction<List<?>> implements ResultTypeQueryable<List<?>> {

    private final StarRocksSourceManager sourceManager;

    public StarRocksSource(StarRocksSourceOptions sourceOptions) {
        this.sourceManager = new StarRocksSourceManager(sourceOptions);
    }

    @Override
    public void run(SourceContext<List<?>> sourceContext) throws Exception {
        this.sourceManager.startToRead();
    }

    @Override
    public void cancel() {

    }

    @Override
    public TypeInformation<List<?>> getProducedType() {
        return null;
    }
}
