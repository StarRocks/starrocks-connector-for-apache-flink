package com.starrocks.connector.flink.manager;

import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.starrocks.data.load.stream.DefaultStreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadStrategy;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

public class StarRocksSinkManagerV2 extends DefaultStreamLoadManager implements Serializable {

    private StarRocksSinkRuntimeContext runtimeContext;

    public StarRocksSinkManagerV2(StreamLoadProperties properties) {
        super(properties);
    }

    public StarRocksSinkManagerV2(StreamLoadProperties properties, StreamLoadStrategy loadStrategy) {
        super(properties, loadStrategy);
    }

    public void setRuntimeContext(RuntimeContext runtimeContext, StarRocksSinkOptions sinkOptions) {
        this.runtimeContext = new StarRocksSinkRuntimeContext(runtimeContext, sinkOptions);
    }

    @Override
    public void callback(StreamLoadResponse response) {
        super.callback(response);
        if (response.getException() != null) {
            StarRocksSinkRuntimeContext.flushFailedRecord(runtimeContext);
        } else {
            StarRocksSinkRuntimeContext.flushSucceedRecord(runtimeContext, response);
        }
    }
}
