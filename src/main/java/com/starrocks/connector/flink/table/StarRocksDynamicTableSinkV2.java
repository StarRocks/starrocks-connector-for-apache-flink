package com.starrocks.connector.flink.table;

import com.starrocks.connector.flink.row.StarRocksTableRowTransformer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;

public class StarRocksDynamicTableSinkV2 implements DynamicTableSink {
    private transient TableSchema flinkSchema;
    private StarRocksSinkOptions sinkOptions;

    public StarRocksDynamicTableSinkV2(StarRocksSinkOptions sinkOptions, TableSchema schema) {
        this.flinkSchema = schema;
        this.sinkOptions = sinkOptions;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final TypeInformation<RowData> rowDataTypeInfo = (TypeInformation<RowData>)context.createTypeInformation(flinkSchema.toRowDataType());
        SinkFunction<RowData> sinkFunction = new StarRocksDynamicSinkFunctionV2<>(
                sinkOptions,
                flinkSchema,
                new StarRocksTableRowTransformer(rowDataTypeInfo)
        );
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new StarRocksDynamicTableSinkV2(sinkOptions, flinkSchema);
    }

    @Override
    public String asSummaryString() {
        return "starrocks_sink";
    }
}
