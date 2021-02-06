package com.dorisdb.table;

import com.dorisdb.row.DorisTableRowTransformer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
 
public class DorisDynamicTableSink implements DynamicTableSink {

    private transient TableSchema flinkSchema;
    private DorisSinkOptions sinkOptions;
 
    public DorisDynamicTableSink(DorisSinkOptions sinkOptions, TableSchema schema) {
        this.flinkSchema = schema;
        this.sinkOptions = sinkOptions;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final TypeInformation<RowData> rowDataTypeInfo =
                context.createTypeInformation(flinkSchema.toRowDataType());
        DorisDynamicSinkFunction<RowData> dorisSinkFunction = new DorisDynamicSinkFunction<>(
            sinkOptions,
            flinkSchema,
            new DorisTableRowTransformer(rowDataTypeInfo)
        );
        return SinkFunctionProvider.of(dorisSinkFunction);
    }
 
    @Override
    public DynamicTableSink copy() {
        return new DorisDynamicTableSink(sinkOptions, flinkSchema);
    }
 
    @Override
    public String asSummaryString() {
        return "dorisdb_sink";
    }
}
