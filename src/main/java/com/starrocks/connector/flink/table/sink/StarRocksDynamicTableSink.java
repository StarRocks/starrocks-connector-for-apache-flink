/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.table.sink;

import com.starrocks.connector.flink.row.sink.StarRocksTableRowTransformer;
import com.starrocks.connector.flink.table.sink.v2.StarRocksSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
 
public class StarRocksDynamicTableSink implements DynamicTableSink {

    private transient TableSchema flinkSchema;
    private StarRocksSinkOptions sinkOptions;
 
    public StarRocksDynamicTableSink(StarRocksSinkOptions sinkOptions, TableSchema schema) {
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
        final TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(flinkSchema.toRowDataType());
        if (sinkOptions.isUseUnifiedSinkApi()) {
            StarRocksSink<RowData> starRocksSink =
                    SinkFunctionFactory.createSink(
                        sinkOptions,
                        flinkSchema,
                        new StarRocksTableRowTransformer(rowDataTypeInfo)
                    );
            return SinkV2Provider.of(starRocksSink, sinkOptions.getSinkParallelism());
        } else {
            StarRocksDynamicSinkFunctionBase<RowData> starrocksSinkFunction =
                    SinkFunctionFactory.createSinkFunction(
                        sinkOptions,
                        flinkSchema,
                        new StarRocksTableRowTransformer(rowDataTypeInfo)
                    );
            return SinkFunctionProvider.of(starrocksSinkFunction, sinkOptions.getSinkParallelism());
        }
    }
 
    @Override
    public DynamicTableSink copy() {
        return new StarRocksDynamicTableSink(sinkOptions, flinkSchema);
    }
 
    @Override
    public String asSummaryString() {
        return "starrocks_sink";
    }
}
